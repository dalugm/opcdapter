// Package client implements the remote opcdapter transport for OPC DA.
package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	rpc "github.com/dalugm/opcdapter/gen/go/opcda/v1"
)

const (
	authTokenHeader = "x-opcdapter-token"
	operationMS     = int64(10_000)
	closeTimeout    = 3 * time.Second
)

var requestSequence atomic.Uint64

// Config describes the opcdapter transport. New constructs a lazy gRPC client
// and does not probe the end
type Config struct {
	Address    string
	Token      string
	CAFile     string
	ServerName string
	TLS        bool
}

type connectionCloser interface {
	Close() error
}

// Connection allows callers to supply an existing gRPC transport. The client
// owns it and closes it in Close; do not share it with another registry owner.
type Connection interface {
	grpc.ClientConnInterface
	Close() error
}

func NewWithConnection(conn Connection) *Client {
	return newClientForRPC(rpc.NewOpcDaServiceClient(conn), conn)
}

// Client owns the complete opcdapter registry for one registry owner.
type Client struct {
	gate        sync.RWMutex
	rpc         rpc.OpcDaServiceClient
	conn        connectionCloser
	ready       bool
	closed      bool
	mayOwn      bool
	connections map[string]struct{}
}

type tokenCredentials struct {
	token      string
	requireTLS bool
}

func (c tokenCredentials) GetRequestMetadata(
	context.Context,
	...string,
) (map[string]string, error) {
	return map[string]string{authTokenHeader: c.token}, nil
}

func (c tokenCredentials) RequireTransportSecurity() bool { return c.requireTLS }

// New creates a lazy opcdapter client. TLS is optional; plaintext deployments
// must keep the bridge on a trusted network because it carries OPC credentials.
func New(cfg Config) (*Client, error) {
	address := strings.TrimSpace(cfg.Address)
	if address == "" {
		return nil, errors.New("opcdapter address is required")
	}
	if !cfg.TLS && (cfg.CAFile != "" || cfg.ServerName != "") {
		return nil, errors.New("opcdapter CA file and server name require TLS")
	}
	options, err := dialOptions(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(address, options...)
	if err != nil {
		return nil, fmt.Errorf("create opcdapter client for %q: %w", address, err)
	}
	return newClientForRPC(rpc.NewOpcDaServiceClient(conn), conn), nil
}

func dialOptions(cfg Config) ([]grpc.DialOption, error) {
	transport, err := transportCredentials(cfg)
	if err != nil {
		return nil, err
	}
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(transport),
		grpc.WithDisableRetry(),
	}
	if cfg.Token != "" {
		options = append(options, grpc.WithPerRPCCredentials(tokenCredentials{
			token: cfg.Token, requireTLS: cfg.TLS,
		}))
	}
	return options, nil
}

func transportCredentials(cfg Config) (credentials.TransportCredentials, error) {
	if !cfg.TLS {
		return insecure.NewCredentials(), nil
	}
	tlsConfig, err := clientTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(tlsConfig), nil
}

func clientTLSConfig(cfg Config) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.ServerName}
	if cfg.CAFile == "" {
		return tlsConfig, nil
	}
	pem, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read opcdapter CA file: %w", err)
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("opcdapter CA file contains no certificates")
	}
	tlsConfig.RootCAs = roots
	return tlsConfig, nil
}

func newClientForRPC(stub rpc.OpcDaServiceClient, conn connectionCloser) *Client {
	return &Client{rpc: stub, conn: conn, connections: make(map[string]struct{})}
}

// SetConnections atomically replaces the complete desired gRPC-backed subset.
// Every invocation reaches opcdapter so its in-memory registry recovers after a
// restart. A failed sync leaves data-plane calls gated until a later success.
func (c *Client) SetConnections(ctx context.Context, configs []ConnectionConfig) error {
	c.gate.Lock()
	defer c.gate.Unlock()
	if c.closed {
		return errors.New("opcdapter client is closed")
	}
	c.ready = false
	connections, ids, err := connectionMessages(configs)
	if err != nil {
		return err
	}
	if len(connections) == 0 && !c.mayOwn {
		c.connections = ids
		c.ready = true
		return nil
	}
	requestID := newRequestID("set-connections")
	if len(connections) > 0 {
		c.mayOwn = true
	}
	response, err := c.rpc.SetConnections(ctx, &rpc.SetConnectionsRequest{
		RequestId: requestID, Connections: connections,
	})
	if err != nil {
		return fmt.Errorf("set opcdapter connections: %w", err)
	}
	if response == nil || response.GetRequestId() != requestID {
		return errors.New("set opcdapter connections response request ID mismatch")
	}
	c.connections = ids
	c.ready = true
	c.mayOwn = len(ids) > 0
	return nil
}

func connectionMessages(
	configs []ConnectionConfig,
) ([]*rpc.Connection, map[string]struct{}, error) {
	connections := make([]*rpc.Connection, 0, len(configs))
	ids := make(map[string]struct{}, len(configs))
	for _, cfg := range configs {
		if cfg.UID == "" || strings.TrimSpace(cfg.Host) == "" ||
			(cfg.CLSID == "" && cfg.ProgID == "") || cfg.SamplingInterval < time.Millisecond ||
			int64(cfg.SamplingInterval/time.Millisecond) > int64(1<<31-1) {
			return nil, nil, fmt.Errorf("invalid opcdapter connection %q", cfg.UID)
		}
		if _, duplicate := ids[cfg.UID]; duplicate {
			return nil, nil, fmt.Errorf("duplicate opcdapter connection %q", cfg.UID)
		}
		ids[cfg.UID] = struct{}{}
		connection := &rpc.Connection{
			Id: cfg.UID, Host: cfg.Host, Domain: cfg.Domain, Username: cfg.Username,
			Credentials:    &rpc.ConnectionCredentials{Password: cfg.Password},
			PollIntervalMs: int64(cfg.SamplingInterval / time.Millisecond),
		}
		if cfg.CLSID != "" {
			connection.ServerIdentifier = &rpc.Connection_ClsId{ClsId: cfg.CLSID}
		} else {
			connection.ServerIdentifier = &rpc.Connection_ProgId{ProgId: cfg.ProgID}
		}
		connections = append(connections, connection)
	}
	return connections, ids, nil
}

// Read performs one unary cache or device read through opcdapter.
func (c *Client) Read(
	ctx context.Context,
	connectionID string,
	itemIDs []string,
	device bool,
) ([]Sample, error) {
	c.gate.RLock()
	defer c.gate.RUnlock()
	if err := c.dataPlaneReady(connectionID); err != nil {
		return nil, err
	}
	if err := validateItemIDs(itemIDs); err != nil {
		return nil, err
	}
	source := rpc.ReadSource_READ_SOURCE_CACHE
	if device {
		source = rpc.ReadSource_READ_SOURCE_DEVICE
	}
	requestID := newRequestID("read")
	response, err := c.rpc.Read(ctx, &rpc.ReadRequest{
		RequestId: requestID, ConnectionId: connectionID, ItemIds: itemIDs,
		Source: source, TimeoutMs: operationMS,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, errors.Join(ErrConnectionNotFound, err)
		}
		return nil, fmt.Errorf("opcdapter read: %w", err)
	}
	if response == nil || response.GetRequestId() != requestID {
		return nil, errors.New("opcdapter read response request ID mismatch")
	}
	batch := response.GetBatch()
	if batch == nil {
		return nil, errors.New("opcdapter read response has no batch")
	}
	return readBatchSamples(connectionID, itemIDs, batch)
}

func readBatchSamples(
	connectionID string,
	itemIDs []string,
	batch *rpc.ConnectionDataBatch,
) ([]Sample, error) {
	if batch.GetConnectionId() != connectionID {
		return nil, fmt.Errorf(
			"opcdapter read connection ID = %q, want %q",
			batch.GetConnectionId(),
			connectionID,
		)
	}

	requested := make(map[string]int, len(itemIDs))
	for index, itemID := range itemIDs {
		requested[itemID] = index
	}
	samples := make([]Sample, len(itemIDs))
	seen := make([]bool, len(itemIDs))
	var responseErr error
	batchFailed := batch.GetError() != nil
	if batchFailed {
		responseErr = remoteError(batch.GetError())
	}
	for _, value := range batch.GetValues() {
		if value == nil {
			responseErr = errors.Join(
				responseErr,
				errors.New("opcdapter read returned a nil value"),
			)
			continue
		}
		index, ok := requested[value.GetItemId()]
		if !ok {
			responseErr = errors.Join(
				responseErr,
				fmt.Errorf("opcdapter read returned unrequested item %q", value.GetItemId()),
			)
			continue
		}
		if seen[index] {
			samples[index] = Sample{
				Address: value.GetItemId(),
				Err:     fmt.Errorf("duplicate read result for %q", value.GetItemId()),
			}
			responseErr = errors.Join(responseErr, samples[index].Err)
			continue
		}
		seen[index] = true
		samples[index] = sampleFromValue(value)
	}
	for index, itemID := range itemIDs {
		if !seen[index] {
			if batchFailed {
				continue
			}
			samples[index] = Sample{
				Address: itemID,
				Err:     fmt.Errorf("missing read result for %q", itemID),
			}
			responseErr = errors.Join(responseErr, samples[index].Err)
		}
	}
	if batchFailed {
		observed := make([]Sample, 0, len(batch.GetValues()))
		for index := range samples {
			if seen[index] {
				observed = append(observed, samples[index])
			}
		}
		return observed, responseErr
	}
	return samples, responseErr
}

func validateItemIDs(itemIDs []string) error {
	if len(itemIDs) == 0 {
		return errors.New("at least one OPC DA item is required")
	}
	seen := make(map[string]struct{}, len(itemIDs))
	for _, itemID := range itemIDs {
		if itemID == "" {
			return errors.New("OPC DA item ID is empty")
		}
		if _, duplicate := seen[itemID]; duplicate {
			return fmt.Errorf("duplicate OPC DA item ID %q", itemID)
		}
		seen[itemID] = struct{}{}
	}
	return nil
}

func sampleFromValue(value *rpc.DataValue) Sample {
	sample := Sample{
		Address: value.GetItemId(),
		Quality: Quality{
			Good: qualityIsGood(value.GetQuality()), Raw: value.GetQuality(),
			Description: qualityDescription(value.GetQuality()),
		},
	}
	if value.GetSourceTimestampMs() != 0 {
		sample.SourceTime = time.UnixMilli(value.GetSourceTimestampMs())
	}
	if value.GetError() != nil {
		sample.Err = remoteError(value.GetError())
		return sample
	}
	switch typed := value.GetValue().(type) {
	case *rpc.DataValue_BoolValue:
		sample.Value = typed.BoolValue
	case *rpc.DataValue_Int16Value:
		// The protobuf uses int32 because protobuf has no int16 scalar. Preserve
		// the wire value so a malformed peer cannot silently wrap it.
		sample.Value = typed.Int16Value
	case *rpc.DataValue_Int32Value:
		sample.Value = typed.Int32Value
	case *rpc.DataValue_Int64Value:
		sample.Value = typed.Int64Value
	case *rpc.DataValue_Uint16Value:
		// The protobuf uses uint32 because protobuf has no uint16 scalar.
		sample.Value = typed.Uint16Value
	case *rpc.DataValue_Uint32Value:
		sample.Value = typed.Uint32Value
	case *rpc.DataValue_Uint64Value:
		sample.Value = typed.Uint64Value
	case *rpc.DataValue_FloatValue:
		sample.Value = typed.FloatValue
	case *rpc.DataValue_DoubleValue:
		sample.Value = typed.DoubleValue
	case *rpc.DataValue_StringValue:
		sample.Value = typed.StringValue
	case *rpc.DataValue_BytesValue:
		sample.Value = typed.BytesValue
	case *rpc.DataValue_DatetimeValue:
		sample.Value = time.UnixMilli(typed.DatetimeValue)
	default:
		sample.Err = errors.New("OPC DA value is missing")
	}
	return sample
}

// Write performs one unary write. Once the RPC is invoked, any transport error
// is an unknown outcome and is never retried here.
func (c *Client) Write(
	ctx context.Context,
	targets []WriteTarget,
) ([]WriteResult, error) {
	c.gate.RLock()
	defer c.gate.RUnlock()
	results := make([]WriteResult, len(targets))
	if c.closed || !c.ready {
		err := errors.New("opcdapter registry is not synchronized")
		for i := range results {
			results[i].Err = err
		}
		return results, err
	}

	rpcTargets := make([]*rpc.WriteTarget, 0, len(targets))
	inputIndexes := make([]int, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for index, target := range targets {
		if _, ok := c.connections[target.ConnectionUID]; !ok {
			results[index] = WriteResult{
				State: WriteNotAttempted,
				Err: errors.Join(
					ErrConnectionNotFound,
					fmt.Errorf("OPC DA connection %q", target.ConnectionUID),
				),
			}
			continue
		}
		key := target.ConnectionUID + "\x00" + target.Address
		if target.Address == "" {
			results[index] = WriteResult{
				State: WriteRejected,
				Err:   errors.New("OPC DA item ID is empty"),
			}
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			results[index] = WriteResult{
				State: WriteRejected,
				Err:   fmt.Errorf("duplicate write target %q", target.Address),
			}
			continue
		}
		seen[key] = struct{}{}
		message, err := writeTarget(target)
		if err != nil {
			results[index] = WriteResult{State: WriteRejected, Err: err}
			continue
		}
		rpcTargets = append(rpcTargets, message)
		inputIndexes = append(inputIndexes, index)
		results[index] = unknownWriteResult(nil)
	}
	if len(rpcTargets) == 0 {
		return results, nil
	}
	if err := ctx.Err(); err != nil {
		for _, index := range inputIndexes {
			results[index] = WriteResult{State: WriteNotAttempted, Err: err}
		}
		return results, err
	}

	requestID := newRequestID("write")
	response, err := c.rpc.Write(ctx, &rpc.WriteRequest{
		RequestId: requestID, TimeoutMs: operationMS, Targets: rpcTargets,
	})
	if err != nil {
		wrapped := fmt.Errorf("opcdapter write: %w", err)
		for _, index := range inputIndexes {
			results[index] = unknownWriteResult(wrapped)
		}
		return results, wrapped
	}
	if response == nil || response.GetRequestId() != requestID {
		err := errors.New("opcdapter write response request ID mismatch")
		return results, err
	}

	seenResults := make([]bool, len(rpcTargets))
	var responseErr error
	for _, outcome := range response.GetResults() {
		if outcome == nil || uint64(outcome.GetTargetIndex()) >= uint64(len(rpcTargets)) {
			responseErr = errors.Join(
				responseErr,
				errors.New("opcdapter write returned an invalid target index"),
			)
			continue
		}
		wireIndex := int(outcome.GetTargetIndex())
		inputIndex := inputIndexes[wireIndex]
		if seenResults[wireIndex] {
			results[inputIndex] = unknownWriteResult(errors.New("duplicate opcdapter write result"))
			responseErr = errors.Join(responseErr, results[inputIndex].Err)
			continue
		}
		seenResults[wireIndex] = true
		target := targets[inputIndex]
		if outcome.GetConnectionId() != target.ConnectionUID ||
			outcome.GetItemId() != target.Address {
			results[inputIndex] = unknownWriteResult(
				errors.New("opcdapter write result identity mismatch"),
			)
			responseErr = errors.Join(responseErr, results[inputIndex].Err)
			continue
		}
		mapped, mappingErr := writeResult(outcome)
		results[inputIndex] = mapped
		responseErr = errors.Join(responseErr, mappingErr)
	}
	for wireIndex, observed := range seenResults {
		if observed {
			continue
		}
		inputIndex := inputIndexes[wireIndex]
		results[inputIndex] = unknownWriteResult(errors.New("missing opcdapter write result"))
		responseErr = errors.Join(responseErr, results[inputIndex].Err)
	}
	return results, responseErr
}

func writeTarget(target WriteTarget) (*rpc.WriteTarget, error) {
	message := &rpc.WriteTarget{ConnectionId: target.ConnectionUID, ItemId: target.Address}
	switch value := target.Value.(type) {
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, errors.New("write value must be finite")
		}
		message.Value = &rpc.WriteTarget_DoubleValue{DoubleValue: value}
	case bool:
		message.Value = &rpc.WriteTarget_BoolValue{BoolValue: value}
	default:
		return nil, errors.New("unsupported write value")
	}
	return message, nil
}

func writeResult(outcome *rpc.PointWriteResult) (WriteResult, error) {
	switch outcome.GetDisposition() {
	case rpc.WriteDisposition_WRITE_DISPOSITION_APPLIED,
		rpc.WriteDisposition_WRITE_DISPOSITION_SKIPPED:
		if outcome.GetError() != nil {
			err := errors.New("successful opcdapter write result contains an error")
			return unknownWriteResult(err), err
		}
		return WriteResult{State: WriteConfirmed}, nil
	case rpc.WriteDisposition_WRITE_DISPOSITION_FAILED:
		info := outcome.GetError()
		if info == nil {
			err := errors.New("failed opcdapter write result has no error")
			return unknownWriteResult(err), err
		}
		err := remoteError(info)
		switch info.GetCode() {
		case rpc.ErrorCode_ERROR_CODE_CONNECTION_NOT_FOUND:
			return WriteResult{
				State: WriteNotAttempted,
				Err:   errors.Join(ErrConnectionNotFound, err),
			}, nil
		case rpc.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			rpc.ErrorCode_ERROR_CODE_UNSUPPORTED_VALUE_TYPE,
			rpc.ErrorCode_ERROR_CODE_ITEM_NOT_FOUND:
			return WriteResult{State: WriteRejected, Err: err}, nil
		case rpc.ErrorCode_ERROR_CODE_OPC_FAILURE:
			if info.Hresult != nil {
				return WriteResult{State: WriteRejected, Err: err}, nil
			}
		}
		return unknownWriteResult(err), err
	default:
		err := fmt.Errorf("invalid opcdapter write disposition %s", outcome.GetDisposition())
		return unknownWriteResult(err), err
	}
}

func unknownWriteResult(err error) WriteResult {
	return WriteResult{
		State: WriteUnknown,
		Err:   errors.Join(ErrWriteOutcomeUnknown, err),
	}
}

func remoteError(info *rpc.ErrorInfo) error {
	if info == nil {
		return errors.New("opcdapter returned an unspecified error")
	}
	message := info.GetMessage()
	if message == "" {
		message = "opcdapter operation failed"
	}
	if info.Hresult != nil {
		return fmt.Errorf("%s (%s, HRESULT 0x%08X)", message, info.GetCode(), info.GetHresult())
	}
	return fmt.Errorf("%s (%s)", message, info.GetCode())
}

func (c *Client) dataPlaneReady(connectionID string) error {
	if c.closed {
		return errors.New("opcdapter client is closed")
	}
	if !c.ready {
		return errors.New("opcdapter registry is not synchronized")
	}
	if _, ok := c.connections[connectionID]; !ok {
		return errors.Join(
			ErrConnectionNotFound,
			fmt.Errorf("OPC DA connection %q", connectionID),
		)
	}
	return nil
}

// Close best-effort clears a registry this client may own, then closes the
// transport. An unused lazy client is closed without making an RPC.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.gate.Lock()
	defer c.gate.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.ready = false
	var clearErr error
	if c.mayOwn {
		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		requestID := newRequestID("close")
		response, err := c.rpc.SetConnections(ctx, &rpc.SetConnectionsRequest{RequestId: requestID})
		cancel()
		if err != nil {
			clearErr = fmt.Errorf("clear opcdapter connections: %w", err)
		} else if response == nil || response.GetRequestId() != requestID {
			clearErr = errors.New("clear opcdapter connections response request ID mismatch")
		}
	}
	var closeErr error
	if c.conn != nil {
		closeErr = c.conn.Close()
	}
	return errors.Join(clearErr, closeErr)
}

func newRequestID(operation string) string {
	return fmt.Sprintf("opcdapter-%s-%d-%d", operation, time.Now().UnixNano(), requestSequence.Add(1))
}
