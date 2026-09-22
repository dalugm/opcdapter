package client

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	rpc "github.com/dalugm/opcdapter/gen/go/opcda/v1"
)

type fakeRPC struct {
	rpc.OpcDaServiceClient
	setConnections func(context.Context, *rpc.SetConnectionsRequest) (*rpc.SetConnectionsResponse, error)
	read           func(context.Context, *rpc.ReadRequest) (*rpc.ReadResponse, error)
	write          func(context.Context, *rpc.WriteRequest) (*rpc.WriteResponse, error)
	poll           func(context.Context, *rpc.PollRequest) (grpc.ServerStreamingClient[rpc.PollResponse], error)
}

func (f *fakeRPC) Poll(
	ctx context.Context,
	request *rpc.PollRequest,
	_ ...grpc.CallOption,
) (grpc.ServerStreamingClient[rpc.PollResponse], error) {
	if f.poll == nil {
		return nil, status.Error(codes.Unimplemented, "unexpected Poll")
	}
	return f.poll(ctx, request)
}

func (f *fakeRPC) SetConnections(
	ctx context.Context,
	request *rpc.SetConnectionsRequest,
	_ ...grpc.CallOption,
) (*rpc.SetConnectionsResponse, error) {
	return f.setConnections(ctx, request)
}

func (f *fakeRPC) Read(
	ctx context.Context,
	request *rpc.ReadRequest,
	_ ...grpc.CallOption,
) (*rpc.ReadResponse, error) {
	return f.read(ctx, request)
}

func (f *fakeRPC) Write(
	ctx context.Context,
	request *rpc.WriteRequest,
	_ ...grpc.CallOption,
) (*rpc.WriteResponse, error) {
	return f.write(ctx, request)
}

type fakeConn struct {
	closed bool
}

func (c *fakeConn) Close() error {
	c.closed = true
	return nil
}

func TestSetConnectionsMapsCompleteDesiredSetAndGatesAfterFailure(t *testing.T) {
	var calls int
	fake := &fakeRPC{}
	fake.setConnections = func(
		_ context.Context,
		request *rpc.SetConnectionsRequest,
	) (*rpc.SetConnectionsResponse, error) {
		calls++
		if calls == 2 {
			return nil, status.Error(codes.Unavailable, "adapter stopped")
		}
		if len(request.GetConnections()) != 1 {
			t.Fatalf("connections = %d, want 1", len(request.GetConnections()))
		}
		connection := request.GetConnections()[0]
		if connection.GetId() != "line-a" || connection.GetHost() != "10.0.0.8" ||
			connection.GetCredentials().GetPassword() != "secret" ||
			connection.GetProgId() != "Vendor.Server" || connection.GetPollIntervalMs() != 750 {
			t.Fatalf("connection = %+v", connection)
		}
		return &rpc.SetConnectionsResponse{RequestId: request.GetRequestId()}, nil
	}
	fake.read = func(_ context.Context, request *rpc.ReadRequest) (*rpc.ReadResponse, error) {
		return &rpc.ReadResponse{
			RequestId: request.GetRequestId(),
			Batch: &rpc.ConnectionDataBatch{
				ConnectionId: request.GetConnectionId(),
				Values: []*rpc.DataValue{{
					ItemId: "A.PV", Quality: 0xC0,
					Value: &rpc.DataValue_DoubleValue{DoubleValue: 4.5},
				}},
			},
		}, nil
	}

	client := newTestClient(fake)
	config := ConnectionConfig{
		UID: "line-a", Host: "10.0.0.8", Password: "secret", ProgID: "Vendor.Server",
		SamplingInterval: 750 * time.Millisecond,
	}
	if err := client.SetConnections(t.Context(), []ConnectionConfig{config}); err != nil {
		t.Fatalf("SetConnections() error = %v", err)
	}
	if _, err := client.Read(t.Context(), "line-a", []string{"A.PV"}, false); err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if err := client.SetConnections(t.Context(), []ConnectionConfig{config}); err == nil {
		t.Fatal("second SetConnections() error = nil")
	}
	if _, err := client.Read(t.Context(), "line-a", []string{"A.PV"}, false); err == nil {
		t.Fatal("Read() after failed sync error = nil")
	}
}

func TestSetConnectionsDoesNotContactAdapterForUnusedEmptyRegistry(t *testing.T) {
	var calls int
	fake := &fakeRPC{}
	fake.setConnections = func(
		_ context.Context,
		request *rpc.SetConnectionsRequest,
	) (*rpc.SetConnectionsResponse, error) {
		calls++
		return &rpc.SetConnectionsResponse{RequestId: request.GetRequestId()}, nil
	}
	client := newTestClient(fake)

	if err := client.SetConnections(t.Context(), nil); err != nil {
		t.Fatalf("SetConnections(nil) error = %v", err)
	}
	if calls != 0 {
		t.Fatalf("SetConnections(nil) calls = %d, want 0", calls)
	}
	if _, err := client.Read(
		t.Context(),
		"missing",
		[]string{"A.PV"},
		false,
	); !errors.Is(
		err,
		ErrConnectionNotFound,
	) {
		t.Fatalf("Read() error = %v, want ErrConnectionNotFound", err)
	}
}

func TestSetConnectionsRetriesOwnedRegistryClearUntilAcknowledged(t *testing.T) {
	var calls int
	fake := &fakeRPC{}
	fake.setConnections = func(
		_ context.Context,
		request *rpc.SetConnectionsRequest,
	) (*rpc.SetConnectionsResponse, error) {
		calls++
		if calls == 2 {
			return nil, status.Error(codes.Unavailable, "adapter stopped during clear")
		}
		return &rpc.SetConnectionsResponse{RequestId: request.GetRequestId()}, nil
	}
	client := newTestClient(fake)
	config := ConnectionConfig{
		UID: "line-a", Host: "host", ProgID: "server", SamplingInterval: time.Second,
	}
	if err := client.SetConnections(t.Context(), []ConnectionConfig{config}); err != nil {
		t.Fatal(err)
	}
	if err := client.SetConnections(t.Context(), nil); err == nil {
		t.Fatal("first SetConnections(nil) error = nil")
	}
	if err := client.SetConnections(t.Context(), nil); err != nil {
		t.Fatalf("second SetConnections(nil) error = %v", err)
	}
	if calls != 3 {
		t.Fatalf("SetConnections RPC calls = %d, want 3", calls)
	}
}

func TestReadValidatesIdentityCompletenessAndMapsValues(t *testing.T) {
	fake := &fakeRPC{}
	fake.setConnections = echoSetConnections
	fake.read = func(_ context.Context, request *rpc.ReadRequest) (*rpc.ReadResponse, error) {
		if request.GetSource() != rpc.ReadSource_READ_SOURCE_DEVICE {
			t.Fatalf("source = %v, want DEVICE", request.GetSource())
		}
		return &rpc.ReadResponse{
			RequestId: request.GetRequestId(),
			Batch: &rpc.ConnectionDataBatch{
				ConnectionId: "line-a",
				Values: []*rpc.DataValue{
					{
						ItemId:            "A.PV",
						SourceTimestampMs: 1234,
						Quality:           0xC0,
						Value:             &rpc.DataValue_BoolValue{BoolValue: true},
					},
					{
						ItemId:  "A.PV",
						Quality: 0xC0,
						Value:   &rpc.DataValue_DoubleValue{DoubleValue: 8},
					},
				},
			},
		}, nil
	}
	client := newTestClient(fake)
	if err := client.SetConnections(
		t.Context(),
		[]ConnectionConfig{
			{UID: "line-a", Host: "host", ProgID: "server", SamplingInterval: time.Second},
		},
	); err != nil {
		t.Fatal(err)
	}

	samples, err := client.Read(t.Context(), "line-a", []string{"A.PV", "B.PV"}, true)
	if err == nil {
		t.Fatal("Read() error = nil, want malformed response error")
	}
	if len(samples) != 2 {
		t.Fatalf("samples = %d, want 2", len(samples))
	}
	if samples[0].Address != "A.PV" || samples[0].Err == nil {
		t.Fatalf("duplicate sample = %+v", samples[0])
	}
	if samples[1].Address != "B.PV" || samples[1].Err == nil {
		t.Fatalf("missing sample = %+v", samples[1])
	}
}

func TestReadKeepsQualitySeparateFromItemError(t *testing.T) {
	fake := &fakeRPC{setConnections: echoSetConnections}
	fake.read = func(_ context.Context, request *rpc.ReadRequest) (*rpc.ReadResponse, error) {
		return &rpc.ReadResponse{
			RequestId: request.GetRequestId(),
			Batch: &rpc.ConnectionDataBatch{
				ConnectionId: "line-a",
				Values: []*rpc.DataValue{
					{
						ItemId:            "GOOD",
						SourceTimestampMs: 1234,
						Quality:           0xC0,
						Value:             &rpc.DataValue_Int32Value{Int32Value: 7},
					},
					{
						ItemId:  "BAD",
						Quality: 0x04,
						Error: &rpc.ErrorInfo{
							Code:    rpc.ErrorCode_ERROR_CODE_ITEM_NOT_FOUND,
							Message: "missing",
						},
					},
				},
			},
		}, nil
	}
	client := newTestClient(fake)
	if err := client.SetConnections(
		t.Context(),
		[]ConnectionConfig{
			{UID: "line-a", Host: "host", ProgID: "server", SamplingInterval: time.Second},
		},
	); err != nil {
		t.Fatal(err)
	}
	samples, err := client.Read(t.Context(), "line-a", []string{"GOOD", "BAD"}, false)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got, ok := samples[0].Value.(int32); !ok || got != 7 || !samples[0].Quality.Good ||
		samples[0].SourceTime.UnixMilli() != 1234 {
		t.Fatalf("good sample = %+v", samples[0])
	}
	if samples[1].Err == nil || samples[1].Quality.Good {
		t.Fatalf("failed sample = %+v", samples[1])
	}
}

func TestReadDoesNotNarrowWireIntegerFields(t *testing.T) {
	fake := &fakeRPC{setConnections: echoSetConnections}
	fake.read = func(_ context.Context, request *rpc.ReadRequest) (*rpc.ReadResponse, error) {
		return &rpc.ReadResponse{
			RequestId: request.GetRequestId(),
			Batch: &rpc.ConnectionDataBatch{
				ConnectionId: "line-a",
				Values: []*rpc.DataValue{
					{
						ItemId:  "I16",
						Quality: 0xC0,
						Value:   &rpc.DataValue_Int16Value{Int16Value: 40_000},
					},
					{
						ItemId:  "U16",
						Quality: 0xC0,
						Value:   &rpc.DataValue_Uint16Value{Uint16Value: 70_000},
					},
				},
			},
		}, nil
	}
	client := newTestClient(fake)
	if err := client.SetConnections(
		t.Context(),
		[]ConnectionConfig{
			{UID: "line-a", Host: "host", ProgID: "server", SamplingInterval: time.Second},
		},
	); err != nil {
		t.Fatal(err)
	}

	samples, err := client.Read(t.Context(), "line-a", []string{"I16", "U16"}, false)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got, ok := samples[0].Value.(int32); !ok || got != 40_000 {
		t.Fatalf("int16 wire value = %#v, want int32(40000)", samples[0].Value)
	}
	if got, ok := samples[1].Value.(uint32); !ok || got != 70_000 {
		t.Fatalf("uint16 wire value = %#v, want uint32(70000)", samples[1].Value)
	}
}

func TestReadBatchFailureReturnsOnlyObservedSamples(t *testing.T) {
	fake := &fakeRPC{setConnections: echoSetConnections}
	fake.read = func(_ context.Context, request *rpc.ReadRequest) (*rpc.ReadResponse, error) {
		return &rpc.ReadResponse{
			RequestId: request.GetRequestId(),
			Batch: &rpc.ConnectionDataBatch{
				ConnectionId: "line-a",
				Error: &rpc.ErrorInfo{
					Code: rpc.ErrorCode_ERROR_CODE_OPC_FAILURE, Message: "connection failed",
				},
				Values: []*rpc.DataValue{{
					ItemId: "OBSERVED", Quality: 0xC0,
					Value: &rpc.DataValue_DoubleValue{DoubleValue: 12.5},
				}},
			},
		}, nil
	}
	client := newTestClient(fake)
	if err := client.SetConnections(
		t.Context(),
		[]ConnectionConfig{
			{UID: "line-a", Host: "host", ProgID: "server", SamplingInterval: time.Second},
		},
	); err != nil {
		t.Fatal(err)
	}

	samples, err := client.Read(
		t.Context(),
		"line-a",
		[]string{"OBSERVED", "NOT_RETURNED"},
		false,
	)
	if err == nil {
		t.Fatal("Read() error = nil")
	}
	if len(samples) != 1 || samples[0].Address != "OBSERVED" || samples[0].Err != nil {
		t.Fatalf("samples = %+v, want only the observed value", samples)
	}
}

func TestWriteMapsOutcomesAndNeverRetriesRPCFailure(t *testing.T) {
	var calls int
	fake := &fakeRPC{setConnections: echoSetConnections}
	fake.write = func(_ context.Context, request *rpc.WriteRequest) (*rpc.WriteResponse, error) {
		calls++
		return nil, status.Error(codes.Unavailable, "connection lost")
	}
	client := newTestClient(fake)
	if err := client.SetConnections(
		t.Context(),
		[]ConnectionConfig{
			{UID: "line-a", Host: "host", ProgID: "server", SamplingInterval: time.Second},
		},
	); err != nil {
		t.Fatal(err)
	}
	targets := []WriteTarget{
		{ConnectionUID: "line-a", Address: "A.PV", Value: float64(3)},
	}
	results, err := client.Write(t.Context(), targets)
	if err == nil {
		t.Fatal("Write() error = nil")
	}
	if calls != 1 {
		t.Fatalf("Write RPC calls = %d, want 1", calls)
	}
	if len(results) != 1 || results[0].State != WriteUnknown ||
		!errors.Is(results[0].Err, ErrWriteOutcomeUnknown) {
		t.Fatalf("results = %+v", results)
	}
}

func TestWritePreservesOrderAndClassifiesStructuredFailures(t *testing.T) {
	hresult := uint32(0xC0040007)
	fake := &fakeRPC{setConnections: echoSetConnections}
	fake.write = func(_ context.Context, request *rpc.WriteRequest) (*rpc.WriteResponse, error) {
		return &rpc.WriteResponse{
			RequestId: request.GetRequestId(),
			Results: []*rpc.PointWriteResult{
				{
					TargetIndex:  2,
					ConnectionId: "line-a",
					ItemId:       "C",
					Disposition:  rpc.WriteDisposition_WRITE_DISPOSITION_FAILED,
					Error: &rpc.ErrorInfo{
						Code:    rpc.ErrorCode_ERROR_CODE_OPC_FAILURE,
						Message: "rejected",
						Hresult: &hresult,
					},
				},
				{
					TargetIndex:  0,
					ConnectionId: "line-a",
					ItemId:       "A",
					Disposition:  rpc.WriteDisposition_WRITE_DISPOSITION_APPLIED,
				},
				{
					TargetIndex:  1,
					ConnectionId: "missing",
					ItemId:       "B",
					Disposition:  rpc.WriteDisposition_WRITE_DISPOSITION_FAILED,
					Error: &rpc.ErrorInfo{
						Code:    rpc.ErrorCode_ERROR_CODE_CONNECTION_NOT_FOUND,
						Message: "unknown connection",
					},
				},
				{
					TargetIndex:  3,
					ConnectionId: "line-a",
					ItemId:       "D",
					Disposition:  rpc.WriteDisposition_WRITE_DISPOSITION_FAILED,
					Error: &rpc.ErrorInfo{
						Code:    rpc.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED,
						Message: "outcome unknown",
					},
				},
			},
		}, nil
	}
	client := newTestClient(fake)
	if err := client.SetConnections(
		t.Context(),
		[]ConnectionConfig{
			{UID: "line-a", Host: "host", ProgID: "server", SamplingInterval: time.Second},
			{UID: "missing", Host: "host", ProgID: "server", SamplingInterval: time.Second},
		},
	); err != nil {
		t.Fatal(err)
	}
	targets := []WriteTarget{
		{ConnectionUID: "line-a", Address: "A", Value: true},
		{ConnectionUID: "missing", Address: "B", Value: float64(2)},
		{ConnectionUID: "line-a", Address: "C", Value: float64(3)},
		{ConnectionUID: "line-a", Address: "D", Value: float64(4)},
	}
	results, err := client.Write(t.Context(), targets)
	if err == nil {
		t.Fatal("Write() error = nil, want unknown-outcome error")
	}
	want := []WriteState{
		WriteConfirmed,
		WriteNotAttempted,
		WriteRejected,
		WriteUnknown,
	}
	for i := range want {
		if results[i].State != want[i] {
			t.Errorf("result[%d].State = %v, want %v", i, results[i].State, want[i])
		}
	}
	if !errors.Is(results[1].Err, ErrConnectionNotFound) {
		t.Errorf("connection error = %v", results[1].Err)
	}
	if !errors.Is(results[3].Err, ErrWriteOutcomeUnknown) {
		t.Errorf("unknown error = %v", results[3].Err)
	}
}

func TestWriteRejectsInvalidValueWithoutRPC(t *testing.T) {
	var calls int
	fake := &fakeRPC{setConnections: echoSetConnections}
	fake.write = func(context.Context, *rpc.WriteRequest) (*rpc.WriteResponse, error) {
		calls++
		return nil, nil
	}
	client := newTestClient(fake)
	if err := client.SetConnections(
		t.Context(),
		[]ConnectionConfig{
			{UID: "line-a", Host: "host", ProgID: "server", SamplingInterval: time.Second},
		},
	); err != nil {
		t.Fatal(err)
	}
	results, err := client.Write(
		t.Context(),
		[]WriteTarget{
			{
				ConnectionUID: "line-a",
				Address:       "A",
				Value:         math.NaN(),
			},
		},
	)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if calls != 0 || results[0].State != WriteRejected {
		t.Fatalf("calls=%d results=%+v", calls, results)
	}
}

func TestCloseClearsOwnedRegistryButUnusedClientDoesNotDial(t *testing.T) {
	var calls int
	fake := &fakeRPC{}
	fake.setConnections = func(_ context.Context, request *rpc.SetConnectionsRequest) (*rpc.SetConnectionsResponse, error) {
		calls++
		return &rpc.SetConnectionsResponse{RequestId: request.GetRequestId()}, nil
	}
	conn := &fakeConn{}
	unused := newClientForRPC(fake, conn)
	if err := unused.Close(); err != nil {
		t.Fatal(err)
	}
	if calls != 0 || !conn.closed {
		t.Fatalf("unused Close calls=%d closed=%v", calls, conn.closed)
	}

	conn = &fakeConn{}
	owned := newClientForRPC(fake, conn)
	if err := owned.SetConnections(
		t.Context(),
		[]ConnectionConfig{
			{UID: "line-a", Host: "host", ProgID: "server", SamplingInterval: time.Second},
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := owned.Close(); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || !conn.closed {
		t.Fatalf("owned Close calls=%d closed=%v", calls, conn.closed)
	}
}

func TestNewAllowsPlaintextDockerEndpoint(t *testing.T) {
	client, err := New(Config{Address: "opcdapter:50051", TLS: false})
	if err != nil {
		t.Fatalf("Docker network endpoint rejected: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
}

func echoSetConnections(
	_ context.Context,
	request *rpc.SetConnectionsRequest,
) (*rpc.SetConnectionsResponse, error) {
	return &rpc.SetConnectionsResponse{RequestId: request.GetRequestId()}, nil
}

func newTestClient(stub rpc.OpcDaServiceClient) *Client {
	return newClientForRPC(stub, &fakeConn{})
}

//go:fix inline
