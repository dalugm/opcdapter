package client

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	rpc "github.com/dalugm/opcdapter/gen/go/opcda/v1"
)

func TestBufconnRoundTripSendsAuthAndPreservesRPCContracts(t *testing.T) {
	const token = "integration-secret"
	listener := bufconn.Listen(1 << 20)
	service := &roundTripServer{}
	server := grpc.NewServer(grpc.UnaryInterceptor(requireTestToken(token, service)))
	rpc.RegisterOpcDaServiceServer(server, service)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	options, err := dialOptions(Config{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	options = append(options, grpc.WithContextDialer(
		func(context.Context, string) (net.Conn, error) { return listener.Dial() },
	))
	conn, err := grpc.NewClient("passthrough:///opcdapter", options...)
	if err != nil {
		t.Fatal(err)
	}
	client := newClientForRPC(rpc.NewOpcDaServiceClient(conn), conn)

	config := ConnectionConfig{
		UID: "line-a", Host: "opc.example", Username: "operator", Password: "secret",
		ProgID: "Vendor.Server", SamplingInterval: time.Second,
	}
	if err := client.SetConnections(t.Context(), []ConnectionConfig{config}); err != nil {
		t.Fatalf("SetConnections() error = %v", err)
	}
	pollCtx, cancelPoll := context.WithCancel(t.Context())
	defer cancelPoll()
	pollRequest := &rpc.PollRequest{
		RequestId:    "poll-roundtrip",
		ConnectionId: "line-a",
		ItemIds:      []string{"A.PV"},
	}
	stream, err := client.openPoll(pollCtx, pollRequest)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := stream.Recv()
	if err != nil {
		t.Fatalf("Poll acknowledgement: %v", err)
	}
	if _, _, err := validatePollAck(pollRequest, ack.GetAck()); err != nil {
		t.Fatal(err)
	}
	data, err := stream.Recv()
	if err != nil || data.GetData().GetSequence() != 1 {
		t.Fatalf("Poll data = %v, %v", data, err)
	}
	samples, err := client.Read(t.Context(), "line-a", []string{"A.PV"}, true)
	if err != nil || len(samples) != 1 || samples[0].Value != float64(12.5) {
		t.Fatalf("Read() samples=%+v error=%v", samples, err)
	}
	results, err := client.Write(t.Context(), []WriteTarget{{
		ConnectionUID: "line-a", Address: "A.PV", Value: true,
	}})
	if err != nil || len(results) != 1 || results[0].State != WriteConfirmed {
		t.Fatalf("Write() results=%+v error=%v", results, err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if got := service.observedMethods(); !slices.Equal(got, []string{
		rpc.OpcDaService_SetConnections_FullMethodName,
		rpc.OpcDaService_Poll_FullMethodName,
		rpc.OpcDaService_Read_FullMethodName,
		rpc.OpcDaService_Write_FullMethodName,
		rpc.OpcDaService_SetConnections_FullMethodName,
	}) {
		t.Fatalf("observed methods = %v", got)
	}
}

func TestTLSConfigVerifiesConfiguredServerName(t *testing.T) {
	config, err := clientTLSConfig(Config{TLS: true, ServerName: "adapter.example"})
	if err != nil {
		t.Fatal(err)
	}
	if config.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify = true")
	}
	if config.ServerName != "adapter.example" {
		t.Fatalf("ServerName = %q", config.ServerName)
	}
}

type roundTripServer struct {
	rpc.UnimplementedOpcDaServiceServer
	mu      sync.Mutex
	methods []string
}

func requireTestToken(token string, service *roundTripServer) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		request any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		values := metadata.ValueFromIncomingContext(ctx, authTokenHeader)
		if len(values) != 1 || values[0] != token {
			return nil, status.Error(codes.Unauthenticated, "missing test token")
		}
		service.mu.Lock()
		service.methods = append(service.methods, info.FullMethod)
		service.mu.Unlock()
		return handler(ctx, request)
	}
}

func (s *roundTripServer) observedMethods() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.methods)
}

func (s *roundTripServer) SetConnections(
	_ context.Context,
	request *rpc.SetConnectionsRequest,
) (*rpc.SetConnectionsResponse, error) {
	if len(request.GetConnections()) > 0 {
		connection := request.GetConnections()[0]
		if len(request.GetConnections()) != 1 || connection.GetId() != "line-a" ||
			connection.GetHost() != "opc.example" || connection.GetProgId() != "Vendor.Server" ||
			connection.GetCredentials().GetPassword() != "secret" {
			return nil, status.Error(codes.InvalidArgument, "connection did not round trip")
		}
	}
	return &rpc.SetConnectionsResponse{RequestId: request.GetRequestId()}, nil
}

func (s *roundTripServer) Read(
	_ context.Context,
	request *rpc.ReadRequest,
) (*rpc.ReadResponse, error) {
	if request.GetConnectionId() != "line-a" ||
		!slices.Equal(request.GetItemIds(), []string{"A.PV"}) ||
		request.GetSource() != rpc.ReadSource_READ_SOURCE_DEVICE {
		return nil, status.Error(codes.InvalidArgument, "read did not round trip")
	}
	return &rpc.ReadResponse{
		RequestId: request.GetRequestId(),
		Batch: &rpc.ConnectionDataBatch{
			ConnectionId: "line-a",
			Values: []*rpc.DataValue{{
				ItemId: "A.PV", Quality: 0xC0,
				Value: &rpc.DataValue_DoubleValue{DoubleValue: 12.5},
			}},
		},
	}, nil
}

func (s *roundTripServer) Write(
	_ context.Context,
	request *rpc.WriteRequest,
) (*rpc.WriteResponse, error) {
	if len(request.GetTargets()) != 1 || request.GetTargets()[0].GetConnectionId() != "line-a" ||
		request.GetTargets()[0].GetItemId() != "A.PV" || !request.GetTargets()[0].GetBoolValue() {
		return nil, status.Error(codes.InvalidArgument, "write did not round trip")
	}
	return &rpc.WriteResponse{
		RequestId: request.GetRequestId(),
		Results: []*rpc.PointWriteResult{{
			TargetIndex: 0, ConnectionId: "line-a", ItemId: "A.PV",
			Disposition: rpc.WriteDisposition_WRITE_DISPOSITION_APPLIED,
		}},
	}, nil
}

func (s *roundTripServer) Poll(
	request *rpc.PollRequest,
	stream grpc.ServerStreamingServer[rpc.PollResponse],
) error {
	tokens := metadata.ValueFromIncomingContext(stream.Context(), authTokenHeader)
	if len(tokens) != 1 || tokens[0] != "integration-secret" {
		return status.Error(codes.Unauthenticated, "missing stream token")
	}
	if request.ConnectionId != "line-a" || !slices.Equal(request.ItemIds, []string{"A.PV"}) {
		return status.Error(codes.InvalidArgument, "poll did not round trip")
	}
	s.mu.Lock()
	s.methods = append(s.methods, rpc.OpcDaService_Poll_FullMethodName)
	s.mu.Unlock()
	if err := stream.Send(pollAck(request)); err != nil {
		return err
	}
	if err := stream.Send(pollData("line-a", 1, goodPollValue("A.PV"))); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *roundTripServer) GetStatus(
	context.Context,
	*rpc.GetStatusRequest,
) (*rpc.GetStatusResponse, error) {
	return nil, fmt.Errorf("get status is not used")
}
