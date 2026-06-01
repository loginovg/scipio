package sdk

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	sagav1 "scipio/gen/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/require"
)

func TestShouldSendStartSagaRequestWhenClientStartsSaga(t *testing.T) {
	t.Parallel()

	client, server := newTestClientAndServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	sagaID, err := client.StartSaga(
		ctx,
		"order_flow",
		map[string]any{"amount": 42, "currency": "USD"},
		[]StartSagaStep{{Name: "charge", GRPCTarget: "billing:9000"}},
	)
	require.NoError(t, err)
	require.Equal(t, "started-saga-id", sagaID)

	request := server.startSagaRequestSnapshot()
	require.NotNil(t, request)
	require.Equal(t, "order_flow", request.GetWorkflow())
	require.Equal(t, "", request.GetIdempotencyKey())
	require.Len(t, request.GetSteps(), 1)
	require.Equal(t, "charge", request.GetSteps()[0].GetName())
	require.Equal(t, "billing:9000", request.GetSteps()[0].GetGrpcTarget())

	var parsedContext map[string]any
	require.NoError(t, json.Unmarshal(request.GetContext(), &parsedContext))
	require.Equal(t, map[string]any{"amount": float64(42), "currency": "USD"}, parsedContext)
}

func TestShouldSendIdempotencyKeyWhenClientStartsSagaWithIdempotencyKey(t *testing.T) {
	t.Parallel()

	client, server := newTestClientAndServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	sagaID, err := client.StartSagaWithIdempotencyKey(
		ctx,
		"order_flow",
		"idempotency-key-1",
		map[string]any{"amount": 42},
		[]StartSagaStep{{Name: "charge", GRPCTarget: "billing:9000"}},
	)
	require.NoError(t, err)
	require.Equal(t, "started-saga-id", sagaID)

	request := server.startSagaRequestSnapshot()
	require.NotNil(t, request)
	require.Equal(t, "idempotency-key-1", request.GetIdempotencyKey())
}

func TestShouldSendGetSagaRequestWhenClientGetsSaga(t *testing.T) {
	t.Parallel()

	client, server := newTestClientAndServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	saga, err := client.GetSaga(ctx, "get-saga-id")
	require.NoError(t, err)
	require.NotNil(t, saga)
	require.Equal(t, "get-saga-id", saga.GetId())

	request := server.getSagaRequestSnapshot()
	require.NotNil(t, request)
	require.Equal(t, "get-saga-id", request.GetId())
}

func TestShouldSendCancelSagaRequestWhenClientCancelsSaga(t *testing.T) {
	t.Parallel()

	client, server := newTestClientAndServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	saga, err := client.CancelSaga(ctx, "cancel-saga-id")
	require.NoError(t, err)
	require.NotNil(t, saga)
	require.Equal(t, "cancel-saga-id", saga.GetId())

	request := server.cancelSagaRequestSnapshot()
	require.NotNil(t, request)
	require.Equal(t, "cancel-saga-id", request.GetId())
}

func newTestClientAndServer(t *testing.T) (*Client, *recordingSagaService) {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() {
		_ = listener.Close()
	})

	grpcServer := grpc.NewServer()
	recordingServer := &recordingSagaService{}
	sagav1.RegisterSagaServiceServer(grpcServer, recordingServer)

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		<-serverErr
	})

	client, err := newClient(
		"passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	return client, recordingServer
}

type recordingSagaService struct {
	sagav1.UnimplementedSagaServiceServer

	mu sync.Mutex

	startSagaRequest  *sagav1.StartSagaRequest
	getSagaRequest    *sagav1.GetSagaRequest
	cancelSagaRequest *sagav1.CancelSagaRequest
}

func (s *recordingSagaService) StartSaga(_ context.Context, request *sagav1.StartSagaRequest) (*sagav1.StartSagaResponse, error) {
	s.mu.Lock()
	s.startSagaRequest = proto.Clone(request).(*sagav1.StartSagaRequest)
	s.mu.Unlock()

	return &sagav1.StartSagaResponse{Id: "started-saga-id"}, nil
}

func (s *recordingSagaService) GetSaga(_ context.Context, request *sagav1.GetSagaRequest) (*sagav1.GetSagaResponse, error) {
	s.mu.Lock()
	s.getSagaRequest = proto.Clone(request).(*sagav1.GetSagaRequest)
	s.mu.Unlock()

	return &sagav1.GetSagaResponse{Saga: &sagav1.Saga{Id: request.GetId()}}, nil
}

func (s *recordingSagaService) CancelSaga(_ context.Context, request *sagav1.CancelSagaRequest) (*sagav1.CancelSagaResponse, error) {
	s.mu.Lock()
	s.cancelSagaRequest = proto.Clone(request).(*sagav1.CancelSagaRequest)
	s.mu.Unlock()

	return &sagav1.CancelSagaResponse{Saga: &sagav1.Saga{Id: request.GetId()}}, nil
}

func (s *recordingSagaService) startSagaRequestSnapshot() *sagav1.StartSagaRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.startSagaRequest == nil {
		return nil
	}

	return proto.Clone(s.startSagaRequest).(*sagav1.StartSagaRequest)
}

func (s *recordingSagaService) getSagaRequestSnapshot() *sagav1.GetSagaRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.getSagaRequest == nil {
		return nil
	}

	return proto.Clone(s.getSagaRequest).(*sagav1.GetSagaRequest)
}

func (s *recordingSagaService) cancelSagaRequestSnapshot() *sagav1.CancelSagaRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancelSagaRequest == nil {
		return nil
	}

	return proto.Clone(s.cancelSagaRequest).(*sagav1.CancelSagaRequest)
}
