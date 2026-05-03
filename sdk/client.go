package sdk

import (
	"context"
	"encoding/json"

	sagav1 "scipio/gen/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn       *grpc.ClientConn
	sagaClient sagav1.SagaServiceClient
}

func NewClient(address string) (*Client, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:       conn,
		sagaClient: sagav1.NewSagaServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) StartSaga(ctx context.Context, workflow string, sagaContext map[string]any) (string, error) {
	if sagaContext == nil {
		sagaContext = map[string]any{}
	}

	payload, err := json.Marshal(sagaContext)
	if err != nil {
		return "", err
	}

	response, err := c.sagaClient.StartSaga(ctx, &sagav1.StartSagaRequest{
		Workflow: workflow,
		Context:  payload,
	})
	if err != nil {
		return "", err
	}

	return response.GetId(), nil
}

func (c *Client) GetSaga(ctx context.Context, sagaID string) (*sagav1.Saga, error) {
	response, err := c.sagaClient.GetSaga(ctx, &sagav1.GetSagaRequest{Id: sagaID})
	if err != nil {
		return nil, err
	}

	return response.GetSaga(), nil
}

func (c *Client) CancelSaga(ctx context.Context, sagaID string) (*sagav1.Saga, error) {
	response, err := c.sagaClient.CancelSaga(ctx, &sagav1.CancelSagaRequest{Id: sagaID})
	if err != nil {
		return nil, err
	}

	return response.GetSaga(), nil
}
