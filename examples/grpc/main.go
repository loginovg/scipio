package main

import (
	"context"
	"fmt"
	"os"
	"time"

	sagav1 "scipio/gen/proto"
	"scipio/sdk"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run() error {
	address := os.Getenv("SCIPIO_GRPC_ADDR")
	if address == "" {
		address = "127.0.0.1:9090"
	}

	client, err := sdk.NewClient(address)
	if err != nil {
		return fmt.Errorf("create grpc client: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to close grpc client: %v\n", closeErr)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sagaID, err := client.StartSaga(ctx, "order_flow", map[string]any{"order_id": "A-100"})
	if err != nil {
		return fmt.Errorf("start saga: %w", err)
	}

	fmt.Printf("started saga: %s\n", sagaID)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		saga, getErr := client.GetSaga(ctx, sagaID)
		if getErr != nil {
			return fmt.Errorf("get saga: %w", getErr)
		}

		fmt.Printf("status: %s\n", saga.GetStatus().String())
		if isTerminal(saga.GetStatus()) {
			fmt.Printf("done: %s\n", saga.GetStatus().String())
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func isTerminal(status sagav1.SagaStatus) bool {
	return status == sagav1.SagaStatus_SAGA_STATUS_COMPLETED ||
		status == sagav1.SagaStatus_SAGA_STATUS_COMPENSATED ||
		status == sagav1.SagaStatus_SAGA_STATUS_FAILED
}
