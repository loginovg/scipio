package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	sagav1 "scipio/gen/proto"
	"scipio/internal/domain"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const stepDispatchTimeout = 5 * time.Second

type stepDispatcher interface {
	Dispatch(ctx context.Context, saga domain.Saga, step domain.SagaStep) error
}

type grpcStepDispatcher struct{}

func newGRPCStepDispatcher() stepDispatcher {
	return grpcStepDispatcher{}
}

func (grpcStepDispatcher) Dispatch(ctx context.Context, saga domain.Saga, step domain.SagaStep) error {
	if step.GRPCTarget == "" {
		return ErrInvalidStepGRPCTarget
	}

	payload, err := json.Marshal(saga.Context)
	if err != nil {
		return fmt.Errorf("marshal saga context: %w", err)
	}

	conn, err := grpc.NewClient(step.GRPCTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial step target %q: %w", step.GRPCTarget, err)
	}

	callCtx, cancel := context.WithTimeout(ctx, stepDispatchTimeout)
	defer cancel()

	_, err = sagav1.NewSagaStepExecutorClient(conn).ExecuteStep(callCtx, &sagav1.ExecuteStepRequest{
		SagaId:   saga.ID,
		Workflow: saga.Workflow,
		StepName: step.Name,
		Attempt:  step.Attempt,
		Context:  payload,
	})
	if err != nil {
		closeErr := conn.Close()
		if closeErr != nil {
			return errors.Join(
				fmt.Errorf("execute step via grpc target %q: %w", step.GRPCTarget, err),
				fmt.Errorf("close step target connection %q: %w", step.GRPCTarget, closeErr),
			)
		}

		return fmt.Errorf("execute step via grpc target %q: %w", step.GRPCTarget, err)
	}

	if closeErr := conn.Close(); closeErr != nil {
		return fmt.Errorf("close step target connection %q: %w", step.GRPCTarget, closeErr)
	}

	return nil
}
