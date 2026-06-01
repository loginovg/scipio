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

	client := sagav1.NewSagaStepExecutorClient(conn)
	operationName := "execute"
	var callErr error
	if step.Status == domain.SagaStepStatusCompensating {
		operationName = "compensate"
		callErr = compensateStep(callCtx, client, saga, step, payload)
	} else {
		callErr = executeStep(callCtx, client, saga, step, payload)
	}

	if callErr != nil {
		wrappedCallErr := fmt.Errorf("%s step via grpc target %q: %w", operationName, step.GRPCTarget, callErr)
		closeErr := conn.Close()
		if closeErr != nil {
			return errors.Join(
				wrappedCallErr,
				fmt.Errorf("close step target connection %q: %w", step.GRPCTarget, closeErr),
			)
		}

		return wrappedCallErr
	}

	if closeErr := conn.Close(); closeErr != nil {
		return fmt.Errorf("close step target connection %q: %w", step.GRPCTarget, closeErr)
	}

	return nil
}

func executeStep(
	ctx context.Context,
	client sagav1.SagaStepExecutorClient,
	saga domain.Saga,
	step domain.SagaStep,
	payload []byte,
) error {
	_, err := client.ExecuteStep(ctx, &sagav1.ExecuteStepRequest{
		SagaId:   saga.ID,
		Workflow: saga.Workflow,
		StepName: step.Name,
		Attempt:  step.Attempt,
		Context:  payload,
	})
	return err
}

func compensateStep(
	ctx context.Context,
	client sagav1.SagaStepExecutorClient,
	saga domain.Saga,
	step domain.SagaStep,
	payload []byte,
) error {
	_, err := client.CompensateStep(ctx, &sagav1.CompensateStepRequest{
		SagaId:   saga.ID,
		Workflow: saga.Workflow,
		StepName: step.Name,
		Attempt:  step.Attempt,
		Context:  payload,
	})
	return err
}
