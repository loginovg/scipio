package domain

import (
	"errors"
	"strings"
)

var ErrInvalidSagaStatus = errors.New("invalid saga status")
var ErrInvalidSagaStepStatus = errors.New("invalid saga step status")

func ParseSagaStatus(raw string) (SagaStatus, error) {
	status := SagaStatus(strings.ToUpper(strings.TrimSpace(raw)))
	switch status {
	case SagaStatusCreated,
		SagaStatusRunning,
		SagaStatusCompleted,
		SagaStatusCanceling,
		SagaStatusCompensated,
		SagaStatusFailed:
		return status, nil
	default:
		return "", ErrInvalidSagaStatus
	}
}

func ParseSagaStepStatus(raw string) (SagaStepStatus, error) {
	status := SagaStepStatus(strings.ToUpper(strings.TrimSpace(raw)))
	switch status {
	case SagaStepStatusPending,
		SagaStepStatusRunning,
		SagaStepStatusCompleted,
		SagaStepStatusCompensating,
		SagaStepStatusCompensated,
		SagaStepStatusFailed:
		return status, nil
	default:
		return "", ErrInvalidSagaStepStatus
	}
}

func MapSagaSteps[T any](steps []SagaStep, mapper func(SagaStep) T) []T {
	mapped := make([]T, 0, len(steps))
	for _, step := range steps {
		mapped = append(mapped, mapper(step))
	}

	return mapped
}
