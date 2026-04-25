package statemachine

import "scipio/internal/domain"

func CanTransition(from domain.SagaStatus, to domain.SagaStatus) bool {
	if from == to {
		return true
	}

	switch from {
	case domain.SagaStatusCreated:
		return to == domain.SagaStatusRunning || to == domain.SagaStatusCanceling || to == domain.SagaStatusFailed
	case domain.SagaStatusRunning:
		return to == domain.SagaStatusCompleted || to == domain.SagaStatusCanceling || to == domain.SagaStatusFailed
	case domain.SagaStatusCompleted:
		return to == domain.SagaStatusCanceling
	case domain.SagaStatusCanceling:
		return to == domain.SagaStatusCompensated || to == domain.SagaStatusFailed
	default:
		return false
	}
}

func IsTerminal(status domain.SagaStatus) bool {
	return status == domain.SagaStatusCompleted || status == domain.SagaStatusCompensated || status == domain.SagaStatusFailed
}
