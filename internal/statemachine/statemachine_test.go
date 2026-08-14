package statemachine

import (
	"testing"

	"scipio/internal/domain"

	"github.com/stretchr/testify/require"
)

func Test_CanTransition_AllowExpectedSagaTransitions(t *testing.T) {
	t.Parallel()

	// given
	testCases := []struct {
		name string
		from domain.SagaStatus
		to   domain.SagaStatus
	}{
		{name: "created_to_running", from: domain.SagaStatusCreated, to: domain.SagaStatusRunning},
		{name: "running_to_completed", from: domain.SagaStatusRunning, to: domain.SagaStatusCompleted},
		{name: "completed_to_canceling", from: domain.SagaStatusCompleted, to: domain.SagaStatusCanceling},
		{name: "canceling_to_compensated", from: domain.SagaStatusCanceling, to: domain.SagaStatusCompensated},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			// when
			allowed := CanTransition(testCase.from, testCase.to)

			// then
			require.True(t, allowed)
		})
	}
}

func Test_CanTransition_RejectTransitionFromRunningToCompensated(t *testing.T) {
	t.Parallel()

	// given
	from := domain.SagaStatusRunning
	to := domain.SagaStatusCompensated

	// when
	allowed := CanTransition(from, to)

	// then
	require.False(t, allowed)
}

func Test_IsTerminal_RecognizeFinalStatuses(t *testing.T) {
	t.Parallel()

	// given
	completed := domain.SagaStatusCompleted
	compensated := domain.SagaStatusCompensated
	failed := domain.SagaStatusFailed
	running := domain.SagaStatusRunning

	// when
	completedTerminal := IsTerminal(completed)
	compensatedTerminal := IsTerminal(compensated)
	failedTerminal := IsTerminal(failed)
	runningTerminal := IsTerminal(running)

	// then
	require.True(t, completedTerminal)
	require.True(t, compensatedTerminal)
	require.True(t, failedTerminal)
	require.False(t, runningTerminal)
}
