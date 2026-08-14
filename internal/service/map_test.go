package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type startStepDTO struct {
	name       string
	grpcTarget string
}

func Test_MapStartSagaSteps_MapSourceItemsWhenTheyAreValues(t *testing.T) {
	t.Parallel()

	source := []startStepDTO{
		{name: "charge", grpcTarget: "billing:9000"},
		{name: "reserve", grpcTarget: "inventory:9000"},
	}

	mapped := MapStartSagaSteps(source, func(step startStepDTO) StartSagaStep {
		return StartSagaStep{
			Name:       step.name,
			GRPCTarget: step.grpcTarget,
		}
	})

	require.Equal(t, []StartSagaStep{
		{Name: "charge", GRPCTarget: "billing:9000"},
		{Name: "reserve", GRPCTarget: "inventory:9000"},
	}, mapped)
}

func Test_MapStartSagaSteps_MapSourceItemsWhenTheyArePointers(t *testing.T) {
	t.Parallel()

	source := []*startStepDTO{
		{name: "charge", grpcTarget: "billing:9000"},
		{name: "reserve", grpcTarget: "inventory:9000"},
	}

	mapped := MapStartSagaSteps(source, func(step *startStepDTO) StartSagaStep {
		return StartSagaStep{
			Name:       step.name,
			GRPCTarget: step.grpcTarget,
		}
	})

	require.Equal(t, []StartSagaStep{
		{Name: "charge", GRPCTarget: "billing:9000"},
		{Name: "reserve", GRPCTarget: "inventory:9000"},
	}, mapped)
}
