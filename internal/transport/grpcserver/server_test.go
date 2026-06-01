package grpcserver

import (
	"errors"
	"testing"

	sagav1 "scipio/gen/proto"
	"scipio/internal/domain"
	"scipio/internal/service"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/stretchr/testify/require"
)

func TestShouldReturnNotFoundStatusWhenMappingServiceNotFoundError(t *testing.T) {
	t.Parallel()

	err := mapError(service.ErrSagaNotFound)

	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, grpcStatus.Code())
}

func TestShouldReturnInvalidArgumentStatusWhenMappingServiceValidationError(t *testing.T) {
	t.Parallel()

	err := mapError(service.ErrInvalidWorkflow)

	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, grpcStatus.Code())
}

func TestShouldReturnAbortedStatusWhenMappingSagaLockContendedError(t *testing.T) {
	t.Parallel()

	err := mapError(service.ErrSagaLockContended)

	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Aborted, grpcStatus.Code())
}

func TestShouldReturnInternalStatusWithGenericMessageWhenMappingUnexpectedError(t *testing.T) {
	t.Parallel()

	err := mapError(errors.New("duplicate key value violates unique constraint"))

	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Internal, grpcStatus.Code())
	require.Equal(t, "internal error", grpcStatus.Message())
}

func TestShouldReturnUnspecifiedWhenMappingUnknownSagaStatus(t *testing.T) {
	t.Parallel()

	require.Equal(t, sagav1.SagaStatus_SAGA_STATUS_UNSPECIFIED, mapSagaStatus(domain.SagaStatus("UNKNOWN_STATUS")))
}

func TestShouldReturnUnspecifiedWhenMappingUnknownSagaStepStatus(t *testing.T) {
	t.Parallel()

	require.Equal(t, sagav1.SagaStepStatus_SAGA_STEP_STATUS_UNSPECIFIED, mapStepStatus(domain.SagaStepStatus("UNKNOWN_STATUS")))
}
