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

func Test_MapError_ReturnNotFoundStatusForServiceNotFoundError(t *testing.T) {
	t.Parallel()

	err := mapError(service.ErrSagaNotFound)

	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, grpcStatus.Code())
}

func Test_MapError_ReturnInvalidArgumentStatusForServiceValidationError(t *testing.T) {
	t.Parallel()

	err := mapError(service.ErrInvalidWorkflow)

	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, grpcStatus.Code())
}

func Test_MapError_ReturnAbortedStatusForSagaLockContendedError(t *testing.T) {
	t.Parallel()

	err := mapError(service.ErrSagaLockContended)

	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Aborted, grpcStatus.Code())
}

func Test_MapError_ReturnFailedPreconditionStatusForSagaCancelNotAllowedError(t *testing.T) {
	t.Parallel()

	err := mapError(service.ErrSagaCancelNotAllowed)

	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, grpcStatus.Code())
}

func Test_MapError_ReturnInternalStatusWithGenericMessageForUnexpectedError(t *testing.T) {
	t.Parallel()

	err := mapError(errors.New("duplicate key value violates unique constraint"))

	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Internal, grpcStatus.Code())
	require.Equal(t, "internal error", grpcStatus.Message())
}

func Test_MapSagaStatus_ReturnUnspecifiedForUnknownSagaStatus(t *testing.T) {
	t.Parallel()

	require.Equal(t, sagav1.SagaStatus_SAGA_STATUS_UNSPECIFIED, mapSagaStatus(domain.SagaStatus("UNKNOWN_STATUS")))
}

func Test_MapStepStatus_ReturnUnspecifiedForUnknownSagaStepStatus(t *testing.T) {
	t.Parallel()

	require.Equal(t, sagav1.SagaStepStatus_SAGA_STEP_STATUS_UNSPECIFIED, mapStepStatus(domain.SagaStepStatus("UNKNOWN_STATUS")))
}
