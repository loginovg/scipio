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

	// given
	serviceErr := service.ErrSagaNotFound

	// when
	err := mapError(serviceErr)

	// then
	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, grpcStatus.Code())
}

func Test_MapError_ReturnInvalidArgumentStatusForServiceValidationError(t *testing.T) {
	t.Parallel()

	// given
	serviceErr := service.ErrInvalidWorkflow

	// when
	err := mapError(serviceErr)

	// then
	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, grpcStatus.Code())
}

func Test_MapError_ReturnAbortedStatusForSagaLockContendedError(t *testing.T) {
	t.Parallel()

	// given
	serviceErr := service.ErrSagaLockContended

	// when
	err := mapError(serviceErr)

	// then
	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Aborted, grpcStatus.Code())
}

func Test_MapError_ReturnFailedPreconditionStatusForSagaCancelNotAllowedError(t *testing.T) {
	t.Parallel()

	// given
	serviceErr := service.ErrSagaCancelNotAllowed

	// when
	err := mapError(serviceErr)

	// then
	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, grpcStatus.Code())
}

func Test_MapError_ReturnInternalStatusWithGenericMessageForUnexpectedError(t *testing.T) {
	t.Parallel()

	// given
	serviceErr := errors.New("duplicate key value violates unique constraint")

	// when
	err := mapError(serviceErr)

	// then
	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Internal, grpcStatus.Code())
	require.Equal(t, "internal error", grpcStatus.Message())
}

func Test_MapSagaStatus_ReturnUnspecifiedForUnknownSagaStatus(t *testing.T) {
	t.Parallel()

	// given
	unknownStatus := domain.SagaStatus("UNKNOWN_STATUS")

	// when
	mapped := mapSagaStatus(unknownStatus)

	// then
	require.Equal(t, sagav1.SagaStatus_SAGA_STATUS_UNSPECIFIED, mapped)
}

func Test_MapStepStatus_ReturnUnspecifiedForUnknownSagaStepStatus(t *testing.T) {
	t.Parallel()

	// given
	unknownStatus := domain.SagaStepStatus("UNKNOWN_STATUS")

	// when
	mapped := mapStepStatus(unknownStatus)

	// then
	require.Equal(t, sagav1.SagaStepStatus_SAGA_STEP_STATUS_UNSPECIFIED, mapped)
}
