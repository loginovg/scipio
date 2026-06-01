package grpcserver

import (
	"errors"
	"testing"

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

func TestShouldReturnInternalStatusWithGenericMessageWhenMappingUnexpectedError(t *testing.T) {
	t.Parallel()

	err := mapError(errors.New("duplicate key value violates unique constraint"))

	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Internal, grpcStatus.Code())
	require.Equal(t, "internal error", grpcStatus.Message())
}
