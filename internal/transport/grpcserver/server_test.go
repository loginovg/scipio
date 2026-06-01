package grpcserver

import (
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
