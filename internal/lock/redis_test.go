package lock

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeRedisClient struct {
	mu           sync.Mutex
	setNXResults []bool
	setNXErr     error
	evalResult   int64
	evalErr      error
	setNXCalls   int
	evalCalls    int
}

func (f *fakeRedisClient) SetNX(_ context.Context, _ string, _ any, _ time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.setNXCalls++
	if f.setNXErr != nil {
		return false, f.setNXErr
	}

	if len(f.setNXResults) == 0 {
		return true, nil
	}

	result := f.setNXResults[0]
	f.setNXResults = f.setNXResults[1:]
	return result, nil
}

func (f *fakeRedisClient) EvalInt64(_ context.Context, _ string, _ []string, _ ...any) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.evalCalls++
	if f.evalErr != nil {
		return 0, f.evalErr
	}

	return f.evalResult, nil
}

func TestShouldAcquireAndReleaseLockWhenRedisAllows(t *testing.T) {
	t.Parallel()

	// given
	client := &fakeRedisClient{}
	locker, err := NewRedis(client, "test:", 2*time.Millisecond)
	require.NoError(t, err)

	// when
	handle, acquireErr := locker.Acquire(context.Background(), "saga-1", time.Second)
	require.NoError(t, acquireErr)
	require.NotNil(t, handle)

	require.NoError(t, handle.Release(context.Background()))

	// then
	client.mu.Lock()
	defer client.mu.Unlock()
	require.Equal(t, 1, client.setNXCalls)
	require.Equal(t, 1, client.evalCalls)
}

func TestShouldRetryUntilLockAcquiredWhenLockAlreadyHeld(t *testing.T) {
	t.Parallel()

	// given
	client := &fakeRedisClient{setNXResults: []bool{false, false, true}}
	locker, err := NewRedis(client, "test:", time.Millisecond)
	require.NoError(t, err)

	// when
	handle, acquireErr := locker.Acquire(context.Background(), "saga-1", time.Second)
	require.NoError(t, acquireErr)
	require.NotNil(t, handle)

	// then
	client.mu.Lock()
	defer client.mu.Unlock()
	require.Equal(t, 3, client.setNXCalls)
}

func TestShouldReturnContextDeadlineExceededWhenLockCannotBeAcquiredBeforeTimeout(t *testing.T) {
	t.Parallel()

	// given
	client := &fakeRedisClient{setNXResults: []bool{false, false, false, false, false, false}}
	locker, err := NewRedis(client, "test:", 5*time.Millisecond)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()

	// when
	handle, acquireErr := locker.Acquire(ctx, "saga-1", time.Second)

	// then
	require.Nil(t, handle)
	require.ErrorIs(t, acquireErr, context.DeadlineExceeded)
}

func TestShouldReturnErrInvalidTTLWhenLockTTLIsNotPositive(t *testing.T) {
	t.Parallel()

	// given
	client := &fakeRedisClient{}
	locker, err := NewRedis(client, "test:", time.Millisecond)
	require.NoError(t, err)

	// when
	handle, acquireErr := locker.Acquire(context.Background(), "saga-1", 0)

	// then
	require.Nil(t, handle)
	require.ErrorIs(t, acquireErr, ErrInvalidTTL)
}

func TestShouldReturnErrInvalidRetryIntervalWhenRetryIntervalIsNotPositive(t *testing.T) {
	t.Parallel()

	// given
	// when
	locker, err := NewRedis(&fakeRedisClient{}, "test:", 0)

	// then
	require.Nil(t, locker)
	require.ErrorIs(t, err, ErrInvalidRetryInterval)
}

func TestShouldRenewLockWhenHeldBeyondTTL(t *testing.T) {
	t.Parallel()

	// given
	client := &fakeRedisClient{evalResult: 1}
	locker, err := NewRedis(client, "test:", time.Millisecond)
	require.NoError(t, err)

	// when
	handle, acquireErr := locker.Acquire(context.Background(), "saga-1", 10*time.Millisecond)
	require.NoError(t, acquireErr)
	require.NotNil(t, handle)

	// then
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.evalCalls >= 1
	}, time.Second, 5*time.Millisecond)

	require.NoError(t, handle.Release(context.Background()))
}
