package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"scipio/internal/lock"

	"github.com/stretchr/testify/require"
)

type watchdogTestLocker struct {
	handle lock.Handle
}

func (l watchdogTestLocker) Acquire(_ context.Context, _ string, _ time.Duration) (lock.Handle, error) {
	return l.handle, nil
}

type blockingAcquireLocker struct{}

func (blockingAcquireLocker) Acquire(ctx context.Context, _ string, _ time.Duration) (lock.Handle, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type watchdogTestHandle struct {
	mu          sync.Mutex
	extendCalls int
	extendErr   error
	extendAt    int
	closed      chan struct{}
}

func (h *watchdogTestHandle) Release(_ context.Context) error {
	close(h.closed)
	return nil
}

func (h *watchdogTestHandle) Extend(_ context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.extendCalls++
	if h.extendErr != nil && h.extendCalls >= h.extendAt {
		return h.extendErr
	}

	return nil
}

func (h *watchdogTestHandle) calls() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.extendCalls
}

func Test_WithSagaLock_RenewLockWhenOperationTakesLongerThanHalfTTL(t *testing.T) {
	t.Parallel()

	// given
	handle := &watchdogTestHandle{closed: make(chan struct{})}
	locker := newSagaLocker(watchdogTestLocker{handle: handle}, 20*time.Millisecond)

	// when
	err := locker.withSagaLock(context.Background(), "saga-1", func(ctx context.Context) error {
		deadline := time.After(200 * time.Millisecond)
		for {
			if handle.calls() > 0 {
				return nil
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-deadline:
				return errors.New("watchdog did not renew lock")
			default:
			}
		}
	})

	// then
	require.NoError(t, err)
	require.Greater(t, handle.calls(), 0)

	select {
	case <-handle.closed:
	default:
		t.Fatal("expected lock handle to be released")
	}
}

func Test_WithSagaLock_ReturnRenewErrorWhenWatchdogCannotExtendLock(t *testing.T) {
	t.Parallel()

	// given
	renewErr := errors.New("renew failed")
	handle := &watchdogTestHandle{extendErr: renewErr, extendAt: 1, closed: make(chan struct{})}
	locker := newSagaLocker(watchdogTestLocker{handle: handle}, 20*time.Millisecond)

	// when
	err := locker.withSagaLock(context.Background(), "saga-1", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	// then
	require.Error(t, err)
	require.ErrorIs(t, err, renewErr)

	select {
	case <-handle.closed:
	default:
		t.Fatal("expected lock handle to be released")
	}
}

func Test_WithSagaLock_ReturnErrLockContendedWhenAcquireTimesOutWithoutRequestDeadline(t *testing.T) {
	t.Parallel()

	// given
	locker := newSagaLocker(blockingAcquireLocker{}, 20*time.Millisecond)

	// when
	err := locker.withSagaLock(context.Background(), "saga-1", func(context.Context) error {
		return nil
	})

	// then
	require.ErrorIs(t, err, lock.ErrLockContended)
}
