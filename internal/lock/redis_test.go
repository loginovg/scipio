package lock

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type fakeMutexFactory struct {
	mu    sync.Mutex
	mutex *fakeMutex
	names []string
}

func (f *fakeMutexFactory) NewMutex(name string, _ ...redsync.Option) redisMutex {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.names = append(f.names, name)
	return f.mutex
}

func (f *fakeMutexFactory) namesSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	snapshot := make([]string, len(f.names))
	copy(snapshot, f.names)
	return snapshot
}

type unlockResult struct {
	ok  bool
	err error
}

type fakeMutex struct {
	mu             sync.Mutex
	lockResults    []error
	defaultLockErr error
	extendResults  []unlockResult
	defaultExtend  unlockResult
	unlockResults  []unlockResult
	defaultUnlock  unlockResult
	lockCalls      int
	extendCalls    int
	unlockCalls    int
}

func newFakeMutex() *fakeMutex {
	return &fakeMutex{
		defaultUnlock: unlockResult{ok: true},
		defaultExtend: unlockResult{ok: true},
	}
}

func (f *fakeMutex) LockContext(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.lockCalls++

	if len(f.lockResults) == 0 {
		return f.defaultLockErr
	}

	result := f.lockResults[0]
	f.lockResults = f.lockResults[1:]
	return result
}

func (f *fakeMutex) UnlockContext(_ context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.unlockCalls++

	if len(f.unlockResults) == 0 {
		return f.defaultUnlock.ok, f.defaultUnlock.err
	}

	result := f.unlockResults[0]
	f.unlockResults = f.unlockResults[1:]
	return result.ok, result.err
}

func (f *fakeMutex) ExtendContext(_ context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.extendCalls++

	if len(f.extendResults) == 0 {
		return f.defaultExtend.ok, f.defaultExtend.err
	}

	result := f.extendResults[0]
	f.extendResults = f.extendResults[1:]
	return result.ok, result.err
}

func (f *fakeMutex) snapshotCalls() (int, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.lockCalls, f.extendCalls, f.unlockCalls
}

func TestShouldAcquireAndReleaseLockWhenMutexAllows(t *testing.T) {
	t.Parallel()

	mutex := newFakeMutex()
	factory := &fakeMutexFactory{mutex: mutex}
	locker := &Redis{
		newMutex:      factory.NewMutex,
		prefix:        "test:",
		retryInterval: 2 * time.Millisecond,
	}

	handle, acquireErr := locker.Acquire(context.Background(), "saga-1", time.Second)
	require.NoError(t, acquireErr)
	require.NotNil(t, handle)

	require.NoError(t, handle.Release(context.Background()))
	require.NoError(t, handle.Release(context.Background()))

	lockCalls, extendCalls, unlockCalls := mutex.snapshotCalls()
	require.Equal(t, 1, lockCalls)
	require.Equal(t, 0, extendCalls)
	require.Equal(t, 1, unlockCalls)
	require.Equal(t, []string{"test:saga-1"}, factory.namesSnapshot())
}

func TestShouldRetryUntilLockAcquiredWhenMutexReportsContention(t *testing.T) {
	t.Parallel()

	mutex := newFakeMutex()
	mutex.lockResults = []error{
		redsync.ErrFailed,
		&redsync.ErrTaken{Nodes: []int{0}},
		nil,
	}
	factory := &fakeMutexFactory{mutex: mutex}
	locker := &Redis{
		newMutex:      factory.NewMutex,
		prefix:        "test:",
		retryInterval: time.Millisecond,
	}

	handle, acquireErr := locker.Acquire(context.Background(), "saga-1", time.Second)
	require.NoError(t, acquireErr)
	require.NotNil(t, handle)

	lockCalls, extendCalls, unlockCalls := mutex.snapshotCalls()
	require.Equal(t, 3, lockCalls)
	require.Equal(t, 0, extendCalls)
	require.Equal(t, 0, unlockCalls)
}

func TestShouldReturnContextDeadlineExceededWhenLockCannotBeAcquiredBeforeTimeout(t *testing.T) {
	t.Parallel()

	mutex := newFakeMutex()
	mutex.defaultLockErr = redsync.ErrFailed
	factory := &fakeMutexFactory{mutex: mutex}
	locker := &Redis{
		newMutex:      factory.NewMutex,
		prefix:        "test:",
		retryInterval: 5 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()

	handle, acquireErr := locker.Acquire(ctx, "saga-1", time.Second)

	require.Nil(t, handle)
	require.ErrorIs(t, acquireErr, context.DeadlineExceeded)
}

func TestShouldReturnAcquireErrorWhenMutexReturnsNonContentionError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("redis unavailable")
	mutex := newFakeMutex()
	mutex.defaultLockErr = expectedErr
	factory := &fakeMutexFactory{mutex: mutex}
	locker := &Redis{
		newMutex:      factory.NewMutex,
		prefix:        "test:",
		retryInterval: time.Millisecond,
	}

	handle, acquireErr := locker.Acquire(context.Background(), "saga-1", time.Second)
	require.Nil(t, handle)
	require.ErrorIs(t, acquireErr, expectedErr)

	lockCalls, extendCalls, unlockCalls := mutex.snapshotCalls()
	require.Equal(t, 1, lockCalls)
	require.Equal(t, 0, extendCalls)
	require.Equal(t, 0, unlockCalls)
}

func TestShouldReturnErrInvalidTTLWhenLockTTLIsNotPositive(t *testing.T) {
	t.Parallel()

	mutex := newFakeMutex()
	factory := &fakeMutexFactory{mutex: mutex}
	locker := &Redis{
		newMutex:      factory.NewMutex,
		prefix:        "test:",
		retryInterval: time.Millisecond,
	}

	handle, acquireErr := locker.Acquire(context.Background(), "saga-1", 0)
	require.Nil(t, handle)
	require.ErrorIs(t, acquireErr, ErrInvalidTTL)
}

func TestShouldReturnErrInvalidRetryIntervalWhenRetryIntervalIsNotPositive(t *testing.T) {
	t.Parallel()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	locker, err := NewRedis(client, "test:", 0)
	require.Nil(t, locker)
	require.ErrorIs(t, err, ErrInvalidRetryInterval)
}

func TestShouldReturnErrInvalidRetryIntervalWhenRetryIntervalFromURLIsNotPositive(t *testing.T) {
	t.Parallel()

	locker, err := NewRedisFromURL("redis://127.0.0.1:6379/0", "test:", 0)
	require.Nil(t, locker)
	require.ErrorIs(t, err, ErrInvalidRetryInterval)
}

func TestShouldReturnErrInvalidRedisURLWhenURLIsBlank(t *testing.T) {
	t.Parallel()

	locker, err := NewRedisFromURL("   ", "test:", time.Millisecond)
	require.Nil(t, locker)
	require.ErrorIs(t, err, ErrInvalidRedisURL)
}

func TestShouldReturnErrInvalidPrefixWhenPrefixIsBlank(t *testing.T) {
	t.Parallel()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	locker, err := NewRedis(client, "   ", time.Millisecond)
	require.Nil(t, locker)
	require.ErrorIs(t, err, ErrInvalidPrefix)
}

func TestShouldReturnUnlockErrorWhenReleaseFails(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("unlock failed")
	mutex := newFakeMutex()
	mutex.unlockResults = []unlockResult{{ok: false, err: expectedErr}}
	factory := &fakeMutexFactory{mutex: mutex}
	locker := &Redis{
		newMutex:      factory.NewMutex,
		prefix:        "test:",
		retryInterval: time.Millisecond,
	}

	handle, acquireErr := locker.Acquire(context.Background(), "saga-1", time.Second)
	require.NoError(t, acquireErr)
	require.NotNil(t, handle)

	releaseErr := handle.Release(context.Background())
	require.ErrorIs(t, releaseErr, expectedErr)
}

func TestShouldIgnoreErrLockAlreadyExpiredWhenReleasingLock(t *testing.T) {
	t.Parallel()

	mutex := newFakeMutex()
	mutex.unlockResults = []unlockResult{{ok: false, err: redsync.ErrLockAlreadyExpired}}
	factory := &fakeMutexFactory{mutex: mutex}
	locker := &Redis{
		newMutex:      factory.NewMutex,
		prefix:        "test:",
		retryInterval: time.Millisecond,
	}

	handle, acquireErr := locker.Acquire(context.Background(), "saga-1", time.Second)
	require.NoError(t, acquireErr)
	require.NotNil(t, handle)

	require.NoError(t, handle.Release(context.Background()))

	lockCalls, extendCalls, unlockCalls := mutex.snapshotCalls()
	require.Equal(t, 1, lockCalls)
	require.Equal(t, 0, extendCalls)
	require.Equal(t, 1, unlockCalls)
}

func TestShouldExtendLockWhenMutexAllows(t *testing.T) {
	t.Parallel()

	mutex := newFakeMutex()
	factory := &fakeMutexFactory{mutex: mutex}
	locker := &Redis{
		newMutex:      factory.NewMutex,
		prefix:        "test:",
		retryInterval: time.Millisecond,
	}

	handle, acquireErr := locker.Acquire(context.Background(), "saga-1", time.Second)
	require.NoError(t, acquireErr)
	require.NotNil(t, handle)

	require.NoError(t, handle.Extend(context.Background()))

	lockCalls, extendCalls, unlockCalls := mutex.snapshotCalls()
	require.Equal(t, 1, lockCalls)
	require.Equal(t, 1, extendCalls)
	require.Equal(t, 0, unlockCalls)
}

func TestShouldReturnExtendErrorWhenMutexExtendFails(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("extend failed")
	mutex := newFakeMutex()
	mutex.extendResults = []unlockResult{{ok: false, err: expectedErr}}
	factory := &fakeMutexFactory{mutex: mutex}
	locker := &Redis{
		newMutex:      factory.NewMutex,
		prefix:        "test:",
		retryInterval: time.Millisecond,
	}

	handle, acquireErr := locker.Acquire(context.Background(), "saga-1", time.Second)
	require.NoError(t, acquireErr)
	require.NotNil(t, handle)

	extendErr := handle.Extend(context.Background())
	require.ErrorIs(t, extendErr, expectedErr)
}

func TestShouldReturnErrExtendFailedWhenMutexExtendReturnsFalseWithoutError(t *testing.T) {
	t.Parallel()

	mutex := newFakeMutex()
	mutex.extendResults = []unlockResult{{ok: false, err: nil}}
	factory := &fakeMutexFactory{mutex: mutex}
	locker := &Redis{
		newMutex:      factory.NewMutex,
		prefix:        "test:",
		retryInterval: time.Millisecond,
	}

	handle, acquireErr := locker.Acquire(context.Background(), "saga-1", time.Second)
	require.NoError(t, acquireErr)
	require.NotNil(t, handle)

	extendErr := handle.Extend(context.Background())
	require.ErrorIs(t, extendErr, redsync.ErrExtendFailed)
}
