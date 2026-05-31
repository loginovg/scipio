package service

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"scipio/internal/lock"
)

type sagaLocker struct {
	locker  lock.Locker
	lockTTL time.Duration
}

func newSagaLocker(locker lock.Locker, lockTTL time.Duration) sagaLocker {
	if locker == nil {
		locker = lock.NewNoop()
	}

	return sagaLocker{
		locker:  locker,
		lockTTL: lockTTL,
	}
}

func (s sagaLocker) withSagaLock(ctx context.Context, sagaID string, fn func(context.Context) error) error {
	handle, err := s.locker.Acquire(ctx, sagaID, s.lockTTL)
	if err != nil {
		return err
	}
	defer releaseSagaLock(handle, sagaID)

	lockCtx, cancelLock := context.WithCancel(ctx)
	defer cancelLock()

	renewErrCh, watchdogDone := s.startLockWatchdog(lockCtx, cancelLock, handle, sagaID)

	fnErr := fn(lockCtx)
	cancelLock()
	<-watchdogDone

	return joinLockErrors(fnErr, pollRenewError(renewErrCh))
}

func (s sagaLocker) startLockWatchdog(lockCtx context.Context, cancelLock context.CancelFunc, handle lock.Handle, sagaID string) (<-chan error, <-chan struct{}) {
	renewErrCh := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		defer close(done)

		ticker := time.NewTicker(lockRenewInterval(s.lockTTL))
		for {
			renewErr := s.waitAndRenew(lockCtx, ticker, handle)
			if renewErr == nil {
				continue
			}

			if errors.Is(renewErr, context.Canceled) {
				return
			}

			slog.WarnContext(lockCtx, "failed to renew saga lock", "saga_id", sagaID, "error", renewErr)
			offerRenewError(renewErrCh, renewErr)
			cancelLock()
			return
		}
	}()

	return renewErrCh, done
}

func (s sagaLocker) waitAndRenew(lockCtx context.Context, ticker *time.Ticker, handle lock.Handle) error {
	select {
	case <-lockCtx.Done():
		return lockCtx.Err()
	case <-ticker.C:
	}

	renewCtx, cancel := context.WithTimeout(context.Background(), lockRenewTimeout(s.lockTTL))
	defer cancel()

	return handle.Extend(renewCtx)
}

func releaseSagaLock(handle lock.Handle, sagaID string) {
	releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if releaseErr := handle.Release(releaseCtx); releaseErr != nil {
		slog.WarnContext(releaseCtx, "failed to release saga lock", "saga_id", sagaID, "error", releaseErr)
	}
}

func offerRenewError(renewErrCh chan<- error, err error) {
	select {
	case renewErrCh <- err:
	default:
	}
}

func pollRenewError(renewErrCh <-chan error) error {
	select {
	case err := <-renewErrCh:
		return err
	default:
		return nil
	}
}

func joinLockErrors(fnErr error, renewErr error) error {
	if renewErr == nil {
		return fnErr
	}

	if fnErr == nil {
		return renewErr
	}

	return errors.Join(renewErr, fnErr)
}

func lockRenewInterval(lockTTL time.Duration) time.Duration {
	interval := lockTTL / 2
	if interval <= 0 {
		return time.Nanosecond
	}

	return interval
}

func lockRenewTimeout(lockTTL time.Duration) time.Duration {
	timeout := lockTTL / 2
	if timeout <= 0 {
		return time.Nanosecond
	}

	return timeout
}
