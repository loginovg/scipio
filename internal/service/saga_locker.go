package service

import (
	"context"
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

	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if releaseErr := handle.Release(releaseCtx); releaseErr != nil {
			slog.WarnContext(releaseCtx, "failed to release saga lock", "saga_id", sagaID, "error", releaseErr)
		}
	}()

	return fn(ctx)
}
