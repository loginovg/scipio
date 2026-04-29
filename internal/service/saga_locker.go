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
	logger  *slog.Logger
}

func newSagaLocker(locker lock.Locker, lockTTL time.Duration, logger *slog.Logger) sagaLocker {
	if locker == nil {
		locker = lock.NewNoop()
	}

	if logger == nil {
		logger = slog.Default()
	}

	return sagaLocker{
		locker:  locker,
		lockTTL: normalizeLockTTL(lockTTL),
		logger:  logger,
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
			s.logger.WarnContext(releaseCtx, "failed to release saga lock", "saga_id", sagaID, "error", releaseErr)
		}
	}()

	return fn(ctx)
}

func normalizeLockTTL(lockTTL time.Duration) time.Duration {
	if lockTTL <= 0 {
		return 5 * time.Second
	}

	return lockTTL
}
