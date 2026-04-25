package lock

import (
	"context"
	"time"
)

type Handle interface {
	Release(ctx context.Context) error
}

type Locker interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (Handle, error)
}

type Noop struct{}

type noopHandle struct{}

func NewNoop() Noop {
	return Noop{}
}

func (Noop) Acquire(_ context.Context, _ string, _ time.Duration) (Handle, error) {
	return noopHandle{}, nil
}

func (noopHandle) Release(_ context.Context) error {
	return nil
}
