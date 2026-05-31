package lock

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

var ErrInvalidTTL = errors.New("lock ttl must be positive")
var ErrInvalidRetryInterval = errors.New("lock retry interval must be positive")
var ErrInvalidRedisURL = errors.New("redis connection url must not be empty")
var ErrInvalidPrefix = errors.New("lock prefix must not be empty")

type redisMutex interface {
	LockContext(ctx context.Context) error
	UnlockContext(ctx context.Context) (bool, error)
	ExtendContext(ctx context.Context) (bool, error)
}

type Redis struct {
	newMutex      func(name string, options ...redsync.Option) redisMutex
	prefix        string
	retryInterval time.Duration
	closeFn       func() error
}

type redisHandle struct {
	mutex       redisMutex
	releaseOnce sync.Once
	releaseErr  error
}

func NewRedisFromURL(redisURL string, prefix string, retryInterval time.Duration) (*Redis, error) {
	trimmedURL := strings.TrimSpace(redisURL)
	if trimmedURL == "" {
		return nil, ErrInvalidRedisURL
	}

	options, err := redis.ParseURL(trimmedURL)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(options)
	return newRedis(client, prefix, retryInterval, client.Close)
}

func NewRedis(client redis.UniversalClient, prefix string, retryInterval time.Duration) (*Redis, error) {
	if client == nil {
		return nil, errors.New("redis client must not be nil")
	}

	return newRedis(client, prefix, retryInterval, nil)
}

func newRedis(client redis.UniversalClient, prefix string, retryInterval time.Duration, closeFn func() error) (*Redis, error) {
	if strings.TrimSpace(prefix) == "" {
		return nil, ErrInvalidPrefix
	}

	if retryInterval <= 0 {
		return nil, ErrInvalidRetryInterval
	}

	pool := redsyncgoredis.NewPool(client)
	syncer := redsync.New(pool)

	return &Redis{
		newMutex: func(name string, options ...redsync.Option) redisMutex {
			return syncer.NewMutex(name, options...)
		},
		prefix:        prefix,
		retryInterval: retryInterval,
		closeFn:       closeFn,
	}, nil
}

func (r *Redis) Acquire(ctx context.Context, key string, ttl time.Duration) (Handle, error) {
	if ttl <= 0 {
		return nil, ErrInvalidTTL
	}

	fullKey := r.prefix + key
	mutex := r.newMutex(
		fullKey,
		redsync.WithExpiry(ttl),
	)

	ticker := time.NewTicker(r.retryInterval)
	defer ticker.Stop()

	for {
		if err := mutex.LockContext(ctx); err == nil {
			return &redisHandle{mutex: mutex}, nil
		} else {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			if !isRaceError(err) {
				return nil, err
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *Redis) Close() error {
	if r.closeFn == nil {
		return nil
	}

	return r.closeFn()
}

func (h *redisHandle) Release(ctx context.Context) error {
	h.releaseOnce.Do(func() {
		_, err := h.mutex.UnlockContext(ctx)
		if errors.Is(err, redsync.ErrLockAlreadyExpired) {
			return
		}
		if err != nil {
			h.releaseErr = err
		}
	})

	return h.releaseErr
}

func (h *redisHandle) Extend(ctx context.Context) error {
	extended, err := h.mutex.ExtendContext(ctx)
	if err != nil {
		return err
	}
	if !extended {
		return redsync.ErrExtendFailed
	}

	return nil
}

func isRaceError(err error) bool {
	if errors.Is(err, redsync.ErrFailed) {
		return true
	}

	var taken *redsync.ErrTaken
	if errors.As(err, &taken) {
		return true
	}

	var nodeTaken *redsync.ErrNodeTaken
	return errors.As(err, &nodeTaken)
}
