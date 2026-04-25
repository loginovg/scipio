package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrInvalidTTL = errors.New("lock ttl must be positive")
var ErrInvalidRetryInterval = errors.New("lock retry interval must be positive")
var ErrInvalidRedisURL = errors.New("redis connection url must not be empty")

type redisClient interface {
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) (bool, error)
	EvalInt64(ctx context.Context, script string, keys []string, args ...any) (int64, error)
}

type goRedisClient struct {
	client *redis.Client
}

type Redis struct {
	client        redisClient
	prefix        string
	retryInterval time.Duration
	closeFn       func() error
}

type redisHandle struct {
	client      redisClient
	key         string
	token       string
	ttl         time.Duration
	renewCancel context.CancelFunc
	renewDone   chan struct{}
	releaseOnce sync.Once
	releaseErr  error
}

const unlockScript = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"
const renewScript = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end"

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
	return &Redis{
		client:        goRedisClient{client: client},
		prefix:        normalizePrefix(prefix),
		retryInterval: normalizeRetryInterval(retryInterval),
		closeFn:       client.Close,
	}, nil
}

func NewRedis(client redisClient, prefix string, retryInterval time.Duration) (*Redis, error) {
	if client == nil {
		return nil, errors.New("redis client must not be nil")
	}

	if retryInterval <= 0 {
		return nil, ErrInvalidRetryInterval
	}

	return &Redis{
		client:        client,
		prefix:        normalizePrefix(prefix),
		retryInterval: retryInterval,
	}, nil
}

func (r *Redis) Acquire(ctx context.Context, key string, ttl time.Duration) (Handle, error) {
	if ttl <= 0 {
		return nil, ErrInvalidTTL
	}

	token, err := randomToken()
	if err != nil {
		return nil, err
	}

	fullKey := r.prefix + key
	ticker := time.NewTicker(r.retryInterval)
	defer ticker.Stop()

	for {
		acquired, acquireErr := r.client.SetNX(ctx, fullKey, token, ttl)
		if acquireErr != nil {
			return nil, acquireErr
		}

		if acquired {
			handle := &redisHandle{
				client: r.client,
				key:    fullKey,
				token:  token,
				ttl:    ttl,
			}
			handle.startRenewal()
			return handle, nil
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
		h.stopRenewal()
		_, err := h.client.EvalInt64(ctx, unlockScript, []string{h.key}, h.token)
		if err != nil {
			h.releaseErr = err
		}
	})

	return h.releaseErr
}

func (g goRedisClient) SetNX(ctx context.Context, key string, value any, expiration time.Duration) (bool, error) {
	return g.client.SetNX(ctx, key, value, expiration).Result()
}

func (g goRedisClient) EvalInt64(ctx context.Context, script string, keys []string, args ...any) (int64, error) {
	return g.client.Eval(ctx, script, keys, args...).Int64()
}

func normalizePrefix(prefix string) string {
	trimmed := strings.TrimSpace(prefix)
	if trimmed == "" {
		return "scipio:lock:saga:"
	}

	return trimmed
}

func normalizeRetryInterval(retryInterval time.Duration) time.Duration {
	if retryInterval <= 0 {
		return 25 * time.Millisecond
	}

	return retryInterval
}

func (h *redisHandle) startRenewal() {
	interval := renewalInterval(h.ttl)
	if interval <= 0 {
		return
	}

	renewCtx, renewCancel := context.WithCancel(context.Background())
	h.renewCancel = renewCancel
	h.renewDone = make(chan struct{})
	go func() {
		defer close(h.renewDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				ttlMillis := h.ttl.Milliseconds()
				if ttlMillis <= 0 {
					ttlMillis = 1
				}

				callCtx, cancel := context.WithTimeout(context.Background(), renewalTimeout(h.ttl))
				result, err := h.client.EvalInt64(callCtx, renewScript, []string{h.key}, h.token, ttlMillis)
				cancel()
				if err != nil {
					continue
				}

				if result == 0 {
					return
				}
			}
		}
	}()
}

func (h *redisHandle) stopRenewal() {
	if h.renewCancel != nil {
		h.renewCancel()
	}

	if h.renewDone != nil {
		<-h.renewDone
	}
}

func renewalInterval(ttl time.Duration) time.Duration {
	interval := ttl / 2
	if interval <= 0 {
		return time.Millisecond
	}

	return interval
}

func renewalTimeout(ttl time.Duration) time.Duration {
	timeout := ttl / 3
	if timeout <= 0 {
		return time.Millisecond
	}

	if timeout > time.Second {
		return time.Second
	}

	return timeout
}

func randomToken() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}

	return hex.EncodeToString(buffer), nil
}
