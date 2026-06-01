package workerpool

import (
	"context"
	"errors"
	"sync"
)

var ErrPoolClosed = errors.New("worker pool is closed")

type Result[R any] struct {
	Value R
	Err   error
}

type Pool[T any, R any] struct {
	handler func(ctx context.Context, task T) (R, error)
	ctx     context.Context
	cancel  context.CancelFunc

	tasks   chan T
	results chan Result[R]
	done    chan struct{}
	stopped chan struct{}

	closeOnce sync.Once
	wg        sync.WaitGroup
}

func New[T any, R any](workers int, handler func(context.Context, T) (R, error)) *Pool[T, R] {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool[T, R]{
		handler: handler,
		ctx:     ctx,
		cancel:  cancel,
		tasks:   make(chan T, workers),
		results: make(chan Result[R], workers),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}

	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}

	return p
}

func (p *Pool[T, R]) worker() {
	defer p.wg.Done()
	for task := range p.tasks {
		v, err := p.handler(p.ctx, task)
		p.results <- Result[R]{Value: v, Err: err}
	}
}

func (p *Pool[T, R]) Submit(ctx context.Context, task T) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			err = ErrPoolClosed
		}
	}()

	select {
	case <-p.done:
		return ErrPoolClosed
	default:
	}

	select {
	case <-p.done:
		return ErrPoolClosed
	case <-ctx.Done():
		return ctx.Err()
	case p.tasks <- task:
		return nil
	}
}

func (p *Pool[T, R]) Results() <-chan Result[R] {
	return p.results
}

func (p *Pool[T, R]) Shutdown(ctx context.Context) error {
	p.closeOnce.Do(func() {
		close(p.done)
		close(p.tasks)

		go func() {
			p.wg.Wait()
			close(p.results)
			p.cancel()
			close(p.stopped)
		}()
	})

	select {
	case <-p.stopped:
		return nil
	case <-ctx.Done():
		p.cancel()
		return ctx.Err()
	}
}
