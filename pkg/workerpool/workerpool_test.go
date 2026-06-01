package workerpool

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShouldProcessSubmittedTasksWhenPoolIsRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// given
		p := New[int, int](2, func(_ context.Context, task int) (int, error) {
			return task * 2, nil
		})

		// when
		require.NoError(t, p.Submit(context.Background(), 1))
		require.NoError(t, p.Submit(context.Background(), 2))
		require.NoError(t, p.Submit(context.Background(), 3))

		gotCh := make(chan []int, 1)
		go func() {
			var got []int
			for res := range p.Results() {
				require.NoError(t, res.Err)
				got = append(got, res.Value)
			}
			gotCh <- got
		}()

		require.NoError(t, p.Shutdown(context.Background()))
		got := <-gotCh

		// then
		require.ElementsMatch(t, []int{2, 4, 6}, got)
	})
}

func TestShouldReturnContextDeadlineExceededWhenShutdownDeadlineExpiresBeforeWorkersStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		block := make(chan struct{})
		p := New[int, int](1, func(ctx context.Context, task int) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-block:
				return task, nil
			}
		})

		require.NoError(t, p.Submit(context.Background(), 1))

		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- p.Shutdown(ctx)
		}()

		synctest.Wait()

		err := <-errCh
		require.ErrorIs(t, err, context.DeadlineExceeded)

		close(block)
		require.NoError(t, p.Shutdown(context.Background()))
		for range p.Results() {
		}
	})
}

func TestShouldReturnContextCanceledWhenSubmitContextIsCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// given
		p := New[int, int](0, func(_ context.Context, task int) (int, error) {
			return task, nil
		})

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// when
		err := p.Submit(ctx, 1)

		// then
		require.ErrorIs(t, err, context.Canceled)
		require.NoError(t, p.Shutdown(context.Background()))
	})
}

func TestShouldReturnErrPoolClosedWhenTaskSubmittedAfterShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// given
		p := New[int, int](1, func(_ context.Context, task int) (int, error) {
			return task, nil
		})

		require.NoError(t, p.Shutdown(context.Background()))

		// when
		err := p.Submit(context.Background(), 1)

		// then
		require.ErrorIs(t, err, ErrPoolClosed)
	})
}

func TestShouldReturnErrPoolClosedWhenBlockedSubmitAndPoolShutsDown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// given
		p := New[int, int](0, func(_ context.Context, task int) (int, error) {
			return task, nil
		})

		errCh := make(chan error, 1)
		go func() {
			errCh <- p.Submit(context.Background(), 1)
		}()

		// when
		synctest.Wait()
		require.NoError(t, p.Shutdown(context.Background()))
		synctest.Wait()

		err := <-errCh

		// then
		require.ErrorIs(t, err, ErrPoolClosed)
	})
}

func TestShouldSucceedWhenShutdownCalledConcurrentlyMultipleTimes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// given
		p := New[int, int](1, func(_ context.Context, task int) (int, error) {
			return task, nil
		})

		errs := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Add(2)

		// when
		go func() {
			defer wg.Done()
			errs <- p.Shutdown(context.Background())
		}()
		go func() {
			defer wg.Done()
			errs <- p.Shutdown(context.Background())
		}()

		wg.Wait()
		close(errs)

		// then
		for err := range errs {
			require.NoError(t, err)
		}
	})
}
