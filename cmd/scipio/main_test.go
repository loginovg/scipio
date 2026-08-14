package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_LoadEmbeddedSchemaSQL_ReturnEmbeddedSchemaSQL(t *testing.T) {
	t.Parallel()

	// given
	expectedStatement := "CREATE TABLE IF NOT EXISTS sagas"

	// when
	schemaSQL, err := loadEmbeddedSchemaSQL()

	// then
	require.NoError(t, err)
	require.NotEmpty(t, strings.TrimSpace(schemaSQL))
	require.Contains(t, schemaSQL, expectedStatement)
}

func Test_ShutdownRuntime_ShutdownIngressBeforeRunner(t *testing.T) {
	t.Parallel()

	// given
	order := make([]string, 0, 3)
	mu := sync.Mutex{}
	runnerDone := make(chan struct{})
	canceled := false

	grpcSrv := &recordingGRPCStopper{
		onGracefulStop: func() {
			mu.Lock()
			order = append(order, "grpc")
			mu.Unlock()
		},
	}
	httpSrv := &recordingHTTPShutdowner{
		onShutdown: func(ctx context.Context) error {
			require.NotNil(t, ctx)
			mu.Lock()
			order = append(order, "http")
			mu.Unlock()
			return nil
		},
	}
	runnerCancel := func() {
		mu.Lock()
		order = append(order, "runner")
		canceled = true
		mu.Unlock()
		close(runnerDone)
	}

	// when
	shutdownRuntime(grpcSrv, httpSrv, runnerCancel, runnerDone)

	// then
	mu.Lock()
	defer mu.Unlock()
	require.True(t, canceled)
	require.Equal(t, []string{"grpc", "http", "runner"}, order)
}

func Test_ShutdownRuntime_CancelRunnerWhenHTTPShutdownFails(t *testing.T) {
	t.Parallel()

	// given
	runnerDone := make(chan struct{})
	canceled := false
	mu := sync.Mutex{}

	grpcSrv := &recordingGRPCStopper{
		onGracefulStop: func() {},
	}
	httpSrv := &recordingHTTPShutdowner{
		onShutdown: func(context.Context) error {
			return errors.New("http shutdown failed")
		},
	}
	runnerCancel := func() {
		mu.Lock()
		canceled = true
		mu.Unlock()
		close(runnerDone)
	}

	// when
	shutdownRuntime(grpcSrv, httpSrv, runnerCancel, runnerDone)

	// then
	mu.Lock()
	defer mu.Unlock()
	require.True(t, canceled)
}

func Test_NewHTTPServer_ConfigureServerTimeouts(t *testing.T) {
	t.Parallel()

	// given
	handler := http.NewServeMux()

	// when
	server := newHTTPServer(8080, handler)

	// then
	require.NotNil(t, server)
	require.Equal(t, ":8080", server.Addr)
	require.Equal(t, handler, server.Handler)
	require.Equal(t, httpReadHeaderTimeout, server.ReadHeaderTimeout)
	require.Equal(t, httpReadTimeout, server.ReadTimeout)
	require.Equal(t, httpWriteTimeout, server.WriteTimeout)
	require.Equal(t, httpIdleTimeout, server.IdleTimeout)
}

type recordingGRPCStopper struct {
	onGracefulStop func()
	onStop         func()
}

func (s *recordingGRPCStopper) GracefulStop() {
	s.onGracefulStop()
}

func (s *recordingGRPCStopper) Stop() {
	if s.onStop != nil {
		s.onStop()
	}
}

type recordingHTTPShutdowner struct {
	onShutdown func(ctx context.Context) error
}

func (s *recordingHTTPShutdowner) Shutdown(ctx context.Context) error {
	return s.onShutdown(ctx)
}

func Test_ShutdownRuntimeWithTimeouts_ForceStopGRPCWhenGracefulStopExceedsTimeout(t *testing.T) {
	t.Parallel()

	// given
	runnerDone := make(chan struct{})
	runnerCanceled := false
	mu := sync.Mutex{}
	unblockGracefulStop := make(chan struct{})
	stopCalled := make(chan struct{})

	grpcSrv := &recordingGRPCStopper{
		onGracefulStop: func() {
			<-unblockGracefulStop
		},
		onStop: func() {
			select {
			case <-stopCalled:
			default:
				close(stopCalled)
			}
			close(unblockGracefulStop)
		},
	}
	httpSrv := &recordingHTTPShutdowner{
		onShutdown: func(context.Context) error {
			return nil
		},
	}
	runnerCancel := func() {
		mu.Lock()
		runnerCanceled = true
		mu.Unlock()
		close(runnerDone)
	}

	// when
	shutdownRuntimeWithTimeouts(grpcSrv, httpSrv, runnerCancel, runnerDone, time.Millisecond, time.Second, time.Second)

	// then
	select {
	case <-stopCalled:
	default:
		t.Fatal("expected grpc stop to be called")
	}

	mu.Lock()
	defer mu.Unlock()
	require.True(t, runnerCanceled)
}
