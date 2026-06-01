package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShouldReturnSchemaSQLWhenSchemaPathIsSQLFile(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	schemaFile := filepath.Join(tempDir, "schema.sql")
	require.NoError(t, os.WriteFile(schemaFile, []byte("SELECT 1;"), 0o644))

	schemaSQL, err := loadSchemaSQL(schemaFile)

	require.NoError(t, err)
	require.Equal(t, "SELECT 1;", schemaSQL)
}

func TestShouldReturnErrorWhenSchemaPathIsDirectory(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	schemaSQL, err := loadSchemaSQL(tempDir)

	require.Error(t, err)
	require.Equal(t, "", schemaSQL)
}

func TestShouldReturnErrorWhenSchemaPathIsNotSQLFile(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	schemaFile := filepath.Join(tempDir, "schema.txt")
	require.NoError(t, os.WriteFile(schemaFile, []byte("SELECT 1;"), 0o644))

	schemaSQL, err := loadSchemaSQL(schemaFile)

	require.Error(t, err)
	require.Equal(t, "", schemaSQL)
}

func TestShouldShutdownIngressBeforeRunnerWhenRuntimeStops(t *testing.T) {
	t.Parallel()

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

	shutdownRuntime(grpcSrv, httpSrv, runnerCancel, runnerDone)

	mu.Lock()
	defer mu.Unlock()
	require.True(t, canceled)
	require.Equal(t, []string{"grpc", "http", "runner"}, order)
}

func TestShouldCancelRunnerWhenHTTPShutdownFails(t *testing.T) {
	t.Parallel()

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

	shutdownRuntime(grpcSrv, httpSrv, runnerCancel, runnerDone)

	mu.Lock()
	defer mu.Unlock()
	require.True(t, canceled)
}

func TestShouldConfigureHTTPServerTimeoutsWhenRuntimeBuildsHTTPServer(t *testing.T) {
	t.Parallel()

	handler := http.NewServeMux()

	server := newHTTPServer(8080, handler)

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

func TestShouldForceStopGRPCWhenGracefulStopExceedsTimeout(t *testing.T) {
	t.Parallel()

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

	shutdownRuntimeWithTimeouts(grpcSrv, httpSrv, runnerCancel, runnerDone, time.Millisecond, time.Second, time.Second)

	select {
	case <-stopCalled:
	default:
		t.Fatal("expected grpc stop to be called")
	}

	mu.Lock()
	defer mu.Unlock()
	require.True(t, runnerCanceled)
}
