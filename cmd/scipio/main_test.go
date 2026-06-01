package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldReturnSortedSqlFilesWhenMigrationPathIsDirectory(t *testing.T) {
	t.Parallel()

	// given
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "a"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "b"), 0o755))

	firstFile := filepath.Join(tempDir, "a", "V001_init.sql")
	secondFile := filepath.Join(tempDir, "b", "V002_next.sql")
	ignoredFile := filepath.Join(tempDir, "b", "README.txt")

	require.NoError(t, os.WriteFile(firstFile, []byte("SELECT 1;"), 0o644))
	require.NoError(t, os.WriteFile(secondFile, []byte("SELECT 2;"), 0o644))
	require.NoError(t, os.WriteFile(ignoredFile, []byte("ignore"), 0o644))

	// when
	files, err := loadMigrationFiles(tempDir)

	// then
	require.NoError(t, err)
	require.Equal(t, []string{firstFile, secondFile}, files)
}

func TestShouldReturnSingleFileWhenMigrationPathIsSqlFile(t *testing.T) {
	t.Parallel()

	// given
	tempDir := t.TempDir()
	migrationFile := filepath.Join(tempDir, "V001_init.sql")
	require.NoError(t, os.WriteFile(migrationFile, []byte("SELECT 1;"), 0o644))

	// when
	files, err := loadMigrationFiles(migrationFile)

	// then
	require.NoError(t, err)
	require.Equal(t, []string{migrationFile}, files)
}

func TestShouldReturnErrorWhenMigrationDirectoryHasNoSqlFiles(t *testing.T) {
	t.Parallel()

	// given
	tempDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "note.txt"), []byte("x"), 0o644))

	// when
	files, err := loadMigrationFiles(tempDir)

	// then
	require.Error(t, err)
	require.Nil(t, files)
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
}

func (s *recordingGRPCStopper) GracefulStop() {
	s.onGracefulStop()
}

type recordingHTTPShutdowner struct {
	onShutdown func(ctx context.Context) error
}

func (s *recordingHTTPShutdowner) Shutdown(ctx context.Context) error {
	return s.onShutdown(ctx)
}
