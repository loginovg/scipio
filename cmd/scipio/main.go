package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	sagav1 "scipio/gen/proto"
	"scipio/internal/config"
	"scipio/internal/lock"
	"scipio/internal/service"
	"scipio/internal/store"
	"scipio/internal/transport/grpcserver"
	"scipio/internal/transport/httpserver"

	"google.golang.org/grpc"
)

const shutdownTimeout = 5 * time.Second
const httpReadHeaderTimeout = 5 * time.Second
const httpReadTimeout = 30 * time.Second
const httpWriteTimeout = 30 * time.Second
const httpIdleTimeout = 2 * time.Minute

type grpcGracefulStopper interface {
	GracefulStop()
}

type httpShutdowner interface {
	Shutdown(ctx context.Context) error
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	if err := run(); err != nil {
		slog.Error("failed to run scipio", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	postgresStore, err := store.NewPostgres(ctx, cfg.PostgresConnectionString)
	if err != nil {
		return fmt.Errorf("failed to initialize postgres store: %w", err)
	}
	defer postgresStore.Close()

	migrationFiles, err := loadMigrationFiles(cfg.MigrationsPath)
	if err != nil {
		return fmt.Errorf("failed to load migrations from %q: %w", cfg.MigrationsPath, err)
	}

	for _, migrationFile := range migrationFiles {
		migrationSQL, readErr := os.ReadFile(migrationFile)
		if readErr != nil {
			return fmt.Errorf("failed to read migration file %q: %w", migrationFile, readErr)
		}

		if migrateErr := postgresStore.Migrate(ctx, string(migrationSQL)); migrateErr != nil {
			return fmt.Errorf("failed to apply migration %q: %w", migrationFile, migrateErr)
		}
	}

	redisLocker, err := lock.NewRedisFromURL(cfg.RedisConnectionString, "scipio:lock:saga:", cfg.LockRetryInterval)
	if err != nil {
		return fmt.Errorf("failed to initialize redis lock: %w", err)
	}
	defer func() {
		if closeErr := redisLocker.Close(); closeErr != nil {
			slog.Error("failed to close redis locker", "error", closeErr)
		}
	}()

	sagaService, err := service.New(postgresStore, redisLocker, cfg.LockTTL)
	if err != nil {
		return fmt.Errorf("failed to initialize saga service: %w", err)
	}

	stepRunner, err := service.NewStepRunner(
		postgresStore,
		redisLocker,
		cfg.LockTTL,
		cfg.StepWorkers,
		cfg.StepPollInterval,
		cfg.StepStaleTimeout,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to initialize step runner: %w", err)
	}

	runnerCtx, runnerCancel := context.WithCancel(context.Background())
	defer runnerCancel()
	runnerDone := make(chan struct{})
	go func() {
		defer close(runnerDone)
		if runErr := stepRunner.Run(runnerCtx); runErr != nil {
			slog.Error("step runner exited", "error", runErr)
		}
	}()

	grpcListen, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("failed to listen for grpc: %w", err)
	}

	grpcSrv := grpc.NewServer()
	sagav1.RegisterSagaServiceServer(grpcSrv, grpcserver.New(sagaService))

	httpHandler, err := httpserver.New(sagaService).Handler()
	if err != nil {
		return fmt.Errorf("failed to configure http server: %w", err)
	}

	httpSrv := newHTTPServer(cfg.HTTPPort, httpHandler)

	errCh := make(chan error, 2)
	go func() {
		slog.Info("grpc server started", "port", cfg.GRPCPort)
		errCh <- grpcSrv.Serve(grpcListen)
	}()

	go func() {
		slog.Info("http server started", "port", cfg.HTTPPort)
		errCh <- httpSrv.ListenAndServe()
	}()

	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case <-signalCtx.Done():
		slog.Info("shutdown signal received")
	case runErr := <-errCh:
		if runErr != nil && !errors.Is(runErr, http.ErrServerClosed) {
			slog.Error("server exited", "error", runErr)
		}
	}

	shutdownRuntime(grpcSrv, httpSrv, runnerCancel, runnerDone)

	return nil
}

func shutdownRuntime(grpcSrv grpcGracefulStopper, httpSrv httpShutdowner, runnerCancel context.CancelFunc, runnerDone <-chan struct{}) {
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	grpcSrv.GracefulStop()
	if shutdownErr := httpSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		slog.Error("failed to shutdown http server", "error", shutdownErr)
	}

	runnerCancel()
	select {
	case <-runnerDone:
	case <-shutdownCtx.Done():
		slog.Warn("step runner shutdown timed out")
	}
}

func newHTTPServer(port int, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           handler,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
}

func loadMigrationFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if !info.IsDir() {
		if strings.EqualFold(filepath.Ext(path), ".sql") {
			return []string{path}, nil
		}
		return nil, fmt.Errorf("migration path is not a sql file: %s", path)
	}

	files := make([]string, 0)
	walkErr := filepath.WalkDir(path, func(currentPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if entry.IsDir() {
			return nil
		}

		if !strings.EqualFold(filepath.Ext(entry.Name()), ".sql") {
			return nil
		}

		files = append(files, currentPath)
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}

	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no sql migrations found in path: %s", path)
	}

	return files, nil
}
