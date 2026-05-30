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

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to parse config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	postgresStore, err := store.NewPostgres(ctx, cfg.PostgresConnectionString)
	if err != nil {
		logger.Error("failed to initialize postgres store", "error", err)
		os.Exit(1)
	}
	defer postgresStore.Close()

	migrationFiles, err := loadMigrationFiles(cfg.MigrationsPath)
	if err != nil {
		logger.Error("failed to load migrations", "path", cfg.MigrationsPath, "error", err)
		os.Exit(1)
	}

	for _, migrationFile := range migrationFiles {
		migrationSQL, readErr := os.ReadFile(migrationFile)
		if readErr != nil {
			logger.Error("failed to read migration file", "path", migrationFile, "error", readErr)
			os.Exit(1)
		}

		if migrateErr := postgresStore.Migrate(ctx, string(migrationSQL)); migrateErr != nil {
			logger.Error("failed to apply migration", "path", migrationFile, "error", migrateErr)
			os.Exit(1)
		}
	}

	redisLocker, err := lock.NewRedisFromURL(cfg.RedisConnectionString, "scipio:lock:saga:", cfg.LockRetryInterval)
	if err != nil {
		logger.Error("failed to initialize redis lock", "error", err)
		os.Exit(1)
	}
	defer func() {
		if closeErr := redisLocker.Close(); closeErr != nil {
			logger.Error("failed to close redis locker", "error", closeErr)
		}
	}()

	sagaService := service.New(postgresStore, redisLocker, cfg.LockTTL, logger)
	stepRunner := service.NewStepRunner(
		postgresStore,
		redisLocker,
		cfg.LockTTL,
		cfg.StepWorkers,
		cfg.StepPollInterval,
		cfg.StepStaleTimeout,
		logger,
	)

	runnerCtx, runnerCancel := context.WithCancel(context.Background())
	runnerDone := make(chan struct{})
	go func() {
		defer close(runnerDone)
		if runErr := stepRunner.Run(runnerCtx); runErr != nil {
			logger.Error("step runner exited", "error", runErr)
		}
	}()

	grpcListen, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		logger.Error("failed to listen for grpc", "error", err)
		os.Exit(1)
	}

	grpcSrv := grpc.NewServer()
	sagav1.RegisterSagaServiceServer(grpcSrv, grpcserver.New(sagaService))

	httpHandler, err := httpserver.New(sagaService).Handler()
	if err != nil {
		logger.Error("failed to configure http server", "error", err)
		os.Exit(1)
	}

	httpSrv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:           httpHandler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("grpc server started", "port", cfg.GRPCPort)
		errCh <- grpcSrv.Serve(grpcListen)
	}()

	go func() {
		logger.Info("http server started", "port", cfg.HTTPPort)
		errCh <- httpSrv.ListenAndServe()
	}()

	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case <-signalCtx.Done():
		logger.Info("shutdown signal received")
	case runErr := <-errCh:
		if runErr != nil && !errors.Is(runErr, http.ErrServerClosed) {
			logger.Error("server exited", "error", runErr)
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	runnerCancel()
	select {
	case <-runnerDone:
	case <-shutdownCtx.Done():
		logger.Warn("step runner shutdown timed out")
	}

	grpcSrv.GracefulStop()
	if shutdownErr := httpSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		logger.Error("failed to shutdown http server", "error", shutdownErr)
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
