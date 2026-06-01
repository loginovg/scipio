package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
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
	sqlschema "scipio/sql"

	"google.golang.org/grpc"
)

const shutdownTimeout = 5 * time.Second
const httpReadHeaderTimeout = 5 * time.Second
const httpReadTimeout = 30 * time.Second
const httpWriteTimeout = 30 * time.Second
const httpIdleTimeout = 2 * time.Minute

type grpcGracefulStopper interface {
	GracefulStop()
	Stop()
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

	schemaSQL, err := loadEmbeddedSchemaSQL()
	if err != nil {
		return fmt.Errorf("failed to load embedded schema: %w", err)
	}

	if err := postgresStore.Migrate(ctx, schemaSQL); err != nil {
		return fmt.Errorf("failed to apply embedded schema: %w", err)
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
	shutdownRuntimeWithTimeouts(grpcSrv, httpSrv, runnerCancel, runnerDone, shutdownTimeout, shutdownTimeout, shutdownTimeout)
}

func shutdownRuntimeWithTimeouts(
	grpcSrv grpcGracefulStopper,
	httpSrv httpShutdowner,
	runnerCancel context.CancelFunc,
	runnerDone <-chan struct{},
	grpcTimeout time.Duration,
	httpTimeout time.Duration,
	runnerTimeout time.Duration,
) {
	grpcCtx, cancelGRPC := context.WithTimeout(context.Background(), grpcTimeout)
	grpcDone := make(chan struct{})
	go func() {
		defer close(grpcDone)
		grpcSrv.GracefulStop()
	}()

	select {
	case <-grpcDone:
	case <-grpcCtx.Done():
		slog.Warn("grpc graceful shutdown timed out")
		grpcSrv.Stop()
	}
	cancelGRPC()

	httpCtx, cancelHTTP := context.WithTimeout(context.Background(), httpTimeout)
	if shutdownErr := httpSrv.Shutdown(httpCtx); shutdownErr != nil {
		slog.Error("failed to shutdown http server", "error", shutdownErr)
	}
	cancelHTTP()

	runnerCancel()
	runnerCtx, cancelRunner := context.WithTimeout(context.Background(), runnerTimeout)
	select {
	case <-runnerDone:
	case <-runnerCtx.Done():
		slog.Warn("step runner shutdown timed out")
	}
	cancelRunner()
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

func loadEmbeddedSchemaSQL() (string, error) {
	schemaSQL := sqlschema.SagaSchema()
	if strings.TrimSpace(schemaSQL) == "" {
		return "", errors.New("embedded schema is empty")
	}

	return schemaSQL, nil
}
