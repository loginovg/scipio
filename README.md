# Scipio

Scipio is a lightweight Saga orchestrator with gRPC and HTTP interfaces.

## Features

- Start, query, list, and cancel sagas
- Deterministic saga state transitions
- PostgreSQL-backed saga persistence via `pgx`
- Redis distributed saga locking via `redis-go`
- gRPC API from `api/proto/saga.proto`
- HTTP API from `api/openapi/saga.yaml`
- Multi-instance safe coordination by saga ID

## Run

```bash
make run
```

Environment variables:

- `SCIPIO_GRPC_PORT`
- `SCIPIO_HTTP_PORT`
- `SCIPIO_STEP_WORKERS`
- `SCIPIO_STEP_POLL_INTERVAL`
- `SCIPIO_STEP_STALE_TIMEOUT`
- `PG_CONN`
- `REDIS_CONN`
- `SCIPIO_LOCK_TTL`
- `SCIPIO_LOCK_RETRY_INTERVAL`
- `SCIPIO_MIGRATIONS_PATH`

## API

gRPC service:

- `StartSaga`
- `GetSaga`
- `CancelSaga`

HTTP endpoints:

- `POST /sagas`
- `GET /sagas`
- `GET /sagas/{id}`
- `POST /sagas/{id}/cancel`
- `GET /healthz`

## Examples

- gRPC example: `examples/grpc/main.go`
- HTTP example: `examples/http/main.go`

Run examples after starting Scipio:

```bash
go run ./examples/grpc
```

```bash
go run ./examples/http
```

## Testing

Unit tests:

```bash
make tests
```

Lint:

```bash
make lint
```

Functional tests with pytest and testcontainers:

```bash
make testsuite-deps
make testsuite
```

## License

MIT
