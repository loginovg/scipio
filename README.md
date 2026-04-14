# Scipio

Lightweight gRPC-based Saga orchestrator for distributed transactions. Scipio manages saga lifecycle, step execution, retries, compensations without heavy dependencies or a message broker.

## Architecture

Each step service implements two gRPC methods: `Execute` and `Compensate`. Scipio calls them in order and merges returned context patches. Scipio handles retries and rollback automatically.

## Quick Start

```bash
git clone https://github.com/loginovg/scipio
cd scipio
docker compose up -d # PostgreSQL + Redis
make migrate         # apply DB migrations
make run             # start Scipio on :9090 (gRPC) :8080 (REST)
```

## Go SDK

### 1. Implement a step service

```go
package main

import (
    "context"
    pb "github.com/loginovg/scipio/proto"
    "google.golang.org/grpc"
    "net"
)

type PaymentService struct {
    pb.UnimplementedStepServiceServer
}

func (s *PaymentService) Execute(ctx context.Context, req *pb.ExecuteRequest) (*pb.ExecuteResponse, error) {
    return &pb.ExecuteResponse{
        ContextPatch: []byte(`{"id": "123"}`),
    }, nil
}

func (s *PaymentService) Compensate(ctx context.Context, req *pb.CompensateRequest) (*pb.CompensateResponse, error) {
    return &pb.CompensateResponse{}, nil
}

func main() {
    listen, _ := net.listenten("tcp", ":9091")
    srv := grpc.NewServer()
    pb.RegisterStepServiceServer(srv, &PaymentService{})
    srv.Serve(listen)
}
```

### 2. Register a workflow

```go
package main

import (
    "github.com/loginovg/scipio/sdk"
)

func main() {
    client, _ := sdk.NewClient("localhost:9090")

    client.Register("order_flow", sdk.SagaDefinition{
        Steps: []sdk.Step{
            {Name: "reserve_order", Address: "orders:9090"},
            {Name: "make_payment", Address: "payments:9090"},
            {Name: "book_delivery", Address: "deliveries:9090"},
        },
    })
}
```

### 3. Start a saga

```go
sagaID, err := client.StartSaga(ctx, "order_flow", map[string]any{
    "user_id":  1,
    "amount":   2,
})
```

### 4. Poll saga status

```go
saga, err := client.GetSaga(ctx, sagaID)
fmt.Println(saga.Status)  // CREATED -> RUNNING -> COMPLETED / COMPENSATED / FAILED
fmt.Println(saga.Context) // Saga context after all steps
```

### 5. Cancel a saga

```go
err := client.CancelSaga(ctx, sagaID)
// triggers compensation chain from the last completed step
```

## Context propagation

Scipio passes the full saga context to each step. Steps return only new fields (`context_patch`), which are merged back:

```
Initial:  {user_id: 1, amount: 2}
After 1:  {user_id: 1, amount: 2, order_id: 3}
After 2:  {user_id: 1, amount: 2, order_id: 3, payment_id: "123"}
After 3:  {user_id: 1, amount: 2, order_id: 3, payment_id: "123", delivery_id: "1234"}
```

Step services decide which fields they need. Scipio just transfers data between them.

## Configuration

```yaml
# configs/config.yaml
server:
  grpc_port: 9090
  http_port: 8080

worker:
  pool_size: 10
  poll_interval: 500ms

saga:
  step_timeout:         30s
  compensation_timeout: 60s
  max_retries:          3
  retry_base_delay:     200ms
```

Environment variables:
```
PG_CONN=postgresql://scipio:scipio@postgres:5432/scipio
REDIS_CONN=redis://scipio:scipio@redis:6380/1
```

## Admin REST API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/sagas` | listent sagas, filter by `?status=` |
| GET | `/sagas/<id>` | Full saga state with step history |
| POST | `/sagas/<id>/cancel` | Trigger compensation |

## Testing

```bash
make tests      # unit tests
make testsuite  # functional tests (pytest, requires Docker)
```

## License

MIT
