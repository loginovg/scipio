-- name: CreateSaga :exec
INSERT INTO sagas (id, workflow, status, context, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: UpdateSaga :exec
UPDATE sagas
SET workflow = $2,
    status = $3,
    context = $4,
    updated_at = $5
WHERE id = $1;

-- name: GetSaga :one
SELECT id, workflow, status, context, created_at, updated_at
FROM sagas
WHERE id = $1;

-- name: GetSagaForUpdate :one
SELECT id, workflow, status, context, created_at, updated_at
FROM sagas
WHERE id = $1
FOR UPDATE;

-- name: ListSagas :many
SELECT id, workflow, status, context, created_at, updated_at
FROM sagas
ORDER BY created_at ASC, id ASC
LIMIT $1 OFFSET $2;

-- name: ListSagasByStatus :many
SELECT id, workflow, status, context, created_at, updated_at
FROM sagas
WHERE status = $1
ORDER BY created_at ASC, id ASC
LIMIT $2 OFFSET $3;

-- name: DeleteSagaStepsFromIndex :exec
DELETE FROM saga_steps
WHERE saga_id = $1
  AND step_index >= $2;

-- name: UpsertSagaStep :exec
INSERT INTO saga_steps (
    saga_id,
    step_index,
    name,
    grpc_target,
    status,
    attempt,
    started_at,
    finished_at,
    error,
    updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
ON CONFLICT (saga_id, step_index) DO UPDATE
SET
    name = EXCLUDED.name,
    grpc_target = EXCLUDED.grpc_target,
    status = EXCLUDED.status,
    attempt = EXCLUDED.attempt,
    started_at = EXCLUDED.started_at,
    finished_at = EXCLUDED.finished_at,
    error = EXCLUDED.error,
    updated_at = CASE
        WHEN saga_steps.name IS DISTINCT FROM EXCLUDED.name
            OR saga_steps.grpc_target IS DISTINCT FROM EXCLUDED.grpc_target
            OR saga_steps.status IS DISTINCT FROM EXCLUDED.status
            OR saga_steps.attempt IS DISTINCT FROM EXCLUDED.attempt
            OR saga_steps.started_at IS DISTINCT FROM EXCLUDED.started_at
            OR saga_steps.finished_at IS DISTINCT FROM EXCLUDED.finished_at
            OR saga_steps.error IS DISTINCT FROM EXCLUDED.error
        THEN NOW()
        ELSE saga_steps.updated_at
    END;

-- name: GetSagaSteps :many
SELECT name, grpc_target, status, attempt, started_at, finished_at, error
FROM saga_steps
WHERE saga_id = $1
ORDER BY step_index ASC;

-- name: GetSagaStepsForUpdate :many
SELECT name, grpc_target, status, attempt, started_at, finished_at, error
FROM saga_steps
WHERE saga_id = $1
ORDER BY step_index ASC
FOR UPDATE;

-- name: ClaimNextStep :one
WITH candidate AS (
    SELECT
        ss.saga_id,
        ss.step_index,
        CASE
            WHEN s.status = 'CANCELING' THEN 'COMPENSATING'
            ELSE 'RUNNING'
        END AS next_status
    FROM saga_steps ss
    INNER JOIN sagas s ON s.id = ss.saga_id
    WHERE (
        s.status IN ('CREATED', 'RUNNING')
        AND (
            ss.status = 'PENDING'
            OR (ss.status = 'RUNNING' AND ss.started_at <= $1)
        )
        AND NOT EXISTS (
            SELECT 1
            FROM saga_steps prev
            WHERE prev.saga_id = ss.saga_id
              AND prev.step_index < ss.step_index
              AND prev.status <> 'COMPLETED'
        )
    ) OR (
        s.status = 'CANCELING'
        AND (
            ss.status IN ('COMPLETED', 'RUNNING')
            OR (ss.status = 'COMPENSATING' AND ss.started_at <= $1)
        )
        AND NOT EXISTS (
            SELECT 1
            FROM saga_steps next
            WHERE next.saga_id = ss.saga_id
              AND next.step_index > ss.step_index
              AND next.status IN ('COMPLETED', 'RUNNING', 'COMPENSATING')
        )
    )
    ORDER BY ss.updated_at ASC, ss.id ASC
    FOR UPDATE OF ss SKIP LOCKED
    LIMIT 1
)
UPDATE saga_steps ss
SET
    status = candidate.next_status,
    attempt = ss.attempt + 1,
    started_at = COALESCE(ss.started_at, NOW()),
    finished_at = NULL,
    error = NULL,
    updated_at = NOW()
FROM candidate
WHERE ss.saga_id = candidate.saga_id
  AND ss.step_index = candidate.step_index
RETURNING ss.saga_id, ss.step_index, ss.name, ss.attempt;
