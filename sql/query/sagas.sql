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

-- name: DeleteSagaSteps :exec
DELETE FROM saga_steps
WHERE saga_id = $1;

-- name: InsertSagaStep :exec
INSERT INTO saga_steps (
    saga_id,
    step_index,
    name,
    status,
    attempt,
    started_at,
    finished_at,
    error,
    created_at,
    updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW());

-- name: GetSagaSteps :many
SELECT name, status, attempt, started_at, finished_at, error
FROM saga_steps
WHERE saga_id = $1
ORDER BY step_index ASC;

-- name: GetSagaStepsForUpdate :many
SELECT name, status, attempt, started_at, finished_at, error
FROM saga_steps
WHERE saga_id = $1
ORDER BY step_index ASC
FOR UPDATE;

-- name: ClaimNextStep :one
WITH candidate AS (
    SELECT ss.saga_id, ss.step_index
    FROM saga_steps ss
    INNER JOIN sagas s ON s.id = ss.saga_id
    WHERE s.status IN ('CREATED', 'RUNNING')
      AND (
        ss.status = 'PENDING'
        OR (ss.status = 'RUNNING' AND ss.updated_at <= $1)
      )
    ORDER BY ss.updated_at ASC, ss.id ASC
    FOR UPDATE OF ss SKIP LOCKED
    LIMIT 1
)
UPDATE saga_steps ss
SET
    status = 'RUNNING',
    attempt = ss.attempt + 1,
    started_at = COALESCE(ss.started_at, NOW()),
    finished_at = NULL,
    error = NULL,
    updated_at = NOW()
FROM candidate
WHERE ss.saga_id = candidate.saga_id
  AND ss.step_index = candidate.step_index
RETURNING ss.saga_id, ss.step_index, ss.name, ss.attempt;
