BEGIN;

ALTER TABLE saga_steps
ADD COLUMN IF NOT EXISTS grpc_target TEXT NOT NULL DEFAULT '';

UPDATE saga_steps
SET grpc_target = name
WHERE grpc_target = '';

ALTER TABLE saga_steps
ALTER COLUMN grpc_target DROP DEFAULT;

COMMIT;
