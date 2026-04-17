CREATE TABLE IF NOT EXISTS sagas (
    id TEXT PRIMARY KEY,
    workflow TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN (
            'CREATED',
            'RUNNING',
            'COMPLETED',
            'CANCELING',
            'COMPENSATED',
            'FAILED'
        )
    ),
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS saga_steps (
    id BIGSERIAL PRIMARY KEY,
    saga_id TEXT NOT NULL REFERENCES sagas(id) ON DELETE CASCADE,
    step_index INTEGER NOT NULL CHECK (step_index >= 0),
    name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN (
            'PENDING',
            'RUNNING',
            'COMPLETED',
            'COMPENSATING',
            'COMPENSATED',
            'FAILED'
        )
    ),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (saga_id, step_index)
);

CREATE INDEX IF NOT EXISTS idx_sagas_status ON sagas(status);
CREATE INDEX IF NOT EXISTS idx_sagas_created_at ON sagas(created_at);
CREATE INDEX IF NOT EXISTS idx_saga_steps_saga_id ON saga_steps(saga_id);
CREATE INDEX IF NOT EXISTS idx_saga_steps_status ON saga_steps(status);
CREATE INDEX IF NOT EXISTS idx_saga_steps_status_updated_at_id ON saga_steps(status, updated_at, id);
