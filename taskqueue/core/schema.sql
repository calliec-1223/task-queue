-- Job history. Redis is the live queue; PostgreSQL is the permanent record.
-- Safe to run more than once (IF NOT EXISTS).

-- One row per job: its current state.
CREATE TABLE IF NOT EXISTS jobs (
    id            UUID PRIMARY KEY,                 -- same id as in Redis
    queue         TEXT NOT NULL,
    payload       JSONB NOT NULL,                   -- JSONB = JSON stored in a queryable binary form
    status        TEXT NOT NULL
                  CHECK (status IN ('pending', 'leased', 'done', 'dead')),
    max_attempts  INT NOT NULL,
    result        JSONB,                            -- return value of the task, if done
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ                       -- when it became done or dead
);

-- Find jobs by status quickly ("show me all dead jobs") without scanning every row.
CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs (status);

-- One row per attempt, never overwritten. Survives replays, unlike Redis's attempts counter.
CREATE TABLE IF NOT EXISTS job_attempts (
    lease_token     UUID PRIMARY KEY,               -- each lease is unique, so it identifies the attempt
    job_id          UUID NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt_number  INT NOT NULL,                   -- Redis's counter at the time (restarts at 1 after a replay)
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,                    -- NULL while running
    outcome         TEXT CHECK (outcome IN ('done', 'error', 'lease_expired')),
    error           TEXT
);

-- Get a job's attempts in order without scanning the whole table.
CREATE INDEX IF NOT EXISTS job_attempts_job_idx ON job_attempts (job_id, started_at);

-- History events that failed to apply several times (poison events).
-- Kept so nothing is silently lost; a person can inspect and fix them.
CREATE TABLE IF NOT EXISTS dead_events (
    event_id   TEXT PRIMARY KEY,                    -- Redis stream entry id
    stream     TEXT NOT NULL,
    fields     JSONB NOT NULL,                      -- the raw event, as received
    error      TEXT NOT NULL,
    failures   INT NOT NULL,
    dead_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Added with job priority. ADD COLUMN IF NOT EXISTS keeps this safe to re-run
-- on databases created before the column existed.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 0;

-- Added with "discard": a dead job someone deliberately gave up on.
-- DROP + ADD keeps this safe to re-run on databases with the older rule.
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('pending', 'leased', 'done', 'dead', 'discarded'));
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS discarded_at TIMESTAMPTZ;

-- Single-writer fencing for the history writer: the highest token that has
-- written for each queue. A writer with an older token is rejected.
CREATE TABLE IF NOT EXISTS history_fence (
    queue  TEXT PRIMARY KEY,
    token  BIGINT NOT NULL
);

-- Added with graceful shutdown: an attempt handed back on purpose ("released")
-- doesn't count as a failure. DROP + ADD keeps this safe to re-run.
ALTER TABLE job_attempts DROP CONSTRAINT IF EXISTS job_attempts_outcome_check;
ALTER TABLE job_attempts ADD CONSTRAINT job_attempts_outcome_check
    CHECK (outcome IN ('done', 'error', 'lease_expired', 'released'));
