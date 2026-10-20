"""Initial schema: series catalogue, bitemporal observations, job queue, API keys.

Revision ID: 0001
Revises:
Create Date: 2026-10-20
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


UPGRADE = """
-- btree_gist lets one exclusion constraint mix equality on scalar columns with
-- overlap on a time range; it ships with PostgreSQL contrib.
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE series (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code            text NOT NULL UNIQUE CHECK (code ~ '^[a-z0-9_]+(\\.[a-z0-9_]+)+$'),
    source          text NOT NULL CHECK (source IN ('ecb', 'eurostat')),
    source_key      text NOT NULL,
    title           text NOT NULL,
    unit            text NOT NULL,
    frequency       text NOT NULL CHECK (frequency IN ('D', 'M', 'Q', 'A')),
    -- Incremented in the same transaction as any observation change. The API
    -- derives ETags from it, so a conditional GET costs one primary-key lookup.
    data_version    bigint NOT NULL DEFAULT 0,
    data_changed_at timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source, source_key)
);

-- One schedule row per series. The row doubles as the work queue item: workers
-- claim it with FOR UPDATE SKIP LOCKED and stamp a lease; the lease token is the
-- fencing token checked again when results are written.
CREATE TABLE ingest_job (
    series_id        bigint PRIMARY KEY REFERENCES series (id) ON DELETE CASCADE,
    state            text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'parked')),
    interval_s       integer NOT NULL CHECK (interval_s >= 60),
    next_run_at      timestamptz NOT NULL DEFAULT now(),
    lease_token      uuid,
    lease_owner      text,
    lease_expires_at timestamptz,
    attempts         integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    cursor           jsonb NOT NULL DEFAULT '{}'::jsonb,
    last_full_at     timestamptz,
    last_success_at  timestamptz,
    CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL))
);
-- The claim query filters on state = 'active' and orders by next_run_at.
CREATE INDEX ingest_job_due ON ingest_job (next_run_at) WHERE state = 'active';

CREATE TABLE ingest_run (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    series_id      bigint NOT NULL REFERENCES series (id) ON DELETE CASCADE,
    worker         text NOT NULL,
    lease_token    uuid NOT NULL,
    mode           text NOT NULL CHECK (mode IN ('full', 'incremental')),
    started_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at    timestamptz,
    status         text NOT NULL DEFAULT 'running'
                   CHECK (status IN ('running', 'succeeded', 'failed', 'lease_lost', 'abandoned')),
    http_requests  integer NOT NULL DEFAULT 0,
    rows_received  integer,
    rows_inserted  integer,
    rows_revised   integer,
    rows_withdrawn integer,
    not_modified   boolean,
    error          text,
    CHECK ((status = 'running') = (finished_at IS NULL))
);
CREATE INDEX ingest_run_series ON ingest_run (series_id, id DESC);
CREATE INDEX ingest_run_running ON ingest_run (series_id) WHERE status = 'running';

-- Jobs that exhausted their attempts. The job is parked (not retried) until an
-- operator requeues it; the row keeps the last error for inspection.
CREATE TABLE dead_letter (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    series_id   bigint NOT NULL REFERENCES series (id) ON DELETE CASCADE,
    run_id      bigint NOT NULL REFERENCES ingest_run (id),
    attempts    integer NOT NULL,
    error       text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz
);
CREATE INDEX dead_letter_open ON dead_letter (series_id) WHERE resolved_at IS NULL;

-- Bitemporal observations. Valid time is the observation period (``period``);
-- transaction time is [known_from, known_to): when this engine held the value.
-- A revision closes the current row and inserts a new one at the same instant,
-- so history is append-only apart from setting known_to once.
CREATE TABLE observation_version (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    series_id  bigint NOT NULL REFERENCES series (id) ON DELETE CASCADE,
    period     date NOT NULL,
    value      numeric,
    status     text,
    known_from timestamptz NOT NULL,
    known_to   timestamptz,
    run_id     bigint NOT NULL REFERENCES ingest_run (id),
    CHECK (known_to IS NULL OR known_to > known_from),
    -- Two versions of the same observation may never be valid at the same time.
    CONSTRAINT observation_version_no_overlap EXCLUDE USING gist (
        series_id WITH =,
        period WITH =,
        tstzrange(known_from, known_to, '[)') WITH &&
    )
);
-- Access path for the "latest values" query (index-only scan via INCLUDE).
-- UNIQUE restates "one current version per observation", which the exclusion
-- constraint already implies, so the invariant is visible in either place.
CREATE UNIQUE INDEX observation_current
    ON observation_version (series_id, period) INCLUDE (value, status, known_from)
    WHERE known_to IS NULL;
-- Access path for as-of reads and revision history: rows for one series in
-- period order, versions of a period in transaction-time order.
CREATE INDEX observation_history
    ON observation_version (series_id, period, known_from) INCLUDE (known_to, value, status);
-- Access path for the revision feed ("what changed since T"): only superseded
-- rows, ordered by the moment they were superseded.
CREATE INDEX observation_superseded
    ON observation_version (series_id, known_to, period) WHERE known_to IS NOT NULL;

-- API keys are 32 random bytes; only the SHA-256 digest is stored. A slow KDF
-- (bcrypt/argon2) protects low-entropy passwords and would only add latency to
-- every request here. The token bucket lives on the same row so one UPDATE both
-- authenticates and rate-limits, consistently across API replicas.
CREATE TABLE api_key (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        text NOT NULL,
    key_prefix  text NOT NULL UNIQUE,
    key_hash    bytea NOT NULL UNIQUE CHECK (octet_length(key_hash) = 32),
    rate_per_s  double precision NOT NULL CHECK (rate_per_s > 0),
    burst       integer NOT NULL CHECK (burst >= 1),
    tokens      double precision NOT NULL,
    refilled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz
);
"""

DOWNGRADE = """
DROP TABLE api_key;
DROP TABLE observation_version;
DROP TABLE dead_letter;
DROP TABLE ingest_run;
DROP TABLE ingest_job;
DROP TABLE series;
"""


def _statements(script: str) -> list[str]:
    # Comments are dropped first because some contain semicolons. The statements
    # go straight to the driver so SQLAlchemy does not parse ``::`` or ``:name``.
    lines = [line for line in script.splitlines() if not line.lstrip().startswith("--")]
    return [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]


def upgrade() -> None:
    bind = op.get_bind()
    for stmt in _statements(UPGRADE):
        bind.exec_driver_sql(stmt)


def downgrade() -> None:
    bind = op.get_bind()
    for stmt in _statements(DOWNGRADE):
        bind.exec_driver_sql(stmt)
