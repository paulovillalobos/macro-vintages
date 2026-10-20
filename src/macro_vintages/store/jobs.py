"""The ingest queue: claiming, finishing and parking jobs.

A job is the ``ingest_job`` row of one series. The lifecycle is

    claim  (short transaction): FOR UPDATE SKIP LOCKED, stamp lease token + expiry,
                                open an ``ingest_run`` row
    fetch  (no transaction, no connection held): HTTP to the source
    finish (short transaction): lock the job row, compare the lease token
                                (fencing), write versions, schedule the next run

Network I/O never happens inside a transaction, so a slow source cannot hold row
locks or pool connections. If a worker dies, its lease expires and another
worker claims the job; if the first worker comes back late, the token check in
``finish`` rejects its write.
"""

from __future__ import annotations

import random
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from macro_vintages.catalogue import SeriesSpec
from macro_vintages.db import Conn
from macro_vintages.sources.base import FetchResult
from macro_vintages.store.versions import WriteCounts, apply_observations


class LeaseLostError(Exception):
    """Another worker owns the job now; this worker's results must be discarded."""


@dataclass(frozen=True, slots=True)
class ClaimedJob:
    series_id: int
    code: str
    source: str
    source_key: str
    frequency: str
    interval_s: int
    lease_token: UUID
    cursor: dict[str, Any]
    attempts: int
    full: bool
    run_id: int


_CLAIM_SQL = """
WITH due AS (
    SELECT j.series_id
    FROM ingest_job j
    WHERE j.state = 'active'
      AND j.next_run_at <= now()
      AND (j.lease_expires_at IS NULL OR j.lease_expires_at <= now())
    ORDER BY j.next_run_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE ingest_job j
SET lease_token = gen_random_uuid(),
    lease_owner = $1,
    lease_expires_at = now() + make_interval(secs => $2)
FROM due, series s
WHERE j.series_id = due.series_id AND s.id = j.series_id
RETURNING j.series_id, j.lease_token, j.cursor, j.attempts, j.interval_s,
          (j.last_full_at IS NULL
           OR j.last_full_at <= now() - make_interval(secs => $3)) AS full,
          s.code, s.source, s.source_key, s.frequency
"""


async def claim(conn: Conn, *, worker: str, lease_s: int, full_refresh_s: int) -> ClaimedJob | None:
    async with conn.transaction():
        row = await conn.fetchrow(_CLAIM_SQL, worker, float(lease_s), float(full_refresh_s))
        if row is None:
            return None
        # A run still marked running belongs to a lease that expired: that worker
        # crashed or stalled. Close it so run history stays truthful.
        await conn.execute(
            "UPDATE ingest_run SET status = 'abandoned', finished_at = clock_timestamp(), "
            "error = 'lease expired before the run finished' "
            "WHERE series_id = $1 AND status = 'running'",
            row["series_id"],
        )
        full = bool(row["full"]) or not row["cursor"]
        run_id = await conn.fetchval(
            "INSERT INTO ingest_run (series_id, worker, lease_token, mode) "
            "VALUES ($1, $2, $3, $4) RETURNING id",
            row["series_id"],
            worker,
            row["lease_token"],
            "full" if full else "incremental",
        )
    return ClaimedJob(
        series_id=row["series_id"],
        code=row["code"],
        source=row["source"],
        source_key=row["source_key"],
        frequency=row["frequency"],
        interval_s=row["interval_s"],
        lease_token=row["lease_token"],
        cursor=dict(row["cursor"]),
        attempts=row["attempts"],
        full=full,
        run_id=run_id,
    )


async def _fence(conn: Conn, job: ClaimedJob) -> None:
    token = await conn.fetchval(
        "SELECT lease_token FROM ingest_job WHERE series_id = $1 FOR UPDATE", job.series_id
    )
    if token != job.lease_token:
        raise LeaseLostError(f"lease on {job.code} is no longer {job.lease_token}")


async def _mark_lease_lost(conn: Conn, job: ClaimedJob, error: str) -> None:
    await conn.execute(
        "UPDATE ingest_run SET status = 'lease_lost', "
        "finished_at = COALESCE(finished_at, clock_timestamp()), error = $2 WHERE id = $1",
        job.run_id,
        error,
    )


def next_delay(interval_s: int, rng: random.Random | None = None) -> float:
    """Schedule with +/-10% jitter so series added together drift apart."""
    r = rng.random() if rng else random.random()
    return interval_s * (0.9 + 0.2 * r)


def failure_delay(attempts: int, interval_s: int, rng: random.Random | None = None) -> float:
    """Job-level backoff after a failed run: 60s, 120s, 240s... capped at the
    normal interval, jittered over the upper half of that window ("equal jitter")."""
    ceiling = min(float(interval_s), 60.0 * 2.0 ** (attempts - 1))
    r = rng.random() if rng else random.random()
    return ceiling * (0.5 + 0.5 * r)


async def finish_success(conn: Conn, job: ClaimedJob, result: FetchResult) -> WriteCounts:
    try:
        async with conn.transaction():
            await _fence(conn, job)
            if result.not_modified:
                counts = WriteCounts(0, 0, 0, 0)
            else:
                counts, _ = await apply_observations(
                    conn,
                    series_id=job.series_id,
                    run_id=job.run_id,
                    observations=result.observations,
                    complete=result.complete,
                )
            await conn.execute(
                """
                UPDATE ingest_job
                SET lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL,
                    attempts = 0,
                    next_run_at = clock_timestamp() + make_interval(secs => $2),
                    cursor = $3,
                    last_full_at = CASE WHEN $4 THEN clock_timestamp() ELSE last_full_at END,
                    last_success_at = clock_timestamp()
                WHERE series_id = $1
                """,
                job.series_id,
                next_delay(job.interval_s),
                result.cursor,
                job.full,
            )
            await conn.execute(
                """
                UPDATE ingest_run
                SET status = 'succeeded', finished_at = clock_timestamp(), http_requests = $2,
                    rows_received = $3, rows_inserted = $4, rows_revised = $5,
                    rows_withdrawn = $6, not_modified = $7
                WHERE id = $1
                """,
                job.run_id,
                result.http_requests,
                counts.received,
                counts.inserted,
                counts.revised,
                counts.withdrawn,
                result.not_modified,
            )
    except LeaseLostError as exc:
        await _mark_lease_lost(conn, job, str(exc))
        raise
    return counts


async def finish_failure(
    conn: Conn,
    job: ClaimedJob,
    *,
    error: str,
    retryable: bool,
    max_attempts: int,
    http_requests: int = 0,
) -> bool:
    """Record a failed run. Returns True if the job was parked in the dead letter."""
    attempts = job.attempts + 1
    parked = not retryable or attempts >= max_attempts
    try:
        async with conn.transaction():
            await _fence(conn, job)
            if parked:
                await conn.execute(
                    "UPDATE ingest_job SET lease_token = NULL, lease_owner = NULL, "
                    "lease_expires_at = NULL, attempts = $2, state = 'parked' "
                    "WHERE series_id = $1",
                    job.series_id,
                    attempts,
                )
                await conn.execute(
                    "INSERT INTO dead_letter (series_id, run_id, attempts, error) "
                    "VALUES ($1, $2, $3, $4)",
                    job.series_id,
                    job.run_id,
                    attempts,
                    error,
                )
            else:
                await conn.execute(
                    "UPDATE ingest_job SET lease_token = NULL, lease_owner = NULL, "
                    "lease_expires_at = NULL, attempts = $2, "
                    "next_run_at = clock_timestamp() + make_interval(secs => $3) "
                    "WHERE series_id = $1",
                    job.series_id,
                    attempts,
                    failure_delay(attempts, job.interval_s),
                )
            await conn.execute(
                "UPDATE ingest_run SET status = 'failed', finished_at = clock_timestamp(), "
                "error = $2, http_requests = $3 WHERE id = $1",
                job.run_id,
                error,
                http_requests,
            )
    except LeaseLostError as exc:
        await _mark_lease_lost(conn, job, f"{exc}; original error: {error}")
        raise
    return parked


async def sync_catalogue(conn: Conn, specs: Iterable[SeriesSpec]) -> int:
    """Upsert the tracked series and their jobs. Existing schedules are kept."""
    n = 0
    async with conn.transaction():
        for spec in specs:
            series_id = await conn.fetchval(
                """
                INSERT INTO series (code, source, source_key, title, unit, frequency)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (code) DO UPDATE
                SET source = EXCLUDED.source, source_key = EXCLUDED.source_key,
                    title = EXCLUDED.title, unit = EXCLUDED.unit, frequency = EXCLUDED.frequency
                RETURNING id
                """,
                spec.code,
                spec.source.value,
                spec.source_key,
                spec.title,
                spec.unit,
                spec.frequency.value,
            )
            await conn.execute(
                "INSERT INTO ingest_job (series_id, interval_s) VALUES ($1, $2) "
                "ON CONFLICT (series_id) DO UPDATE SET interval_s = EXCLUDED.interval_s",
                series_id,
                spec.interval_s,
            )
            n += 1
    return n


async def requeue(conn: Conn, code: str) -> bool:
    """Reactivate a parked job and resolve its dead letters."""
    async with conn.transaction():
        series_id = await conn.fetchval(
            "UPDATE ingest_job j SET state = 'active', attempts = 0, next_run_at = now() "
            "FROM series s WHERE s.id = j.series_id AND s.code = $1 RETURNING j.series_id",
            code,
        )
        if series_id is None:
            return False
        await conn.execute(
            "UPDATE dead_letter SET resolved_at = now() "
            "WHERE series_id = $1 AND resolved_at IS NULL",
            series_id,
        )
    return True
