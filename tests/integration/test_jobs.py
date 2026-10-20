"""Queue semantics: claiming, leases, fencing, backoff and the dead letter."""

from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
import pytest

from macro_vintages.catalogue import CATALOGUE
from macro_vintages.db import Conn
from macro_vintages.sources.base import (
    FetchRequest,
    FetchResult,
    PermanentSourceError,
    RetryableSourceError,
)
from macro_vintages.store import jobs
from macro_vintages.worker import Worker
from tests.conftest import FakeSource, add_series, daily, make_due, make_settings, obs, one

pytestmark = pytest.mark.integration


async def _claim(conn: Conn, worker: str = "w1", lease_s: int = 60) -> jobs.ClaimedJob | None:
    return await jobs.claim(conn, worker=worker, lease_s=lease_s, full_refresh_s=86400)


async def test_claim_takes_due_jobs_only_once(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        await add_series(conn, "a.one")
        await add_series(conn, "a.two", due=False)
        job = await _claim(conn)
        assert job is not None and job.code == "a.one" and job.full
        assert await _claim(conn, "w2") is None  # leased, and a.two is not due
        run = await one(conn, "SELECT status, mode, worker FROM ingest_run")
        assert dict(run) == {"status": "running", "mode": "full", "worker": "w1"}


async def test_success_schedules_next_run_and_keeps_cursor(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        sid = await add_series(conn, interval_s=600)
        job = await _claim(conn)
        assert job is not None
        result = FetchResult(daily(5), complete=True, cursor={"updated": "v1"})
        counts = await jobs.finish_success(conn, job, result)
        assert counts.inserted == 5
        row = await one(
            conn,
            "SELECT lease_token, attempts, cursor, last_full_at IS NOT NULL AS full_done, "
            "extract(epoch FROM next_run_at - now()) AS wait FROM ingest_job WHERE series_id=$1",
            sid,
        )
        assert row["lease_token"] is None and row["attempts"] == 0
        assert row["cursor"] == {"updated": "v1"} and row["full_done"]
        assert 530 <= row["wait"] <= 670  # 600 s +/- 10 % jitter
        await make_due(conn)
        again = await _claim(conn)
        assert again is not None and not again.full and again.cursor == {"updated": "v1"}


async def test_expired_lease_is_reclaimed_and_stale_writer_is_fenced(
    clean: asyncpg.Pool[Any],
) -> None:
    async with clean.acquire() as conn:
        sid = await add_series(conn)
        stale = await _claim(conn, "slow-worker", lease_s=60)
        assert stale is not None
        await conn.execute("UPDATE ingest_job SET lease_expires_at = now() - interval '1 second'")
        fresh = await _claim(conn, "fast-worker")
        assert fresh is not None and fresh.lease_token != stale.lease_token
        assert (
            await conn.fetchval("SELECT status FROM ingest_run WHERE id = $1", stale.run_id)
            == "abandoned"
        )

        await jobs.finish_success(
            conn, fresh, FetchResult([obs(daily(1)[0].period, "2")], complete=True, cursor={})
        )
        with pytest.raises(jobs.LeaseLostError):
            await jobs.finish_success(
                conn, stale, FetchResult([obs(daily(1)[0].period, "1")], complete=True, cursor={})
            )
        # The late write changed nothing: one version, the fresh worker's value.
        values = await conn.fetch("SELECT value FROM observation_version WHERE series_id=$1", sid)
        assert [str(v["value"]) for v in values] == ["2"]
        assert (
            await conn.fetchval("SELECT status FROM ingest_run WHERE id = $1", stale.run_id)
            == "lease_lost"
        )


async def test_retryable_failures_back_off_then_park(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        sid = await add_series(conn, interval_s=3600)
        for attempt in range(1, 4):
            job = await _claim(conn)
            assert job is not None and job.attempts == attempt - 1
            parked = await jobs.finish_failure(
                conn, job, error="HTTP 503", retryable=True, max_attempts=3
            )
            row = await one(
                conn,
                "SELECT state, attempts, extract(epoch FROM next_run_at - now()) AS wait "
                "FROM ingest_job WHERE series_id = $1",
                sid,
            )
            assert row["attempts"] == attempt
            if attempt < 3:
                assert not parked and row["state"] == "active"
                ceiling = 60 * 2 ** (attempt - 1)
                assert ceiling / 2 - 1 <= row["wait"] <= ceiling + 1
                await make_due(conn)
            else:
                assert parked and row["state"] == "parked"
        assert await _claim(conn) is None  # parked jobs are not claimed
        dead = await one(conn, "SELECT attempts, error, resolved_at FROM dead_letter")
        assert dict(dead) == {"attempts": 3, "error": "HTTP 503", "resolved_at": None}
        assert await conn.fetchval("SELECT count(*) FROM ingest_run WHERE status = 'failed'") == 3

        assert await jobs.requeue(conn, "test.series.one")
        assert not await jobs.requeue(conn, "no.such.series")
        assert await conn.fetchval("SELECT resolved_at IS NOT NULL FROM dead_letter")
        job = await _claim(conn)
        assert job is not None and job.attempts == 0


async def test_permanent_failure_parks_immediately(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        await add_series(conn)
        job = await _claim(conn)
        assert job is not None
        assert await jobs.finish_failure(conn, job, error="drift", retryable=False, max_attempts=5)


async def test_failure_after_lease_loss_is_fenced(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        await add_series(conn)
        stale = await _claim(conn, lease_s=60)
        assert stale is not None
        await conn.execute("UPDATE ingest_job SET lease_expires_at = now()")
        assert await _claim(conn, "other") is not None
        with pytest.raises(jobs.LeaseLostError):
            await jobs.finish_failure(conn, stale, error="x", retryable=True, max_attempts=5)
        assert await conn.fetchval("SELECT attempts FROM ingest_job") == 0


async def test_sync_catalogue_is_idempotent(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        assert await jobs.sync_catalogue(conn, CATALOGUE) == len(CATALOGUE)
        await conn.execute("UPDATE ingest_job SET next_run_at = now() + interval '5 hours'")
        await jobs.sync_catalogue(conn, CATALOGUE)
        assert await conn.fetchval("SELECT count(*) FROM series") == len(CATALOGUE)
        # Re-syncing must not reset schedules of running deployments.
        assert await conn.fetchval(
            "SELECT bool_and(next_run_at > now() + interval '4 hours') FROM ingest_job"
        )


# ------------------------------------------------------------- worker loop


async def test_worker_success_failure_and_drift_paths(clean: asyncpg.Pool[Any]) -> None:
    def payload(req: FetchRequest) -> FetchResult:
        if "boom" in req.source_key:
            raise RuntimeError("adapter bug")
        if "flaky" in req.source_key:
            raise RetryableSourceError("HTTP 503")
        if "gone" in req.source_key:
            raise PermanentSourceError("HTTP 410")
        if "dupe" in req.source_key:
            return FetchResult([obs(daily(1)[0].period, "1")] * 2, complete=True, cursor={})
        return FetchResult(daily(10), complete=True, cursor={"updated": "x"})

    async with clean.acquire() as conn:
        for code in ("w.ok", "w.boom", "w.flaky", "w.gone", "w.dupe"):
            await add_series(conn, code)
    worker = Worker(clean, {"ecb": FakeSource(payload)}, make_settings(max_attempts=3))
    while await worker.run_once():
        pass
    async with clean.acquire() as conn:
        rows = await conn.fetch(
            "SELECT s.code, j.state, j.attempts, r.status, r.error FROM series s "
            "JOIN ingest_job j ON j.series_id = s.id JOIN ingest_run r ON r.series_id = s.id "
            "ORDER BY s.code"
        )
    result = {r["code"]: (r["state"], r["attempts"], r["status"]) for r in rows}
    assert result == {
        "w.boom": ("active", 1, "failed"),
        "w.dupe": ("parked", 1, "failed"),
        "w.flaky": ("active", 1, "failed"),
        "w.gone": ("parked", 1, "failed"),
        "w.ok": ("active", 0, "succeeded"),
    }
    errors = {r["code"]: r["error"] for r in rows}
    assert "RuntimeError: adapter bug" in errors["w.boom"]
    assert "SchemaDriftError" in errors["w.dupe"]


async def test_worker_fetch_timeout_is_a_retryable_failure(clean: asyncpg.Pool[Any]) -> None:
    class Hanging:
        async def fetch(self, request: FetchRequest) -> FetchResult:
            await asyncio.sleep(10)
            raise AssertionError("unreachable")

    async with clean.acquire() as conn:
        await add_series(conn)
    worker = Worker(clean, {"ecb": Hanging()}, make_settings(lease_seconds=1))
    assert await worker.run_once()
    async with clean.acquire() as conn:
        row = await one(conn, "SELECT status, error FROM ingest_run")
    assert row["status"] == "failed" and "80% of the lease" in row["error"]


async def test_worker_run_loop_stops_on_signal(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        await add_series(conn, "loop.a")
        await add_series(conn, "loop.b")
    source = FakeSource()
    worker = Worker(clean, {"ecb": source}, make_settings(worker_concurrency=3))
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))
    for _ in range(100):
        if len(source.calls) == 2:
            break
        await asyncio.sleep(0.02)
    stop.set()
    await asyncio.wait_for(task, timeout=5)
    assert sorted(c.source_key for c in source.calls) == ["KEY/loop.a", "KEY/loop.b"]
