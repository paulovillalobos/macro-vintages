"""Concurrency tests with real OS processes against one PostgreSQL.

Each test starts several ``tests.helpers.race_worker`` processes (the
production Worker with a deliberately slow fake source) and then checks
invariants in the database and in an append-only fetch log the processes
share. Nothing here mocks the database or the locking.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from collections import Counter, defaultdict
from itertools import pairwise
from pathlib import Path
from typing import Any

import asyncpg
import pytest

from tests.conftest import add_series

pytestmark = [pytest.mark.integration, pytest.mark.race]

ROOT = Path(__file__).resolve().parents[2]


async def _spawn(database_url: str, *args: str) -> asyncio.subprocess.Process:
    env = {**os.environ, "MV_LOG_LEVEL": "WARNING"}
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.helpers.race_worker",
        "--db",
        database_url,
        *args,
        cwd=ROOT,
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )


async def _wait_ok(procs: list[asyncio.subprocess.Process], limit_s: float = 60) -> None:
    for proc in procs:
        _, err = await asyncio.wait_for(proc.communicate(), timeout=limit_s)
        assert proc.returncode == 0, err.decode()[-2000:]


def _fetches(log: Path) -> list[tuple[str, int, int]]:
    if not log.exists():
        return []
    out = []
    for line in log.read_text().splitlines():
        key, gen, pid, _ = line.split()
        out.append((key, int(gen), int(pid)))
    return out


async def test_parallel_workers_never_double_ingest(
    clean: asyncpg.Pool[Any], database_url: str, tmp_path: Path
) -> None:
    """60 due series, 6 processes x 4 concurrent slots, all started on the same
    instant: every series is fetched exactly once and written exactly once."""
    n_series, periods, processes = 60, 40, 6
    async with clean.acquire() as conn:
        for i in range(n_series):
            await add_series(conn, f"race.s{i:03d}")
    log = tmp_path / "fetches.log"
    start_at = f"{time.time() + 1.5:.3f}"
    procs = [
        await _spawn(
            database_url,
            "--fetch-log",
            str(log),
            "--slots",
            "4",
            "--periods",
            str(periods),
            "--start-at",
            start_at,
        )
        for _ in range(processes)
    ]
    await _wait_ok(procs)

    fetches = _fetches(log)
    per_key = Counter(key for key, _, _ in fetches)
    assert len(fetches) == n_series and set(per_key.values()) == {1}
    # Work really was shared: more than one process did some of it.
    assert len({pid for _, _, pid in fetches}) > 1

    async with clean.acquire() as conn:
        runs = await conn.fetch("SELECT series_id, status FROM ingest_run")
        assert len(runs) == n_series and {r["status"] for r in runs} == {"succeeded"}
        assert len({r["series_id"] for r in runs}) == n_series
        assert await conn.fetchval("SELECT count(*) FROM observation_version") == (
            n_series * periods
        )
        assert await conn.fetchval("SELECT bool_and(data_version = 1) FROM series")


async def test_continuous_workers_with_revisions_keep_history_linear(
    clean: asyncpg.Pool[Any], database_url: str, tmp_path: Path
) -> None:
    """Workers run continuously for several seconds while the test keeps every
    job due and keeps changing the source data (a new "generation" every 0.25 s).
    Afterwards the history must be exactly what serial processing would give:
    runs of one series never overlap, every version chain is contiguous, values
    only move forward in generation (no stale overwrite), and the current value
    is the one fetched by the last successful run (no lost update)."""
    n_series, periods = 8, 20
    async with clean.acquire() as conn:
        for i in range(n_series):
            await add_series(conn, f"loop.s{i}")
    log, gen_file = tmp_path / "fetches.log", tmp_path / "generation"
    gen_file.write_text("1")
    procs = [
        await _spawn(
            database_url,
            "--mode",
            "loop",
            "--fetch-log",
            str(log),
            "--generation-file",
            str(gen_file),
            "--slots",
            "3",
            "--periods",
            str(periods),
            "--min-delay",
            "0.005",
            "--max-delay",
            "0.06",
        )
        for _ in range(5)
    ]
    try:
        deadline = time.monotonic() + 6
        generation, last_bump = 1, time.monotonic()
        async with clean.acquire() as conn:
            while time.monotonic() < deadline:
                await conn.execute(
                    "UPDATE ingest_job SET next_run_at = now() - interval '1 second' "
                    "WHERE lease_token IS NULL"
                )
                if time.monotonic() - last_bump > 0.25:
                    generation += 1
                    tmp = gen_file.with_suffix(".tmp")
                    tmp.write_text(str(generation))
                    tmp.replace(gen_file)  # atomic swap; readers never see a torn value
                    last_bump = time.monotonic()
                await asyncio.sleep(0.01)
    finally:
        for proc in procs:
            proc.send_signal(signal.SIGTERM)
        await _wait_ok(procs, limit_s=30)

    async with clean.acquire() as conn:
        runs = await conn.fetch(
            "SELECT r.id, s.source_key, r.status, r.started_at, r.finished_at, r.rows_revised "
            "FROM ingest_run r JOIN series s ON s.id = r.series_id ORDER BY r.id"
        )
        versions = await conn.fetch(
            "SELECT series_id, period, value, known_from, known_to FROM observation_version "
            "ORDER BY series_id, period, known_from"
        )
        current = await conn.fetch(
            "SELECT s.source_key, v.value FROM observation_version v "
            "JOIN series s ON s.id = v.series_id "
            "WHERE v.known_to IS NULL AND v.period = date '2026-01-01'"
        )

    fetches = _fetches(log)
    statuses = Counter(r["status"] for r in runs)
    assert statuses["succeeded"] > 40, statuses  # the test did real work
    assert sum(r["rows_revised"] or 0 for r in runs) > 0  # and real revisions
    # Every fetch belongs to exactly one claimed run (no fetch without a lease).
    assert len(fetches) == len(runs)

    # 1. Runs of the same series never overlap in time.
    by_key: dict[str, list[asyncpg.Record]] = defaultdict(list)
    for r in runs:
        by_key[r["source_key"]].append(r)
    for key_runs in by_key.values():
        key_runs.sort(key=lambda r: r["started_at"])
        for a, b in pairwise(key_runs):
            assert a["finished_at"] is not None and a["finished_at"] <= b["started_at"]

    # 2. Version chains are contiguous and 3. generations never go backwards.
    chains: dict[tuple[int, Any], list[asyncpg.Record]] = defaultdict(list)
    for v in versions:
        chains[(v["series_id"], v["period"])].append(v)
    assert len(chains) == n_series * periods
    for chain in chains.values():
        for older, newer in pairwise(chain):
            assert older["known_to"] == newer["known_from"]
            assert int(older["value"]) < int(newer["value"])
        assert chain[-1]["known_to"] is None

    # 4. No lost update: the current value is the generation of the last fetch
    # that ended in a successful run for that series.
    last_ok_gen: dict[str, int] = {}
    ok_run_keys = [r["source_key"] for r in runs if r["status"] == "succeeded"]
    fetch_by_key: dict[str, list[int]] = defaultdict(list)
    for key, gen, _ in fetches:
        fetch_by_key[key].append(gen)
    for key in set(ok_run_keys):
        # runs and fetches of one key are both in claim order
        key_runs = [r for r in runs if r["source_key"] == key]
        gens = fetch_by_key[key]
        assert len(gens) == len(key_runs)
        last_ok_gen[key] = next(
            g
            for r, g in zip(reversed(key_runs), reversed(gens), strict=True)
            if r["status"] == "succeeded"
        )
    assert {r["source_key"]: int(r["value"]) for r in current} == last_ok_gen


async def test_killed_worker_job_is_recovered_after_lease_expiry(
    clean: asyncpg.Pool[Any], database_url: str, tmp_path: Path
) -> None:
    """SIGKILL a worker in the middle of a fetch. Its lease expires, another
    worker takes the job, and the dead worker's run is recorded as abandoned."""
    async with clean.acquire() as conn:
        sid = await add_series(conn, "crash.s1")
    log = tmp_path / "fetches.log"
    victim = await _spawn(
        database_url,
        "--mode",
        "loop",
        "--fetch-log",
        str(log),
        "--slots",
        "1",
        "--lease",
        "2",
        "--hang",
    )
    async with clean.acquire() as conn:
        for _ in range(200):
            if await conn.fetchval("SELECT count(*) FROM ingest_run") == 1:
                break
            await asyncio.sleep(0.05)
        victim.kill()  # SIGKILL: no cleanup, no lease release
        await victim.wait()
        assert await conn.fetchval("SELECT lease_token IS NOT NULL FROM ingest_job")

        rescuer = await _spawn(
            database_url, "--mode", "loop", "--fetch-log", str(log), "--slots", "1", "--lease", "2"
        )
        try:
            for _ in range(200):
                if (
                    await conn.fetchval(
                        "SELECT count(*) FROM ingest_run WHERE status = 'succeeded'"
                    )
                    == 1
                ):
                    break
                await asyncio.sleep(0.05)
        finally:
            rescuer.send_signal(signal.SIGTERM)
            await _wait_ok([rescuer])
        runs = await conn.fetch("SELECT status, error FROM ingest_run ORDER BY id")
        assert [r["status"] for r in runs] == ["abandoned", "succeeded"]
        assert "lease expired" in runs[0]["error"]
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM observation_version WHERE series_id = $1", sid
            )
            == 50
        )
