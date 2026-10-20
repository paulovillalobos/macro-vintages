"""Migrations on scratch databases: reversible, and safe to run concurrently."""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import asyncpg
import pytest

from macro_vintages import db
from tests.conftest import TEST_DATABASE_URL

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
async def scratch_url() -> AsyncIterator[str]:
    name = "macro_vintages_scratch"
    admin = await asyncpg.connect(TEST_DATABASE_URL)
    try:
        await admin.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
        await admin.execute(f"CREATE DATABASE {name}")
        yield TEST_DATABASE_URL.rsplit("/", 1)[0] + "/" + name
        await admin.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
    finally:
        await admin.close()


async def _tables(url: str) -> set[str]:
    conn = await asyncpg.connect(url)
    try:
        rows = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        return {r["tablename"] for r in rows}
    finally:
        await conn.close()


async def test_upgrade_downgrade_upgrade(scratch_url: str) -> None:
    await db.migrate_async(scratch_url)
    assert await _tables(scratch_url) >= {
        "series",
        "observation_version",
        "ingest_job",
        "ingest_run",
        "dead_letter",
        "api_key",
    }
    await asyncio.to_thread(db.downgrade, scratch_url, "base")
    assert await _tables(scratch_url) == {"alembic_version"}
    await db.migrate_async(scratch_url)
    assert "observation_version" in await _tables(scratch_url)


async def test_concurrent_migrate_processes_serialise_on_the_advisory_lock(
    scratch_url: str,
) -> None:
    """Six processes run `macro-vintages migrate` on an empty database at once,
    as API and worker containers do on a fresh deploy. All must succeed and the
    schema must be created exactly once."""
    env = {**os.environ, "MV_DATABASE_URL": scratch_url, "MV_LOG_LEVEL": "WARNING"}
    procs = [
        await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "macro_vintages.cli",
            "migrate",
            cwd=ROOT,
            env=env,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        for _ in range(6)
    ]
    for proc in procs:
        _, err = await asyncio.wait_for(proc.communicate(), timeout=60)
        assert proc.returncode == 0, err.decode()[-1500:]
    conn = await asyncpg.connect(scratch_url)
    try:
        rows = await conn.fetch("SELECT version_num FROM alembic_version")
        assert [r["version_num"] for r in rows] == ["0002"]
    finally:
        await conn.close()


async def test_0002_backfills_series_stats_from_existing_versions(scratch_url: str) -> None:
    await db.migrate_async(scratch_url, "0001")
    conn = await asyncpg.connect(scratch_url)
    try:
        await conn.execute(
            """
            INSERT INTO series (code, source, source_key, title, unit, frequency)
            VALUES ('a.b', 'ecb', 'K/a', 't', 'u', 'D'), ('a.c', 'ecb', 'K/c', 't', 'u', 'D');
            INSERT INTO ingest_run (series_id, worker, lease_token, mode)
            VALUES (1, 'w', gen_random_uuid(), 'full');
            INSERT INTO observation_version (series_id, period, value, known_from, known_to, run_id)
            VALUES (1, '2026-01-01', 1, now() - interval '2 days', now() - interval '1 day', 1),
                   (1, '2026-01-01', 2, now() - interval '1 day', NULL, 1),
                   (1, '2026-01-05', 3, now() - interval '1 day', NULL, 1),
                   (1, '2025-12-31', 4, now() - interval '2 days', now() - interval '1 day', 1);
            """
        )
        await db.migrate_async(scratch_url)
        rows = await conn.fetch(
            "SELECT code, observations, first_period::text, last_period::text "
            "FROM series ORDER BY code"
        )
        assert [tuple(r) for r in rows] == [
            ("a.b", 2, "2026-01-01", "2026-01-05"),
            ("a.c", 0, None, None),
        ]
    finally:
        await conn.close()
