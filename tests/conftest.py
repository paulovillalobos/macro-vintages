from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Callable
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import asyncpg
import pytest

from macro_vintages import db
from macro_vintages.config import Settings
from macro_vintages.db import Conn
from macro_vintages.sources.base import FetchRequest, FetchResult, SourceObservation

FIXTURES = Path(__file__).parent / "fixtures"
TEST_DATABASE_URL = os.environ.get(
    "MV_TEST_DATABASE_URL", "postgresql://postgres@127.0.0.1:5441/macro_vintages_test"
)
TABLES = "api_key, observation_version, dead_letter, ingest_run, ingest_job, series"


def fixture_path(name: str) -> Path:
    return FIXTURES / name


def obs(period: date, value: str | None, status: str | None = None) -> SourceObservation:
    return SourceObservation(period, None if value is None else Decimal(value), status)


def daily(n: int, start: date = date(2026, 1, 1), value: str = "1.0") -> list[SourceObservation]:
    return [obs(start + timedelta(days=i), value) for i in range(n)]


def make_settings(**overrides: Any) -> Settings:
    base = Settings.from_env()
    values = {f: getattr(base, f) for f in base.__dataclass_fields__}
    values.update(database_url=TEST_DATABASE_URL, poll_seconds=0.05, worker_concurrency=2)
    values.update(overrides)
    return Settings(**values)


class FakeSource:
    """In-memory source: returns whatever ``payload`` currently says."""

    def __init__(self, payload: Callable[[FetchRequest], FetchResult] | None = None) -> None:
        self.calls: list[FetchRequest] = []
        self._payload = payload or (lambda req: FetchResult(daily(3), complete=True, cursor={}))

    async def fetch(self, request: FetchRequest) -> FetchResult:
        self.calls.append(request)
        return self._payload(request)


@pytest.fixture(scope="session")
def database_url() -> str:
    async def reset() -> None:
        try:
            conn = await asyncpg.connect(TEST_DATABASE_URL, timeout=5)
        except (OSError, asyncpg.PostgresError) as exc:
            pytest.exit(
                f"integration tests need PostgreSQL at {TEST_DATABASE_URL} ({exc}). "
                "Run `make db-up` or set MV_TEST_DATABASE_URL.",
                returncode=2,
            )
        try:
            await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        finally:
            await conn.close()

    asyncio.run(reset())
    db.migrate(TEST_DATABASE_URL)
    return TEST_DATABASE_URL


@pytest.fixture(scope="session")
async def pool(database_url: str) -> AsyncIterator[asyncpg.Pool[Any]]:
    p = await db.create_pool(database_url, min_size=2, max_size=40)
    yield p
    await p.close()


@pytest.fixture
async def clean(pool: asyncpg.Pool[Any]) -> asyncpg.Pool[Any]:
    async with pool.acquire() as conn:
        await conn.execute(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE")
    return pool


async def one(conn: Conn, sql: str, *args: Any) -> asyncpg.Record:
    """fetchrow that fails the test instead of returning None."""
    row: asyncpg.Record | None = await conn.fetchrow(sql, *args)
    assert row is not None, sql
    return row


async def add_series(
    conn: Conn,
    code: str = "test.series.one",
    *,
    source: str = "ecb",
    frequency: str = "D",
    interval_s: int = 3600,
    due: bool = True,
) -> int:
    series_id: int = await conn.fetchval(
        "INSERT INTO series (code, source, source_key, title, unit, frequency) "
        "VALUES ($1, $2, $3, $4, 'x', $5) RETURNING id",
        code,
        source,
        f"KEY/{code}",
        f"Title {code}",
        frequency,
    )
    await conn.execute(
        "INSERT INTO ingest_job (series_id, interval_s, next_run_at) "
        "VALUES ($1, $2, CASE WHEN $3 THEN now() - interval '1 second' "
        "ELSE now() + interval '1 day' END)",
        series_id,
        interval_s,
        due,
    )
    return series_id


async def make_due(conn: Conn, series_id: int | None = None) -> None:
    await conn.execute(
        "UPDATE ingest_job SET next_run_at = now() - interval '1 second' "
        "WHERE $1::bigint IS NULL OR series_id = $1",
        series_id,
    )
