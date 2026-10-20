"""API keys and the token bucket under concurrency."""

from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
import pytest

from macro_vintages.store.keys import check_and_consume, create_key, hash_key, revoke_key
from tests.conftest import one

pytestmark = pytest.mark.integration


async def test_only_the_hash_is_stored(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        key = await create_key(conn, "k", rate_per_s=1, burst=5)
        row = await one(conn, "SELECT key_prefix, key_hash FROM api_key")
        assert row["key_hash"] == hash_key(key)
        assert key not in str(dict(row)).replace(row["key_prefix"], "")
        assert await check_and_consume(conn, key + "x") is None


async def test_burst_then_refill(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        key = await create_key(conn, "k", rate_per_s=20, burst=3)
        results = [await check_and_consume(conn, key) for _ in range(4)]
        assert [r.allowed for r in results if r] == [True, True, True, False]
        await asyncio.sleep(0.12)  # 20/s refills ~2.4 tokens
        again = await check_and_consume(conn, key)
        assert again is not None and again.allowed


async def test_concurrent_requests_cannot_overspend(clean: asyncpg.Pool[Any]) -> None:
    """300 requests race on one key from 30 connections. With burst 25 and a
    negligible refill rate, exactly 25 may pass: a read-then-write limiter
    would let several requests spend the same token."""
    async with clean.acquire() as conn:
        key = await create_key(conn, "k", rate_per_s=0.001, burst=25)

    async def hit() -> bool:
        async with clean.acquire() as conn:
            check = await check_and_consume(conn, key)
            assert check is not None
            return check.allowed

    sem = asyncio.Semaphore(30)

    async def bounded() -> bool:
        async with sem:
            return await hit()

    allowed = await asyncio.gather(*(bounded() for _ in range(300)))
    assert sum(allowed) == 25


async def test_revoked_key_is_rejected(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        key = await create_key(conn, "k", rate_per_s=1, burst=5)
        assert await revoke_key(conn, key[:11])
        assert not await revoke_key(conn, key[:11])
        assert await check_and_consume(conn, key) is None
