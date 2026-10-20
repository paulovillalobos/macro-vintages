"""Version-writing semantics and the constraints that back them."""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from decimal import Decimal
from itertools import pairwise
from typing import Any

import asyncpg
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from macro_vintages.db import Conn
from macro_vintages.sources.base import SchemaDriftError, SourceObservation
from macro_vintages.store.versions import apply_observations
from tests.conftest import add_series, obs

pytestmark = pytest.mark.integration

D1, D2, D3 = date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)


async def _run(conn: Conn, series_id: int) -> int:
    run_id: int = await conn.fetchval(
        "INSERT INTO ingest_run (series_id, worker, lease_token, mode) "
        "VALUES ($1, 'test', gen_random_uuid(), 'full') RETURNING id",
        series_id,
    )
    return run_id


async def _apply(
    conn: Conn,
    series_id: int,
    rows: list[SourceObservation],
    *,
    complete: bool = True,
) -> tuple[Any, datetime]:
    run_id = await _run(conn, series_id)
    async with conn.transaction():
        return await apply_observations(
            conn, series_id=series_id, run_id=run_id, observations=rows, complete=complete
        )


async def _as_of(
    conn: Conn, series_id: int, t: datetime
) -> dict[date, tuple[Decimal | None, str | None]]:
    rows = await conn.fetch(
        "SELECT period, value, status FROM observation_version WHERE series_id = $1 "
        "AND known_from <= $2 AND (known_to IS NULL OR known_to > $2)",
        series_id,
        t,
    )
    return {r["period"]: (r["value"], r["status"]) for r in rows}


async def test_insert_unchanged_revise_withdraw(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        sid = await add_series(conn)
        c1, t1 = await _apply(conn, sid, [obs(D1, "1.0"), obs(D2, "2.0"), obs(D3, "3.0")])
        assert (c1.inserted, c1.revised, c1.withdrawn) == (3, 0, 0)

        c2, _ = await _apply(conn, sid, [obs(D1, "1.0"), obs(D2, "2.0"), obs(D3, "3.0")])
        assert not c2.changed  # idempotent re-run

        c3, t3 = await _apply(conn, sid, [obs(D1, "1.0"), obs(D2, "2.5")])
        assert (c3.inserted, c3.revised, c3.withdrawn) == (0, 1, 1)

        assert await _as_of(conn, sid, t1) == {
            D1: (Decimal("1.0"), None),
            D2: (Decimal("2.0"), None),
            D3: (Decimal("3.0"), None),
        }
        assert await _as_of(conn, sid, t3) == {
            D1: (Decimal("1.0"), None),
            D2: (Decimal("2.5"), None),
        }
        assert await conn.fetchval("SELECT count(*) FROM observation_version") == 4
        assert await conn.fetchval("SELECT data_version FROM series WHERE id = $1", sid) == 2


async def test_numeric_equality_not_text_equality(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        sid = await add_series(conn)
        await _apply(conn, sid, [obs(D1, "2.5")])
        counts, _ = await _apply(conn, sid, [obs(D1, "2.50")])
        assert not counts.changed


async def test_status_only_change_is_a_revision(clean: asyncpg.Pool[Any]) -> None:
    # Eurostat flags provisional values ("p", "e"); dropping the flag is news.
    async with clean.acquire() as conn:
        sid = await add_series(conn)
        await _apply(conn, sid, [obs(D1, "0.6", "e")])
        counts, _ = await _apply(conn, sid, [obs(D1, "0.6")])
        assert counts.revised == 1


async def test_null_value_round_trip(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        sid = await add_series(conn)
        await _apply(conn, sid, [obs(D1, None, ":")])
        assert not (await _apply(conn, sid, [obs(D1, None, ":")]))[0].changed
        assert (await _apply(conn, sid, [obs(D1, "1.0")]))[0].revised == 1


async def test_incomplete_payload_never_withdraws(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        sid = await add_series(conn)
        await _apply(conn, sid, [obs(D1, "1"), obs(D2, "2")])
        counts, _ = await _apply(conn, sid, [obs(D2, "2.1")], complete=False)
        assert (counts.revised, counts.withdrawn) == (1, 0)
        assert (
            await conn.fetchval("SELECT count(*) FROM observation_version WHERE known_to IS NULL")
            == 2
        )


async def test_withdrawn_period_can_reappear(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        sid = await add_series(conn)
        await _apply(conn, sid, [obs(D1, "1")])
        await _apply(conn, sid, [])
        counts, _ = await _apply(conn, sid, [obs(D1, "1")])
        assert counts.inserted == 1
        assert await conn.fetchval("SELECT count(*) FROM observation_version") == 2


async def test_duplicate_period_in_payload_is_rejected(clean: asyncpg.Pool[Any]) -> None:
    async with clean.acquire() as conn:
        sid = await add_series(conn)
        with pytest.raises(SchemaDriftError):
            await _apply(conn, sid, [obs(D1, "1"), obs(D1, "2")])


async def test_database_rejects_overlapping_versions(clean: asyncpg.Pool[Any]) -> None:
    # Defence in depth: even a buggy writer cannot create two current values.
    async with clean.acquire() as conn:
        sid = await add_series(conn)
        await _apply(conn, sid, [obs(D1, "1")])
        run_id = await _run(conn, sid)
        # Either guard may fire first; PostgreSQL checks the exclusion constraint
        # before the partial unique index here.
        with pytest.raises((asyncpg.UniqueViolationError, asyncpg.ExclusionViolationError)):
            await conn.execute(
                "INSERT INTO observation_version (series_id, period, value, known_from, run_id) "
                "VALUES ($1, $2, 9, now(), $3)",
                sid,
                D1,
                run_id,
            )
        with pytest.raises(asyncpg.ExclusionViolationError):
            await conn.execute(
                "INSERT INTO observation_version "
                "(series_id, period, value, known_from, known_to, run_id) "
                "VALUES ($1, $2, 9, now() - interval '1 day', now() + interval '1 day', $3)",
                sid,
                D1,
                run_id,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "INSERT INTO observation_version "
                "(series_id, period, value, known_from, known_to, run_id) "
                "VALUES ($1, $2, 9, now(), now() - interval '1 second', $3)",
                sid,
                D2,
                run_id,
            )


snapshots = st.lists(
    st.dictionaries(
        st.integers(min_value=0, max_value=12),
        st.one_of(st.none(), st.sampled_from(["0", "0.1", "1.5", "-2", "1.50", "100"])),
        max_size=10,
    ),
    min_size=1,
    max_size=8,
)


@settings(
    max_examples=60, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(history=snapshots)
async def test_as_of_reconstructs_every_past_snapshot(
    clean: asyncpg.Pool[Any], history: list[dict[int, str | None]]
) -> None:
    """Property: after applying any sequence of complete payloads, asking
    "as of the moment payload i was written" returns exactly payload i, and the
    per-period version chains have no gaps inside a run of presence."""
    async with clean.acquire() as conn:
        await conn.execute(
            "TRUNCATE observation_version, ingest_run, ingest_job, series RESTART IDENTITY CASCADE"
        )
        sid = await add_series(conn)
        stamps = []
        for snap in history:
            rows = [obs(date(2026, 1, 1 + k), v) for k, v in sorted(snap.items())]
            _, ts = await _apply(conn, sid, rows)
            stamps.append(ts)
        for snap, ts in zip(history, stamps, strict=True):
            expected = {
                date(2026, 1, 1 + k): (None if v is None else Decimal(v), None)
                for k, v in snap.items()
            }
            assert await _as_of(conn, sid, ts) == expected
        current = await conn.fetchval(
            "SELECT count(*) FROM observation_version WHERE known_to IS NULL"
        )
        assert current == len(history[-1])
        # The catalogue stats maintained by the writer match a full recount.
        stats = await conn.fetchrow(
            "SELECT s.observations = b.n AND s.first_period IS NOT DISTINCT FROM b.lo "
            "AND s.last_period IS NOT DISTINCT FROM b.hi AS ok FROM series s, "
            "(SELECT count(*) AS n, min(period) AS lo, max(period) AS hi "
            " FROM observation_version WHERE known_to IS NULL) b"
        )
        assert stats is not None and stats["ok"]


async def test_transaction_time_is_read_after_the_series_lock(clean: asyncpg.Pool[Any]) -> None:
    """Transaction A starts first but gets the series lock second. Its versions
    must still be stamped after B's, or closing B's row would produce
    known_to < known_from. This is why apply_observations uses clock_timestamp()
    after the lock instead of now() (the transaction start)."""
    async with clean.acquire() as setup:
        sid = await add_series(setup)
        await _apply(setup, sid, [obs(D1, "1")])
        run_id = await _run(setup, sid)
    lock = "SELECT 1 FROM ingest_job WHERE series_id = $1 FOR UPDATE"
    async with clean.acquire() as a, clean.acquire() as b:
        txn_a = a.transaction()
        await txn_a.start()
        await a.fetchval("SELECT now()")  # A's transaction has started
        await asyncio.sleep(0.05)
        async with b.transaction():
            await b.execute(lock, sid)
            await apply_observations(
                b, series_id=sid, run_id=run_id, observations=[obs(D1, "2")], complete=True
            )
        await a.execute(lock, sid)
        await apply_observations(
            a, series_id=sid, run_id=run_id, observations=[obs(D1, "3")], complete=True
        )
        await txn_a.commit()
    async with clean.acquire() as conn:
        chain = await conn.fetch(
            "SELECT value, known_from, known_to FROM observation_version ORDER BY known_from"
        )
    assert [str(r["value"]) for r in chain] == ["1", "2", "3"]
    for older, newer in pairwise(chain):
        assert older["known_to"] == newer["known_from"]
