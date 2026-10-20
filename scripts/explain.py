"""EXPLAIN ANALYZE for the hot queries, on a synthetic history of realistic shape.

Creates (and afterwards drops) a separate database and fills it with:

* ``--series`` daily series of ``--periods`` observations; 30% of observations
  revised once and 5% twice (roughly what revising sources look like),
* one densely revised series: 500 periods x 20 versions each,
* 5,000 catalogue-only series with scheduled jobs and no data, so the queue
  and catalogue queries run against a non-trivial number of rows.

It then runs ``EXPLAIN (ANALYZE, BUFFERS)`` on the exact SQL strings the API and
the worker use. Each query is re-timed with its supporting index dropped inside
a rolled-back transaction, and the write path is timed with and without
``observation_current`` to put a number on that index's write cost.

    uv run python scripts/explain.py [--series 40] [--periods 5000] [--keep]

Writes docs/results/explain.md.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import statistics
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import asyncpg

from macro_vintages import db
from macro_vintages.api import routes
from macro_vintages.sources.base import SourceObservation
from macro_vintages.store import jobs, keys
from macro_vintages.store.versions import apply_observations

# Any database on the target server; the script creates and drops its own.
ADMIN_URL = os.environ.get("MV_ADMIN_URL", "postgresql://postgres@127.0.0.1:5441/postgres")
OUT = Path(__file__).resolve().parents[1] / "docs" / "results" / "explain.md"

FILL_SQL = """
INSERT INTO series (code, source, source_key, title, unit, frequency)
SELECT 'bench.s' || lpad(i::text, 4, '0'), 'ecb', 'BENCH/' || i, 'bench', 'x', 'D'
FROM generate_series(1, {series}) AS i;

INSERT INTO series (code, source, source_key, title, unit, frequency)
VALUES ('bench.dense', 'ecb', 'BENCH/dense', 'bench', 'x', 'D');

INSERT INTO ingest_run (series_id, worker, lease_token, mode, status, finished_at)
SELECT id, 'bench', gen_random_uuid(), 'full', 'succeeded', now() FROM series;

INSERT INTO observation_version (series_id, period, value, known_from, known_to, run_id)
SELECT s.id,
       date '2000-01-01' + p,
       round((random() * 10)::numeric, 4),
       timestamptz '2025-01-01' + k * interval '60 days' + p * interval '1 second',
       CASE WHEN k < nv.n - 1
            THEN timestamptz '2025-01-01' + (k + 1) * interval '60 days' + p * interval '1 second'
       END,
       r.id
FROM series s
JOIN ingest_run r ON r.series_id = s.id
CROSS JOIN generate_series(0, {periods} - 1) AS p
-- referencing s.id and p forces a fresh random() per observation
CROSS JOIN LATERAL (
    SELECT 1 + (random() + 0 * s.id + 0 * p < 0.30)::int
             + (random() + 0 * s.id + 0 * p < 0.05)::int AS n
) nv
CROSS JOIN LATERAL generate_series(0, nv.n - 1) AS k
WHERE s.code <> 'bench.dense';

INSERT INTO observation_version (series_id, period, value, known_from, known_to, run_id)
SELECT s.id, date '2020-01-01' + p, k,
       timestamptz '2025-01-01' + k * interval '7 days' + p * interval '1 second',
       CASE WHEN k < 19
            THEN timestamptz '2025-01-01' + (k + 1) * interval '7 days' + p * interval '1 second'
       END,
       r.id
FROM series s
JOIN ingest_run r ON r.series_id = s.id
CROSS JOIN generate_series(0, 499) AS p
CROSS JOIN generate_series(0, 19) AS k
WHERE s.code = 'bench.dense';

INSERT INTO series (code, source, source_key, title, unit, frequency)
SELECT 'bench.empty' || lpad(i::text, 5, '0'), 'eurostat', 'EMPTY/' || i, 'bench', 'x', 'M'
FROM generate_series(1, 5000) AS i;

-- Every job scheduled in the future except a handful, as in a quiet steady state.
INSERT INTO ingest_job (series_id, interval_s, next_run_at)
SELECT id, 3600, now() + (1 + random()) * interval '1 hour' FROM series;
UPDATE ingest_job SET next_run_at = now() - interval '1 minute' WHERE series_id % 1000 = 7;

UPDATE series s SET observations = b.n, first_period = b.lo, last_period = b.hi
FROM (SELECT series_id, count(*) AS n, min(period) AS lo, max(period) AS hi
      FROM observation_version WHERE known_to IS NULL GROUP BY series_id) b
WHERE b.series_id = s.id;
"""

# The catalogue query before migration 0002, kept to measure the change.
OLD_SERIES_SQL = """
SELECT s.code, s.source, s.source_key, s.title, s.unit, s.frequency, s.data_version,
       s.data_changed_at, j.last_success_at, j.state AS ingest_state,
       b.first_period, b.last_period, b.observations
FROM series s
JOIN ingest_job j ON j.series_id = s.id
CROSS JOIN LATERAL (
    SELECT min(period) AS first_period, max(period) AS last_period, count(*) AS observations
    FROM observation_version v
    WHERE v.series_id = s.id AND v.known_to IS NULL
) b
"""
CATALOGUE_PAGE = (
    " WHERE s.code > $1 AND ($2::text IS NULL OR s.source = $2)"
    " AND ($3::text IS NULL OR s.frequency = $3) ORDER BY s.code LIMIT $4"
)


@dataclass
class Case:
    name: str
    sql: str
    args: tuple[Any, ...]
    index: str | None = None  # index to drop for the comparison run
    note: str = ""


def _exec_ms(plan: str) -> float:
    m = re.search(r"Execution Time: ([\d.]+) ms", plan)
    assert m, plan
    return float(m.group(1))


async def _explain(conn: asyncpg.Connection[Any], case: Case, runs: int) -> tuple[str, float]:
    plan = ""
    times = []
    for _ in range(runs):
        rows = await conn.fetch("EXPLAIN (ANALYZE, BUFFERS) " + case.sql, *case.args)
        plan = "\n".join(r[0] for r in rows)
        times.append(_exec_ms(plan))
    return plan, statistics.median(times)


async def _write_cost(
    conn: asyncpg.Connection[Any], periods: int, runs: int, ddl: str | None
) -> float:
    """Median ms for writing ``periods`` new observations and then revising all
    of them, through apply_observations, on a fresh series."""
    rows = [
        SourceObservation(date(2000, 1, 1) + timedelta(days=i), Decimal(i)) for i in range(periods)
    ]
    revised = [SourceObservation(r.period, Decimal(1) + (r.value or 0)) for r in rows]
    times = []
    for _ in range(runs):
        tx = conn.transaction()
        await tx.start()
        try:
            if ddl:
                await conn.execute(ddl)
            sid = await conn.fetchval(
                "INSERT INTO series (code, source, source_key, title, unit, frequency) "
                "VALUES ('bench.write', 'ecb', 'W/1', 't', 'u', 'D') RETURNING id"
            )
            run_id = await conn.fetchval(
                "INSERT INTO ingest_run (series_id, worker, lease_token, mode) "
                "VALUES ($1, 'b', gen_random_uuid(), 'full') RETURNING id",
                sid,
            )
            t = time.perf_counter()
            await apply_observations(
                conn, series_id=sid, run_id=run_id, observations=rows, complete=True
            )
            await apply_observations(
                conn, series_id=sid, run_id=run_id, observations=revised, complete=True
            )
            times.append((time.perf_counter() - t) * 1000)
        finally:
            await tx.rollback()
    return statistics.median(times)


async def main(args: argparse.Namespace) -> None:
    name = "macro_vintages_bench"
    url = ADMIN_URL.rsplit("/", 1)[0] + "/" + name
    admin = await asyncpg.connect(ADMIN_URL)
    pg_version = (await admin.fetchval("SHOW server_version")).split()[0]
    await admin.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
    await admin.execute(f"CREATE DATABASE {name}")
    try:
        await db.migrate_async(url)
        conn = await asyncpg.connect(url)
        try:
            t0 = time.perf_counter()
            # Both values are ints from argparse, so formatting them in is safe.
            await conn.execute(FILL_SQL.format(series=int(args.series), periods=int(args.periods)))
            key = await keys.create_key(conn, "bench", rate_per_s=1e9, burst=10**9)
            await conn.execute("VACUUM ANALYZE")
            fill_s = time.perf_counter() - t0
            stats = await conn.fetchrow(
                "SELECT count(*) AS rows, count(*) FILTER (WHERE known_to IS NULL) AS current, "
                "pg_size_pretty(pg_total_relation_size('observation_version')) AS size, "
                "(SELECT count(*) FROM series) AS series FROM observation_version"
            )
            assert stats is not None
            sid = await conn.fetchval(
                "SELECT id FROM series WHERE code = $1", f"bench.s{args.series // 2:04d}"
            )
            dense = await conn.fetchval("SELECT id FROM series WHERE code = 'bench.dense'")
            deep = date(2000, 1, 1) + timedelta(days=args.periods - 150)
            as_of = datetime(2025, 3, 15, tzinfo=UTC)
            since = datetime(2025, 4, 1, tzinfo=UTC)
            lo, hi = date.min, date.max
            cases = [
                Case(
                    "Latest values, first page",
                    routes._CURRENT_SQL,
                    (sid, lo, hi, date.min, 101),
                    "observation_current",
                ),
                Case(
                    "Latest values, page near the end",
                    routes._CURRENT_SQL,
                    (sid, lo, hi, deep, 101),
                    "observation_current",
                ),
                Case(
                    "Latest values, densely revised series",
                    routes._CURRENT_SQL,
                    (dense, lo, hi, date.min, 101),
                    "observation_current",
                    "20 versions per period",
                ),
                Case(
                    "As-of read, first page",
                    routes._AS_OF_SQL,
                    (sid, lo, hi, date.min, 101, as_of),
                    "observation_history",
                ),
                Case(
                    "As-of read, page near the end",
                    routes._AS_OF_SQL,
                    (sid, lo, hi, deep, 101, as_of),
                    "observation_history",
                ),
                Case(
                    "As-of read, densely revised series",
                    routes._AS_OF_SQL,
                    (dense, lo, hi, date.min, 101, datetime(2025, 3, 1, tzinfo=UTC)),
                    "observation_history",
                    "20 versions per period",
                ),
                Case(
                    "Version history of one period",
                    "SELECT value, status, known_from, known_to, run_id FROM observation_version "
                    "WHERE series_id = $1 AND period = $2 ORDER BY known_from",
                    (dense, date(2020, 6, 1)),
                    "observation_history",
                    "20 versions",
                ),
                Case(
                    "Revision feed since T",
                    routes._REVISIONS_SQL,
                    (sid, since, since, date.min, 101),
                    "observation_superseded",
                ),
                Case(
                    "Catalogue page, stats on the series row (0002)",
                    routes._SERIES_SQL + CATALOGUE_PAGE,
                    ("", "ecb", None, 101),
                    note="the series that have data",
                ),
                Case(
                    "Catalogue page, stats computed per request (0001)",
                    OLD_SERIES_SQL + CATALOGUE_PAGE,
                    ("", "ecb", None, 101),
                    note="same page, the query 0002 replaced",
                ),
                Case(
                    "Claim one due job",
                    jobs._CLAIM_SQL,
                    ("bench", 60.0, 86400.0),
                    "ingest_job_due",
                    f"{stats['series']:,} jobs, 5 due",
                ),
                Case(
                    "Key check and token bucket",
                    keys._CHECK_SQL,
                    (keys.hash_key(key),),
                    note="runs on every request",
                ),
            ]
            results = []
            for case in cases:
                plan, ms = await _explain(conn, case, args.runs)
                without = None
                if case.index:
                    tx = conn.transaction()
                    await tx.start()
                    try:
                        await conn.execute(f"DROP INDEX {case.index}")
                        _, without = await _explain(conn, case, args.runs)
                    finally:
                        await tx.rollback()
                results.append((case, plan, ms, without))
                suffix = f" (without {case.index}: {without:.3f} ms)" if without else ""
                print(f"{case.name}: {ms:.3f} ms{suffix}")
            write_all = await _write_cost(conn, args.periods, args.runs, None)
            write_less = await _write_cost(
                conn, args.periods, args.runs, "DROP INDEX observation_current"
            )
            write_noexcl = await _write_cost(
                conn,
                args.periods,
                args.runs,
                "ALTER TABLE observation_version DROP CONSTRAINT observation_version_no_overlap",
            )
            print(
                f"write {args.periods} + revise all: {write_all:.1f} ms with all indexes, "
                f"{write_less:.1f} ms without observation_current, "
                f"{write_noexcl:.1f} ms without the exclusion constraint"
            )
        finally:
            await conn.close()
    finally:
        if not args.keep:
            await admin.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
        await admin.close()

    lines = [
        "# Query plans for the hot paths",
        "",
        f"Generated by `scripts/explain.py` on {datetime.now(UTC):%Y-%m-%d %H:%M UTC}, "
        f"PostgreSQL {pg_version}, Apple M4 laptop, data fully cached (shared hits).",
        "",
        f"Synthetic history: {args.series} daily series x {args.periods:,} periods with 30% of "
        "observations revised once and 5% twice, one series with 500 periods x 20 versions, "
        f"and 5,000 catalogue-only series: **{stats['rows']:,} version rows** "
        f"({stats['current']:,} current), {stats['series']:,} series, observation_version "
        f"with indexes {stats['size']}, loaded in {fill_s:.1f}s.",
        "",
        f"Times are the median `Execution Time` of {args.runs} `EXPLAIN (ANALYZE, BUFFERS)` runs "
        "of the SQL string the code uses. They exclude network, protocol and Python time. "
        '"Without" drops that index in a transaction that is rolled back.',
        "",
        "| Query | Time | Without its index | Note |",
        "|---|---:|---:|---|",
    ]
    for case, _, ms, without in results:
        w = f"{without:.3f} ms (`{case.index}`)" if without is not None else ""
        lines.append(f"| {case.name} | {ms:.3f} ms | {w} | {case.note} |")
    lines += [
        "",
        f"Write path, `apply_observations` inserting {args.periods:,} new periods and then "
        f"revising all of them (median of {args.runs}): **{write_all:.1f} ms** with every index "
        f"and constraint; {write_less:.1f} ms without `observation_current` (the writer's own "
        f"lookups use it); {write_noexcl:.1f} ms without the exclusion constraint (the price "
        "of the database refusing overlapping versions).",
        "",
        "## Plans",
        "",
    ]
    for case, plan, _, _ in results:
        lines += [f"### {case.name}", "", "```", plan, "```", ""]
    OUT.write_text("\n".join(lines))
    print(f"wrote {OUT}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--series", type=int, default=40)
    parser.add_argument("--periods", type=int, default=5000)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--keep", action="store_true", help="keep the bench database")
    asyncio.run(main(parser.parse_args()))
