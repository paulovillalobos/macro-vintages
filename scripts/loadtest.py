"""Load test: the real API under uvicorn, a mixed read workload, and a writer
revising data underneath it.

1. Creates (and afterwards drops) a separate database, migrates it and writes a
   history through ``apply_observations``: ``--series`` daily series of
   ``--periods`` observations, then 30% of them revised, then 5% again.
   ``T_mid`` is an instant between the two revision rounds.
2. Starts ``macro-vintages api --workers N`` as a subprocess.
3. Runs ``--clients`` client processes for ``--duration`` seconds, each with
   ``--concurrency`` connections and its own API key, on this request mix:

   latest page, as-of page at a random instant, as-of page at ``T_mid``,
   version history of one period, revision feed, catalogue page, and a
   conditional GET that sends the last ETag it saw for that series.

4. Meanwhile a writer revises 20 periods and appends one in a random series
   every 200 ms, through the same locked write path the worker uses.

Every as-of read at ``T_mid`` is compared with a snapshot taken before the
load: writes after ``T_mid`` must never change what was known at ``T_mid``.
Any mismatch, 5xx or transport error fails the run (exit code 1).

    uv run python scripts/loadtest.py [--duration 30] [--clients 4] [--concurrency 16]

Writes docs/results/loadtest.md. Client and server share the machine, so the
latencies include client CPU time and are an upper bound on server latency.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import multiprocessing
import os
import platform
import random
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

import asyncpg
import httpx

from macro_vintages import db
from macro_vintages.sources.base import SourceObservation
from macro_vintages.store import keys
from macro_vintages.store.versions import apply_observations

# Any database on the target server; the script creates and drops its own.
ADMIN_URL = os.environ.get("MV_ADMIN_URL", "postgresql://postgres@127.0.0.1:5441/postgres")
DB_NAME = "macro_vintages_load"
DB_URL = ADMIN_URL.rsplit("/", 1)[0] + "/" + DB_NAME
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "results" / "loadtest.md"
FIRST = date(2000, 1, 1)

# name -> weight
MIX = {
    "latest": 30,
    "as_of_random": 20,
    "as_of_stable": 10,
    "versions": 10,
    "revisions": 10,
    "catalogue": 10,
    "conditional": 10,
}


@dataclass
class Plan:
    base_url: str
    keys: list[str]
    codes: list[str]
    periods: int
    t_seed: str
    t_mid: str
    snapshot: dict[str, Any]  # code -> data of the first as-of(T_mid) page
    duration: float
    warmup: float
    concurrency: int


@dataclass
class ClientResult:
    latencies: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    statuses: Counter[tuple[str, int]] = field(default_factory=Counter)
    transport_errors: int = 0
    mismatches: int = 0


async def _lock_and_write(
    conn: asyncpg.Connection[Any], series_id: int, rows: list[SourceObservation], complete: bool
) -> None:
    async with conn.transaction():
        await conn.execute("SELECT 1 FROM ingest_job WHERE series_id = $1 FOR UPDATE", series_id)
        run_id = await conn.fetchval(
            "INSERT INTO ingest_run (series_id, worker, lease_token, mode) "
            "VALUES ($1, 'loadtest', gen_random_uuid(), 'incremental') RETURNING id",
            series_id,
        )
        await apply_observations(
            conn, series_id=series_id, run_id=run_id, observations=rows, complete=complete
        )


async def seed(
    conn: asyncpg.Connection[Any], n_series: int, periods: int
) -> tuple[dict[str, int], datetime, datetime]:
    ids: dict[str, int] = {}
    for i in range(n_series):
        code = f"load.s{i:03d}"
        ids[code] = await conn.fetchval(
            "INSERT INTO series (code, source, source_key, title, unit, frequency) "
            "VALUES ($1, 'ecb', $2, 'load test', 'x', 'D') RETURNING id",
            code,
            f"LOAD/{i}",
        )
        await conn.execute(
            "INSERT INTO ingest_job (series_id, interval_s, next_run_at) "
            "VALUES ($1, 3600, now() + interval '1 day')",
            ids[code],
        )
    rng = random.Random(7)
    values = {
        sid: [Decimal(rng.randint(1000, 99999)) / 100 for _ in range(periods)]
        for sid in ids.values()
    }

    def payload(sid: int) -> list[SourceObservation]:
        return [SourceObservation(FIRST + timedelta(days=p), v) for p, v in enumerate(values[sid])]

    for sid in ids.values():
        await _lock_and_write(conn, sid, payload(sid), complete=True)

    async def revise(share: float) -> None:
        for sid in ids.values():
            for p in rng.sample(range(periods), int(periods * share)):
                values[sid][p] += Decimal("0.01")
            await _lock_and_write(conn, sid, payload(sid), complete=True)

    t_seed: datetime = await conn.fetchval("SELECT clock_timestamp()")
    await revise(0.30)
    t_mid: datetime = await conn.fetchval("SELECT clock_timestamp()")
    await revise(0.05)
    await conn.execute("VACUUM ANALYZE")
    return ids, t_seed, t_mid


async def writer(stop: asyncio.Event, ids: dict[str, int], periods: int) -> int:
    """Revise 20 periods and append one in a random series every 200 ms."""
    conn = await asyncpg.connect(DB_URL)
    rng = random.Random(11)
    next_period = dict.fromkeys(ids.values(), periods)
    writes = 0
    try:
        while not stop.is_set():
            sid = rng.choice(list(ids.values()))
            rows = [
                SourceObservation(FIRST + timedelta(days=p), Decimal(rng.randint(1, 10**6)) / 100)
                for p in rng.sample(range(next_period[sid]), 20)
            ]
            rows.append(SourceObservation(FIRST + timedelta(days=next_period[sid]), Decimal(1)))
            next_period[sid] += 1
            await _lock_and_write(conn, sid, rows, complete=False)
            writes += 1
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), 0.2)
    finally:
        await conn.close()
    return writes


def _random_day(rng: random.Random, periods: int) -> str:
    return (FIRST + timedelta(days=rng.randrange(periods))).isoformat()


class Http11:
    """One keep-alive HTTP/1.1 connection, GET only.

    The first version of this script used httpx. Its connection pool, not the
    API, was the bottleneck: throughput fell from 2,600 to 900 requests/s as
    connections per client process went from 8 to 32, while ApacheBench against
    the same server and a single API key held 4,000 requests/s from 16 to 128
    concurrent connections. Each lane here owns one socket and no pool.
    """

    def __init__(self, base_url: str, headers: dict[str, str]) -> None:
        url = urlsplit(base_url)
        self._host, self._port = url.hostname or "127.0.0.1", url.port or 80
        self._fixed = "".join(f"{k}: {v}\r\n" for k, v in headers.items())
        self._fixed += f"Host: {self._host}:{self._port}\r\n"
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def get(
        self, path: str, params: dict[str, str], headers: dict[str, str]
    ) -> tuple[int, dict[str, str], bytes]:
        if self._reader is None or self._writer is None:
            self._reader, self._writer = await asyncio.open_connection(self._host, self._port)
        target = f"{path}?{urlencode(params)}" if params else path
        extra = "".join(f"{k}: {v}\r\n" for k, v in headers.items())
        try:
            self._writer.write(f"GET {target} HTTP/1.1\r\n{self._fixed}{extra}\r\n".encode())
            head = (await self._reader.readuntil(b"\r\n\r\n")).decode("latin-1")
            status_line, *lines = head.split("\r\n")[:-2]
            fields = {k.lower(): v.strip() for k, _, v in (h.partition(":") for h in lines)}
            if "chunked" in fields.get("transfer-encoding", ""):
                raise ConnectionError("chunked responses are not supported here")
            length = int(fields.get("content-length", "0"))
            body = await self._reader.readexactly(length) if length else b""
        except (OSError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            await self.close()
            raise
        if fields.get("connection", "").lower() == "close":
            await self.close()
        return int(status_line.split(" ", 2)[1]), fields, body

    async def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            with contextlib.suppress(OSError):
                await self._writer.wait_closed()
        self._reader = self._writer = None


async def _client(plan: Plan, index: int) -> ClientResult:
    result = ClientResult()
    rng = random.Random(index)
    names, weights = list(MIX), list(MIX.values())
    etags: dict[str, str] = {}
    headers = {"X-API-Key": plan.keys[index]}
    t_seed = datetime.fromisoformat(plan.t_seed)
    span = (datetime.now(UTC) - t_seed).total_seconds()
    loop = asyncio.get_running_loop()
    start = loop.time()
    record_from, end = start + plan.warmup, start + plan.warmup + plan.duration

    async def one(http: Http11) -> None:
        op = rng.choices(names, weights)[0]
        code = rng.choice(plan.codes)
        obs = f"/v1/series/{code}/observations"
        extra: dict[str, str] = {}
        params: dict[str, str] = {"limit": "100"}
        if op == "latest":
            url = obs
            params["start"] = _random_day(rng, plan.periods)
        elif op == "as_of_random":
            url = obs
            at = t_seed + timedelta(seconds=rng.uniform(0, span))
            params.update(start=_random_day(rng, plan.periods), as_of=at.isoformat())
        elif op == "as_of_stable":
            url = obs
            params["as_of"] = plan.t_mid
        elif op == "versions":
            url, params = f"{obs}/{_random_day(rng, plan.periods)}/versions", {}
        elif op == "revisions":
            url = f"/v1/series/{code}/revisions"
            at = t_seed + timedelta(seconds=rng.uniform(0, span))
            params["since"] = at.isoformat()
        elif op == "catalogue":
            url = "/v1/series"
        else:  # conditional
            url = obs
            if code in etags:
                extra["If-None-Match"] = etags[code]
        t = time.perf_counter()
        try:
            status, fields, body = await http.get(url, params, extra)
        except (OSError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            result.transport_errors += 1
            return
        elapsed = (time.perf_counter() - t) * 1000
        if op == "conditional" and status == 200:
            etags[code] = fields["etag"]
        if (
            op == "as_of_stable"
            and status == 200
            and json.loads(body)["data"] != plan.snapshot[code]
        ):
            result.mismatches += 1
        if loop.time() >= record_from:
            result.latencies[op].append(elapsed)
            result.statuses[(op, status)] += 1

    async def lane() -> None:
        http = Http11(plan.base_url, headers)
        try:
            while loop.time() < end:
                await one(http)
        finally:
            await http.close()

    await asyncio.gather(*(lane() for _ in range(plan.concurrency)))
    return result


def client_process(plan: Plan, index: int) -> ClientResult:
    return asyncio.run(_client(plan, index))


def _pct(xs: list[float], q: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


async def _wait_ready(base_url: str, proc: asyncio.subprocess.Process) -> None:
    async with httpx.AsyncClient(base_url=base_url) as http:
        for _ in range(100):
            if proc.returncode is not None:
                raise RuntimeError("API process exited during startup")
            try:
                if (await http.get("/readyz")).status_code == 200:
                    return
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.1)
    raise RuntimeError("API did not become ready")


async def main(args: argparse.Namespace) -> int:
    admin = await asyncpg.connect(ADMIN_URL)
    pg_version = (await admin.fetchval("SHOW server_version")).split()[0]
    await admin.execute(f"DROP DATABASE IF EXISTS {DB_NAME} WITH (FORCE)")
    await admin.execute(f"CREATE DATABASE {DB_NAME}")
    proc: asyncio.subprocess.Process | None = None
    try:
        await db.migrate_async(DB_URL)
        conn = await asyncpg.connect(DB_URL)
        try:
            t0 = time.perf_counter()
            ids, t_seed, t_mid = await seed(conn, args.series, args.periods)
            seed_s = time.perf_counter() - t0
            versions = await conn.fetchval("SELECT count(*) FROM observation_version")
            api_keys = [
                await keys.create_key(conn, f"load{i}", rate_per_s=1e9, burst=10**9)
                for i in range(args.clients)
            ]
        finally:
            await conn.close()
        print(f"seeded {versions:,} version rows in {seed_s:.1f}s")

        base_url = f"http://127.0.0.1:{args.port}"
        env = {**os.environ, "MV_DATABASE_URL": DB_URL, "MV_LOG_LEVEL": "WARNING"}
        cmd = [sys.executable, "-m", "macro_vintages.cli", "api", "--host", "127.0.0.1"]
        cmd += ["--port", str(args.port), "--workers", str(args.api_workers)]
        proc = await asyncio.create_subprocess_exec(*cmd, env=env, cwd=ROOT)
        await _wait_ready(base_url, proc)

        snapshot = {}
        async with httpx.AsyncClient(base_url=base_url, headers={"X-API-Key": api_keys[0]}) as h:
            for code in ids:
                r = await h.get(
                    f"/v1/series/{code}/observations",
                    params={"limit": "100", "as_of": t_mid.isoformat()},
                )
                r.raise_for_status()
                snapshot[code] = r.json()["data"]

        plan = Plan(
            base_url=base_url,
            keys=api_keys,
            codes=list(ids),
            periods=args.periods,
            t_seed=t_seed.isoformat(),
            t_mid=t_mid.isoformat(),
            snapshot=snapshot,
            duration=args.duration,
            warmup=args.warmup,
            concurrency=args.concurrency,
        )
        stop = asyncio.Event()
        writes_task = asyncio.create_task(writer(stop, ids, args.periods))
        loop = asyncio.get_running_loop()
        ctx = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(args.clients, mp_context=ctx) as pool:
            futures = [
                loop.run_in_executor(pool, client_process, plan, i) for i in range(args.clients)
            ]
            results: list[ClientResult] = await asyncio.gather(*futures)
        stop.set()
        writes = await writes_task
    finally:
        if proc is not None:
            proc.terminate()
            await asyncio.wait_for(proc.wait(), 10)
        if not args.keep:
            await admin.execute(f"DROP DATABASE IF EXISTS {DB_NAME} WITH (FORCE)")
        await admin.close()

    latencies: dict[str, list[float]] = defaultdict(list)
    statuses: Counter[tuple[str, int]] = Counter()
    for res in results:
        for op, xs in res.latencies.items():
            latencies[op].extend(xs)
        statuses.update(res.statuses)
    transport = sum(r.transport_errors for r in results)
    mismatches = sum(r.mismatches for r in results)
    server_errors = sum(n for (_, code), n in statuses.items() if code >= 500)
    total = sum(len(xs) for xs in latencies.values())
    every = [x for xs in latencies.values() for x in xs]
    ok = transport == 0 and mismatches == 0 and server_errors == 0

    rows = []
    for op in MIX:
        xs = latencies[op]
        codes = ", ".join(f"{code}: {n:,}" for (o, code), n in sorted(statuses.items()) if o == op)
        rows.append(
            f"| {op} | {len(xs):,} | {_pct(xs, 0.50):.1f} | {_pct(xs, 0.95):.1f} "
            f"| {_pct(xs, 0.99):.1f} | {max(xs):.1f} | {codes} |"
        )
    rows.append(
        f"| **all** | **{total:,}** | {_pct(every, 0.50):.1f} | {_pct(every, 0.95):.1f} "
        f"| {_pct(every, 0.99):.1f} | {max(every):.1f} | |"
    )
    lines = [
        "# Load test",
        "",
        f"Generated by `scripts/loadtest.py` on {datetime.now(UTC):%Y-%m-%d %H:%M UTC}: "
        f"{platform.machine()} {platform.system()}, {os.cpu_count()} cores "
        f"(client, API and PostgreSQL {pg_version} on the same machine), "
        f"{args.api_workers} uvicorn workers with a pool of up to 10 connections each.",
        "",
        f"Data: {args.series} daily series x {args.periods:,} periods written through "
        f"`apply_observations`, 30% then 5% revised: {versions:,} version rows "
        f"(seeded in {seed_s:.1f}s).",
        "",
        f"Load: {args.clients} client processes x {args.concurrency} connections, closed loop "
        f"(each connection sends its next request when the previous one returns), "
        f"{args.duration:.0f}s measured after {args.warmup:.0f}s warm-up. One API key per "
        f"client process. A writer committed {writes:,} revisions during the run "
        "(20 periods revised and 1 appended per write, every 200 ms).",
        "",
        f"**{total / args.duration:,.0f} requests/s**; {server_errors} server errors, "
        f"{transport} transport errors, {mismatches} as-of mismatches. "
        f"Result: **{'pass' if ok else 'FAIL'}**.",
        "",
        "Latency in ms, measured by the client (includes client CPU and loopback).",
        "",
        "| Request | Count | p50 | p95 | p99 | max | Status codes |",
        "|---|---:|---:|---:|---:|---:|---|",
        *rows,
        "",
        "`as_of_stable` reads the first page at an instant between the two seeding "
        "revision rounds and compares it with a snapshot taken before the load; "
        "`conditional` sends the ETag of the last 200 it got for that series, so it "
        "answers 304 unless the writer has changed the series since.",
        "",
    ]
    args.out.write_text("\n".join(lines))
    print("\n".join(lines))
    return 0 if ok else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--series", type=int, default=20)
    parser.add_argument("--periods", type=int, default=3000)
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--warmup", type=float, default=3)
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--api-workers", type=int, default=4)
    parser.add_argument("--port", type=int, default=8077)
    parser.add_argument("--keep", action="store_true", help="keep the load database")
    parser.add_argument("--out", type=Path, default=OUT, help="where to write the report")
    raise SystemExit(asyncio.run(main(parser.parse_args())))
