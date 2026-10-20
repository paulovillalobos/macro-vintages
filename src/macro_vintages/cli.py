"""Command line entry point: ``macro-vintages <command>``."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from collections.abc import Sequence

from macro_vintages import db, logs
from macro_vintages.catalogue import CATALOGUE
from macro_vintages.config import Settings
from macro_vintages.sources import build_sources, http_client
from macro_vintages.store import jobs, keys
from macro_vintages.worker import Worker

log = logging.getLogger("macro_vintages.cli")


async def _sync(settings: Settings) -> int:
    pool = await db.create_pool(settings.database_url, min_size=1, max_size=1)
    try:
        async with pool.acquire() as conn:
            return await jobs.sync_catalogue(conn, CATALOGUE)
    finally:
        await pool.close()


async def _worker(settings: Settings, *, drain: bool) -> None:
    pool = await db.create_pool(
        settings.database_url, min_size=1, max_size=settings.worker_concurrency + 1
    )
    client = http_client(settings)
    worker = Worker(pool, build_sources(client, settings), settings)
    try:
        if drain:
            # Process due jobs until none are left; used for demos and CI smoke tests.
            while await worker.run_once():
                pass
            return
        if settings.worker_metrics_port:
            from prometheus_client import start_http_server

            start_http_server(settings.worker_metrics_port)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await worker.run(stop)
    finally:
        await client.aclose()
        await pool.close()


async def _create_key(settings: Settings, name: str, rate: float, burst: int) -> str:
    pool = await db.create_pool(settings.database_url, min_size=1, max_size=1)
    try:
        async with pool.acquire() as conn:
            return await keys.create_key(conn, name, rate_per_s=rate, burst=burst)
    finally:
        await pool.close()


async def _one_conn_call(settings: Settings, fn: str, arg: str) -> bool:
    pool = await db.create_pool(settings.database_url, min_size=1, max_size=1)
    try:
        async with pool.acquire() as conn:
            if fn == "requeue":
                return await jobs.requeue(conn, arg)
            return await keys.revoke_key(conn, arg)
    finally:
        await pool.close()


def main(argv: Sequence[str] | None = None) -> int:
    settings = Settings.from_env()
    parser = argparse.ArgumentParser(prog="macro-vintages")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate", help="apply database migrations (safe to run concurrently)")
    sub.add_parser("sync-catalogue", help="upsert tracked series and their ingest jobs")
    w = sub.add_parser("worker", help="run the ingest worker until SIGTERM")
    w.add_argument("--drain", action="store_true", help="process due jobs, then exit")
    k = sub.add_parser("create-key", help="create an API key and print it once")
    k.add_argument("--name", required=True)
    k.add_argument("--rate", type=float, default=settings.default_rate_per_s)
    k.add_argument("--burst", type=int, default=settings.default_burst)
    r = sub.add_parser("revoke-key", help="revoke an API key by its prefix")
    r.add_argument("prefix")
    q = sub.add_parser("requeue", help="reactivate a parked (dead-lettered) job")
    q.add_argument("code")
    a = sub.add_parser("api", help="serve the HTTP API")
    a.add_argument("--host", default="0.0.0.0")  # noqa: S104 - inside a container
    a.add_argument("--port", type=int, default=8000)
    a.add_argument("--workers", type=int, default=1)
    args = parser.parse_args(argv)

    logs.configure(settings.log_level)
    if args.command == "migrate":
        db.migrate(settings.database_url)
        log.info("migrations applied")
    elif args.command == "sync-catalogue":
        n = asyncio.run(_sync(settings))
        log.info("catalogue synced", extra={"series": n})
    elif args.command == "worker":
        asyncio.run(_worker(settings, drain=args.drain))
    elif args.command == "create-key":
        key = asyncio.run(_create_key(settings, args.name, args.rate, args.burst))
        print(key)  # the only time the plaintext key exists
    elif args.command in ("requeue", "revoke-key"):
        arg = args.code if args.command == "requeue" else args.prefix
        ok = asyncio.run(_one_conn_call(settings, args.command.split("-")[0], arg))
        if not ok:
            print(f"{args.command}: nothing matched {arg!r}", file=sys.stderr)
            return 1
    elif args.command == "api":
        import uvicorn

        uvicorn.run(
            "macro_vintages.api.app:create_app",
            factory=True,
            host=args.host,
            port=args.port,
            workers=args.workers,
            log_config=None,
            access_log=False,
            proxy_headers=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
