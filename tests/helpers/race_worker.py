"""A worker process for the race tests: the production ``Worker`` with a fake
source that is slow on purpose (to widen race windows) and that logs every
fetch to a shared append-only file, so the test can count fetches across
processes independently of what the database says.

Run as ``python -m tests.helpers.race_worker --db URL --fetch-log PATH ...``.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import signal
import time
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal

from macro_vintages import db
from macro_vintages.config import Settings
from macro_vintages.sources.base import FetchRequest, FetchResult, SourceObservation
from macro_vintages.worker import Worker


class RaceSource:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args

    def _generation(self) -> int:
        if not self.args.generation_file:
            return 1
        with open(self.args.generation_file) as fh:
            return int(fh.read().strip() or "1")

    async def fetch(self, request: FetchRequest) -> FetchResult:
        gen = self._generation()
        line = f"{request.source_key} {gen} {os.getpid()} {time.time():.6f}\n".encode()
        fd = os.open(self.args.fetch_log, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        try:
            os.write(fd, line)  # one small O_APPEND write: atomic across processes
        finally:
            os.close(fd)
        if self.args.hang:
            await asyncio.sleep(3600)
        await asyncio.sleep(random.uniform(self.args.min_delay, self.args.max_delay))
        start = date(2026, 1, 1)
        rows = [
            SourceObservation(start + timedelta(days=i), Decimal(f"{gen}.{i:04d}"))
            for i in range(self.args.periods)
        ]
        return FetchResult(rows, complete=True, cursor={"gen": gen})


async def _main(args: argparse.Namespace) -> None:
    settings = replace(
        Settings.from_env(),
        database_url=args.db,
        lease_seconds=args.lease,
        poll_seconds=0.02,
        worker_concurrency=args.slots,
        max_attempts=50,
    )
    pool = await db.create_pool(args.db, min_size=1, max_size=args.slots + 1)
    worker = Worker(pool, {"ecb": RaceSource(args)}, settings, name=f"race-{os.getpid()}")
    try:
        if args.start_at:
            await asyncio.sleep(max(0.0, args.start_at - time.time()))
        if args.mode == "drain":

            async def drain() -> None:
                while await worker.run_once():
                    pass

            await asyncio.gather(*(drain() for _ in range(args.slots)))
        else:
            stop = asyncio.Event()
            asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
            await worker.run(stop)
    finally:
        await pool.close()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--fetch-log", required=True)
    p.add_argument("--generation-file")
    p.add_argument("--mode", choices=["drain", "loop"], default="drain")
    p.add_argument("--slots", type=int, default=4)
    p.add_argument("--periods", type=int, default=50)
    p.add_argument("--lease", type=int, default=30)
    p.add_argument("--min-delay", type=float, default=0.01)
    p.add_argument("--max-delay", type=float, default=0.05)
    p.add_argument("--start-at", type=float, default=0.0)
    p.add_argument("--hang", action="store_true")
    asyncio.run(_main(p.parse_args()))


if __name__ == "__main__":
    main()
