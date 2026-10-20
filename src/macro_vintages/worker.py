"""The ingest worker: N concurrent claim loops per process, any number of processes.

Safety does not depend on how many workers run. Claiming uses
``FOR UPDATE SKIP LOCKED`` (no two workers get the same due job), and every
write is fenced by the lease token (a worker whose lease expired cannot
overwrite the newer owner). Correctness under concurrency is tested by
``tests/integration/test_races.py``, which runs several worker processes
against one database.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random
import socket
import time
import traceback
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import asyncpg

from macro_vintages import metrics
from macro_vintages.config import Settings
from macro_vintages.sources.base import FetchRequest, Source, SourceError
from macro_vintages.store import jobs

log = logging.getLogger(__name__)


def default_worker_name() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


class Worker:
    def __init__(
        self,
        pool: asyncpg.Pool[Any],
        sources: Mapping[str, Source],
        settings: Settings,
        *,
        name: str | None = None,
    ) -> None:
        self._pool = pool
        self._sources = sources
        self._settings = settings
        self.name = name or default_worker_name()

    async def run(self, stop: asyncio.Event) -> None:
        slots = [
            asyncio.create_task(self._slot(stop, i), name=f"slot-{i}")
            for i in range(self._settings.worker_concurrency)
        ]
        log.info("worker started", extra={"worker": self.name, "slots": len(slots)})
        try:
            await asyncio.gather(*slots)
        finally:
            for task in slots:
                task.cancel()
            log.info("worker stopped", extra={"worker": self.name})

    async def _slot(self, stop: asyncio.Event, index: int) -> None:
        while not stop.is_set():
            try:
                claimed = await self.run_once()
            except (OSError, asyncpg.PostgresError) as exc:
                # Database unreachable or similar: back off, do not crash the slot.
                log.exception("claim loop error", extra={"slot": index, "error": repr(exc)})
                claimed = False
            if claimed:
                continue
            # Idle: poll again later; jitter keeps idle slots from polling in lockstep.
            delay = self._settings.poll_seconds * (0.5 + random.random())
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)

    async def run_once(self) -> bool:
        """Claim and process at most one due job. Returns whether a job was claimed."""
        async with self._pool.acquire() as conn:
            job = await jobs.claim(
                conn,
                worker=self.name,
                lease_s=self._settings.lease_seconds,
                full_refresh_s=self._settings.full_refresh_seconds,
            )
        if job is None:
            return False
        metrics.JOBS_IN_FLIGHT.inc()
        try:
            await self._process(job)
        finally:
            metrics.JOBS_IN_FLIGHT.dec()
        return True

    async def _process(self, job: jobs.ClaimedJob) -> None:
        started = time.perf_counter()
        source = self._sources[job.source]
        request = FetchRequest(
            source_key=job.source_key,
            frequency=job.frequency,
            cursor=job.cursor,
            full=job.full,
            started_at=datetime.now(UTC),
        )
        context = {
            "series": job.code,
            "run_id": job.run_id,
            "mode": "full" if job.full else "incremental",
            "worker": self.name,
        }
        error: str | None = None
        retryable = True
        try:
            # Stay well inside the lease; fencing makes overruns safe, not useful.
            async with asyncio.timeout(self._settings.lease_seconds * 0.8):
                result = await source.fetch(request)
        except SourceError as exc:
            error, retryable = f"{type(exc).__name__}: {exc}", exc.retryable
        except TimeoutError:
            error = "fetch exceeded 80% of the lease"
        except Exception as exc:  # a bug in an adapter; record it, retry, then park
            error = "".join(traceback.format_exception_only(exc)).strip()
            log.exception("unexpected adapter error", extra=context)

        try:
            async with self._pool.acquire() as conn:
                if error is not None:
                    parked = await jobs.finish_failure(
                        conn,
                        job,
                        error=error,
                        retryable=retryable,
                        max_attempts=self._settings.max_attempts,
                    )
                    status = "dead" if parked else "failed"
                    log.warning(
                        "ingest run failed", extra={**context, "error": error, "parked": parked}
                    )
                else:
                    try:
                        counts = await jobs.finish_success(conn, job, result)
                    except SourceError as exc:  # payload failed validation at write time
                        parked = await jobs.finish_failure(
                            conn,
                            job,
                            error=f"{type(exc).__name__}: {exc}",
                            retryable=exc.retryable,
                            max_attempts=self._settings.max_attempts,
                            http_requests=result.http_requests,
                        )
                        status = "dead" if parked else "failed"
                    else:
                        status = "succeeded"
                        for kind in ("inserted", "revised", "withdrawn"):
                            n = getattr(counts, kind)
                            if n:
                                metrics.INGEST_ROWS.labels(source=job.source, kind=kind).inc(n)
                        log.info(
                            "ingest run succeeded",
                            extra={
                                **context,
                                "received": counts.received,
                                "inserted": counts.inserted,
                                "revised": counts.revised,
                                "withdrawn": counts.withdrawn,
                                "not_modified": result.not_modified,
                                "http_requests": result.http_requests,
                            },
                        )
        except jobs.LeaseLostError:
            status = "lease_lost"
            log.warning("lease lost; results discarded", extra=context)
        metrics.INGEST_RUNS.labels(source=job.source, status=status).inc()
        metrics.INGEST_DURATION.labels(source=job.source).observe(time.perf_counter() - started)
