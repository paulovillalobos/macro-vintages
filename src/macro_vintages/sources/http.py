"""HTTP with bounded retries, exponential backoff and full jitter.

Classification follows what the two APIs actually return (see
docs/DATA_SOURCES.md): transport errors, timeouts, 429 and 5xx are retried;
Eurostat's 413 ``ASYNCHRONOUS_RESPONSE`` is retried as well, because Eurostat
documents it as "try again later". Other 4xx are permanent. 404 is returned to
the caller, because for the ECB it can mean "nothing changed since updatedAfter".
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass

import httpx

from macro_vintages import metrics
from macro_vintages.sources.base import PermanentSourceError, RetryableSourceError

log = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({408, 413, 425, 429, 500, 502, 503, 504})


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_retries: int = 4
    base_delay: float = 0.5
    max_delay: float = 20.0

    def delay(self, attempt: int, rng: random.Random | None = None) -> float:
        """Full jitter: uniform in [0, min(max_delay, base * 2**attempt)].

        Spreading retries over the whole window keeps many workers that failed
        together from retrying together (AWS Architecture Blog, "Exponential
        Backoff And Jitter").
        """
        ceiling = min(self.max_delay, self.base_delay * (2**attempt))
        return (rng or random).uniform(0.0, ceiling)


def _retry_after(response: httpx.Response) -> float | None:
    raw = response.headers.get("retry-after")
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        return None  # HTTP-date form; neither API was seen sending it


async def get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: Mapping[str, str] | None = None,
    source: str,
    policy: RetryPolicy,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> tuple[httpx.Response, int]:
    """Return the first non-retryable response and the number of requests made.

    Raises RetryableSourceError when retries are exhausted, so the job-level
    backoff in the worker takes over.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            response = await client.get(url, params=params)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            metrics.SOURCE_REQUESTS.labels(source=source, outcome="transport_error").inc()
            if attempt > policy.max_retries:
                raise RetryableSourceError(f"{type(exc).__name__}: {exc}") from exc
            delay = policy.delay(attempt)
            log.warning(
                "source request failed, retrying",
                extra={
                    "source": source,
                    "attempt": attempt,
                    "delay_s": round(delay, 3),
                    "error": type(exc).__name__,
                },
            )
            await sleep(delay)
            continue

        metrics.SOURCE_REQUESTS.labels(source=source, outcome=str(response.status_code)).inc()
        if response.status_code in RETRYABLE_STATUS:
            if attempt > policy.max_retries:
                raise RetryableSourceError(
                    f"HTTP {response.status_code} after {attempt} attempts: {response.text[:200]}",
                    http_status=response.status_code,
                )
            delay = policy.delay(attempt)
            hinted = _retry_after(response)
            if hinted is not None:
                delay = max(delay, min(hinted, policy.max_delay))
            log.warning(
                "source returned retryable status",
                extra={
                    "source": source,
                    "attempt": attempt,
                    "status": response.status_code,
                    "delay_s": round(delay, 3),
                },
            )
            await sleep(delay)
            continue

        if response.status_code >= 400 and response.status_code != 404:
            raise PermanentSourceError(
                f"HTTP {response.status_code}: {response.text[:300]}",
                http_status=response.status_code,
            )
        return response, attempt
