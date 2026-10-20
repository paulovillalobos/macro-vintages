from __future__ import annotations

import random

import httpx
import pytest
from hypothesis import given
from hypothesis import strategies as st

from macro_vintages.sources.base import PermanentSourceError, RetryableSourceError
from macro_vintages.sources.http import RetryPolicy, get_with_retry


@given(
    attempt=st.integers(min_value=0, max_value=30),
    base=st.floats(min_value=0.01, max_value=5),
    cap=st.floats(min_value=0.01, max_value=60),
    seed=st.integers(),
)
def test_full_jitter_stays_within_bounds(attempt: int, base: float, cap: float, seed: int) -> None:
    policy = RetryPolicy(base_delay=base, max_delay=cap)
    delay = policy.delay(attempt, random.Random(seed))
    assert 0.0 <= delay <= min(cap, base * 2**attempt)


def test_jitter_actually_spreads_retries() -> None:
    policy = RetryPolicy(base_delay=1.0, max_delay=30.0)
    rng = random.Random(7)
    delays = [policy.delay(4, rng) for _ in range(1000)]
    # 1000 workers failing together should not retry together: in one-second
    # buckets over U(0, 16) each bucket expects ~62 retries; none should spike.
    buckets = [0] * 16
    for d in delays:
        buckets[min(15, int(d))] += 1
    assert max(buckets) < 100 and min(buckets) > 30
    assert 7.0 < sum(delays) / len(delays) < 9.0


class _Recorder:
    def __init__(self, responses: list[httpx.Response | Exception]) -> None:
        self.responses = responses
        self.calls = 0
        self.sleeps: list[float] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        item = self.responses[min(self.calls, len(self.responses) - 1)]
        self.calls += 1
        if isinstance(item, Exception):
            raise item
        return item

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)


async def _get(rec: _Recorder, policy: RetryPolicy) -> tuple[httpx.Response, int]:
    async with httpx.AsyncClient(transport=httpx.MockTransport(rec.handler)) as client:
        return await get_with_retry(
            client, "https://x.test/", source="test", policy=policy, sleep=rec.sleep
        )


async def test_retries_transient_failures_then_succeeds() -> None:
    rec = _Recorder(
        [
            httpx.ConnectError("refused"),
            httpx.Response(503),
            httpx.ReadTimeout("slow"),
            httpx.Response(200, text="ok"),
        ]
    )
    response, attempts = await _get(rec, RetryPolicy(max_retries=4, base_delay=0.1))
    assert response.status_code == 200 and attempts == 4 and len(rec.sleeps) == 3


async def test_honours_retry_after_up_to_the_cap() -> None:
    rec = _Recorder([httpx.Response(429, headers={"retry-after": "7"}), httpx.Response(200)])
    await _get(rec, RetryPolicy(max_retries=2, base_delay=0.0, max_delay=5.0))
    assert rec.sleeps == [5.0]


async def test_ignores_unparseable_retry_after() -> None:
    rec = _Recorder(
        [
            httpx.Response(503, headers={"retry-after": "Wed, 21 Oct 2026 07:28:00 GMT"}),
            httpx.Response(200),
        ]
    )
    await _get(rec, RetryPolicy(max_retries=2, base_delay=0.0, max_delay=5.0))
    assert rec.sleeps == [0.0]


async def test_gives_up_with_retryable_error() -> None:
    rec = _Recorder([httpx.Response(502)])
    with pytest.raises(RetryableSourceError) as info:
        await _get(rec, RetryPolicy(max_retries=3, base_delay=0.0))
    assert rec.calls == 4 and info.value.http_status == 502


async def test_gives_up_on_transport_errors() -> None:
    rec = _Recorder([httpx.ConnectError("down")])
    with pytest.raises(RetryableSourceError, match="ConnectError"):
        await _get(rec, RetryPolicy(max_retries=1, base_delay=0.0))
    assert rec.calls == 2


@pytest.mark.parametrize("status", [400, 401, 403, 410])
async def test_client_errors_are_permanent_and_not_retried(status: int) -> None:
    rec = _Recorder([httpx.Response(status)])
    with pytest.raises(PermanentSourceError):
        await _get(rec, RetryPolicy(max_retries=3, base_delay=0.0))
    assert rec.calls == 1


async def test_404_is_returned_to_the_caller() -> None:
    rec = _Recorder([httpx.Response(404)])
    response, attempts = await _get(rec, RetryPolicy(max_retries=3))
    assert response.status_code == 404 and attempts == 1
