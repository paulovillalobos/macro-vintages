"""HTTP API against a real database, through the ASGI app (no network)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

import asyncpg
import httpx
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from macro_vintages.api.app import create_app
from macro_vintages.store.keys import create_key
from macro_vintages.store.versions import apply_observations
from tests.conftest import add_series, daily, make_settings, obs

pytestmark = pytest.mark.integration

PROBLEM = "application/problem+json"


class Api:
    def __init__(self, client: httpx.AsyncClient, pool: asyncpg.Pool[Any], key: str) -> None:
        self.client, self.pool, self.key = client, pool, key

    async def get(
        self, path: str, *, key: str | None = None, auth: bool = True, **kw: Any
    ) -> httpx.Response:
        headers = dict(kw.pop("headers", {}))
        if auth:
            headers.setdefault("X-API-Key", key or self.key)
        return await self.client.get(path, headers=headers, **kw)

    async def write(self, series_id: int, rows: list[Any], *, complete: bool = True) -> datetime:
        async with self.pool.acquire() as conn:
            run_id = await conn.fetchval(
                "INSERT INTO ingest_run (series_id, worker, lease_token, mode) "
                "VALUES ($1, 'test', gen_random_uuid(), 'full') RETURNING id",
                series_id,
            )
            async with conn.transaction():
                _, ts = await apply_observations(
                    conn, series_id=series_id, run_id=run_id, observations=rows, complete=complete
                )
        return ts


@pytest.fixture
async def api(clean: asyncpg.Pool[Any]) -> AsyncIterator[Api]:
    app = create_app(make_settings(), pool=clean)
    async with clean.acquire() as conn:
        key = await create_key(conn, "test", rate_per_s=1000, burst=1000)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield Api(client, clean, key)


async def _series(api: Api, code: str = "ecb.test.daily", freq: str = "D") -> int:
    async with api.pool.acquire() as conn:
        return await add_series(conn, code, frequency=freq)


# ------------------------------------------------------------------ auth


async def test_auth_required_and_problem_shape(api: Api) -> None:
    r = await api.get("/v1/series", auth=False)
    assert r.status_code == 401 and r.headers["content-type"] == PROBLEM
    body = r.json()
    assert body["title"] == "Unauthorized" and body["type"].endswith("#missing-api-key")
    assert body["request_id"] == r.headers["x-request-id"]
    assert r.headers["www-authenticate"] == "Bearer"
    bad = await api.get("/v1/series", key="mv_nope")
    assert bad.status_code == 401 and bad.json()["type"].endswith("#invalid-api-key")


async def test_bearer_token_also_works(api: Api) -> None:
    r = await api.client.get("/v1/series", headers={"Authorization": f"Bearer {api.key}"})
    assert r.status_code == 200 and "ratelimit-remaining" in r.headers


async def test_rate_limit_returns_429_with_retry_after(api: Api) -> None:
    async with api.pool.acquire() as conn:
        key = await create_key(conn, "tiny", rate_per_s=0.5, burst=2)
    codes = [(await api.get("/v1/series", key=key)).status_code for _ in range(3)]
    assert codes == [200, 200, 429]
    r = await api.get("/v1/series", key=key)
    assert r.status_code == 429 and int(r.headers["retry-after"]) >= 1
    assert r.headers["content-type"] == PROBLEM


async def test_health_and_metrics_need_no_key(api: Api) -> None:
    assert (await api.client.get("/healthz")).json() == {"status": "ok", "checks": {}}
    ready = await api.client.get("/readyz")
    assert ready.status_code == 200 and ready.json()["checks"]["schema"] == "0001"
    await api.get("/v1/series")
    metrics = (await api.client.get("/metrics")).text
    assert 'mv_http_requests_total{method="GET",route="/v1/series",status="200"}' in metrics


async def test_readyz_fails_when_schema_is_behind(api: Api) -> None:
    async with api.pool.acquire() as conn:
        await conn.execute("UPDATE alembic_version SET version_num = '0000'")
        try:
            r = await api.client.get("/readyz")
        finally:
            await conn.execute("UPDATE alembic_version SET version_num = '0001'")
    assert r.status_code == 503 and r.json()["checks"]["schema"] == "behind"


async def test_request_id_is_propagated_or_generated(api: Api) -> None:
    r = await api.get("/v1/series", headers={"X-Request-ID": "abc-123"})
    assert r.headers["x-request-id"] == "abc-123"
    r = await api.get("/v1/series", headers={"X-Request-ID": "bad id with spaces"})
    assert (
        r.headers["x-request-id"] != "bad id with spaces" and len(r.headers["x-request-id"]) == 32
    )


async def test_openapi_lists_every_route(api: Api) -> None:
    spec = (await api.client.get("/openapi.json")).json()
    assert set(spec["paths"]) >= {
        "/v1/series",
        "/v1/series/{code}",
        "/v1/series/{code}/observations",
        "/v1/series/{code}/observations/{period}/versions",
        "/v1/series/{code}/revisions",
        "/v1/series/{code}/runs",
        "/healthz",
        "/readyz",
    }
    schemes = spec["components"]["securitySchemes"]
    assert {"HTTPBearer", "APIKeyHeader"} <= set(schemes)


# ------------------------------------------------------------- catalogue


async def test_catalogue_pagination_and_filters(api: Api) -> None:
    for i in range(5):
        await _series(api, f"ecb.cat.s{i}")
    sid = await _series(api, "estat.cat.m", freq="M")
    await api.write(sid, [obs(date(2026, 1, 1), "1"), obs(date(2026, 2, 1), "2")])

    seen, cursor = [], None
    while True:
        r = await api.get(
            "/v1/series", params={"limit": 2, **({"cursor": cursor} if cursor else {})}
        )
        assert r.status_code == 200
        seen += [s["code"] for s in r.json()["data"]]
        cursor = r.json()["next_cursor"]
        if cursor is None:
            break
    assert seen == sorted(seen) and len(seen) == 6

    monthly = (await api.get("/v1/series", params={"frequency": "M"})).json()["data"]
    assert [s["code"] for s in monthly] == ["estat.cat.m"]
    assert monthly[0]["first_period"] == "2026-01" and monthly[0]["observations"] == 2
    one = (await api.get("/v1/series/estat.cat.m")).json()
    assert one["last_period"] == "2026-02" and one["data_version"] == 1
    assert (await api.get("/v1/series/nope")).status_code == 404
    assert (await api.get("/v1/series", params={"source": "imf"})).status_code == 422


# ------------------------------------------------------------ observations


async def test_observations_as_of_and_versions(api: Api) -> None:
    sid = await _series(api, "estat.gdp.q", freq="Q")
    q1, q2 = date(2026, 1, 1), date(2026, 4, 1)
    t1 = await api.write(sid, [obs(q1, "0.1", "p"), obs(q2, "0.6", "e")])
    t2 = await api.write(sid, [obs(q1, "0.2"), obs(q2, "0.6", "e")])

    now = (await api.get("/v1/series/estat.gdp.q/observations")).json()
    assert [(d["period"], d["value"], d["status"]) for d in now["data"]] == [
        ("2026-Q1", "0.2", None),
        ("2026-Q2", "0.6", "e"),
    ]

    then = (
        await api.get(
            "/v1/series/estat.gdp.q/observations",
            params={"as_of": (t1 + (t2 - t1) / 2).isoformat()},
        )
    ).json()
    assert [(d["period"], d["value"]) for d in then["data"]] == [
        ("2026-Q1", "0.1"),
        ("2026-Q2", "0.6"),
    ]
    before = (
        await api.get(
            "/v1/series/estat.gdp.q/observations",
            params={"as_of": (t1 - timedelta(seconds=1)).isoformat()},
        )
    ).json()
    assert before["data"] == []

    hist = (await api.get("/v1/series/estat.gdp.q/observations/2026-Q1/versions")).json()
    assert [(v["value"], v["status"], v["known_to"] is None) for v in hist["versions"]] == [
        ("0.1", "p", False),
        ("0.2", None, True),
    ]
    assert (
        await api.get("/v1/series/estat.gdp.q/observations/2027-Q1/versions")
    ).status_code == 404
    assert (
        await api.get("/v1/series/estat.gdp.q/observations/2026-02/versions")
    ).status_code == 422


@pytest.mark.parametrize(
    ("params", "code"),
    [
        ({"start": "2026-13-01"}, "#invalid-period"),
        ({"start": "2026-03-01", "end": "2026-01-01"}, "#invalid-range"),
        ({"as_of": "2026-01-01T00:00:00"}, "#invalid-as-of"),
        ({"as_of": "2999-01-01T00:00:00Z"}, "#invalid-as-of"),
        ({"limit": "0"}, "#invalid-request"),
        ({"limit": "1001"}, "#invalid-request"),
        ({"as_of": "yesterday"}, "#invalid-request"),
        ({"cursor": "garbage"}, "#invalid-cursor"),
    ],
)
async def test_observation_validation_errors(api: Api, params: dict[str, str], code: str) -> None:
    await _series(api)
    r = await api.get("/v1/series/ecb.test.daily/observations", params=params)
    assert r.status_code in (400, 422) and r.json()["type"].endswith(code), r.json()


async def test_cursor_cannot_be_reused_with_other_filters(api: Api) -> None:
    sid = await _series(api)
    await api.write(sid, daily(10))
    page = (await api.get("/v1/series/ecb.test.daily/observations", params={"limit": 3})).json()
    r = await api.get(
        "/v1/series/ecb.test.daily/observations",
        params={"limit": 3, "cursor": page["next_cursor"], "start": "2026-01-05"},
    )
    assert r.status_code == 400 and r.json()["type"].endswith("#invalid-cursor")


@settings(
    max_examples=25, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(
    limit=st.integers(min_value=1, max_value=40),
    start=st.integers(min_value=0, max_value=60),
    span=st.integers(min_value=0, max_value=80),
)
async def test_keyset_pages_cover_the_range_exactly_once(
    api: Api, limit: int, start: int, span: int
) -> None:
    async with api.pool.acquire() as conn:
        sid = await conn.fetchval("SELECT id FROM series WHERE code = 'ecb.page.d'")
    if sid is None:
        sid = await _series(api, "ecb.page.d")
        await api.write(sid, daily(100))
    lo = date(2026, 1, 1) + timedelta(days=start)
    hi = lo + timedelta(days=span)
    params: dict[str, Any] = {"limit": limit, "start": lo.isoformat(), "end": hi.isoformat()}
    got: list[str] = []
    for _ in range(200):
        body = (await api.get("/v1/series/ecb.page.d/observations", params=params)).json()
        got += [d["period"] for d in body["data"]]
        if body["next_cursor"] is None:
            break
        params["cursor"] = body["next_cursor"]
    last = min(hi, date(2026, 1, 1) + timedelta(days=99))
    expected = [(lo + timedelta(days=i)).isoformat() for i in range((last - lo).days + 1)]
    assert got == expected


async def test_etag_304_and_invalidation(api: Api) -> None:
    sid = await _series(api)
    await api.write(sid, daily(3))
    url = "/v1/series/ecb.test.daily/observations"
    first = await api.get(url)
    etag = first.headers["etag"]
    assert etag.startswith('W/"')
    cached = await api.get(url, headers={"If-None-Match": etag})
    assert cached.status_code == 304 and cached.content == b""
    other_query = await api.get(url, params={"limit": 2}, headers={"If-None-Match": etag})
    assert other_query.status_code == 200

    await api.write(sid, daily(3, value="2.0"))  # a revision bumps data_version
    changed = await api.get(url, headers={"If-None-Match": etag})
    assert changed.status_code == 200 and changed.headers["etag"] != etag
    assert {d["value"] for d in changed.json()["data"]} == {"2.0"}


async def test_revision_feed(api: Api) -> None:
    sid = await _series(api)
    t0 = datetime.now(UTC)
    await api.write(sid, daily(4))
    t1 = await api.write(sid, daily(3, value="1.5"))  # 3 revised, 1 withdrawn
    await api.write(sid, daily(1, value="1.7"), complete=False)  # 1 more revision

    feed = (
        await api.get(
            "/v1/series/ecb.test.daily/revisions", params={"since": t0.isoformat(), "limit": 2}
        )
    ).json()
    items = feed["data"]
    cursor = feed["next_cursor"]
    while cursor:
        page = (
            await api.get(
                "/v1/series/ecb.test.daily/revisions",
                params={"since": t0.isoformat(), "limit": 2, "cursor": cursor},
            )
        ).json()
        items += page["data"]
        cursor = page["next_cursor"]
    kinds = [(i["kind"], i["period"], i["previous_value"], i["value"]) for i in items]
    assert kinds == [
        ("revised", "2026-01-01", "1.0", "1.5"),
        ("revised", "2026-01-02", "1.0", "1.5"),
        ("revised", "2026-01-03", "1.0", "1.5"),
        ("withdrawn", "2026-01-04", "1.0", None),
        ("revised", "2026-01-01", "1.5", "1.7"),
    ]
    later = (
        await api.get("/v1/series/ecb.test.daily/revisions", params={"since": t1.isoformat()})
    ).json()["data"]
    assert [i["value"] for i in later] == ["1.7"]
    assert (
        await api.get(
            "/v1/series/ecb.test.daily/revisions", params={"since": "2026-01-01T00:00:00"}
        )
    ).status_code == 422


async def test_runs_endpoint(api: Api) -> None:
    sid = await _series(api)
    await api.write(sid, daily(2))
    runs = (await api.get("/v1/series/ecb.test.daily/runs")).json()["data"]
    assert len(runs) == 1 and runs[0]["status"] == "running" and runs[0]["mode"] == "full"


async def test_unknown_route_uses_problem_json(api: Api) -> None:
    r = await api.get("/v2/nothing")
    assert r.status_code == 404 and r.headers["content-type"] == PROBLEM
    r = await api.client.post("/v1/series", headers={"X-API-Key": api.key})
    assert r.status_code == 405 and r.json()["type"].endswith("#method-not-allowed")
