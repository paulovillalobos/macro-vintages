"""HTTP routes. Each handler does at most three queries on one pooled
connection: the key check (auth + rate limit), the series lookup (which also
answers conditional requests), and the data query."""

from __future__ import annotations

import math
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime
from typing import Annotated, Any

import asyncpg
from fastapi import APIRouter, Depends, Query, Request, Response
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from macro_vintages import metrics
from macro_vintages.api import schemas
from macro_vintages.api.errors import ApiError
from macro_vintages.api.pagination import (
    decode_cursor,
    encode_cursor,
    etag_for,
    etag_matches,
    fingerprint,
)
from macro_vintages.db import Conn
from macro_vintages.periods import Frequency, PeriodError, format_period, parse_period
from macro_vintages.store.keys import check_and_consume

router = APIRouter()
ops = APIRouter()

_bearer = HTTPBearer(auto_error=False, description="`Authorization: Bearer <key>`")
_header = APIKeyHeader(name="X-API-Key", auto_error=False, description="`X-API-Key: <key>`")

Limit = Annotated[int, Query(ge=1, le=1000, description="Page size")]
CursorQ = Annotated[
    str | None, Query(max_length=512, description="next_cursor from the previous page")
]


async def get_conn(request: Request) -> AsyncIterator[Conn]:
    pool: asyncpg.Pool[Any] = request.app.state.pool
    async with pool.acquire() as conn:
        yield conn


async def require_key(
    request: Request,
    response: Response,
    conn: Annotated[Conn, Depends(get_conn)],
    bearer: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
    header_key: Annotated[str | None, Depends(_header)],
) -> str:
    key = header_key or (bearer.credentials if bearer else None)
    if not key:
        raise ApiError(
            401,
            "missing-api-key",
            "send the key as X-API-Key or a Bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    check = await check_and_consume(conn, key)
    if check is None:
        raise ApiError(
            401,
            "invalid-api-key",
            "unknown or revoked API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    request.scope.setdefault("state", {})["api_key_name"] = check.name
    if not check.allowed:
        metrics.RATE_LIMITED.inc()
        retry = max(1, math.ceil(check.retry_after_s))
        raise ApiError(
            429,
            "rate-limited",
            f"rate limit exceeded; retry in {retry}s",
            headers={"Retry-After": str(retry), "RateLimit-Remaining": "0"},
        )
    response.headers["RateLimit-Remaining"] = str(math.floor(check.remaining))
    return check.name


Auth = Annotated[str, Depends(require_key)]
DbConn = Annotated[Conn, Depends(get_conn)]


async def _series(conn: Conn, code: str) -> asyncpg.Record:
    row: asyncpg.Record | None = await conn.fetchrow(
        "SELECT id, code, frequency, data_version FROM series WHERE code = $1", code
    )
    if row is None:
        raise ApiError(404, "series-not-found", f"no series {code!r}")
    return row


def _not_modified(request: Request, response: Response, etag: str) -> Response | None:
    if etag_matches(request.headers.get("if-none-match"), etag):
        return Response(status_code=304, headers={"ETag": etag})
    response.headers["ETag"] = etag
    response.headers["Cache-Control"] = "private, no-cache"
    return None


def _parse_bound(label: str | None, freq: Frequency, name: str) -> date | None:
    if label is None:
        return None
    try:
        return parse_period(label, freq)
    except PeriodError as exc:
        raise ApiError(422, "invalid-period", f"{name}: {exc}") from exc


def _validate_as_of(as_of: datetime | None) -> datetime | None:
    if as_of is None:
        return None
    if as_of.tzinfo is None:
        raise ApiError(422, "invalid-as-of", "as_of needs a timezone, e.g. 2026-10-20T12:00:00Z")
    if as_of > datetime.now(UTC):
        # The future is not fixed: answering now and differently later would make
        # the same URL return different "historical" data.
        raise ApiError(422, "invalid-as-of", "as_of is in the future")
    return as_of


_SERIES_SQL = """
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


def _series_out(row: asyncpg.Record) -> schemas.SeriesOut:
    freq = Frequency(row["frequency"])
    return schemas.SeriesOut(
        code=row["code"],
        source=row["source"],
        source_key=row["source_key"],
        title=row["title"],
        unit=row["unit"],
        frequency=row["frequency"],
        first_period=format_period(row["first_period"], freq) if row["first_period"] else None,
        last_period=format_period(row["last_period"], freq) if row["last_period"] else None,
        observations=row["observations"],
        data_version=row["data_version"],
        data_changed_at=row["data_changed_at"],
        last_success_at=row["last_success_at"],
        ingest_state=row["ingest_state"],
    )


@router.get("/v1/series", response_model=schemas.SeriesPage, tags=["series"])
async def list_series(
    _: Auth,
    conn: DbConn,
    source: Annotated[str | None, Query(pattern="^(ecb|eurostat)$")] = None,
    frequency: Annotated[str | None, Query(pattern="^[DMQA]$")] = None,
    limit: Limit = 100,
    cursor: CursorQ = None,
) -> schemas.SeriesPage:
    """The series catalogue, ordered by code."""
    fp = fingerprint(("series", source, frequency))
    after = decode_cursor(cursor, fp)["code"] if cursor else ""
    rows = await conn.fetch(
        _SERIES_SQL + " WHERE s.code > $1 AND ($2::text IS NULL OR s.source = $2)"
        " AND ($3::text IS NULL OR s.frequency = $3) ORDER BY s.code LIMIT $4",
        after,
        source,
        frequency,
        limit + 1,
    )
    page = rows[:limit]
    next_cursor = encode_cursor({"code": page[-1]["code"]}, fp) if len(rows) > limit else None
    return schemas.SeriesPage(data=[_series_out(r) for r in page], next_cursor=next_cursor)


@router.get("/v1/series/{code}", response_model=schemas.SeriesOut, tags=["series"])
async def get_series(_: Auth, conn: DbConn, code: str) -> schemas.SeriesOut:
    row = await conn.fetchrow(_SERIES_SQL + " WHERE s.code = $1", code)
    if row is None:
        raise ApiError(404, "series-not-found", f"no series {code!r}")
    return _series_out(row)


_CURRENT_SQL = """
SELECT period, value, status, known_from
FROM observation_version
WHERE series_id = $1 AND known_to IS NULL
  AND period >= $2 AND period <= $3 AND period > $4
ORDER BY period
LIMIT $5
"""

_AS_OF_SQL = """
SELECT period, value, status, known_from
FROM observation_version
WHERE series_id = $1
  AND period >= $2 AND period <= $3 AND period > $4
  AND known_from <= $6 AND (known_to IS NULL OR known_to > $6)
ORDER BY period
LIMIT $5
"""


@router.get(
    "/v1/series/{code}/observations",
    response_model=schemas.ObservationPage,
    tags=["observations"],
    responses={304: {"description": "Not modified (If-None-Match matched)"}},
)
async def observations(
    request: Request,
    response: Response,
    _: Auth,
    conn: DbConn,
    code: str,
    start: Annotated[str | None, Query(description="First period, e.g. 2024-01 or 2024-Q1")] = None,
    end: Annotated[str | None, Query(description="Last period (inclusive)")] = None,
    as_of: Annotated[
        datetime | None,
        Query(description="Return what was known at this instant (ISO 8601 with timezone)"),
    ] = None,
    limit: Limit = 100,
    cursor: CursorQ = None,
) -> schemas.ObservationPage | Response:
    """Observations in period order; with ``as_of``, the values as known then."""
    as_of = _validate_as_of(as_of)
    series = await _series(conn, code)
    freq = Frequency(series["frequency"])
    lo = _parse_bound(start, freq, "start") or date.min
    hi = _parse_bound(end, freq, "end") or date.max
    if lo > hi:
        raise ApiError(422, "invalid-range", "start is after end")
    fp = fingerprint((code, lo, hi, as_of.isoformat() if as_of else None))
    etag = etag_for((fp, series["data_version"], limit, cursor))
    if (cached := _not_modified(request, response, etag)) is not None:
        return cached
    after = date.min
    if cursor:
        after = date.fromisoformat(decode_cursor(cursor, fp)["p"])

    if as_of is None:
        rows = await conn.fetch(_CURRENT_SQL, series["id"], lo, hi, after, limit + 1)
    else:
        rows = await conn.fetch(_AS_OF_SQL, series["id"], lo, hi, after, limit + 1, as_of)
    page = rows[:limit]
    next_cursor = (
        encode_cursor({"p": page[-1]["period"].isoformat()}, fp) if len(rows) > limit else None
    )
    return schemas.ObservationPage(
        series=code,
        frequency=freq.value,
        as_of=as_of,
        data=[
            schemas.ObservationOut(
                period=format_period(r["period"], freq),
                value=r["value"],
                status=r["status"],
                known_from=r["known_from"],
            )
            for r in page
        ],
        next_cursor=next_cursor,
    )


@router.get(
    "/v1/series/{code}/observations/{period}/versions",
    response_model=schemas.VersionHistory,
    tags=["observations"],
    responses={304: {"description": "Not modified (If-None-Match matched)"}},
)
async def versions(
    request: Request, response: Response, _: Auth, conn: DbConn, code: str, period: str
) -> schemas.VersionHistory | Response:
    """Every value this engine has held for one period, oldest first."""
    series = await _series(conn, code)
    freq = Frequency(series["frequency"])
    start = _parse_bound(period, freq, "period")
    etag = etag_for(("versions", code, start, series["data_version"]))
    if (cached := _not_modified(request, response, etag)) is not None:
        return cached
    rows = await conn.fetch(
        "SELECT value, status, known_from, known_to, run_id FROM observation_version "
        "WHERE series_id = $1 AND period = $2 ORDER BY known_from",
        series["id"],
        start,
    )
    if not rows:
        raise ApiError(404, "period-not-found", f"{code} has no observation for {period}")
    return schemas.VersionHistory(
        series=code,
        period=format_period(start, freq) if start else period,
        versions=[schemas.VersionOut(**dict(r)) for r in rows],
    )


_REVISIONS_SQL = """
SELECT p.period, p.value AS previous_value, p.status AS previous_status,
       p.known_to AS revised_at, n.value, n.status, n.run_id, n.id IS NOT NULL AS has_successor
FROM observation_version p
LEFT JOIN observation_version n
       ON n.series_id = p.series_id AND n.period = p.period AND n.known_from = p.known_to
WHERE p.series_id = $1
  AND p.known_to IS NOT NULL
  AND p.known_to > $2
  AND (p.known_to, p.period) > ($3, $4)
ORDER BY p.known_to, p.period
LIMIT $5
"""


@router.get(
    "/v1/series/{code}/revisions",
    response_model=schemas.RevisionPage,
    tags=["observations"],
    responses={304: {"description": "Not modified (If-None-Match matched)"}},
)
async def revisions(
    request: Request,
    response: Response,
    _: Auth,
    conn: DbConn,
    code: str,
    since: Annotated[datetime | None, Query(description="Only changes after this instant")] = None,
    limit: Limit = 100,
    cursor: CursorQ = None,
) -> schemas.RevisionPage | Response:
    """Changes to already-published observations, in the order they were recorded.

    Polling this feed with ``since`` = the last ``revised_at`` seen is how a
    downstream consumer keeps a copy in sync."""
    if since is not None and since.tzinfo is None:
        raise ApiError(422, "invalid-since", "since needs a timezone")
    series = await _series(conn, code)
    freq = Frequency(series["frequency"])
    floor = since or datetime(1970, 1, 1, tzinfo=UTC)
    fp = fingerprint(("revisions", code, floor.isoformat()))
    etag = etag_for((fp, series["data_version"], limit, cursor))
    if (cached := _not_modified(request, response, etag)) is not None:
        return cached
    after_t, after_p = floor, date.min
    if cursor:
        key = decode_cursor(cursor, fp)
        after_t, after_p = datetime.fromisoformat(key["t"]), date.fromisoformat(key["p"])
    rows = await conn.fetch(_REVISIONS_SQL, series["id"], floor, after_t, after_p, limit + 1)
    page = rows[:limit]
    next_cursor = (
        encode_cursor(
            {"t": page[-1]["revised_at"].isoformat(), "p": page[-1]["period"].isoformat()}, fp
        )
        if len(rows) > limit
        else None
    )
    return schemas.RevisionPage(
        series=code,
        data=[
            schemas.RevisionOut(
                kind="revised" if r["has_successor"] else "withdrawn",
                period=format_period(r["period"], freq),
                previous_value=r["previous_value"],
                value=r["value"],
                previous_status=r["previous_status"],
                status=r["status"],
                revised_at=r["revised_at"],
                run_id=r["run_id"],
            )
            for r in page
        ],
        next_cursor=next_cursor,
    )


@router.get("/v1/series/{code}/runs", response_model=schemas.RunList, tags=["ingest"])
async def runs(
    _: Auth,
    conn: DbConn,
    code: str,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> schemas.RunList:
    """Most recent ingest runs for a series (newest first)."""
    series = await _series(conn, code)
    rows = await conn.fetch(
        "SELECT id, mode, status, worker, started_at, finished_at, http_requests, "
        "rows_received, rows_inserted, rows_revised, rows_withdrawn, not_modified, error "
        "FROM ingest_run WHERE series_id = $1 ORDER BY id DESC LIMIT $2",
        series["id"],
        limit,
    )
    return schemas.RunList(series=code, data=[schemas.RunOut(**dict(r)) for r in rows])


@ops.get("/healthz", response_model=schemas.Health, tags=["ops"])
async def healthz() -> schemas.Health:
    """Liveness: the process is serving requests. Does not touch the database."""
    return schemas.Health(status="ok")


@ops.get(
    "/readyz",
    response_model=schemas.Health,
    tags=["ops"],
    responses={503: {"description": "Not ready"}},
)
async def readyz(request: Request) -> schemas.Health:
    """Readiness: the database answers and the schema is at the expected revision."""
    pool: asyncpg.Pool[Any] = request.app.state.pool
    expected: str = request.app.state.schema_head
    try:
        async with pool.acquire(timeout=2) as conn:
            current = await conn.fetchval("SELECT version_num FROM alembic_version")
    except (OSError, TimeoutError, asyncpg.PostgresError) as exc:
        raise ApiError(
            503,
            "not-ready",
            f"database unavailable: {type(exc).__name__}",
            extra={"checks": {"database": "down"}},
        ) from exc
    if current != expected:
        raise ApiError(
            503,
            "not-ready",
            f"schema at {current}, expected {expected}",
            extra={"checks": {"database": "ok", "schema": "behind"}},
        )
    return schemas.Health(status="ok", checks={"database": "ok", "schema": str(current)})


@ops.get("/metrics", tags=["ops"], include_in_schema=False)
async def prometheus_metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
