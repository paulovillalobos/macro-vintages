"""ECB Data Portal (SDMX 2.1 REST), CSV representation.

Behaviour this adapter relies on, all observed on 2026-10-20:

* ``format=csvdata&detail=dataonly`` returns ``KEY,<dimensions>,TIME_PERIOD,OBS_VALUE``
  (no status attributes) and is about half the size of the full CSV.
* ``updatedAfter=<timestamp>`` returns only observations changed after that
  instant. When nothing changed the API answers **404** with an
  ``application/problem+json`` body ("No Series was returned"), not 304 and
  not an empty 200. ``If-Modified-Since`` is ignored (always 200).
* A key that does not exist is also 404 with the same body. The adapter
  therefore only reads 404 as "not modified" on incremental fetches, which run
  after a full fetch has validated the key; the daily full refresh turns a
  series that disappeared into a permanent error.
* A valid key with no data in the requested window is 200 with an empty body.
* ``includeHistory=true`` returns a single version (the current one), so earlier
  vintages cannot be backfilled from this API.
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, timedelta
from decimal import Decimal, InvalidOperation

import httpx

from macro_vintages.periods import Frequency, PeriodError, parse_period
from macro_vintages.sources.base import (
    FetchRequest,
    FetchResult,
    SchemaDriftError,
    SourceObservation,
)
from macro_vintages.sources.http import RetryPolicy, get_with_retry

# Changes published while our previous request was in flight could carry an
# updatedAfter-relevant timestamp slightly before the moment we asked. Re-reading
# a small overlap costs a few rows; writes are idempotent, so overlap is free.
CURSOR_OVERLAP = timedelta(minutes=10)


class EcbSource:
    def __init__(self, client: httpx.AsyncClient, base_url: str, policy: RetryPolicy) -> None:
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._policy = policy

    async def fetch(self, request: FetchRequest) -> FetchResult:
        flow, _, key = request.source_key.partition("/")
        if not flow or not key:
            raise SchemaDriftError(f"bad ECB source key {request.source_key!r}")
        params = {"format": "csvdata", "detail": "dataonly"}
        incremental = not request.full and "updated_after" in request.cursor
        if incremental:
            params["updatedAfter"] = str(request.cursor["updated_after"])
        response, attempts = await get_with_retry(
            self._client,
            f"{self._base_url}/{flow}/{key}",
            params=params,
            source="ecb",
            policy=self._policy,
        )
        next_cursor = {
            "updated_after": (request.started_at.astimezone(UTC) - CURSOR_OVERLAP).isoformat()
        }
        if response.status_code == 404:
            if incremental:
                return FetchResult(
                    [],
                    complete=False,
                    cursor=next_cursor,
                    http_requests=attempts,
                    not_modified=True,
                )
            raise SchemaDriftError(
                f"ECB has no series {flow}.{key}: {response.text[:200]}", http_status=404
            )
        if not response.content.strip():
            if incremental:
                return FetchResult(
                    [],
                    complete=False,
                    cursor=next_cursor,
                    http_requests=attempts,
                    not_modified=True,
                )
            raise SchemaDriftError(f"ECB returned an empty body for {flow}.{key}")
        content_type = response.headers.get("content-type", "")
        if "csv" not in content_type:
            raise SchemaDriftError(f"expected CSV from ECB, got {content_type!r}")
        observations = parse_csv(response.text, f"{flow}.{key}", Frequency(request.frequency))
        return FetchResult(
            observations,
            complete=not incremental,
            cursor=next_cursor,
            http_requests=attempts,
        )


def parse_csv(text: str, expected_key: str, frequency: Frequency) -> list[SourceObservation]:
    reader = csv.DictReader(io.StringIO(text))
    required = {"KEY", "TIME_PERIOD", "OBS_VALUE"}
    if reader.fieldnames is None or not required <= set(reader.fieldnames):
        raise SchemaDriftError(f"ECB CSV header {reader.fieldnames!r} lacks {sorted(required)}")
    out: list[SourceObservation] = []
    seen: set[str] = set()
    for line_no, row in enumerate(reader, start=2):
        if row["KEY"] != expected_key:
            raise SchemaDriftError(f"line {line_no}: series {row['KEY']!r} != {expected_key!r}")
        label = row["TIME_PERIOD"]
        if label in seen:
            raise SchemaDriftError(f"line {line_no}: duplicate period {label}")
        seen.add(label)
        try:
            period = parse_period(label, frequency)
        except PeriodError as exc:
            raise SchemaDriftError(f"line {line_no}: {exc}") from exc
        raw = (row["OBS_VALUE"] or "").strip()
        value: Decimal | None
        if raw in ("", "NaN"):
            value = None
        else:
            try:
                value = Decimal(raw)
            except InvalidOperation as exc:
                raise SchemaDriftError(f"line {line_no}: bad value {raw!r}") from exc
        out.append(SourceObservation(period, value, (row.get("OBS_STATUS") or None)))
    return out
