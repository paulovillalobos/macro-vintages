"""Eurostat dissemination API (JSON-stat 2.0).

Observed on 2026-10-20 (docs/DATA_SOURCES.md has the probe output):

* No ETag/Last-Modified and no gzip; every call returns the whole selection.
  The dataset-level ``updated`` timestamp is the only change signal, so the
  incremental mode downloads the series (20-25 KB here) and skips the database
  write when ``updated`` matches the stored cursor.
* A filter value that does not exist answers **413** ``ASYNCHRONOUS_RESPONSE``
  ("try again later") rather than 400. It is retried like any 413, and ends in
  the dead-letter table if it never succeeds.
* A filter value that *used to* exist can return **200 with a zero-sized
  dimension**: after 2026-01-01 ``une_rt_m?geo=EA20`` returns ``size [..., 0, ...]``
  and no values. That is treated as schema drift, never as "no data".
* Values arrive as JSON numbers; they are parsed straight to Decimal so 0.1
  stays 0.1 and is not stored as 0.1000000000000000055511151231257827.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any
from urllib.parse import parse_qsl

import httpx

from macro_vintages.periods import Frequency, PeriodError, parse_period
from macro_vintages.sources.base import (
    FetchRequest,
    FetchResult,
    PermanentSourceError,
    SchemaDriftError,
    SourceObservation,
)
from macro_vintages.sources.http import RetryPolicy, get_with_retry


class EurostatSource:
    def __init__(self, client: httpx.AsyncClient, base_url: str, policy: RetryPolicy) -> None:
        self._client = client
        self._base_url = base_url.rstrip("/")
        self._policy = policy

    async def fetch(self, request: FetchRequest) -> FetchResult:
        dataset, _, query = request.source_key.partition("?")
        params = dict(parse_qsl(query, strict_parsing=True)) if query else {}
        params.update({"format": "JSON", "lang": "EN"})
        response, attempts = await get_with_retry(
            self._client,
            f"{self._base_url}/{dataset}",
            params=params,
            source="eurostat",
            policy=self._policy,
        )
        if response.status_code == 404:
            raise PermanentSourceError(f"Eurostat 404: {response.text[:300]}", http_status=404)
        try:
            doc = json.loads(response.content, parse_float=Decimal)
        except ValueError as exc:
            raise SchemaDriftError(f"Eurostat body is not JSON: {exc}") from exc
        updated = doc.get("updated") if isinstance(doc, dict) else None
        if not request.full and updated is not None and request.cursor.get("updated") == updated:
            return FetchResult(
                [],
                complete=False,
                cursor={"updated": updated},
                http_requests=attempts,
                not_modified=True,
                source_updated=updated,
            )
        observations = parse_jsonstat(doc, Frequency(request.frequency))
        return FetchResult(
            observations,
            complete=True,
            cursor={"updated": updated} if updated else {},
            http_requests=attempts,
            source_updated=updated,
        )


def parse_jsonstat(doc: Any, frequency: Frequency) -> list[SourceObservation]:
    if not isinstance(doc, dict) or doc.get("class") != "dataset":
        raise SchemaDriftError("not a JSON-stat dataset")
    ids: list[str] = doc.get("id", [])
    sizes: list[int] = doc.get("size", [])
    if "time" not in ids or len(ids) != len(sizes):
        raise SchemaDriftError(f"unexpected JSON-stat dimensions {ids} / {sizes}")
    for dim, size in zip(ids, sizes, strict=True):
        if dim == "time":
            continue
        if size == 0:
            raise SchemaDriftError(
                f"dimension {dim!r} matched nothing: a filter value no longer exists"
            )
        if size != 1:
            raise SchemaDriftError(f"dimension {dim!r} has {size} members; expected one series")

    time_index: dict[str, int] = doc["dimension"]["time"]["category"]["index"]
    by_position = {pos: label for label, pos in time_index.items()}
    values: dict[str, Any] = doc.get("value") or {}
    statuses: dict[str, str] = doc.get("status") or {}
    if isinstance(values, list):  # JSON-stat allows a dense array; Eurostat sends a dict
        values = {str(i): v for i, v in enumerate(values) if v is not None}

    out: list[SourceObservation] = []
    for key in sorted(set(values) | set(statuses), key=int):
        label = by_position.get(int(key))
        if label is None:
            raise SchemaDriftError(f"value index {key} outside the time dimension")
        try:
            period = parse_period(label, frequency)
        except PeriodError as exc:
            raise SchemaDriftError(str(exc)) from exc
        raw = values.get(key)
        value = None if raw is None else Decimal(str(raw)) if isinstance(raw, int) else raw
        if value is not None and not isinstance(value, Decimal):
            raise SchemaDriftError(f"non-numeric value {raw!r} at {label}")
        out.append(SourceObservation(period, value, statuses.get(key)))
    return out
