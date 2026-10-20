"""Source adapters against recorded real responses (tests/fixtures) served by
httpx.MockTransport, so status codes, headers and bodies are the real ones."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest

from macro_vintages.periods import Frequency
from macro_vintages.sources.base import (
    FetchRequest,
    PermanentSourceError,
    RetryableSourceError,
    SchemaDriftError,
)
from macro_vintages.sources.ecb import CURSOR_OVERLAP, EcbSource, parse_csv
from macro_vintages.sources.eurostat import EurostatSource, parse_jsonstat
from macro_vintages.sources.http import RetryPolicy
from tests.conftest import fixture_path

NO_WAIT = RetryPolicy(max_retries=2, base_delay=0.0, max_delay=0.0)
STARTED = datetime(2026, 10, 20, 17, 0, tzinfo=UTC)
CSV = {"content-type": "text/csv"}
PROBLEM = {"content-type": "application/problem+json"}
JSON = {"content-type": "application/json"}


def _client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _req(
    key: str, freq: str = "D", *, full: bool = True, cursor: dict[str, Any] | None = None
) -> FetchRequest:
    return FetchRequest(key, freq, cursor or {}, full, STARTED)


def _fixture(name: str) -> bytes:
    return fixture_path(name).read_bytes()


# ---------------------------------------------------------------- ECB


def test_parse_real_ecb_csv() -> None:
    rows = parse_csv(
        _fixture("ecb_exr_usd_last30.csv").decode(), "EXR.D.USD.EUR.SP00.A", Frequency.DAILY
    )
    assert len(rows) == 30
    assert rows == sorted(rows, key=lambda r: r.period)
    assert all(isinstance(r.value, Decimal) for r in rows)
    assert rows[-1].period == date(2026, 10, 20)
    assert rows[-1].value == Decimal("1.1215")


def test_ecb_csv_key_mismatch_is_drift() -> None:
    with pytest.raises(SchemaDriftError, match="series"):
        parse_csv(
            _fixture("ecb_exr_usd_last30.csv").decode(), "EXR.D.GBP.EUR.SP00.A", Frequency.DAILY
        )


@pytest.mark.parametrize(
    ("body", "match"),
    [
        ("A,B\n1,2\n", "lacks"),
        ("KEY,TIME_PERIOD,OBS_VALUE\nK.X,2026-01-01,1\nK.X,2026-01-01,2\n", "duplicate"),
        ("KEY,TIME_PERIOD,OBS_VALUE\nK.X,2026-13-01,1\n", "invalid"),
        ("KEY,TIME_PERIOD,OBS_VALUE\nK.X,2026-01-01,abc\n", "bad value"),
    ],
)
def test_ecb_csv_rejects_malformed(body: str, match: str) -> None:
    with pytest.raises(SchemaDriftError, match=match):
        parse_csv(body, "K.X", Frequency.DAILY)


def test_ecb_csv_missing_values_become_null() -> None:
    body = "KEY,TIME_PERIOD,OBS_VALUE\nK.X,2026-01-01,\nK.X,2026-01-02,NaN\n"
    assert [r.value for r in parse_csv(body, "K.X", Frequency.DAILY)] == [None, None]


async def test_ecb_full_fetch_sends_dataonly_and_sets_cursor() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, content=_fixture("ecb_exr_usd_last30.csv"), headers=CSV)

    async with _client(handler) as client:
        result = await EcbSource(client, "https://ecb.test/data", NO_WAIT).fetch(
            _req("EXR/D.USD.EUR.SP00.A")
        )
    assert seen[0].url.path == "/data/EXR/D.USD.EUR.SP00.A"
    assert seen[0].url.params["detail"] == "dataonly"
    assert "updatedAfter" not in seen[0].url.params
    assert result.complete and len(result.observations) == 30
    assert datetime.fromisoformat(result.cursor["updated_after"]) == STARTED - CURSOR_OVERLAP


async def test_ecb_incremental_404_means_not_modified() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["updatedAfter"] == "2026-10-20T16:00:00+00:00"
        return httpx.Response(404, content=_fixture("ecb_not_modified_404.json"), headers=PROBLEM)

    async with _client(handler) as client:
        result = await EcbSource(client, "https://ecb.test/data", NO_WAIT).fetch(
            _req(
                "EXR/D.USD.EUR.SP00.A",
                full=False,
                cursor={"updated_after": "2026-10-20T16:00:00+00:00"},
            )
        )
    assert result.not_modified and not result.complete and result.observations == []


async def test_ecb_incremental_payload_is_not_complete() -> None:
    # Only changed periods come back; absence must not be read as withdrawal.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=_fixture("ecb_dfr_last5.csv"), headers=CSV)

    async with _client(handler) as client:
        result = await EcbSource(client, "https://ecb.test/data", NO_WAIT).fetch(
            _req("FM/D.U2.EUR.4F.KR.DFR.LEV", full=False, cursor={"updated_after": "x"})
        )
    assert not result.complete and len(result.observations) == 5


async def test_ecb_full_fetch_404_is_drift_not_empty() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, content=_fixture("ecb_unknown_key_404.json"), headers=PROBLEM)

    async with _client(handler) as client:
        with pytest.raises(SchemaDriftError, match="no series"):
            await EcbSource(client, "https://ecb.test/data", NO_WAIT).fetch(
                _req("EXR/D.XXX.EUR.SP00.A")
            )


@pytest.mark.parametrize("full", [True, False])
async def test_ecb_empty_body(full: bool) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"", headers=CSV)

    async with _client(handler) as client:
        source = EcbSource(client, "https://ecb.test/data", NO_WAIT)
        request = _req("EXR/D.USD.EUR.SP00.A", full=full, cursor={"updated_after": "x"})
        if full:
            with pytest.raises(SchemaDriftError, match="empty"):
                await source.fetch(request)
        else:
            assert (await source.fetch(request)).not_modified


async def test_ecb_html_with_200_is_drift() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, content=b"<html>maintenance</html>", headers={"content-type": "text/html"}
        )

    async with _client(handler) as client:
        with pytest.raises(SchemaDriftError, match="expected CSV"):
            await EcbSource(client, "https://ecb.test/data", NO_WAIT).fetch(
                _req("EXR/D.USD.EUR.SP00.A")
            )


async def test_ecb_bad_source_key() -> None:
    async with _client(lambda r: httpx.Response(500)) as client:
        with pytest.raises(SchemaDriftError):
            await EcbSource(client, "https://ecb.test", NO_WAIT).fetch(_req("no-slash"))


# ---------------------------------------------------------------- Eurostat


def test_parse_real_jsonstat_gdp_keeps_decimals_and_status() -> None:
    doc = json.loads(_fixture("eurostat_gdp_ea21.json"), parse_float=Decimal)
    rows = parse_jsonstat(doc, Frequency.QUARTERLY)
    assert len(rows) == 125
    by_period = {r.period: r for r in rows}
    assert by_period[date(2026, 4, 1)].value == Decimal("0.6")
    assert {r.status for r in rows} == {None, "e"}
    assert all(r.value is None or isinstance(r.value, Decimal) for r in rows)


def test_parse_real_jsonstat_zero_sized_dimension_is_drift() -> None:
    doc = json.loads(_fixture("eurostat_une_ea20_empty.json"), parse_float=Decimal)
    with pytest.raises(SchemaDriftError, match="'geo' matched nothing"):
        parse_jsonstat(doc, Frequency.MONTHLY)


def test_jsonstat_multi_member_dimension_is_drift() -> None:
    doc = json.loads(_fixture("eurostat_hicp_ea_last6.json"), parse_float=Decimal)
    doc["size"][doc["id"].index("geo")] = 2
    with pytest.raises(SchemaDriftError, match="2 members"):
        parse_jsonstat(doc, Frequency.MONTHLY)


def test_jsonstat_dense_array_and_status_without_value() -> None:
    doc = {
        "class": "dataset",
        "id": ["geo", "time"],
        "size": [1, 3],
        "dimension": {
            "geo": {},
            "time": {"category": {"index": {"2026-01": 0, "2026-02": 1, "2026-03": 2}}},
        },
        "value": [Decimal("1.5"), None, 2],
        "status": {"1": ":"},
    }
    rows = parse_jsonstat(doc, Frequency.MONTHLY)
    assert [(r.value, r.status) for r in rows] == [
        (Decimal("1.5"), None),
        (None, ":"),
        (Decimal("2"), None),
    ]


@pytest.mark.parametrize(
    "doc",
    [
        {"class": "collection"},
        {"class": "dataset", "id": ["geo"], "size": [1]},
        {
            "class": "dataset",
            "id": ["time"],
            "size": [1],
            "dimension": {"time": {"category": {"index": {"2026-01": 0}}}},
            "value": {"5": 1},
        },
        {
            "class": "dataset",
            "id": ["time"],
            "size": [1],
            "dimension": {"time": {"category": {"index": {"2026-Q1": 0}}}},
            "value": {"0": 1},
        },
        {
            "class": "dataset",
            "id": ["time"],
            "size": [1],
            "dimension": {"time": {"category": {"index": {"2026-01": 0}}}},
            "value": {"0": "x"},
        },
    ],
)
def test_jsonstat_rejects_malformed(doc: dict[str, Any]) -> None:
    with pytest.raises(SchemaDriftError):
        parse_jsonstat(doc, Frequency.MONTHLY)


async def test_eurostat_fetch_and_unchanged_updated_stamp() -> None:
    body = _fixture("eurostat_hicp_ea_last6.json")
    stamp = json.loads(body)["updated"]
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, content=body, headers=JSON)

    async with _client(handler) as client:
        source = EurostatSource(client, "https://estat.test/data", NO_WAIT)
        key = "prc_hicp_minr?coicop18=TOTAL&geo=EA&unit=RCH_A"
        first = await source.fetch(_req(key, "M"))
        again = await source.fetch(_req(key, "M", full=False, cursor={"updated": stamp}))
        forced = await source.fetch(_req(key, "M", full=True, cursor={"updated": stamp}))
    assert seen[0].url.params["geo"] == "EA" and seen[0].url.params["format"] == "JSON"
    assert first.complete and len(first.observations) == 6 and first.cursor == {"updated": stamp}
    assert again.not_modified and again.observations == []
    assert forced.complete and len(forced.observations) == 6


async def test_eurostat_async_413_is_retried_then_retryable() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(413, content=_fixture("eurostat_async_413.json"), headers=JSON)

    async with _client(handler) as client:
        with pytest.raises(RetryableSourceError, match="413"):
            await EurostatSource(client, "https://estat.test/data", NO_WAIT).fetch(
                _req("une_rt_m?geo=QQ", "M")
            )
    assert calls == NO_WAIT.max_retries + 1


async def test_eurostat_404_is_permanent() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404, content=_fixture("eurostat_unknown_dataset_404.json"), headers=JSON
        )

    async with _client(handler) as client:
        with pytest.raises(PermanentSourceError) as info:
            await EurostatSource(client, "https://estat.test/data", NO_WAIT).fetch(
                _req("nonexistent_xyz", "M")
            )
    assert info.value.http_status == 404 and not info.value.retryable


async def test_eurostat_non_json_is_drift() -> None:
    async with _client(lambda r: httpx.Response(200, content=b"<html/>")) as client:
        with pytest.raises(SchemaDriftError, match="not JSON"):
            await EurostatSource(client, "https://estat.test/data", NO_WAIT).fetch(_req("x", "M"))
