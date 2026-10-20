"""Re-run the probes behind docs/DATA_SOURCES.md and write docs/results/probe.md.

Every behaviour the adapters depend on is checked here against the live APIs,
so the documentation can be regenerated instead of trusted:

    uv run python scripts/probe_sources.py [--skip-async]

The asynchronous-413 probe uses a fresh random filter value so it always
starts from a cache miss, then polls until Eurostat stops answering 413.
"""

from __future__ import annotations

import argparse
import json
import random
import string
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

ECB = "https://data-api.ecb.europa.eu/service/data"
ESTAT = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
OUT = Path(__file__).resolve().parents[1] / "docs" / "results" / "probe.md"


@dataclass
class Probe:
    name: str
    request: str
    observed: str
    expectation_held: bool


def _csv_rows(text: str) -> list[list[str]]:
    lines = [line for line in text.splitlines() if line]
    return [line.split(",") for line in lines[1:]]


def run(client: httpx.Client, skip_async: bool) -> list[Probe]:
    probes: list[Probe] = []

    def add(name: str, request: str, check: Callable[[], tuple[str, bool]]) -> None:
        try:
            observed, ok = check()
        except Exception as exc:  # a probe failing is a result, not a crash
            observed, ok = f"probe error: {type(exc).__name__}: {exc}", False
        probes.append(Probe(name, request, observed, ok))
        print(f"[{'ok' if ok else 'CHANGED'}] {name}: {observed}")

    usd = f"{ECB}/EXR/D.USD.EUR.SP00.A"

    def full_csv() -> tuple[str, bool]:
        t = time.perf_counter()
        r = client.get(usd, params={"format": "csvdata", "detail": "dataonly"})
        rows = _csv_rows(r.text)
        header = r.text.splitlines()[0]
        return (
            f"{r.status_code}, {len(r.content):,} bytes in {time.perf_counter() - t:.2f}s, "
            f"{len(rows):,} rows {rows[0][6]}..{rows[-1][6]}; header `{header}`",
            r.status_code == 200 and header.endswith("TIME_PERIOD,OBS_VALUE"),
        )

    add(
        "ECB full series, CSV dataonly",
        "EXR/D.USD.EUR.SP00.A?format=csvdata&detail=dataonly",
        full_csv,
    )

    def not_modified() -> tuple[str, bool]:
        after = (datetime.now(UTC) - timedelta(seconds=5)).isoformat(timespec="seconds")
        r = client.get(usd, params={"format": "csvdata", "updatedAfter": after})
        return (
            f"{r.status_code} {r.headers.get('content-type')}: {r.text[:80]}...",
            r.status_code == 404,
        )

    add("ECB updatedAfter with nothing new", "updatedAfter=<now-5s>", not_modified)

    def recent_change() -> tuple[str, bool]:
        after = (datetime.now(UTC) - timedelta(days=3)).isoformat(timespec="seconds")
        r = client.get(
            usd, params={"format": "csvdata", "detail": "dataonly", "updatedAfter": after}
        )
        rows = _csv_rows(r.text) if r.status_code == 200 else []
        return (
            f"{r.status_code}, {len(rows)} changed observations "
            f"(periods {[x[6] for x in rows][:5]})",
            r.status_code in (200, 404),
        )

    add("ECB updatedAfter three days back", "updatedAfter=<now-3d>", recent_change)

    def ims() -> tuple[str, bool]:
        r = client.get(
            usd,
            params={"format": "csvdata", "lastNObservations": "1"},
            headers={"If-Modified-Since": "Tue, 20 Oct 2099 00:00:00 GMT"},
        )
        return f"{r.status_code} (If-Modified-Since in the future)", r.status_code == 200

    add("ECB ignores If-Modified-Since", "If-Modified-Since: <2099>", ims)

    def history() -> tuple[str, bool]:
        r = client.get(
            f"{ECB}/MNA/Q.Y.I9.W2.S1.S1.B.B1GQ._Z._Z._Z.EUR.LR.GY",
            params={"format": "jsondata", "includeHistory": "true", "startPeriod": "2025-Q1"},
        )
        sets = r.json()["dataSets"]
        return (
            f"{r.status_code}, {len(sets)} dataSet(s), validFrom "
            f"{[s.get('validFrom') for s in sets]}",
            len(sets) == 1,
        )

    add("ECB includeHistory keeps one version", "MNA GDP ...&includeHistory=true", history)

    def unknown_key() -> tuple[str, bool]:
        r = client.get(f"{ECB}/EXR/D.XXX.EUR.SP00.A", params={"format": "csvdata"})
        return f"{r.status_code} {r.headers.get('content-type')}", r.status_code == 404

    add("ECB unknown key", "EXR/D.XXX.EUR.SP00.A", unknown_key)

    def empty_window() -> tuple[str, bool]:
        r = client.get(
            usd,
            params={"format": "csvdata", "startPeriod": "1990-01-01", "endPeriod": "1990-12-31"},
        )
        return f"{r.status_code}, {len(r.content)} bytes", r.status_code == 200 and not r.content

    add(
        "ECB valid key, no data in window",
        "startPeriod=1990-01-01&endPeriod=1990-12-31",
        empty_window,
    )

    def icp_stopped() -> tuple[str, bool]:
        r = client.get(
            f"{ECB}/ICP/M.U2.N.000000.4.ANR",
            params={"format": "csvdata", "detail": "dataonly", "lastNObservations": "1"},
        )
        last = _csv_rows(r.text)[-1][7]
        return f"last period {last}", last == "2025-12"

    add(
        "ECB HICP (ICP, ECOICOP 1) stopped",
        "ICP/M.U2.N.000000.4.ANR?lastNObservations=1",
        icp_stopped,
    )

    gdp_q = {"geo": "EA21", "na_item": "B1GQ", "s_adj": "SCA", "unit": "CLV_PCH_PRE"}

    def estat_headers() -> tuple[str, bool]:
        r = client.get(f"{ESTAT}/namq_10_gdp", params=gdp_q, headers={"Accept-Encoding": "gzip"})
        d = r.json()
        names = {k.lower() for k in r.headers}
        return (
            f"{r.status_code}, updated={d.get('updated')}, ETag={'etag' in names}, "
            f"Last-Modified={'last-modified' in names}, "
            f"Content-Encoding={r.headers.get('content-encoding')}",
            "etag" not in names and "last-modified" not in names,
        )

    add("Eurostat has no validators", "namq_10_gdp?geo=EA21&...", estat_headers)

    def ea20_empty() -> tuple[str, bool]:
        r = client.get(
            f"{ESTAT}/une_rt_m",
            params={"geo": "EA20", "age": "TOTAL", "sex": "T", "s_adj": "SA", "unit": "PC_ACT"},
        )
        d = r.json()
        sizes = dict(zip(d.get("id", []), d.get("size", []), strict=False))
        return (
            f"{r.status_code}, geo size {sizes.get('geo')}, {len(d.get('value', {}))} values",
            r.status_code == 200 and sizes.get("geo") == 0,
        )

    add("Eurostat dropped EA20 silently", "une_rt_m?geo=EA20&...", ea20_empty)

    def unknown_dataset() -> tuple[str, bool]:
        r = client.get(f"{ESTAT}/nonexistent_xyz")
        return f"{r.status_code}: {r.text[:90]}", r.status_code == 404

    add("Eurostat unknown dataset", "nonexistent_xyz", unknown_dataset)

    def frozen() -> tuple[str, bool]:
        old = client.get(
            f"{ESTAT}/prc_hicp_manr", params={"geo": "EA", "coicop": "CP00", "lastTimePeriod": "1"}
        ).json()
        new = client.get(
            f"{ESTAT}/prc_hicp_minr",
            params={"geo": "EA", "coicop18": "TOTAL", "unit": "RCH_A", "lastTimePeriod": "1"},
        ).json()
        lo = list(old["dimension"]["time"]["category"]["index"])[-1]
        ln = list(new["dimension"]["time"]["category"]["index"])[-1]
        return f"prc_hicp_manr last {lo}; prc_hicp_minr last {ln}", lo == "2025-12" and ln > lo

    add("Eurostat old HICP dataset frozen", "prc_hicp_manr vs prc_hicp_minr", frozen)

    if not skip_async:
        code = "Q" + "".join(random.choices(string.ascii_uppercase, k=3))

        def async_413() -> tuple[str, bool]:
            seen: list[str] = []
            started = time.monotonic()
            while time.monotonic() - started < 600:
                r = client.get(f"{ESTAT}/une_rt_m", params={"geo": code})
                elapsed = time.monotonic() - started
                if r.status_code != 413:
                    d = r.json()
                    sizes = dict(zip(d.get("id", []), d.get("size", []), strict=False))
                    seen.append(f"{r.status_code} at {elapsed:.0f}s (geo size {sizes.get('geo')})")
                    break
                if not seen or not seen[-1].startswith("413"):
                    seen.append(f"413 at {elapsed:.0f}s")
                else:
                    seen[-1] = f"413 from 0s to {elapsed:.0f}s"
                time.sleep(15)
            return "; ".join(seen), bool(seen) and seen[0].startswith("413")

        add("Eurostat unknown filter value: 413, then 200 empty", f"une_rt_m?geo={code}", async_413)
    return probes


def write(probes: list[Probe]) -> None:
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Source probe results",
        "",
        f"Generated by `scripts/probe_sources.py` on {now}. "
        "`held` = the behaviour the adapters rely on was observed again.",
        "",
        "| Probe | Request | Observed | Held |",
        "|---|---|---|---|",
    ]
    for p in probes:
        observed = p.observed.replace("|", "\\|")
        held = "yes" if p.expectation_held else "**no**"
        lines.append(f"| {p.name} | `{p.request}` | {observed} | {held} |")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(lines) + "\n")
    print(f"wrote {OUT}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-async", action="store_true", help="skip the slow 413 probe")
    args = parser.parse_args()
    with httpx.Client(timeout=60, headers={"User-Agent": "macro-vintages-probe/0.1"}) as client:
        probes = run(client, args.skip_async)
    write(probes)
    print(json.dumps({"held": sum(p.expectation_held for p in probes), "total": len(probes)}))


if __name__ == "__main__":
    main()
