# Data sources

Both adapters are written against what the APIs did when probed, not only
against their documentation. `scripts/probe_sources.py` re-runs every probe
below against the live APIs and writes
[results/probe.md](results/probe.md) (last run 2026-10-20, all held). Run it
again (`make probe`) before trusting these notes on a later date.

## ECB Data Portal

`https://data-api.ecb.europa.eu/service/data/{flow}/{key}`, SDMX 2.1 REST.

**Request.** `format=csvdata&detail=dataonly`: a CSV with `KEY`, the
dimensions, `TIME_PERIOD` and `OBS_VALUE`, about half the size of the full
CSV. The daily USD/EUR rate is 408 KB for 7,180 rows (1999-01-04 onwards).

**Change detection.** `updatedAfter=<instant>` returns only observations
changed after that instant (a 3-day window returned 2 rows). The adapter
keeps `updated_after` as its cursor, set to the start of the previous fetch
minus 10 minutes. The overlap costs a few rows and catches changes published
while the previous request was in flight. Writes are idempotent, so the
re-read rows change nothing.

**Pitfalls the adapter handles:**

* With nothing new, `updatedAfter` answers **404** with an
  `application/problem+json` body, not `304` and not an empty `200`.
  An unknown key answers the same 404. The adapter reads 404 as
  "not modified" only on incremental fetches, which run after a full fetch
  has proved the key exists. On a full fetch, a 404 is schema drift: the job
  fails and, if the series stays gone, parks.
* `If-Modified-Since` is ignored: a date in 2099 still gets `200` and the
  full body. So the cursor is the only change signal.
* A valid key with no data in the window is `200` with an empty body.
* `includeHistory=true` returns only the current version
  (`validFrom 2026-07-15` for euro-area GDP). Past vintages cannot be
  backfilled from this API, so history here starts at the first ingest.
* The HICP series in flow `ICP` (ECOICOP 1) stopped at 2025-12. HICP is
  taken from Eurostat instead (below).

Incremental payloads are partial, so they never withdraw anything
(`complete=False`). The daily full refresh (`MV_FULL_REFRESH_SECONDS`) is a
complete payload: a period missing from it is closed as withdrawn.

## Eurostat dissemination API

`https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}?<filters>`,
JSON-stat 2.0.

**Change detection.** There is none at the HTTP level: no `ETag`, no
`Last-Modified`, no gzip, and every call returns the whole selection. The
document's dataset-level `updated` timestamp is the only signal. The adapter
stores it as the cursor and skips the database write when it has not moved.
The download still happens, at 20–25 KB per series here.

**Pitfalls the adapter handles:**

* **Euro-area code changed.** Bulgaria joined the euro on 2026-01-01.
  `une_rt_m` dropped `EA20`, and `geo=EA20` now answers **200 with a
  zero-sized `geo` dimension and no values**, not an error. A zero-sized
  dimension is treated as schema drift, never as "no data". The catalogue
  uses `EA21` (GDP, unemployment) and `EA` (HICP).
* **HICP dataset replaced.** `prc_hicp_manr` stops at 2025-12; the ECOICOP
  ver. 2 dataset `prc_hicp_minr` (filter `coicop18=TOTAL`) continues
  (last period 2026-09 at the time of the probe).
* **An unknown filter value answers 413** `ASYNCHRONOUS_RESPONSE`
  ("try again later"), and the same request later answers 200 with an empty
  dimension (17 s later in the probe). 413 is retried as Eurostat documents;
  the empty dimension then fails as drift.
* An unknown dataset is a 404 with a JSON error body, treated as permanent.
* Values are JSON numbers. They are parsed straight to `Decimal`
  (`parse_float=Decimal`), so `0.1` is stored as `0.1`, not as the nearest
  binary float.

Every Eurostat payload is complete (`complete=True`), so an observation that
disappears from the selection is recorded as withdrawn.

## Retries (both sources)

Transport errors, timeouts, `408`, `413`, `425`, `429` and `5xx` are retried up
to `MV_HTTP_MAX_RETRIES` times. The delay is exponential with full jitter
(uniform in `[0, min(20 s, 0.5 s · 2^attempt)]`), or `Retry-After` when the
response sends it. Other 4xx are permanent. 404 goes back to the adapter,
because for the ECB it can mean "nothing changed". A run that still fails is
recorded on `ingest_run`; the job backs off (60 s, 120 s, 240 s … capped at
its interval) and is parked in `dead_letter` after `MV_MAX_ATTEMPTS`.
Permanent errors park it at once.

The worker identifies itself with a `User-Agent` naming the project, and keeps
at most 8 connections across both hosts.
