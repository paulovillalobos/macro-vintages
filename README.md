# macro-vintages

Ingests euro-area macro series from the ECB and Eurostat into PostgreSQL,
keeps every value the engine has ever held for every observation, and serves
the data over an async HTTP API. You can ask what a series says now, what it
said at any past instant (`as_of`), how one observation was revised, and
which observations changed since a given time.

Python 3.12, FastAPI, asyncpg, PostgreSQL 17. The job queue lives in
PostgreSQL too (`FOR UPDATE SKIP LOCKED`), so there is no broker.

## What it guarantees, and the test that checks it

| Guarantee | Enforced by | Checked by |
|---|---|---|
| Two versions of one observation are never valid at the same instant | exclusion constraint on `tstzrange(known_from, known_to)` | `test_database_rejects_overlapping_versions` |
| `as_of=T` returns exactly what the engine held at `T`, for every past `T` | write path closes and opens versions at one timestamp | property test `test_as_of_reconstructs_every_past_snapshot` (Hypothesis) |
| Re-ingesting unchanged data writes nothing | value/status diff before insert | `test_insert_unchanged_revise_withdraw` |
| No series is fetched or written twice by concurrent workers | `SKIP LOCKED` claim + lease token | `test_parallel_workers_never_double_ingest` (6 processes) |
| A worker whose lease expired cannot overwrite the new owner | lease token re-checked under the row lock (fencing) | `test_expired_lease_is_reclaimed_and_stale_writer_is_fenced` |
| Version chains stay contiguous and no update is lost while workers race | series lock, `clock_timestamp()` read after it | `test_continuous_workers_with_revisions_keep_history_linear` |
| A killed worker's job is picked up again | lease expiry | `test_killed_worker_job_is_recovered_after_lease_expiry` (SIGKILL) |
| An API key cannot spend more than its bucket, whatever the concurrency | one locked `UPDATE` per request | `test_concurrent_requests_cannot_overspend` (300 requests) |
| As-of answers do not move while new data is written | the above, end to end through HTTP | `scripts/loadtest.py` (0 mismatches in 9,406 reads) |

`make mutation-check` removes each of the four locking/fencing safeguards in
turn, in a copy of the repository, and confirms that a test fails every time.

## Quick start

Local, with Homebrew PostgreSQL 17 and [uv](https://docs.astral.sh/uv/):

```bash
make db-up        # PostgreSQL on 127.0.0.1:5441, data in .local/ (no Docker)
make test         # ruff, mypy --strict, 120 tests with coverage
make demo         # migrate, ingest the 10 live series, print an API key, serve on :8000
```

With Docker:

```bash
docker compose up --build -d                         # PostgreSQL, API, 2 workers
docker compose exec api macro-vintages create-key --name demo
```

Then:

```bash
KEY=mv_...   # the key printed above
curl -H "X-API-Key: $KEY" localhost:8000/v1/series
curl -H "X-API-Key: $KEY" "localhost:8000/v1/series/ecb.exr.usd_eur/observations?start=2026-10-01"
curl -H "X-API-Key: $KEY" "localhost:8000/v1/series/estat.gdp.ea21.qoq/observations?as_of=2026-10-20T12:00:00Z"
curl -H "X-API-Key: $KEY" localhost:8000/v1/series/estat.gdp.ea21.qoq/observations/2026-Q2/versions
curl -H "X-API-Key: $KEY" "localhost:8000/v1/series/estat.gdp.ea21.qoq/revisions?since=2026-10-01T00:00:00Z"
```

Interactive docs are at `/docs`; the endpoints and error codes are in
[docs/API.md](docs/API.md).

## How it works

```
             ┌──────────── PostgreSQL ────────────┐
 worker ──── │ ingest_job   (queue + schedule)    │ ──── API (FastAPI, N uvicorn workers)
 worker ──── │ ingest_run   (one row per attempt) │        /v1/series ...
   │         │ observation_version (bitemporal)   │        as_of, versions, revisions
   │         │ series, api_key, dead_letter       │
   ▼         └────────────────────────────────────┘
 ECB SDMX CSV · Eurostat JSON-stat
```

1. Claim. A worker slot takes one due job with `FOR UPDATE SKIP LOCKED`,
   stamps a fresh lease token and opens an `ingest_run` row. Short transaction.
2. Fetch. HTTP to the source with bounded retries and jitter, with no
   transaction open and no pooled connection held.
3. Finish. In one transaction: lock the job row, check the lease token is
   still ours, diff the payload against the current versions, close changed
   rows and insert new ones at a single `clock_timestamp()`, bump the series'
   `data_version`, schedule the next run.

Failures back off exponentially and park in `dead_letter` after
`MV_MAX_ATTEMPTS`; `macro-vintages requeue <code>` reactivates a parked job.
API reads of series data are indexed `SELECT`s under MVCC, so they never wait
on the workers' writes.

The design decisions, what was measured and what was found along the way are in
[docs/ENGINEERING.md](docs/ENGINEERING.md). Source quirks the adapters depend
on are in [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md).

## Measured

On an Apple M4 laptop (client, API and PostgreSQL on the same machine):

* Hot queries, from `EXPLAIN ANALYZE` on 280k version rows: latest page
  0.014 ms, as-of page 0.017 ms, revision feed 0.64 ms, job claim 0.034 ms
  ([docs/results/explain.md](docs/results/explain.md)).
* API under load, 64 connections against 4 uvicorn workers while a
  writer commits revisions underneath: 3,144 requests/s sustained over 30 s,
  p50 15.5 ms, p99 81 ms, no errors
  ([docs/results/loadtest.md](docs/results/loadtest.md)). The ceiling is Python
  CPU in the API workers; PostgreSQL stays mostly idle.

## Layout

```
src/macro_vintages/
  api/          FastAPI app, routes, problem+json errors, cursors and ETags, middleware
  sources/      ECB and Eurostat adapters, HTTP retry policy
  store/        versions (write path), jobs (queue), keys (auth + token bucket)
  worker.py     claim loop; cli.py: migrate | sync-catalogue | worker | api | create-key | ...
migrations/     Alembic, serialised by an advisory lock
tests/          unit, integration (real PostgreSQL), race (several OS processes)
scripts/        explain.py, loadtest.py, mutation_check.py, probe_sources.py, dev-db.sh
docs/           API, data sources, engineering notes, generated results
```

## Limits

* History starts when the engine first ingests a series. Neither API exposes
  earlier vintages (ECB `includeHistory` returns only the current one; Eurostat
  has no vintage endpoint), so `as_of` before the first run returns nothing.
* `as_of` is transaction time in this engine, not the publisher's release
  time. A revision is dated when a worker saw it, up to one polling interval
  (1–6 h, per series) after publication.
* Ten series are configured (`src/macro_vintages/catalogue.py`). The schema
  and the queue are built for thousands; the benchmarks use 5,000 jobs.

## License

MIT
