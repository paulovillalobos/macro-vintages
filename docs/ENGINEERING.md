# Engineering notes

Why the system is built the way it is, what was measured, and what turned out
differently from the first guess. Numbers come from `docs/results/`, which
the scripts in `scripts/` regenerate.

## 1. Storage: one bitemporal table

`observation_version` has one row per value the engine has held for one
observation:

* valid time is `period`, the `date` the period starts. One sortable
  column works for every frequency; the label (`2026-Q2`) is derived on output.
* transaction time is `[known_from, known_to)`, when this engine held the
  value. `known_to IS NULL` marks the current version.

A revision sets `known_to` on the current row and inserts the new row with
`known_from` equal to that same instant. Nothing else is ever updated, so the
table is append-only apart from closing a row once. An `as_of=T` read is one
predicate: `known_from <= T AND (known_to IS NULL OR known_to > T)`.

**Overlaps are impossible, not merely unlikely.** An exclusion constraint
(`btree_gist`) forbids two rows for the same `(series_id, period)` with
overlapping `tstzrange(known_from, known_to)`. That is the invariant every
as-of answer depends on, so the database enforces it, whatever the
application does. It adds about 40% to the write path (580 ms with it, 417 ms
without, for 5,000 inserts followed by 5,000 revisions). Writes are rare and
reads are not, so the constraint stays.

**Three indexes, each for one query shape**, measured with and without
(`docs/results/explain.md`, 280k rows):

| Index | Serves | With | Without |
|---|---|---:|---:|
| `observation_current` (partial `known_to IS NULL`, covering) | latest values; the writer's own lookups | 0.013 ms | 0.097 ms on a densely revised series |
| `observation_history` (`series_id, period, known_from`, covering) | as-of pages, version history of a period | 0.017 ms | 1.19 ms |
| `observation_superseded` (partial `known_to IS NOT NULL`) | revision feed "since T" | 0.64 ms | 0.92 ms |

`observation_current` adds little for plain latest reads (the history index
can serve them), but it is unique. That restates "one current version per
observation" in a second place, and it keeps the latest-value query flat
however many times a series is revised.

**Denormalised catalogue stats (migration 0002).** The catalogue endpoint
first counted current observations and their period bounds per series on
every request: 13.1 ms per page. Those three numbers change only when the
writer changes data, so the writer now maintains them on the `series` row in
the same transaction, and a page costs 0.29 ms. The migration backfills
existing rows. The property test checks the stored stats against a full
recount after every generated history.

## 2. Write path

`apply_observations` diffs a payload against the current versions in two
statements: close the rows whose value or status changed (or that are
missing from a complete payload), then insert rows for periods with no
current version. Unchanged rows are not touched, so a re-run of the same
payload writes nothing and does not bump `data_version`. Equality is numeric
(`1.10` equals `1.1`), and a status-only change counts as a revision.

**The timestamp is read after the lock.** The caller holds the series' job
row `FOR UPDATE`. The version timestamp is `clock_timestamp()` read after
that lock is granted, not `now()`. `now()` is the transaction's start: a
transaction that started earlier but waited for the lock would close the
previous writer's row at an instant before that row began
(`known_to < known_from`). `CHECK (known_to > known_from)` rejects it, and the
run fails instead of committing. `test_transaction_time_is_read_after_the_series_lock`
reproduces the interleaving, and the mutation check confirms the test fails
when the code uses `now()`.

## 3. The queue is a table

There is one `ingest_job` row per series. It holds the schedule
(`next_run_at`, `interval_s` with ±10% jitter), the lease and the source
cursor. A broker would add a second system whose state must agree with the
database. Here the claim, the fencing check and the data write sit in the
same database, so consistency between them comes from PostgreSQL's own
locks and transactions. At ten series, or ten thousand, the queue's load is
negligible: the claim query takes 0.034 ms over 5,041 jobs, using the partial
index `ingest_job_due`.

Lifecycle:

1. Claim (short transaction). `FOR UPDATE SKIP LOCKED` on the next due
   active job, then stamp a new random `lease_token`, `lease_owner` and
   `lease_expires_at`, and open an `ingest_run` row.
2. Fetch with no transaction and no pooled connection held. A slow
   source cannot pin row locks or exhaust the pool. The fetch is cut off at
   80% of the lease.
3. Finish (short transaction). Lock the job row, compare the token with
   the one claimed (**fencing**), write versions, reset attempts, schedule
   the next run, close the run row. If the token has changed, the lease
   expired and another worker took the job: the late result is discarded,
   and the run is recorded as `lease_lost`.

A SIGKILLed worker leaves a lease that simply expires. The next claimant
marks the dead worker's open run `abandoned`, so `ingest_run` shows what
happened rather than a run that is "running" forever.

Failures back off at the job level with equal jitter (60 s, 120 s, 240 s …,
capped at the series interval). After `MV_MAX_ATTEMPTS` the job is parked
and a `dead_letter` row keeps the last error; `macro-vintages requeue <code>`
reactivates it. HTTP retries inside one run are a separate, inner layer
(see [DATA_SOURCES.md](DATA_SOURCES.md)).

**Migrations** take a PostgreSQL advisory lock around Alembic, so every
container can run `migrate` on start. Two processes racing on an empty
database serialise; the second finds the head already applied
(`test_concurrent_migrate_processes_serialise_on_the_advisory_lock`).

## 4. API

* Async end to end: FastAPI on asyncpg, one pool per uvicorn worker.
  The request middleware is plain ASGI rather than `BaseHTTPMiddleware`,
  which wraps every response in an extra task and stream.
* Keyset pagination on the index order (period, or `(known_to, period)`
  for the revision feed). Deep pages cost the same as the first: 0.017 ms
  for both as-of pages in the benchmark. Cursors are opaque and carry a
  fingerprint of their query. Reusing one with other filters is a `400`,
  not a silently wrong page.
* ETags come from `series.data_version`, which the writer increments in
  the same transaction as the data. A matching `If-None-Match` returns `304`
  after one primary-key lookup. In the load test, 94% of conditional
  requests were 304s, even with a writer active.
* `as_of` in the future is refused. Otherwise the same URL would
  return different "historical" answers before and after the instant.
* API keys are 32 random bytes, stored as SHA-256 digests. A slow KDF
  protects low-entropy passwords; for random keys it would only add latency
  to every request.
* Rate limiting is a token bucket on the key's row, consumed by one
  `UPDATE` that locks the row, refills it by elapsed time and spends a token.
  All API replicas share it with no extra infrastructure. A read-then-write
  limiter lets concurrent requests spend the same token. The test fires 300
  requests from 30 connections at a bucket of 25, and exactly 25 pass.
* Errors are RFC 9457 problem documents whose `type` links to
  [API.md](API.md), and they carry the request id.
* Operations: `/healthz` (process), `/readyz` (database and schema
  head, so a replica built for a newer schema reports not-ready until the
  migration has run), `/metrics` (Prometheus: requests by route template,
  latency, ingest runs and rows, source HTTP calls, rate-limit refusals),
  and JSON logs with the request id. Workers stop on SIGTERM after their
  current job; the entry point `exec`s the command, so the signal reaches
  Python.

## 5. How correctness is tested

* Against a real PostgreSQL, never a mock. Locking, `SKIP LOCKED`,
  exclusion constraints and MVCC are exactly the behaviour under test.
* Race tests use OS processes. `tests/helpers/race_worker.py` runs the
  production `Worker` with a deliberately slow fake source. The tests start
  6 processes on the same instant, or run them continuously while the source
  changes every 250 ms, or SIGKILL one mid-fetch. They then check the
  invariants in the database and in a fetch log the processes append to:
  no double fetch, no overlapping runs, contiguous version chains, no stale
  overwrite, no lost update.
* Property test (Hypothesis): random sequences of payloads (insert,
  revise, withdraw, re-add, status-only changes). After each step the test
  records the expected snapshot, then checks that `as_of` reproduces every
  past snapshot exactly.
* Mutation check (`make mutation-check`): removes each safeguard in a
  temporary copy (`SKIP LOCKED`, the lease fence, the post-lock timestamp,
  the key row lock) and requires a test to fail each time. It also checks
  that the copy really shadows the installed package, so a "caught" cannot
  come from testing the unmutated code. One finding: the single-process
  claim test passes without `SKIP LOCKED`, because sequential claims never
  collide. Only the multi-process race test catches that mutation.
* Load test (`scripts/loadtest.py`): reads at a fixed past instant are
  compared with a snapshot while a writer commits revisions. 9,406 such
  reads, 0 mismatches.

CI runs all of it: lint, `mypy --strict`, tests with coverage (88%, lines
and branches), the mutation check, a short load test, and a container smoke test.

## 6. Performance findings

**The first load test measured the client, not the server.** With httpx
as the client, throughput *fell* as concurrency rose: 2,600 requests/s at 8
connections per process, 900 at 32. Falling rather than plateauing pointed
to contention, and the first suspect was the one-row token bucket: every
request updates its key's row. Giving each connection a different key
appeared to confirm it (2,586 vs 1,124 requests/s at 64 connections). But
that run also changed the number of client processes. Two further checks
ruled the key out:

* `synchronous_commit = off` for the API's connections, so a key update
  holds its row lock only until commit, not through the WAL flush: no change
  (968 vs 913 requests/s).
* ApacheBench against the same server, with **one** key: 4,162, 4,090 and
  3,946 requests/s at 16, 64 and 128 connections. The server was flat; the
  collapse was inside httpx's connection pool.

The load generator now uses one raw HTTP/1.1 connection per lane over asyncio
streams, and matches ApacheBench (about 4,000 requests/s at 32–128
connections in short runs). The recorded 30-second run gives 3,144
requests/s, p50 15.5 ms, p99 81 ms at 64 connections. The machine was also
running other work, which accounts for the gap to the short runs. The
`synchronous_commit` change was reverted because it bought nothing.

**Where the ceiling is.** During load, sampled `pg_stat_activity` shows
PostgreSQL mostly idle: one or two active backends, no lock waits. The four
uvicorn workers are CPU-bound in Python (routing, Pydantic validation and
JSON encoding of 100-row pages). More API replicas scale this linearly until
the database becomes the limit. Before that, the per-key `UPDATE` would
matter only for a single key above roughly the measured 4,000 requests/s.

## 7. Configuration

All settings are environment variables (`src/macro_vintages/config.py`):

| Variable | Default | |
|---|---|---|
| `MV_DATABASE_URL` | `postgresql://postgres@127.0.0.1:5441/macro_vintages` | |
| `MV_LOG_LEVEL` | `INFO` | JSON logs |
| `MV_DB_POOL_MIN` / `MV_DB_POOL_MAX` | 2 / 10 | per API process |
| `MV_DEFAULT_RATE_PER_S` / `MV_DEFAULT_BURST` | 20 / 40 | for new keys |
| `MV_WORKER_CONCURRENCY` | 4 | claim loops per worker process |
| `MV_LEASE_SECONDS` | 120 | fetch is cut at 80% of it |
| `MV_POLL_SECONDS` | 5 | idle poll, jittered ×0.5–1.5 |
| `MV_FULL_REFRESH_SECONDS` | 86400 | complete re-download (detects withdrawals) |
| `MV_MAX_ATTEMPTS` | 5 | then parked in `dead_letter` |
| `MV_HTTP_TIMEOUT_SECONDS` / `MV_HTTP_MAX_RETRIES` | 30 / 4 | |
| `MV_WORKER_METRICS_PORT` | 9100 | 0 disables |
| `MV_ECB_BASE_URL` / `MV_EUROSTAT_BASE_URL` | public APIs | |

## 8. Known gaps

* History starts at first ingest. Neither source serves past vintages
  (section on the ECB in [DATA_SOURCES.md](DATA_SOURCES.md)). ALFRED-style
  vintage backfill would need a different source.
* Transaction time is not release time. A revision is dated when a
  worker saw it, up to one polling interval after publication. Recording the
  publisher's own timestamp (Eurostat's `updated`) as a third time axis would
  close most of that gap.
* Eurostat is polled by full download, since it offers no validators. At
  ten series that is about 75 KB every three hours; at thousands it would
  call for the bulk download service.
* The compose file has not been run with `docker compose`, which is not
  installed on the development machine. The same topology (PostgreSQL, API,
  two workers on one network) was run with `docker run`, ingesting the live
  series. The CI workflow's two jobs were run locally step by step; the
  workflow itself has not yet run on GitHub.
* One PostgreSQL. There is no read replica or failover. The API reads
  are single statements and could move to a replica, but the per-request key
  `UPDATE` would have to stay on the primary or move out of the database.
