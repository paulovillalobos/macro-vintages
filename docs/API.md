# API

Base path `/v1`. Every `/v1` route needs an API key, sent as `X-API-Key: <key>`
or `Authorization: Bearer <key>`. Keys are created with
`macro-vintages create-key --name <name> [--rate R] [--burst B]`; the
plaintext is printed once and only its SHA-256 digest is stored.

The OpenAPI schema is served at `/openapi.json` and rendered at `/docs`.

## Conventions

**Periods.** Labels follow the series frequency: `2026-10-20` (D),
`2026-09` (M), `2026-Q2` (Q), `2026` (A). They are accepted in `start`, `end`
and the `{period}` path segment and returned in the same form.

**Values** are JSON strings holding the decimal exactly as the source
published it (`"1.1208"`), or `null`. `status` carries the source's
observation flag when there is one.

**Time.** `known_from` / `known_to` are the instants this engine started and
stopped holding a value (transaction time). `as_of` and `since` must carry a
timezone; `as_of` may not be in the future, because a future instant has no
fixed answer yet.

**Pagination** is keyset-based. A response with more rows carries
`next_cursor`; pass it back as `cursor` with the same other parameters. A
cursor encodes a fingerprint of the query that produced it, and is rejected
(`invalid-cursor`) when reused with different filters. `limit` is 1–1000,
default 100.

**Conditional GET.** Observation, version and revision responses carry a weak
`ETag` derived from the series' `data_version`, which increments in the same
transaction as any data change. Sending it back in `If-None-Match` returns
`304 Not Modified` without reading observations.

**Rate limits.** Each key has a token bucket (`rate_per_s`, `burst`) held on
its database row, so the limit is shared by every API replica. Responses carry
`RateLimit-Remaining`; a refused request gets `429` with `Retry-After`.

**Request ids.** Every response has `X-Request-Id` (echoed from the request if
it is a safe token, otherwise generated). The same id is in the JSON logs and
in error bodies.

## Endpoints

| Method and path | Returns |
|---|---|
| `GET /v1/series?source=&frequency=&limit=&cursor=` | Catalogue page, ordered by code, with per-series counts, first/last period, `data_version`, ingest state |
| `GET /v1/series/{code}` | One series |
| `GET /v1/series/{code}/observations?start=&end=&as_of=&limit=&cursor=` | Observations in period order: current values, or with `as_of` the values held at that instant |
| `GET /v1/series/{code}/observations/{period}/versions` | Every value held for one period, oldest first, with `known_from`, `known_to` and the ingest run that wrote it |
| `GET /v1/series/{code}/revisions?since=&limit=&cursor=` | Changes to already-held observations, in the order they were recorded: `revised` (with old and new value) or `withdrawn` |
| `GET /v1/series/{code}/runs?limit=` | Most recent ingest runs (mode, status, worker, row counts, error) |
| `GET /healthz` | Process is up (no key, no database) |
| `GET /readyz` | Database reachable and schema at the head revision; `503` otherwise |
| `GET /metrics` | Prometheus text format (no key) |

To keep a downstream copy in sync, poll `/revisions?since=<last revised_at
seen>` and `/observations?start=<last period seen>`.

## Errors

Errors are `application/problem+json` (RFC 9457):

```json
{
  "type": "https://github.com/imperiobaal69-web/macro-vintages/blob/main/docs/API.md#series-not-found",
  "title": "Not found",
  "status": 404,
  "detail": "no series 'ecb.exr.xxx'",
  "instance": "/v1/series/ecb.exr.xxx",
  "request_id": "5f0c..."
}
```

The `type` links to one of the sections below.

### missing-api-key
401. No `X-API-Key` header and no bearer token.

### invalid-api-key
401. The key is unknown or was revoked (`macro-vintages revoke-key <prefix>`).

### rate-limited
429. The key's bucket is empty. `Retry-After` says when one token will be
available again.

### invalid-request
422. A query parameter failed validation (type, range, pattern). `errors`
lists each failure with its location.

### invalid-period
422. `start`, `end` or `{period}` is not a valid label for the series'
frequency.

### invalid-range
422. `start` is after `end`.

### invalid-as-of
422. `as_of` has no timezone or lies in the future.

### invalid-since
422. `since` has no timezone.

### invalid-cursor
400. The cursor is malformed, or belongs to a request with different
parameters.

### series-not-found
404. No series with that code.

### period-not-found
404. The series has never held a value for that period.

### not-found
404. No such route.

### method-not-allowed
405. The route exists but not for this method (the API is read-only).

### http-error
Any other HTTP error raised by the framework, with its own status.

### internal-error
500. An unexpected server error. The detail is generic; the logs carry the
traceback under the same `request_id`.
