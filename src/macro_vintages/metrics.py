"""Prometheus metrics. Route labels use the route template, never the raw path,
so ``/v1/series/{code}`` stays one time series however many codes exist."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

HTTP_REQUESTS = Counter("mv_http_requests_total", "API requests", ["method", "route", "status"])
HTTP_LATENCY = Histogram(
    "mv_http_request_duration_seconds",
    "API request latency",
    ["method", "route"],
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
RATE_LIMITED = Counter("mv_rate_limited_total", "Requests rejected by the rate limiter")

SOURCE_REQUESTS = Counter(
    "mv_source_requests_total", "HTTP requests to data sources", ["source", "outcome"]
)
INGEST_RUNS = Counter("mv_ingest_runs_total", "Finished ingest runs", ["source", "status"])
INGEST_ROWS = Counter("mv_ingest_rows_total", "Observation versions written", ["source", "kind"])
INGEST_DURATION = Histogram(
    "mv_ingest_run_duration_seconds",
    "Ingest run duration (claim to commit)",
    ["source"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)
JOBS_IN_FLIGHT = Gauge("mv_ingest_jobs_in_flight", "Jobs currently held by this worker")
