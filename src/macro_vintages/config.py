"""Runtime settings, read once from environment variables (prefix ``MV_``)."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return default if raw is None or raw == "" else int(raw)


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return default if raw is None or raw == "" else float(raw)


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    log_level: str
    # API
    db_pool_min: int
    db_pool_max: int
    default_rate_per_s: float
    default_burst: int
    # Worker
    worker_concurrency: int
    lease_seconds: int
    poll_seconds: float
    full_refresh_seconds: int
    max_attempts: int
    http_timeout_seconds: float
    http_max_retries: int
    worker_metrics_port: int
    ecb_base_url: str
    eurostat_base_url: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            database_url=os.environ.get(
                "MV_DATABASE_URL", "postgresql://postgres@127.0.0.1:5441/macro_vintages"
            ),
            log_level=os.environ.get("MV_LOG_LEVEL", "INFO"),
            db_pool_min=_int("MV_DB_POOL_MIN", 2),
            db_pool_max=_int("MV_DB_POOL_MAX", 10),
            default_rate_per_s=_float("MV_DEFAULT_RATE_PER_S", 20.0),
            default_burst=_int("MV_DEFAULT_BURST", 40),
            worker_concurrency=_int("MV_WORKER_CONCURRENCY", 4),
            lease_seconds=_int("MV_LEASE_SECONDS", 120),
            poll_seconds=_float("MV_POLL_SECONDS", 5.0),
            full_refresh_seconds=_int("MV_FULL_REFRESH_SECONDS", 24 * 3600),
            max_attempts=_int("MV_MAX_ATTEMPTS", 5),
            http_timeout_seconds=_float("MV_HTTP_TIMEOUT_SECONDS", 30.0),
            http_max_retries=_int("MV_HTTP_MAX_RETRIES", 4),
            worker_metrics_port=_int("MV_WORKER_METRICS_PORT", 9100),
            ecb_base_url=os.environ.get(
                "MV_ECB_BASE_URL", "https://data-api.ecb.europa.eu/service/data"
            ),
            eurostat_base_url=os.environ.get(
                "MV_EUROSTAT_BASE_URL",
                "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data",
            ),
        )
