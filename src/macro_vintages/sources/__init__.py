"""Source adapters and the factory that wires them to one shared HTTP client."""

from __future__ import annotations

import httpx

from macro_vintages.config import Settings
from macro_vintages.sources.base import Source
from macro_vintages.sources.ecb import EcbSource
from macro_vintages.sources.eurostat import EurostatSource
from macro_vintages.sources.http import RetryPolicy

USER_AGENT = "macro-vintages/0.1 (+https://github.com/imperiobaal69-web/macro-vintages)"


def http_client(settings: Settings) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(settings.http_timeout_seconds, connect=10.0),
        # A handful of connections per host is plenty and polite to public APIs.
        limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
    )


def build_sources(client: httpx.AsyncClient, settings: Settings) -> dict[str, Source]:
    policy = RetryPolicy(max_retries=settings.http_max_retries)
    return {
        "ecb": EcbSource(client, settings.ecb_base_url, policy),
        "eurostat": EurostatSource(client, settings.eurostat_base_url, policy),
    }
