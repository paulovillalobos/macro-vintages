"""Types shared by the source adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class SourceObservation:
    period: date
    value: Decimal | None
    status: str | None = None


@dataclass(frozen=True, slots=True)
class FetchRequest:
    source_key: str
    frequency: str
    cursor: dict[str, Any]
    full: bool
    started_at: datetime


@dataclass(slots=True)
class FetchResult:
    observations: list[SourceObservation]
    # True when the payload is the whole series: periods missing from it were
    # withdrawn at the source. Incremental payloads only carry changed periods.
    complete: bool
    # Incremental state to persist if (and only if) the write commits.
    cursor: dict[str, Any]
    http_requests: int = 1
    # The source reported nothing new since the cursor.
    not_modified: bool = False
    source_updated: str | None = None
    notes: list[str] = field(default_factory=list)


class SourceError(Exception):
    """Base class. ``retryable`` decides between backoff and the dead-letter path."""

    retryable = False

    def __init__(self, message: str, *, http_status: int | None = None) -> None:
        super().__init__(message)
        self.http_status = http_status


class RetryableSourceError(SourceError):
    retryable = True


class PermanentSourceError(SourceError):
    retryable = False


class SchemaDriftError(PermanentSourceError):
    """The response parsed, but does not look like the series we asked for."""


class Source(Protocol):
    async def fetch(self, request: FetchRequest) -> FetchResult: ...
