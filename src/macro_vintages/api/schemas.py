"""Response models. Values are decimal strings: the store keeps exactly what the
source published (``numeric``), and a JSON float would not."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field


class SeriesOut(BaseModel):
    code: str
    source: Literal["ecb", "eurostat"]
    source_key: str
    title: str
    unit: str
    frequency: Literal["D", "M", "Q", "A"]
    first_period: str | None
    last_period: str | None
    observations: int = Field(description="Current (not superseded) observations")
    data_version: int = Field(description="Increments whenever any observation changes")
    data_changed_at: datetime | None
    last_success_at: datetime | None
    ingest_state: Literal["active", "parked"]


class SeriesPage(BaseModel):
    data: list[SeriesOut]
    next_cursor: str | None


class ObservationOut(BaseModel):
    period: str
    value: Decimal | None
    status: str | None
    known_from: datetime = Field(description="When this engine first held this value")


class ObservationPage(BaseModel):
    series: str
    frequency: str
    as_of: datetime | None = Field(description="Null means: current knowledge")
    data: list[ObservationOut]
    next_cursor: str | None


class VersionOut(BaseModel):
    value: Decimal | None
    status: str | None
    known_from: datetime
    known_to: datetime | None = Field(description="Null for the current version")
    run_id: int


class VersionHistory(BaseModel):
    series: str
    period: str
    versions: list[VersionOut]


class RevisionOut(BaseModel):
    kind: Literal["revised", "withdrawn"]
    period: str
    previous_value: Decimal | None
    value: Decimal | None
    previous_status: str | None
    status: str | None
    revised_at: datetime
    run_id: int | None = Field(description="Run that wrote the new value (null if withdrawn)")


class RevisionPage(BaseModel):
    series: str
    data: list[RevisionOut]
    next_cursor: str | None


class RunOut(BaseModel):
    id: int
    mode: str
    status: str
    worker: str
    started_at: datetime
    finished_at: datetime | None
    http_requests: int
    rows_received: int | None
    rows_inserted: int | None
    rows_revised: int | None
    rows_withdrawn: int | None
    not_modified: bool | None
    error: str | None


class RunList(BaseModel):
    series: str
    data: list[RunOut]


class Health(BaseModel):
    status: str
    checks: dict[str, str] = Field(default_factory=dict)
