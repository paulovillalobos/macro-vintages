"""Observation periods.

Every observation period is stored as the ``date`` on which it starts, together
with the series frequency. That keeps one sortable, indexable column for all
frequencies; the human label (``2026-Q2``, ``2026-09``) is derived on the way out.
"""

from __future__ import annotations

import re
from datetime import date
from enum import StrEnum


class Frequency(StrEnum):
    DAILY = "D"
    MONTHLY = "M"
    QUARTERLY = "Q"
    ANNUAL = "A"


_DAY = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_MONTH = re.compile(r"^(\d{4})-(\d{2})$")
_QUARTER = re.compile(r"^(\d{4})-?Q([1-4])$")
_YEAR = re.compile(r"^(\d{4})$")


class PeriodError(ValueError):
    pass


def parse_period(label: str, frequency: Frequency) -> date:
    """Parse a source period label into the first day of that period.

    The label must match the series frequency exactly; a monthly series never
    accepts ``2026-Q2``. This is the same function the API uses to validate
    ``start``/``end`` query parameters, so sources and clients agree.
    """
    label = label.strip()
    try:
        if frequency is Frequency.DAILY:
            m = _DAY.match(label)
            if m:
                return date(int(m[1]), int(m[2]), int(m[3]))
        elif frequency is Frequency.MONTHLY:
            m = _MONTH.match(label)
            if m:
                return date(int(m[1]), int(m[2]), 1)
        elif frequency is Frequency.QUARTERLY:
            m = _QUARTER.match(label)
            if m:
                return date(int(m[1]), 3 * (int(m[2]) - 1) + 1, 1)
        elif frequency is Frequency.ANNUAL:
            m = _YEAR.match(label)
            if m:
                return date(int(m[1]), 1, 1)
    except ValueError as exc:  # e.g. 2026-02-30
        raise PeriodError(f"invalid {frequency.name.lower()} period {label!r}: {exc}") from exc
    raise PeriodError(f"invalid {frequency.name.lower()} period {label!r}")


def format_period(start: date, frequency: Frequency) -> str:
    if frequency is Frequency.DAILY:
        return start.isoformat()
    if frequency is Frequency.MONTHLY:
        return f"{start.year:04d}-{start.month:02d}"
    if frequency is Frequency.QUARTERLY:
        return f"{start.year:04d}-Q{(start.month - 1) // 3 + 1}"
    return f"{start.year:04d}"


def is_period_start(start: date, frequency: Frequency) -> bool:
    """True when ``start`` is a valid first day for the frequency."""
    if frequency is Frequency.DAILY:
        return True
    if frequency is Frequency.MONTHLY:
        return start.day == 1
    if frequency is Frequency.QUARTERLY:
        return start.day == 1 and start.month in (1, 4, 7, 10)
    return start.day == 1 and start.month == 1
