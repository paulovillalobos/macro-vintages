from __future__ import annotations

from datetime import date

import pytest
from hypothesis import given
from hypothesis import strategies as st

from macro_vintages.periods import (
    Frequency,
    PeriodError,
    format_period,
    is_period_start,
    parse_period,
)

dates = st.dates(min_value=date(1900, 1, 1), max_value=date(2100, 12, 31))


def _start(d: date, freq: Frequency) -> date:
    if freq is Frequency.DAILY:
        return d
    if freq is Frequency.MONTHLY:
        return d.replace(day=1)
    if freq is Frequency.QUARTERLY:
        return date(d.year, 3 * ((d.month - 1) // 3) + 1, 1)
    return date(d.year, 1, 1)


@given(d=dates, freq=st.sampled_from(list(Frequency)))
def test_format_then_parse_round_trips(d: date, freq: Frequency) -> None:
    start = _start(d, freq)
    assert is_period_start(start, freq)
    assert parse_period(format_period(start, freq), freq) == start


@given(a=dates, b=dates, freq=st.sampled_from(list(Frequency)))
def test_period_order_matches_label_order(a: date, b: date, freq: Frequency) -> None:
    # Keyset pagination orders by the stored date; labels must sort the same way.
    sa, sb = _start(a, freq), _start(b, freq)
    assert (sa < sb) == (format_period(sa, freq) < format_period(sb, freq))


@pytest.mark.parametrize(
    ("label", "freq", "expected"),
    [
        ("2026-10-20", Frequency.DAILY, date(2026, 10, 20)),
        ("2026-09", Frequency.MONTHLY, date(2026, 9, 1)),
        ("2026-Q2", Frequency.QUARTERLY, date(2026, 4, 1)),
        ("2026Q4", Frequency.QUARTERLY, date(2026, 10, 1)),
        ("2025", Frequency.ANNUAL, date(2025, 1, 1)),
        (" 2026-09 ", Frequency.MONTHLY, date(2026, 9, 1)),
    ],
)
def test_parse_known_labels(label: str, freq: Frequency, expected: date) -> None:
    assert parse_period(label, freq) == expected


@pytest.mark.parametrize(
    ("label", "freq"),
    [
        ("2026-Q2", Frequency.MONTHLY),
        ("2026-09", Frequency.QUARTERLY),
        ("2026-13", Frequency.MONTHLY),
        ("2026-02-30", Frequency.DAILY),
        ("2026-Q5", Frequency.QUARTERLY),
        ("26-09", Frequency.MONTHLY),
        ("", Frequency.ANNUAL),
    ],
)
def test_parse_rejects_mismatched_or_invalid(label: str, freq: Frequency) -> None:
    with pytest.raises(PeriodError):
        parse_period(label, freq)


def test_is_period_start_rejects_mid_period_dates() -> None:
    assert not is_period_start(date(2026, 5, 1), Frequency.QUARTERLY)
    assert not is_period_start(date(2026, 1, 2), Frequency.MONTHLY)
    assert not is_period_start(date(2026, 2, 1), Frequency.ANNUAL)
