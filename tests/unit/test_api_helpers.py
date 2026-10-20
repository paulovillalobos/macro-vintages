from __future__ import annotations

import json
import logging
import random

import pytest
from hypothesis import given
from hypothesis import strategies as st

from macro_vintages.api.errors import ApiError
from macro_vintages.api.pagination import (
    decode_cursor,
    encode_cursor,
    etag_for,
    etag_matches,
    fingerprint,
)
from macro_vintages.logs import JsonFormatter, request_id_var
from macro_vintages.store.jobs import failure_delay, next_delay
from macro_vintages.store.keys import KeyCheck, generate_key, hash_key


@given(
    key=st.dictionaries(st.text(min_size=1, max_size=5), st.text(max_size=20), max_size=3),
    fp=st.text(min_size=1, max_size=16),
)
def test_cursor_round_trips(key: dict[str, str], fp: str) -> None:
    assert decode_cursor(encode_cursor(key, fp), fp) == key


def test_cursor_from_another_query_is_rejected() -> None:
    cursor = encode_cursor({"p": "2026-01-01"}, fingerprint(("a", 1)))
    with pytest.raises(ApiError) as info:
        decode_cursor(cursor, fingerprint(("a", 2)))
    assert info.value.status == 400 and info.value.code == "invalid-cursor"


@pytest.mark.parametrize("cursor", ["", "!!!", "bm90IGpzb24", encode_cursor({"p": 1}, "q")[:-3]])
def test_garbage_cursor_is_rejected(cursor: str) -> None:
    with pytest.raises(ApiError):
        decode_cursor(cursor, "q")


def test_etag_weak_comparison() -> None:
    tag = etag_for(("x", 1))
    assert tag.startswith('W/"')
    assert etag_matches(tag, tag)
    assert etag_matches(tag.removeprefix("W/"), tag)
    assert etag_matches(f'"other", {tag}', tag)
    assert etag_matches("*", tag)
    assert not etag_matches(None, tag)
    assert not etag_matches(etag_for(("x", 2)), tag)


def test_keys_are_random_and_hashes_fixed_length() -> None:
    a, b = generate_key(), generate_key()
    assert a != b and a.startswith("mv_") and len(a) > 40
    assert len(hash_key(a)) == 32 and hash_key(a) == hash_key(a)


def test_retry_after_is_time_to_next_whole_token() -> None:
    assert KeyCheck(1, "k", False, 0.25, 0.5).retry_after_s == pytest.approx(1.5)
    assert KeyCheck(1, "k", True, 3.0, 0.5).retry_after_s == 0.0


def test_schedule_jitter_bounds() -> None:
    rng = random.Random(1)
    for _ in range(500):
        assert 3240 <= next_delay(3600, rng) <= 3960
    assert [failure_delay(n, 3600, random.Random(0)) <= 60 * 2 ** (n - 1) for n in (1, 2, 3)]
    assert all(failure_delay(10, 600, rng) <= 600 for _ in range(100))
    assert all(failure_delay(1, 600, rng) >= 30 for _ in range(100))


def test_json_log_lines_carry_request_id_and_extras() -> None:
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "hello %s", ("world",), None)
    record.series = "ecb.exr.usd_eur"
    token = request_id_var.set("req-1")
    try:
        line = json.loads(JsonFormatter().format(record))
    finally:
        request_id_var.reset(token)
    assert line["msg"] == "hello world"
    assert line["request_id"] == "req-1"
    assert line["series"] == "ecb.exr.usd_eur"
    assert line["level"] == "info"
