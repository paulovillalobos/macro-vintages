"""Opaque keyset cursors and weak ETags.

A cursor carries the last key returned plus a fingerprint of the query that
produced it. Reusing a cursor with different filters is rejected instead of
silently returning a page of some other result set.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
from collections.abc import Iterable
from typing import Any

from macro_vintages.api.errors import ApiError


def fingerprint(parts: Iterable[object]) -> str:
    raw = "\x1f".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def encode_cursor(key: dict[str, Any], query_fp: str) -> str:
    payload = json.dumps({"k": key, "q": query_fp}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def decode_cursor(cursor: str, query_fp: str) -> dict[str, Any]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode()))
        key, fp = payload["k"], payload["q"]
    except (ValueError, KeyError, TypeError, binascii.Error) as exc:
        raise ApiError(400, "invalid-cursor", "cursor is malformed") from exc
    if fp != query_fp or not isinstance(key, dict):
        raise ApiError(400, "invalid-cursor", "cursor belongs to a different query")
    return key


def etag_for(parts: Iterable[object]) -> str:
    return f'W/"{fingerprint(parts)}"'


def etag_matches(if_none_match: str | None, etag: str) -> bool:
    if not if_none_match:
        return False
    if if_none_match.strip() == "*":
        return True
    # Weak comparison (RFC 9110 8.8.3.2): ignore the W/ prefix on both sides.
    wanted = etag.removeprefix("W/")
    return any(tag.strip().removeprefix("W/") == wanted for tag in if_none_match.split(","))
