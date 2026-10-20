"""API keys: generation, hashing, and an atomic token-bucket check."""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass

from macro_vintages.db import Conn

KEY_PREFIX = "mv_"


def hash_key(key: str) -> bytes:
    return hashlib.sha256(key.encode()).digest()


def generate_key() -> str:
    return KEY_PREFIX + secrets.token_urlsafe(32)


@dataclass(frozen=True, slots=True)
class KeyCheck:
    key_id: int
    name: str
    allowed: bool
    remaining: float
    rate_per_s: float

    @property
    def retry_after_s(self) -> float:
        # Time until one whole token is available again.
        return 0.0 if self.allowed else max(0.0, (1.0 - self.remaining) / self.rate_per_s)


async def create_key(conn: Conn, name: str, *, rate_per_s: float, burst: int) -> str:
    key = generate_key()
    await conn.execute(
        "INSERT INTO api_key (name, key_prefix, key_hash, rate_per_s, burst, tokens) "
        "VALUES ($1, $2, $3, $4, $5::int, $5::float8)",
        name,
        key[:11],
        hash_key(key),
        rate_per_s,
        burst,
    )
    return key


# The CTE locks the key row and computes the refilled balance from the locked
# (newest) version, so concurrent requests serialise on the row and none of
# them can spend a token another one already spent. clock_timestamp() is read
# after the lock is granted, which keeps refilled_at monotonic.
_CHECK_SQL = """
WITH cur AS (
    SELECT id, clock_timestamp() AS t,
           LEAST(burst::float8,
                 tokens + rate_per_s
                          * GREATEST(0, EXTRACT(EPOCH FROM clock_timestamp() - refilled_at))
           ) AS avail
    FROM api_key
    WHERE key_hash = $1 AND revoked_at IS NULL
    FOR UPDATE
)
UPDATE api_key k
SET tokens = CASE WHEN cur.avail >= 1 THEN cur.avail - 1 ELSE cur.avail END,
    refilled_at = GREATEST(k.refilled_at, cur.t)
FROM cur
WHERE k.id = cur.id
RETURNING k.id, k.name, cur.avail >= 1 AS allowed, k.tokens, k.rate_per_s
"""


async def check_and_consume(conn: Conn, key: str) -> KeyCheck | None:
    """None when the key is unknown or revoked."""
    row = await conn.fetchrow(_CHECK_SQL, hash_key(key))
    if row is None:
        return None
    return KeyCheck(row["id"], row["name"], row["allowed"], row["tokens"], row["rate_per_s"])


async def revoke_key(conn: Conn, prefix: str) -> bool:
    status = await conn.execute(
        "UPDATE api_key SET revoked_at = now() WHERE key_prefix = $1 AND revoked_at IS NULL",
        prefix,
    )
    return status.endswith(" 1")
