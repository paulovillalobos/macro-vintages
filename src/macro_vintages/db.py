"""Connection pools and migrations."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeAlias

import asyncpg
import asyncpg.pool

if TYPE_CHECKING:
    # A pooled connection is a proxy object; both expose the same query API.
    Conn: TypeAlias = asyncpg.Connection[Any] | asyncpg.pool.PoolConnectionProxy[Any]  # noqa: UP040
else:  # generic only in the stubs; a plain alias so FastAPI can introspect it
    Conn: TypeAlias = asyncpg.Connection | asyncpg.pool.PoolConnectionProxy  # noqa: UP040

# Arbitrary constant shared by every process that runs migrations.
MIGRATION_LOCK_ID = 7_302_211_001

_ROOT = Path(__file__).resolve().parents[2]


def sqlalchemy_url(url: str) -> str:
    """Alembic goes through SQLAlchemy, which needs the driver in the scheme."""
    for prefix in ("postgresql://", "postgres://"):
        if url.startswith(prefix):
            return "postgresql+asyncpg://" + url[len(prefix) :]
    return url


async def _init_connection(conn: asyncpg.Connection[Any]) -> None:
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def create_pool(url: str, *, min_size: int = 2, max_size: int = 10) -> asyncpg.Pool[Any]:
    pool = await asyncpg.create_pool(
        url,
        min_size=min_size,
        max_size=max_size,
        init=_init_connection,
        command_timeout=30,
        server_settings={"application_name": "macro-vintages"},
    )
    assert pool is not None
    return pool


def alembic_config(url: str) -> Any:
    from alembic.config import Config

    ini = _ROOT / "alembic.ini"
    cfg = Config(str(ini)) if ini.exists() else Config()
    if not ini.exists():  # installed package without the repo checkout
        cfg.set_main_option("script_location", str(_ROOT / "migrations"))
    cfg.attributes["database_url"] = url
    return cfg


def migrate(url: str, revision: str = "head") -> None:
    from alembic import command

    command.upgrade(alembic_config(url), revision)


def downgrade(url: str, revision: str) -> None:
    from alembic import command

    command.downgrade(alembic_config(url), revision)


async def migrate_async(url: str, revision: str = "head") -> None:
    # Alembic's env.py calls asyncio.run(), so it needs a thread without a loop.
    await asyncio.to_thread(migrate, url, revision)
