"""Alembic environment: async engine on asyncpg, serialised by an advisory lock.

The API and the worker containers both run ``macro-vintages migrate`` on start.
Without the lock two processes could race on an empty database and both try to
create the schema; with it the second one waits, then finds the head applied.
"""

from __future__ import annotations

import asyncio

from alembic import context
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from macro_vintages.config import Settings
from macro_vintages.db import MIGRATION_LOCK_ID, sqlalchemy_url


def _run(connection: Connection) -> None:
    connection.exec_driver_sql(f"SELECT pg_advisory_lock({MIGRATION_LOCK_ID})")
    try:
        context.configure(connection=connection, transaction_per_migration=True)
        with context.begin_transaction():
            context.run_migrations()
        connection.commit()
    finally:
        connection.exec_driver_sql(f"SELECT pg_advisory_unlock({MIGRATION_LOCK_ID})")
        connection.commit()


async def _main() -> None:
    url = context.config.attributes.get("database_url") or Settings.from_env().database_url
    engine = create_async_engine(sqlalchemy_url(url))
    async with engine.connect() as conn:
        await conn.run_sync(_run)
    await engine.dispose()


if context.is_offline_mode():
    raise SystemExit("offline mode is not supported; run against a database")
asyncio.run(_main())
