"""FastAPI application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from alembic.script import ScriptDirectory
from fastapi import FastAPI

from macro_vintages import db, logs
from macro_vintages.api import errors
from macro_vintages.api.middleware import RequestContextMiddleware
from macro_vintages.api.routes import ops, router
from macro_vintages.config import Settings

DESCRIPTION = """
Euro-area macro series from the ECB and Eurostat, with their full revision history.

Every observation is stored with the period it describes and the interval during
which this engine held that value. `as_of` answers "what did we know at time T";
`/versions` and `/revisions` show how published numbers changed.

Authenticate with `X-API-Key: <key>` or `Authorization: Bearer <key>`.
Errors use `application/problem+json`.
"""


def schema_head() -> str:
    head = ScriptDirectory.from_config(db.alembic_config("")).get_current_head()
    assert head is not None
    return head


def create_app(settings: Settings | None = None, pool: asyncpg.Pool[Any] | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    logs.configure(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owned = pool is None
        app.state.pool = pool or await db.create_pool(
            settings.database_url, min_size=settings.db_pool_min, max_size=settings.db_pool_max
        )
        app.state.schema_head = schema_head()
        try:
            yield
        finally:
            if owned:
                await app.state.pool.close()

    app = FastAPI(
        title="macro-vintages",
        version="0.1.0",
        description=DESCRIPTION,
        lifespan=lifespan,
    )
    errors.install(app)
    app.include_router(ops)
    app.include_router(router)
    app.add_middleware(RequestContextMiddleware)
    return app
