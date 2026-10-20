# One image for the API and the worker; the command picks the role.

FROM ghcr.io/astral-sh/uv:0.13.1 AS uv

FROM python:3.12.12-slim-bookworm AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never UV_NO_CACHE=1
WORKDIR /app
# Dependencies first, so source edits do not reinstall them.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY README.md LICENSE alembic.ini ./
COPY migrations ./migrations
COPY src ./src
RUN uv sync --frozen --no-dev

FROM python:3.12.12-slim-bookworm
RUN useradd --system --uid 10001 --home-dir /app app
WORKDIR /app
COPY --from=build --chown=app:app /app /app
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER app
EXPOSE 8000 9100
ENTRYPOINT ["docker-entrypoint"]
CMD ["api"]
