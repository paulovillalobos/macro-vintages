"""Keep per-series observation stats on the series row.

The catalogue endpoint computed count/min/max of current observations for every
series on every request: 13 ms for a 41-series page at 5,000 observations each,
against 0.3 ms when read from the series row (docs/results/explain.md). Writes are rare and reads are frequent, so the
writer now maintains these columns in the same transaction as the versions.

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-20
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    bind.exec_driver_sql(
        "ALTER TABLE series "
        "ADD COLUMN observations bigint NOT NULL DEFAULT 0 CHECK (observations >= 0), "
        "ADD COLUMN first_period date, "
        "ADD COLUMN last_period date, "
        "ADD CONSTRAINT series_period_bounds "
        "CHECK ((observations = 0) = (first_period IS NULL AND last_period IS NULL))"
    )
    # Backfill from the current versions, in one pass over observation_current.
    bind.exec_driver_sql(
        "UPDATE series s SET observations = b.n, first_period = b.lo, last_period = b.hi "
        "FROM (SELECT series_id, count(*) AS n, min(period) AS lo, max(period) AS hi "
        "      FROM observation_version WHERE known_to IS NULL GROUP BY series_id) b "
        "WHERE b.series_id = s.id"
    )


def downgrade() -> None:
    op.get_bind().exec_driver_sql(
        "ALTER TABLE series DROP CONSTRAINT series_period_bounds, "
        "DROP COLUMN last_period, DROP COLUMN first_period, DROP COLUMN observations"
    )
