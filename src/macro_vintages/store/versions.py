"""Writing observation versions.

``apply_observations`` turns one fetched payload into version rows:

* period not held yet            -> insert (``inserted``)
* period held, same value+status -> nothing (this is what makes re-runs idempotent)
* period held, different         -> close current row, insert new one (``revised``)
* period held, absent from a *complete* payload -> close current row (``withdrawn``)

The caller must already hold the series' fencing lock (the ``ingest_job`` row,
``FOR UPDATE``), so writers of one series are serialised. The transaction time
is read with ``clock_timestamp()`` *after* that lock is granted: ``now()`` is the
transaction start, and a transaction that started earlier but got the lock later
would otherwise stamp a ``known_to`` before the previous writer's ``known_from``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from macro_vintages.db import Conn
from macro_vintages.sources.base import SchemaDriftError, SourceObservation

_CLOSE_SQL = """
WITH incoming AS (
    SELECT * FROM unnest($2::date[], $3::numeric[], $4::text[]) AS t (period, value, status)
)
UPDATE observation_version v
SET known_to = $5
WHERE v.series_id = $1
  AND v.known_to IS NULL
  AND (
        EXISTS (SELECT 1 FROM incoming i
                WHERE i.period = v.period
                  AND (i.value IS DISTINCT FROM v.value OR i.status IS DISTINCT FROM v.status))
     OR ($6 AND NOT EXISTS (SELECT 1 FROM incoming i WHERE i.period = v.period))
  )
RETURNING v.period
"""

_INSERT_SQL = """
INSERT INTO observation_version (series_id, period, value, status, known_from, run_id)
SELECT $1, i.period, i.value, i.status, $5, $6
FROM unnest($2::date[], $3::numeric[], $4::text[]) AS i (period, value, status)
WHERE NOT EXISTS (
    SELECT 1 FROM observation_version v
    WHERE v.series_id = $1 AND v.period = i.period AND v.known_to IS NULL
)
"""


@dataclass(frozen=True, slots=True)
class WriteCounts:
    received: int
    inserted: int
    revised: int
    withdrawn: int

    @property
    def changed(self) -> bool:
        return bool(self.inserted or self.revised or self.withdrawn)


async def apply_observations(
    conn: Conn,
    *,
    series_id: int,
    run_id: int,
    observations: Sequence[SourceObservation],
    complete: bool,
) -> tuple[WriteCounts, datetime]:
    periods = [o.period for o in observations]
    if len(set(periods)) != len(periods):
        raise SchemaDriftError("payload contains the same period twice")
    values = [o.value for o in observations]
    statuses = [o.status for o in observations]

    ts: datetime = await conn.fetchval("SELECT clock_timestamp()")
    closed = await conn.fetch(_CLOSE_SQL, series_id, periods, values, statuses, ts, complete)
    incoming = set(periods)
    revised = sum(1 for r in closed if r["period"] in incoming)
    withdrawn = len(closed) - revised
    status = await conn.execute(_INSERT_SQL, series_id, periods, values, statuses, ts, run_id)
    written = int(status.rsplit(" ", 1)[1])
    counts = WriteCounts(
        received=len(observations),
        inserted=written - revised,
        revised=revised,
        withdrawn=withdrawn,
    )
    if counts.changed:
        await conn.execute(
            "UPDATE series SET data_version = data_version + 1, data_changed_at = $2 WHERE id = $1",
            series_id,
            ts,
        )
    return counts, ts
