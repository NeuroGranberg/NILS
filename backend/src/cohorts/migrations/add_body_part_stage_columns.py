"""Idempotent migration: add stage/commit columns to ``cohort_body_part_qc_state``.

Adds three columns introduced for Milestone C (stage→commit gate):

* ``stage_status``           TEXT NOT NULL DEFAULT 'none'
* ``last_committed_at``      TIMESTAMPTZ NULL
* ``last_commit_signature``  VARCHAR(64) NULL

Backfill rules (run once per missing column):

* Existing rows with ``current_picks`` (i.e. an Apply has already run)
  are marked ``stage_status='committed'``. The previous behaviour of
  Apply was to write through to the metadata DB synchronously, so those
  picks really are reflected on disk.
* ``last_committed_at`` is backfilled from ``current_run_at`` for
  committed rows.
* ``last_commit_signature`` is left NULL — the service recomputes it on
  the next override / commit; a NULL signature simply forces a recompute.

Safe to call repeatedly; checks ``information_schema`` for column
presence before issuing any DDL.
"""

from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


_TABLE = "cohort_body_part_qc_state"


def _has_column(conn: Connection, column: str) -> bool:
    sql = text(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = :t AND column_name = :c
        LIMIT 1
        """
    )
    return conn.execute(sql, {"t": _TABLE, "c": column}).first() is not None


def _table_exists(conn: Connection) -> bool:
    sql = text(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = :t
        LIMIT 1
        """
    )
    return conn.execute(sql, {"t": _TABLE}).first() is not None


def ensure_migrated() -> None:
    """Add the stage/commit columns if absent and backfill defaults."""
    from db.session import engine

    with engine.begin() as conn:
        if not _table_exists(conn):
            # First-time deploys create the table via Base.metadata.create_all
            # which already includes the new columns. Nothing to do.
            return

        added: list[str] = []
        if not _has_column(conn, "stage_status"):
            conn.execute(text(
                f"ALTER TABLE {_TABLE} "
                "ADD COLUMN stage_status VARCHAR(16) NOT NULL DEFAULT 'none'"
            ))
            added.append("stage_status")
        if not _has_column(conn, "last_committed_at"):
            conn.execute(text(
                f"ALTER TABLE {_TABLE} "
                "ADD COLUMN last_committed_at TIMESTAMPTZ NULL"
            ))
            added.append("last_committed_at")
        if not _has_column(conn, "last_commit_signature"):
            conn.execute(text(
                f"ALTER TABLE {_TABLE} "
                "ADD COLUMN last_commit_signature VARCHAR(64) NULL"
            ))
            added.append("last_commit_signature")

        if not added:
            return

        # Backfill: rows that already have an Apply on record were
        # auto-committing under the old behaviour, so they're committed.
        conn.execute(text(
            f"""
            UPDATE {_TABLE}
            SET stage_status = 'committed',
                last_committed_at = COALESCE(last_committed_at, current_run_at)
            WHERE stage_status = 'none'
              AND current_picks IS NOT NULL
              AND jsonb_array_length(
                    CASE
                        WHEN jsonb_typeof(current_picks::jsonb) = 'array'
                            THEN current_picks::jsonb
                        ELSE '[]'::jsonb
                    END
                  ) > 0
            """
        ))
        logger.info(
            "cohort_body_part_qc_state: added columns %s and backfilled defaults",
            added,
        )


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    ensure_migrated()
