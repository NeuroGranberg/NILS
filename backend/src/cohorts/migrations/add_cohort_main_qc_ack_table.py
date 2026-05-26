"""Idempotent migration: create / upgrade the ``cohort_main_qc_ack`` table.

Stores per-(cohort, subject, session_date, axis) user acknowledgements of
Main QC picks ("In NILS we trust"). Lives separately from
``cohort_main_qc_state.current_picks`` so an acknowledgement survives a
cohort-wide Apply (which rewrites the picks JSON from scratch).

Sessions are keyed by ``(subject_id, session_date)`` because PACS often
splits a single visit into multiple ``StudyInstanceUID``s.

Upgrade path: an earlier version of this table used ``study_id`` as part of
the PK. Detect that shape and DROP+recreate (the feature is new and has no
production rows worth preserving).
"""

from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


_TABLE = "cohort_main_qc_ack"


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


def ensure_migrated() -> None:
    """Create the table if it doesn't exist; upgrade if it has the old shape."""
    from db.session import engine

    with engine.begin() as conn:
        if _table_exists(conn):
            # Old shape used study_id; new shape uses subject_id + session_date.
            if _has_column(conn, "study_id") and not _has_column(conn, "subject_id"):
                conn.execute(text(f"DROP TABLE {_TABLE}"))
                logger.info("Dropped legacy %s (study_id-keyed); recreating", _TABLE)
            else:
                return

        conn.execute(text(
            f"""
            CREATE TABLE {_TABLE} (
                cohort_id INTEGER NOT NULL REFERENCES cohorts(id) ON DELETE CASCADE,
                subject_id INTEGER NOT NULL,
                session_date VARCHAR(32) NOT NULL,
                axis VARCHAR(16) NOT NULL,
                acknowledged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (cohort_id, subject_id, session_date, axis)
            )
            """
        ))
        logger.info("Created table %s", _TABLE)


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    ensure_migrated()
