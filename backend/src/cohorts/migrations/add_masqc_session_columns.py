"""Idempotent migration: add display_id_type / only_multi_stack to ``masqc_sessions``.

Lets a saved Main Acquisition QC session round-trip the user-visible
display ID (``display_id_type``, e.g. ``BROMS``) and the
"only multi-stack bundles" toggle, so loading a saved session restores
the same view the user had when they saved it.

Both columns are nullable / have safe defaults; existing rows are
backfilled implicitly via the column defaults.

Safe to call repeatedly; checks ``information_schema`` before issuing DDL.
"""

from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


_TABLE = "masqc_sessions"


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
    """Add the two columns if absent."""
    from db.session import engine

    with engine.begin() as conn:
        if not _table_exists(conn):
            # First-time deploys create the table via Base.metadata.create_all
            # which already includes the new columns. Nothing to do.
            return

        added: list[str] = []
        if not _has_column(conn, "display_id_type"):
            conn.execute(text(
                f"ALTER TABLE {_TABLE} "
                "ADD COLUMN display_id_type VARCHAR(64) NULL"
            ))
            added.append("display_id_type")
        if not _has_column(conn, "only_multi_stack"):
            conn.execute(text(
                f"ALTER TABLE {_TABLE} "
                "ADD COLUMN only_multi_stack BOOLEAN NOT NULL DEFAULT FALSE"
            ))
            added.append("only_multi_stack")

        if added:
            logger.info("masqc_sessions: added columns %s", added)


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    ensure_migrated()
