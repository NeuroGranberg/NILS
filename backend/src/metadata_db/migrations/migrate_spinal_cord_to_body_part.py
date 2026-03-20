"""
Migration to add body_part TEXT column to series_classification_cache.

Backfills from the existing spinal_cord INTEGER column:
  spinal_cord = 1   → body_part = 'spine'
  spinal_cord = 0   → body_part = 'brain'
  spinal_cord IS NULL → body_part = NULL

The spinal_cord column is kept for backward compatibility.

Backward compatible: If restoring a pre-body_part backup, this migration
detects the missing column and re-applies on next startup.

Usage:
    Runs automatically on server startup via lifecycle.py
"""

from __future__ import annotations

import logging
import time

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)


def _needs_migration(conn: Connection) -> bool:
    inspector = inspect(conn)
    if "series_classification_cache" not in inspector.get_table_names():
        return False
    columns = [col["name"] for col in inspector.get_columns("series_classification_cache")]
    return "body_part" not in columns


def run_migration(engine: Engine, dry_run: bool = False) -> dict:
    results: dict = {
        "success": False,
        "already_migrated": False,
        "changes_made": [],
        "elapsed_seconds": 0.0,
    }

    start_time = time.time()

    with engine.begin() as conn:
        if not _needs_migration(conn):
            results["success"] = True
            results["already_migrated"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results

        if dry_run:
            logger.info("DRY RUN: Would add body_part column and backfill from spinal_cord")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results

        logger.info("Adding body_part TEXT column to series_classification_cache...")
        conn.execute(text(
            "ALTER TABLE series_classification_cache ADD COLUMN body_part TEXT"
        ))
        results["changes_made"].append("Added column body_part TEXT")

        logger.info("Backfilling body_part from spinal_cord...")
        result = conn.execute(text("""
            UPDATE series_classification_cache
            SET body_part = CASE
                WHEN spinal_cord = 1 THEN 'spine'
                WHEN spinal_cord = 0 THEN 'brain'
                ELSE NULL
            END
        """))
        rows_updated = result.rowcount
        results["changes_made"].append(f"Backfilled {rows_updated} rows")

        logger.info("body_part migration completed: %d rows backfilled", rows_updated)
        results["success"] = True
        results["elapsed_seconds"] = time.time() - start_time

    return results
