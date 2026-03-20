"""
Migration to make assignment_date column nullable in subject_disease_types table.

Old backups have assignment_date as NOT NULL, but the current schema defines it
as optional (nullable=True) since disease type assignments don't always have a
known date.

Usage:
    Runs automatically on server startup via lifecycle.py
"""

from __future__ import annotations

import logging
import time
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)


def _needs_migration(conn: Connection) -> bool:
    """Check if assignment_date on subject_disease_types is still NOT NULL."""
    inspector = inspect(conn)

    if "subject_disease_types" not in inspector.get_table_names():
        return False

    columns = inspector.get_columns("subject_disease_types")
    for col in columns:
        if col["name"] == "assignment_date":
            return not col["nullable"]  # True if NOT NULL (needs migration)

    return False


def run_migration(engine: Engine, dry_run: bool = False) -> dict[str, Any]:
    start_time = time.time()
    results: dict[str, Any] = {
        "success": False,
        "already_migrated": False,
        "dry_run": dry_run,
        "elapsed_seconds": 0.0,
        "changes_made": [],
    }

    with engine.connect() as conn:
        if not _needs_migration(conn):
            results["already_migrated"] = True
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results

        if dry_run:
            logger.info("DRY RUN: Would make subject_disease_types.assignment_date nullable")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results

        logger.info("Making subject_disease_types.assignment_date nullable...")
        conn.execute(text(
            "ALTER TABLE subject_disease_types ALTER COLUMN assignment_date DROP NOT NULL"
        ))
        results["changes_made"].append("Changed assignment_date to nullable")
        conn.commit()

        logger.info("subject_disease_types.assignment_date nullable migration completed")
        results["success"] = True
        results["elapsed_seconds"] = time.time() - start_time

    return results
