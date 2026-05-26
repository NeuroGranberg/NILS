"""
Migration: add through-plane resolution column to series_classification_cache.

The other geometry columns (``rows``, ``columns``, ``pixsp_row_mm``,
``pixsp_col_mm``) were created by an earlier schema (rename_derived_to_construct
and friends) but never populated by the extraction pipeline. Sort Step 3 is now
extended to populate all five geometry columns. The first four already exist on
the table; this migration only needs to add ``slice_thickness_mm``.

Idempotent — safe to run on every boot.
"""

from __future__ import annotations

import logging
import time

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)

_CACHE_COLUMNS = [
    ("slice_thickness_mm", "DOUBLE PRECISION"),
]


def _needs_migration(conn: Connection) -> bool:
    inspector = inspect(conn)
    if "series_classification_cache" not in inspector.get_table_names():
        return False
    cache_cols = {c["name"] for c in inspector.get_columns("series_classification_cache")}
    return any(name not in cache_cols for name, _ in _CACHE_COLUMNS)


def run_migration(engine: Engine, dry_run: bool = False) -> dict:
    results: dict = {
        "success": False,
        "already_migrated": False,
        "changes_made": [],
        "elapsed_seconds": 0.0,
    }

    start = time.time()

    with engine.begin() as conn:
        if not _needs_migration(conn):
            results["success"] = True
            results["already_migrated"] = True
            results["elapsed_seconds"] = time.time() - start
            return results

        if dry_run:
            logger.info("DRY RUN: would add geometry columns to series_classification_cache")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start
            return results

        inspector = inspect(conn)
        existing = {c["name"] for c in inspector.get_columns("series_classification_cache")}
        for col_name, col_type in _CACHE_COLUMNS:
            if col_name not in existing:
                conn.execute(text(
                    f"ALTER TABLE series_classification_cache ADD COLUMN {col_name} {col_type}"
                ))
                results["changes_made"].append(f"series_classification_cache.{col_name} {col_type}")
                logger.info("Added series_classification_cache.%s %s", col_name, col_type)

        results["success"] = True
        results["elapsed_seconds"] = time.time() - start
        logger.info(
            "Geometry columns migration completed: %d columns added (%.1fs)",
            len(results["changes_made"]),
            results["elapsed_seconds"],
        )

    return results
