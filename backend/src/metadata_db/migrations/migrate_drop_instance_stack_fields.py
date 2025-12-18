"""
Migration to DROP stack-defining fields from the instance table.

These fields are now stored in series_stack table only. This migration
ensures backward compatibility when restoring old database backups that
still have these columns on the instance table.

Columns dropped:
- MR: inversion_time, echo_time, echo_numbers, echo_train_length,
      repetition_time, flip_angle, receive_coil_name, image_orientation_patient, image_type
- CT: xray_exposure, kvp, tube_current
- PET: pet_bed_index, pet_frame_type

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


# Columns to drop from instance table (now only in series_stack)
INSTANCE_STACK_COLUMNS_TO_DROP: list[str] = [
    # MR stack-defining fields
    "inversion_time",
    "echo_time",
    "echo_numbers",
    "echo_train_length",
    "repetition_time",
    "flip_angle",
    "receive_coil_name",
    "image_orientation_patient",
    "image_type",
    # CT stack-defining fields
    "xray_exposure",
    "kvp",
    "tube_current",
    # PET stack-defining fields
    "pet_bed_index",
    "pet_frame_type",
]


def _get_existing_columns(conn: Connection, table: str) -> set[str]:
    """Get the set of existing column names for a table."""
    inspector = inspect(conn)
    try:
        columns = inspector.get_columns(table)
        return {col["name"] for col in columns}
    except Exception:
        return set()


def _table_exists(conn: Connection, table: str) -> bool:
    """Check if a table exists."""
    inspector = inspect(conn)
    return table in inspector.get_table_names()


def _needs_migration(conn: Connection) -> bool:
    """Check if migration is needed (any stack columns still exist on instance)."""
    if not _table_exists(conn, "instance"):
        return False
    
    existing = _get_existing_columns(conn, "instance")
    # Check if any of the columns to drop still exist
    columns_present = existing.intersection(INSTANCE_STACK_COLUMNS_TO_DROP)
    return len(columns_present) > 0


def _drop_column_if_exists(conn: Connection, table: str, column: str) -> bool:
    """Drop a column from a table if it exists. Returns True if dropped."""
    existing = _get_existing_columns(conn, table)
    if column not in existing:
        logger.debug(f"Column {table}.{column} does not exist, skipping")
        return False
    
    # PostgreSQL syntax for dropping column
    sql = f'ALTER TABLE "{table}" DROP COLUMN IF EXISTS "{column}"'
    conn.execute(text(sql))
    logger.info(f"Dropped column {table}.{column}")
    return True


def _drop_instance_stack_columns(conn: Connection) -> int:
    """Drop all stack-defining columns from the instance table. Returns count of columns dropped."""
    if not _table_exists(conn, "instance"):
        logger.warning("Instance table does not exist, skipping column drops")
        return 0
    
    dropped = 0
    for column in INSTANCE_STACK_COLUMNS_TO_DROP:
        if _drop_column_if_exists(conn, "instance", column):
            dropped += 1
    
    return dropped


def run_migration(engine: Engine, *, dry_run: bool = False) -> dict[str, Any]:
    """
    Run the migration to drop instance stack fields.
    
    Args:
        engine: SQLAlchemy engine
        dry_run: If True, don't commit changes
        
    Returns:
        Dictionary with migration results
    """
    results = {
        "success": False,
        "already_migrated": False,
        "columns_dropped": 0,
        "columns_to_drop": [],
        "elapsed_seconds": 0,
        "error": None,
    }
    
    start_time = time.time()
    
    with engine.begin() as conn:
        # Check if migration is needed
        if not _needs_migration(conn):
            logger.info("Instance stack fields migration not needed (columns already removed)")
            results["already_migrated"] = True
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results
        
        # Find which columns need to be dropped
        existing = _get_existing_columns(conn, "instance")
        columns_to_drop = list(existing.intersection(INSTANCE_STACK_COLUMNS_TO_DROP))
        results["columns_to_drop"] = columns_to_drop
        
        if dry_run:
            logger.info(f"DRY RUN: Would drop {len(columns_to_drop)} columns: {columns_to_drop}")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results
        
        try:
            # Drop columns from instance table
            logger.info("=== Dropping stack-defining columns from instance table ===")
            columns_dropped = _drop_instance_stack_columns(conn)
            
            results["columns_dropped"] = columns_dropped
            results["success"] = True
            
            logger.info(f"Dropped {columns_dropped} columns from instance table")
            
        except Exception as exc:
            results["error"] = str(exc)
            logger.exception("Migration failed")
            raise
    
    results["elapsed_seconds"] = time.time() - start_time
    return results


def check_migration_status(engine: Engine) -> dict[str, Any]:
    """
    Check the current migration status.
    
    Returns:
        Dictionary with status information
    """
    status = {
        "migrated": False,
        "columns_present": [],
        "columns_absent": [],
    }
    
    with engine.connect() as conn:
        status["migrated"] = not _needs_migration(conn)
        
        if _table_exists(conn, "instance"):
            existing = _get_existing_columns(conn, "instance")
            for column in INSTANCE_STACK_COLUMNS_TO_DROP:
                if column in existing:
                    status["columns_present"].append(column)
                else:
                    status["columns_absent"].append(column)
    
    return status
