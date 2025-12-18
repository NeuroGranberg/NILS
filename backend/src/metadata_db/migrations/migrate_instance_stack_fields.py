"""
One-time migration to add stack-defining fields to the instance table.

This enables per-stack classification by storing MR/CT/PET acquisition parameters
at the instance level, which can later be used to group instances into stacks.

Usage:
    neuro-api migrate-instance-stack-fields
"""

from __future__ import annotations

import logging
import time
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)


# New columns to add to the instance table: (column_name, sql_type)
# Using DOUBLE PRECISION for floats to match mri_series_details and avoid range issues
INSTANCE_STACK_COLUMNS: list[tuple[str, str]] = [
    # MR stack-defining fields
    ("inversion_time", "DOUBLE PRECISION"),
    ("echo_time", "DOUBLE PRECISION"),
    ("echo_numbers", "TEXT"),  # VM=1-n, backslash-separated like mri_series_details
    ("echo_train_length", "INTEGER"),
    ("repetition_time", "DOUBLE PRECISION"),
    ("flip_angle", "DOUBLE PRECISION"),
    ("receive_coil_name", "TEXT"),
    ("image_orientation_patient", "TEXT"),
    ("image_type", "TEXT"),
    # CT stack-defining fields
    ("xray_exposure", "DOUBLE PRECISION"),
    ("kvp", "DOUBLE PRECISION"),
    ("tube_current", "DOUBLE PRECISION"),
    # PET stack-defining fields
    ("pet_bed_index", "INTEGER"),
    ("pet_frame_type", "TEXT"),
    # FK to series_stack
    ("series_stack_id", "INTEGER"),
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


def _is_already_migrated(conn: Connection) -> bool:
    """Check if migration has already been applied."""
    if not _table_exists(conn, "instance"):
        return False
    existing = _get_existing_columns(conn, "instance")
    # Check if echo_time column exists (one of the first columns we add)
    return "echo_time" in existing


def _add_column_if_not_exists(conn: Connection, table: str, column: str, sql_type: str) -> bool:
    """Add a column to a table if it doesn't already exist. Returns True if added."""
    existing = _get_existing_columns(conn, table)
    if column in existing:
        logger.debug(f"Column {table}.{column} already exists, skipping")
        return False
    
    # PostgreSQL syntax for adding nullable column
    sql = f'ALTER TABLE "{table}" ADD COLUMN "{column}" {sql_type}'
    conn.execute(text(sql))
    logger.info(f"Added column {table}.{column} ({sql_type})")
    return True


def _add_instance_columns(conn: Connection) -> int:
    """Add all stack-defining columns to the instance table. Returns count of columns added."""
    if not _table_exists(conn, "instance"):
        logger.warning("Instance table does not exist, skipping column additions")
        return 0
    
    added = 0
    for column, sql_type in INSTANCE_STACK_COLUMNS:
        if _add_column_if_not_exists(conn, "instance", column, sql_type):
            added += 1
    
    return added


def run_migration(engine: Engine, *, dry_run: bool = False) -> dict[str, Any]:
    """
    Run the instance stack fields migration.
    
    Args:
        engine: SQLAlchemy engine
        dry_run: If True, don't commit changes
        
    Returns:
        Dictionary with migration results
    """
    results = {
        "success": False,
        "already_migrated": False,
        "columns_added": 0,
        "elapsed_seconds": 0,
        "error": None,
    }
    
    start_time = time.time()
    
    with engine.begin() as conn:
        # Check if already migrated
        if _is_already_migrated(conn):
            logger.info("Instance stack fields migration already applied, skipping")
            results["already_migrated"] = True
            results["success"] = True
            return results
        
        try:
            # Add columns to instance table
            logger.info("=== Adding stack-defining columns to instance table ===")
            columns_added = _add_instance_columns(conn)
            
            results["columns_added"] = columns_added
            results["success"] = True
            
            if dry_run:
                logger.info("Dry run mode - rolling back changes")
                conn.rollback()
            else:
                logger.info(f"Added {columns_added} columns to instance table")
            
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
        "instance_columns": {},
        "missing_columns": [],
    }
    
    with engine.connect() as conn:
        status["migrated"] = _is_already_migrated(conn)
        
        if _table_exists(conn, "instance"):
            existing = _get_existing_columns(conn, "instance")
            for column, sql_type in INSTANCE_STACK_COLUMNS:
                present = column in existing
                status["instance_columns"][column] = {
                    "present": present,
                    "type": sql_type,
                }
                if not present:
                    status["missing_columns"].append(column)
    
    return status
