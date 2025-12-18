"""
Migration to rename provenance_csv to provenance in series_classification_cache.

Provenance is a single value (not CSV) - the column name was misleading.
This migration brings the database in sync with the schema.

Backward compatible: If restoring a backup with old column name,
this migration will simply rename it again on next startup.

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
    """
    Check if the migration needs to be applied.
    
    Returns True if:
    - series_classification_cache table exists
    - provenance_csv column exists
    - provenance column does NOT exist
    """
    inspector = inspect(conn)
    
    # Check if table exists
    if "series_classification_cache" not in inspector.get_table_names():
        return False
    
    # Get column names
    columns = [col["name"] for col in inspector.get_columns("series_classification_cache")]
    
    # Need migration if old column exists and new column doesn't
    has_old = "provenance_csv" in columns
    has_new = "provenance" in columns
    
    return has_old and not has_new


def _check_already_migrated(conn: Connection) -> bool:
    """Check if already migrated (provenance exists)."""
    inspector = inspect(conn)
    
    if "series_classification_cache" not in inspector.get_table_names():
        return True  # No table, nothing to migrate
    
    columns = [col["name"] for col in inspector.get_columns("series_classification_cache")]
    return "provenance" in columns


def migrate(engine: Engine, dry_run: bool = False) -> dict:
    """
    Rename provenance_csv to provenance in series_classification_cache.
    
    Args:
        engine: SQLAlchemy engine for metadata database
        dry_run: If True, only check if migration is needed without applying
        
    Returns:
        Dict with migration results:
        {
            "success": bool,
            "changes_made": list[str],
            "elapsed_seconds": float
        }
    """
    results = {
        "success": False,
        "changes_made": [],
        "elapsed_seconds": 0.0
    }
    
    start_time = time.time()
    
    with engine.begin() as conn:
        # Check if already done
        if _check_already_migrated(conn):
            logger.info("rename_provenance_csv_to_provenance migration not needed (already migrated or no table)")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results
        
        if not _needs_migration(conn):
            logger.info("rename_provenance_csv_to_provenance migration not needed")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results
        
        if dry_run:
            logger.info("DRY RUN: Would rename provenance_csv to provenance")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results
        
        logger.info("Starting rename_provenance_csv_to_provenance migration...")
        
        # PostgreSQL uses ALTER TABLE ... RENAME COLUMN
        try:
            conn.execute(text("""
                ALTER TABLE series_classification_cache 
                RENAME COLUMN provenance_csv TO provenance;
            """))
            results["changes_made"].append("Renamed column provenance_csv to provenance")
            logger.info("Renamed column provenance_csv to provenance")
        except Exception as e:
            logger.error(f"Failed to rename provenance_csv to provenance: {e}")
            raise
        
        logger.info("rename_provenance_csv_to_provenance migration completed successfully")
        results["success"] = True
        results["elapsed_seconds"] = time.time() - start_time
    
    return results


def get_status(engine: Engine) -> dict:
    """
    Get the current migration status.
    
    Returns:
        Dict with status info:
        {
            "migrated": bool,
            "has_provenance_csv": bool,
            "has_provenance": bool,
            "table_exists": bool
        }
    """
    status = {
        "migrated": False,
        "has_provenance_csv": False,
        "has_provenance": False,
        "table_exists": False,
    }
    
    with engine.connect() as conn:
        inspector = inspect(conn)
        status["table_exists"] = "series_classification_cache" in inspector.get_table_names()
        
        if status["table_exists"]:
            columns = [col["name"] for col in inspector.get_columns("series_classification_cache")]
            status["has_provenance_csv"] = "provenance_csv" in columns
            status["has_provenance"] = "provenance" in columns
            status["migrated"] = status["has_provenance"]
    
    return status
