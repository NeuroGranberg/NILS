"""
Migration to make stack_key column nullable in series_stack table.

This fixes the schema mismatch where the design document specifies stack_key
should be NULL for single-stack series (the majority case), but the database
schema had a NOT NULL constraint.

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
    """Check if the migration needs to be applied."""
    inspector = inspect(conn)
    
    if "series_stack" not in inspector.get_table_names():
        # Table doesn't exist yet, will be created with correct schema
        return False
    
    # Check if stack_key is nullable
    columns = inspector.get_columns("series_stack")
    for col in columns:
        if col["name"] == "stack_key":
            return not col["nullable"]  # True if NOT NULL (needs migration)
    
    return False


def _check_constraint_exists(conn: Connection, constraint_name: str) -> bool:
    """Check if a constraint exists."""
    result = conn.execute(text("""
        SELECT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = :constraint_name
        )
    """), {"constraint_name": constraint_name})
    return result.scalar()


def run_migration(engine: Engine, dry_run: bool = False) -> dict[str, Any]:
    """
    Run the stack_key nullable migration.
    
    Args:
        engine: SQLAlchemy engine
        dry_run: If True, only check status without making changes
        
    Returns:
        Dictionary with migration results
    """
    start_time = time.time()
    results = {
        "success": False,
        "already_migrated": False,
        "dry_run": dry_run,
        "elapsed_seconds": 0.0,
        "changes_made": [],
    }
    
    with engine.connect() as conn:
        # Check if migration needed
        if not _needs_migration(conn):
            logger.info("stack_key nullable migration already applied or table doesn't exist")
            results["already_migrated"] = True
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results
        
        if dry_run:
            logger.info("DRY RUN: Would make stack_key nullable")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results
        
        logger.info("Starting stack_key nullable migration...")
        
        # Step 1: Drop the problematic unique constraint if it exists
        if _check_constraint_exists(conn, "uq_series_stack_key"):
            logger.info("Dropping constraint: uq_series_stack_key")
            conn.execute(text("""
                ALTER TABLE series_stack
                DROP CONSTRAINT IF EXISTS uq_series_stack_key;
            """))
            results["changes_made"].append("Dropped constraint uq_series_stack_key")
        
        # Step 2: Make stack_key nullable
        logger.info("Altering column: stack_key to nullable")
        conn.execute(text("""
            ALTER TABLE series_stack
            ALTER COLUMN stack_key DROP NOT NULL;
        """))
        results["changes_made"].append("Changed stack_key to nullable")
        
        conn.commit()
        
        logger.info("stack_key nullable migration completed successfully")
        results["success"] = True
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
        "stack_key_nullable": False,
        "constraint_exists": False,
    }
    
    with engine.connect() as conn:
        status["migrated"] = not _needs_migration(conn)
        
        if "series_stack" in inspect(conn).get_table_names():
            columns = inspect(conn).get_columns("series_stack")
            for col in columns:
                if col["name"] == "stack_key":
                    status["stack_key_nullable"] = col["nullable"]
            
            status["constraint_exists"] = _check_constraint_exists(conn, "uq_series_stack_key")
    
    return status
