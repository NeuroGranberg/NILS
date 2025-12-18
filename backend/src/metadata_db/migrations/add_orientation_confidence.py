"""
Migration to add stack_orientation_confidence column to series_stack table.

This column stores the confidence score (0.0-1.0) indicating how aligned
the stack's orientation is to the primary anatomical axis (Axial/Coronal/Sagittal).

Lower confidence values indicate oblique orientations that may need manual review.

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
    """Check if the migration needs to be applied."""
    inspector = inspect(conn)
    
    if "series_stack" not in inspector.get_table_names():
        # Table doesn't exist yet, will be created with correct schema
        return False
    
    # Check if stack_orientation_confidence column exists
    columns = inspector.get_columns("series_stack")
    column_names = {col["name"] for col in columns}
    
    return "stack_orientation_confidence" not in column_names


def migrate(engine: Engine, dry_run: bool = False) -> dict:
    """
    Add stack_orientation_confidence column to series_stack table.
    
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
        if not _needs_migration(conn):
            logger.info("stack_orientation_confidence migration not needed (already applied)")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results
        
        if dry_run:
            logger.info("DRY RUN: Would add stack_orientation_confidence column")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results
        
        logger.info("Starting stack_orientation_confidence migration...")
        
        # Step 1: Add stack_orientation_confidence column
        logger.info("Adding column: stack_orientation_confidence REAL")
        conn.execute(text("""
            ALTER TABLE series_stack
            ADD COLUMN stack_orientation_confidence REAL;
        """))
        results["changes_made"].append("Added stack_orientation_confidence column")
        
        # Step 2: Create partial index for low-confidence stacks
        logger.info("Creating index for low-confidence filtering")
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_series_stack_orientation_confidence
            ON series_stack(stack_orientation_confidence)
            WHERE stack_orientation_confidence < 0.7;
        """))
        results["changes_made"].append("Created index idx_series_stack_orientation_confidence")
        
        conn.commit()
        
        logger.info("stack_orientation_confidence migration completed successfully")
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
            "column_exists": bool,
            "index_exists": bool
        }
    """
    status = {
        "migrated": False,
        "column_exists": False,
        "index_exists": False,
    }
    
    with engine.connect() as conn:
        status["migrated"] = not _needs_migration(conn)
        
        if "series_stack" in inspect(conn).get_table_names():
            columns = inspect(conn).get_columns("series_stack")
            column_names = {col["name"] for col in columns}
            status["column_exists"] = "stack_orientation_confidence" in column_names
            
            # Check if index exists
            indexes = inspect(conn).get_indexes("series_stack")
            status["index_exists"] = any(
                idx["name"] == "idx_series_stack_orientation_confidence"
                for idx in indexes
            )
    
    return status
