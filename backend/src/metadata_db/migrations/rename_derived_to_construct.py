"""
Migration to rename derived_csv to construct_csv in series_classification_cache.

This is a semantic rename - "construct" better describes computed outputs
(ADC, FA, T1map, etc.) without confusion with DICOM's DERIVED ImageType.

The code uses construct_csv internally but the DB still had derived_csv.
This migration brings them in sync.

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
    - derived_csv column exists
    - construct_csv column does NOT exist
    """
    inspector = inspect(conn)
    
    # Check if table exists
    if "series_classification_cache" not in inspector.get_table_names():
        return False
    
    # Get column names
    columns = [col["name"] for col in inspector.get_columns("series_classification_cache")]
    
    # Need migration if old column exists and new column doesn't
    has_derived = "derived_csv" in columns
    has_construct = "construct_csv" in columns
    
    return has_derived and not has_construct


def _check_already_migrated(conn: Connection) -> bool:
    """Check if already migrated (construct_csv exists)."""
    inspector = inspect(conn)
    
    if "series_classification_cache" not in inspector.get_table_names():
        return True  # No table, nothing to migrate
    
    columns = [col["name"] for col in inspector.get_columns("series_classification_cache")]
    return "construct_csv" in columns


def migrate(engine: Engine, dry_run: bool = False) -> dict:
    """
    Rename derived_csv to construct_csv in series_classification_cache.
    
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
            logger.info("rename_derived_to_construct migration not needed (already migrated or no table)")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results
        
        if not _needs_migration(conn):
            logger.info("rename_derived_to_construct migration not needed")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results
        
        if dry_run:
            logger.info("DRY RUN: Would rename derived_csv to construct_csv")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results
        
        logger.info("Starting rename_derived_to_construct migration...")
        
        # PostgreSQL uses ALTER TABLE ... RENAME COLUMN
        # SQLite 3.25+ supports this too
        try:
            conn.execute(text("""
                ALTER TABLE series_classification_cache 
                RENAME COLUMN derived_csv TO construct_csv;
            """))
            results["changes_made"].append("Renamed column derived_csv to construct_csv")
            logger.info("Renamed column derived_csv to construct_csv")
        except Exception as e:
            # SQLite < 3.25 doesn't support RENAME COLUMN
            # In that case, we need a more complex migration
            logger.warning(f"Direct rename failed: {e}")
            logger.info("Attempting fallback migration for older SQLite...")
            
            # Fallback: Create new table, copy data, rename
            # This is the SQLite way for older versions
            conn.execute(text("""
                -- Create new table with correct column name
                CREATE TABLE series_classification_cache_new (
                    series_stack_id INTEGER PRIMARY KEY,
                    series_id INTEGER,
                    series_instance_uid TEXT NOT NULL,
                    dicom_origin_cohort TEXT,
                    classification_string TEXT,
                    unique_series_under_string INTEGER,
                    fov_x_mm REAL,
                    fov_y_mm REAL,
                    aspect_ratio REAL,
                    slices_count INTEGER,
                    rows INTEGER,
                    columns INTEGER,
                    pixsp_row_mm REAL,
                    pixsp_col_mm REAL,
                    orientation_patient TEXT,
                    echo_number INTEGER,
                    directory_type TEXT,
                    base TEXT,
                    modifier_csv TEXT,
                    technique TEXT,
                    construct_csv TEXT,
                    provenance TEXT,
                    acceleration_csv TEXT,
                    post_contrast INTEGER,
                    localizer INTEGER,
                    spinal_cord INTEGER,
                    study_id INTEGER,
                    subject_id INTEGER,
                    manual_review_required INTEGER,
                    manual_review_reasons_csv TEXT,
                    created_at TEXT,
                    updated_at TEXT
                );
            """))
            
            conn.execute(text("""
                -- Copy data
                INSERT INTO series_classification_cache_new 
                SELECT 
                    series_stack_id, series_id, series_instance_uid, dicom_origin_cohort,
                    classification_string, unique_series_under_string,
                    fov_x_mm, fov_y_mm, aspect_ratio, slices_count,
                    rows, columns, pixsp_row_mm, pixsp_col_mm,
                    orientation_patient, echo_number, directory_type,
                    base, modifier_csv, technique, 
                    derived_csv,  -- This becomes construct_csv
                    provenance, acceleration_csv,
                    post_contrast, localizer, spinal_cord,
                    study_id, subject_id,
                    manual_review_required, manual_review_reasons_csv,
                    created_at, updated_at
                FROM series_classification_cache;
            """))
            
            conn.execute(text("""
                DROP TABLE series_classification_cache;
            """))
            
            conn.execute(text("""
                ALTER TABLE series_classification_cache_new 
                RENAME TO series_classification_cache;
            """))
            
            results["changes_made"].append("Recreated table with construct_csv column (SQLite fallback)")
            logger.info("Recreated table with construct_csv column")
        
        conn.commit()
        
        logger.info("rename_derived_to_construct migration completed successfully")
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
            "has_derived_csv": bool,
            "has_construct_csv": bool,
            "table_exists": bool
        }
    """
    status = {
        "migrated": False,
        "has_derived_csv": False,
        "has_construct_csv": False,
        "table_exists": False,
    }
    
    with engine.connect() as conn:
        inspector = inspect(conn)
        status["table_exists"] = "series_classification_cache" in inspector.get_table_names()
        
        if status["table_exists"]:
            columns = [col["name"] for col in inspector.get_columns("series_classification_cache")]
            status["has_derived_csv"] = "derived_csv" in columns
            status["has_construct_csv"] = "construct_csv" in columns
            status["migrated"] = status["has_construct_csv"]
    
    return status
