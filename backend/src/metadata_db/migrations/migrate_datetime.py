"""
One-time migration to convert Text date/time columns to proper PostgreSQL DATE/TIME types.

This is a development-phase fix to avoid re-ingesting millions of DICOM records.
After migration, all future extractions will ingest data in the correct format.

Usage:
    neuro-api migrate-datetime
"""

from __future__ import annotations

import logging
import time
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)


# Column definitions: (table, column, target_type)
DATE_COLUMNS: list[tuple[str, str]] = [
    # Instance table (30M rows)
    ("instance", "acquisition_date"),
    ("instance", "content_date"),
    # Series table (346K rows)
    ("series", "series_date"),
    # Study table (33K rows)
    ("study", "study_date"),
    # Event table
    ("event", "event_date"),
    # Subject disease types
    ("subject_disease_types", "assignment_date"),
]

TIME_COLUMNS: list[tuple[str, str]] = [
    # Instance table
    ("instance", "acquisition_time"),
    ("instance", "content_time"),
    # Series table
    ("series", "series_time"),
    ("series", "contrast_bolus_start_time"),
    # Study table
    ("study", "study_time"),
    # PET series details
    ("pet_series_details", "radiopharmaceutical_start_time"),
    ("pet_series_details", "radiopharmaceutical_stop_time"),
]

# Large tables that need batched processing
LARGE_TABLES = {"instance"}
BATCH_SIZE = 100000


def _create_helper_functions(conn: Connection) -> None:
    """Create PostgreSQL helper functions for DICOM date/time conversion."""
    
    # Function to convert DICOM date (YYYYMMDD) to PostgreSQL DATE
    conn.execute(text("""
        CREATE OR REPLACE FUNCTION dicom_to_date(val TEXT) RETURNS DATE AS $$
        BEGIN
            IF val IS NULL OR val = '' THEN
                RETURN NULL;
            END IF;
            -- YYYYMMDD format (DICOM standard)
            IF LENGTH(val) = 8 AND val ~ '^[0-9]{8}$' THEN
                RETURN TO_DATE(val, 'YYYYMMDD');
            END IF;
            -- Already ISO format YYYY-MM-DD
            IF val ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                RETURN TO_DATE(val, 'YYYY-MM-DD');
            END IF;
            -- Return NULL for unrecognized formats
            RETURN NULL;
        EXCEPTION WHEN OTHERS THEN
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql IMMUTABLE;
    """))
    
    # Function to convert DICOM time (HHMMSS[.ffffff]) to PostgreSQL TIME
    conn.execute(text("""
        CREATE OR REPLACE FUNCTION dicom_to_time(val TEXT) RETURNS TIME AS $$
        DECLARE
            time_part TEXT;
            frac_part TEXT;
            formatted TEXT;
        BEGIN
            IF val IS NULL OR val = '' THEN
                RETURN NULL;
            END IF;
            
            -- Handle HHMMSS.ffffff format
            IF POSITION('.' IN val) > 0 THEN
                time_part := SPLIT_PART(val, '.', 1);
                frac_part := SPLIT_PART(val, '.', 2);
            ELSE
                time_part := val;
                frac_part := NULL;
            END IF;
            
            -- Convert HHMMSS to HH:MM:SS
            IF LENGTH(time_part) >= 6 AND time_part ~ '^[0-9]+$' THEN
                formatted := SUBSTRING(time_part, 1, 2) || ':' ||
                            SUBSTRING(time_part, 3, 2) || ':' ||
                            SUBSTRING(time_part, 5, 2);
                IF frac_part IS NOT NULL AND LENGTH(frac_part) > 0 THEN
                    formatted := formatted || '.' || frac_part;
                END IF;
                RETURN formatted::TIME;
            END IF;
            
            -- Already in HH:MM:SS format
            IF val ~ '^[0-9]{2}:[0-9]{2}:[0-9]{2}' THEN
                RETURN val::TIME;
            END IF;
            
            -- Return NULL for unrecognized formats
            RETURN NULL;
        EXCEPTION WHEN OTHERS THEN
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql IMMUTABLE;
    """))
    
    logger.info("Created helper functions: dicom_to_date, dicom_to_time")


def _drop_helper_functions(conn: Connection) -> None:
    """Drop helper functions after migration."""
    conn.execute(text("DROP FUNCTION IF EXISTS dicom_to_date(TEXT)"))
    conn.execute(text("DROP FUNCTION IF EXISTS dicom_to_time(TEXT)"))
    logger.info("Dropped helper functions")


def _get_column_type(conn: Connection, table: str, column: str) -> str | None:
    """Get the current data type of a column."""
    inspector = inspect(conn)
    try:
        columns = inspector.get_columns(table)
        for col in columns:
            if col["name"] == column:
                return str(col["type"]).upper()
    except Exception:
        pass
    return None


def _table_exists(conn: Connection, table: str) -> bool:
    """Check if a table exists."""
    inspector = inspect(conn)
    return table in inspector.get_table_names()


def _get_row_count(conn: Connection, table: str) -> int:
    """Get the row count for a table."""
    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
    return result.scalar() or 0


def _is_already_migrated(conn: Connection) -> bool:
    """Check if migration has already been applied."""
    # Check if instance.acquisition_date is already DATE type
    col_type = _get_column_type(conn, "instance", "acquisition_date")
    if col_type and "DATE" in col_type and "TEXT" not in col_type:
        return True
    return False


def _migrate_column_direct(
    conn: Connection,
    table: str,
    column: str,
    target_type: str,
    conversion_func: str,
) -> None:
    """Migrate a column directly using ALTER TABLE."""
    sql = f"""
        ALTER TABLE {table}
        ALTER COLUMN {column} TYPE {target_type}
        USING {conversion_func}({column})
    """
    conn.execute(text(sql))


def _migrate_date_columns(conn: Connection) -> None:
    """Migrate all DATE columns."""
    for table, column in DATE_COLUMNS:
        if not _table_exists(conn, table):
            logger.debug(f"Table {table} does not exist, skipping")
            continue
        
        col_type = _get_column_type(conn, table, column)
        if col_type and "DATE" in col_type and "TEXT" not in col_type:
            logger.debug(f"{table}.{column} already migrated, skipping")
            continue
        
        row_count = _get_row_count(conn, table)
        logger.info(f"Migrating {table}.{column} to DATE ({row_count:,} rows)")
        
        start = time.time()
        _migrate_column_direct(conn, table, column, "DATE", "dicom_to_date")
        elapsed = time.time() - start
        
        logger.info(f"  Completed {table}.{column} in {elapsed:.1f}s")


def _migrate_time_columns(conn: Connection) -> None:
    """Migrate all TIME columns."""
    for table, column in TIME_COLUMNS:
        if not _table_exists(conn, table):
            logger.debug(f"Table {table} does not exist, skipping")
            continue
        
        col_type = _get_column_type(conn, table, column)
        if col_type and "TIME" in col_type and "TEXT" not in col_type:
            logger.debug(f"{table}.{column} already migrated, skipping")
            continue
        
        row_count = _get_row_count(conn, table)
        logger.info(f"Migrating {table}.{column} to TIME ({row_count:,} rows)")
        
        start = time.time()
        _migrate_column_direct(conn, table, column, "TIME", "dicom_to_time")
        elapsed = time.time() - start
        
        logger.info(f"  Completed {table}.{column} in {elapsed:.1f}s")


def run_migration(engine: Engine, *, dry_run: bool = False) -> dict[str, Any]:
    """
    Run the datetime migration.
    
    Args:
        engine: SQLAlchemy engine
        dry_run: If True, don't commit changes
        
    Returns:
        Dictionary with migration results
    """
    results = {
        "success": False,
        "already_migrated": False,
        "tables_migrated": [],
        "columns_migrated": 0,
        "elapsed_seconds": 0,
        "error": None,
    }
    
    start_time = time.time()
    
    with engine.begin() as conn:
        # Check if already migrated
        if _is_already_migrated(conn):
            logger.info("Migration already applied, skipping")
            results["already_migrated"] = True
            results["success"] = True
            return results
        
        try:
            # Create helper functions
            _create_helper_functions(conn)
            
            # Get initial row counts for verification
            initial_counts = {}
            tables = set(t for t, _ in DATE_COLUMNS + TIME_COLUMNS)
            for table in tables:
                if _table_exists(conn, table):
                    initial_counts[table] = _get_row_count(conn, table)
            
            # Migrate DATE columns
            logger.info("=== Migrating DATE columns ===")
            _migrate_date_columns(conn)
            
            # Migrate TIME columns
            logger.info("=== Migrating TIME columns ===")
            _migrate_time_columns(conn)
            
            # Verify row counts
            logger.info("=== Verifying row counts ===")
            for table, initial_count in initial_counts.items():
                final_count = _get_row_count(conn, table)
                if initial_count != final_count:
                    raise RuntimeError(
                        f"Row count mismatch for {table}: "
                        f"initial={initial_count}, final={final_count}"
                    )
                logger.info(f"  {table}: {final_count:,} rows (verified)")
            
            # Drop helper functions
            _drop_helper_functions(conn)
            
            # Calculate results
            results["tables_migrated"] = list(tables)
            results["columns_migrated"] = len(DATE_COLUMNS) + len(TIME_COLUMNS)
            results["success"] = True
            
            if dry_run:
                logger.info("Dry run mode - rolling back changes")
                conn.rollback()
            
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
        "date_columns": {},
        "time_columns": {},
    }
    
    with engine.connect() as conn:
        status["migrated"] = _is_already_migrated(conn)
        
        for table, column in DATE_COLUMNS:
            if _table_exists(conn, table):
                col_type = _get_column_type(conn, table, column)
                status["date_columns"][f"{table}.{column}"] = col_type
        
        for table, column in TIME_COLUMNS:
            if _table_exists(conn, table):
                col_type = _get_column_type(conn, table, column)
                status["time_columns"][f"{table}.{column}"] = col_type
    
    return status
