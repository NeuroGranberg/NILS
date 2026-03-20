"""
Migration to fix slice counts for multi-frame Enhanced DICOM.

This migration corrects the stack_n_instances column in series_stack table
and slices_count column in series_classification_cache table to properly
account for multi-frame DICOM files (Enhanced MR, etc.) where a single
instance file contains multiple frames.

Background:
- Classic DICOM: 1 file = 1 slice, COUNT(*) works correctly
- Enhanced DICOM: 1 file = N frames, need SUM(COALESCE(number_of_frames, 1))

Example: SWI series with 1 DICOM file containing 72 frames was incorrectly
showing slices_count=1 instead of 72.

Usage:
    Runs automatically on server startup via lifecycle.py
    Safe for repeated execution (idempotent)
"""

from __future__ import annotations

import logging
import time

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)


def run_migration(engine: Engine) -> None:
    """Fix slice counts for multi-frame Enhanced DICOM in existing data."""
    start_time = time.time()
    logger.info("Starting multi-frame slice count fix migration...")

    with engine.begin() as conn:
        # Step 1: Update series_stack.stack_n_instances
        # This is the source of truth for slice counts
        affected_stacks = _fix_stack_n_instances(conn)
        
        # Step 2: Update series_classification_cache.slices_count
        # This is derived from stack_n_instances during classification
        affected_cache = _fix_classification_cache_slices_count(conn)
        
        # Step 3: Update stack_fingerprint.stack_n_instances
        # This is used by the classification pipeline
        affected_fingerprints = _fix_fingerprint_stack_n_instances(conn)

    elapsed = time.time() - start_time
    logger.info(
        f"Multi-frame slice count fix completed in {elapsed:.2f}s: "
        f"{affected_stacks} stacks, {affected_cache} cache entries, "
        f"{affected_fingerprints} fingerprints updated"
    )


def _fix_stack_n_instances(conn: Connection) -> int:
    """
    Update series_stack.stack_n_instances to use SUM(number_of_frames).
    
    Only updates rows where the current value differs from the correct value.
    """
    # First, identify stacks that need fixing
    check_query = text("""
        SELECT 
            ss.series_stack_id,
            ss.stack_n_instances as current_value,
            SUM(COALESCE(i.number_of_frames, 1)) as correct_value
        FROM series_stack ss
        JOIN instance i ON ss.series_stack_id = i.series_stack_id
        GROUP BY ss.series_stack_id, ss.stack_n_instances
        HAVING ss.stack_n_instances != SUM(COALESCE(i.number_of_frames, 1))
           OR ss.stack_n_instances IS NULL
    """)
    
    result = conn.execute(check_query)
    mismatched = result.fetchall()
    
    if not mismatched:
        logger.info("series_stack.stack_n_instances: All values already correct")
        return 0
    
    logger.info(f"series_stack.stack_n_instances: Found {len(mismatched)} stacks needing update")
    
    # Log some examples
    for row in mismatched[:5]:
        logger.info(
            f"  Stack {row.series_stack_id}: {row.current_value} -> {row.correct_value}"
        )
    if len(mismatched) > 5:
        logger.info(f"  ... and {len(mismatched) - 5} more")
    
    # Update all affected stacks
    update_query = text("""
        UPDATE series_stack ss
        SET stack_n_instances = sub.total_slices
        FROM (
            SELECT series_stack_id, SUM(COALESCE(number_of_frames, 1)) as total_slices
            FROM instance
            WHERE series_stack_id IS NOT NULL
            GROUP BY series_stack_id
        ) sub
        WHERE ss.series_stack_id = sub.series_stack_id
          AND (ss.stack_n_instances != sub.total_slices OR ss.stack_n_instances IS NULL)
    """)
    
    result = conn.execute(update_query)
    return result.rowcount


def _fix_classification_cache_slices_count(conn: Connection) -> int:
    """
    Update series_classification_cache.slices_count from series_stack.stack_n_instances.
    
    This ensures the cached classification data reflects the correct slice counts.
    """
    # First, identify cache entries that need fixing
    check_query = text("""
        SELECT 
            scc.series_stack_id,
            scc.slices_count as current_value,
            ss.stack_n_instances as correct_value
        FROM series_classification_cache scc
        JOIN series_stack ss ON scc.series_stack_id = ss.series_stack_id
        WHERE scc.slices_count != ss.stack_n_instances
           OR (scc.slices_count IS NULL AND ss.stack_n_instances IS NOT NULL)
    """)
    
    result = conn.execute(check_query)
    mismatched = result.fetchall()
    
    if not mismatched:
        logger.info("series_classification_cache.slices_count: All values already correct")
        return 0
    
    logger.info(f"series_classification_cache.slices_count: Found {len(mismatched)} entries needing update")
    
    # Update from series_stack
    update_query = text("""
        UPDATE series_classification_cache scc
        SET slices_count = ss.stack_n_instances
        FROM series_stack ss
        WHERE scc.series_stack_id = ss.series_stack_id
          AND (scc.slices_count != ss.stack_n_instances
               OR (scc.slices_count IS NULL AND ss.stack_n_instances IS NOT NULL))
    """)
    
    result = conn.execute(update_query)
    return result.rowcount


def _fix_fingerprint_stack_n_instances(conn: Connection) -> int:
    """
    Update stack_fingerprint.stack_n_instances from series_stack.stack_n_instances.
    
    This ensures the fingerprint data used by classification reflects correct counts.
    """
    # Check if stack_fingerprint table exists
    table_check = text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'stack_fingerprint'
        )
    """)
    result = conn.execute(table_check)
    if not result.scalar():
        logger.info("stack_fingerprint table does not exist, skipping")
        return 0
    
    # First, identify fingerprints that need fixing
    check_query = text("""
        SELECT 
            fp.series_stack_id,
            fp.stack_n_instances as current_value,
            ss.stack_n_instances as correct_value
        FROM stack_fingerprint fp
        JOIN series_stack ss ON fp.series_stack_id = ss.series_stack_id
        WHERE fp.stack_n_instances != ss.stack_n_instances
           OR (fp.stack_n_instances IS NULL AND ss.stack_n_instances IS NOT NULL)
    """)
    
    result = conn.execute(check_query)
    mismatched = result.fetchall()
    
    if not mismatched:
        logger.info("stack_fingerprint.stack_n_instances: All values already correct")
        return 0
    
    logger.info(f"stack_fingerprint.stack_n_instances: Found {len(mismatched)} entries needing update")
    
    # Update from series_stack
    update_query = text("""
        UPDATE stack_fingerprint fp
        SET stack_n_instances = ss.stack_n_instances
        FROM series_stack ss
        WHERE fp.series_stack_id = ss.series_stack_id
          AND (fp.stack_n_instances != ss.stack_n_instances
               OR (fp.stack_n_instances IS NULL AND ss.stack_n_instances IS NOT NULL))
    """)
    
    result = conn.execute(update_query)
    return result.rowcount
