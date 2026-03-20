"""
Migration: Add DWI enrichment columns.

Adds 6 raw private-tag columns to mri_series_details (populated during extraction)
and 4 computed columns to series_classification_cache (populated during Sort Step 4).

mri_series_details new columns:
  dwi_siemens_b_value         INTEGER  - Siemens (0019,100C) b-value
  dwi_siemens_directionality  TEXT     - Siemens (0019,100D) 'DIRECTIONAL'|'NONE'
  dwi_siemens_pe_dir_positive INTEGER  - Siemens CSA (0029,1010) PhaseEncodingDirectionPositive
  dwi_ge_b_value              INTEGER  - GE (0043,1039)[0] b-value
  dwi_ge_n_directions         INTEGER  - GE (0043,1030) number of diffusion directions
  dwi_philips_b_value         REAL     - Philips (2001,1003) b-value

series_classification_cache new columns:
  dwi_b_value      REAL     - max non-zero b-value in stack (naming shell)
  dwi_b_values_csv TEXT     - all unique b-values in stack, comma-separated
  dwi_pe_direction TEXT     - anatomical PE direction: AP/PA/RL/LR/IS/SI
  dwi_n_directions INTEGER  - count of unique gradient directions (excl. b0)

Backward compatible: detects missing columns and applies only once.
"""

from __future__ import annotations

import logging
import time

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)

_MRI_COLUMNS = [
    ("dwi_siemens_b_value", "INTEGER"),
    ("dwi_siemens_directionality", "TEXT"),
    ("dwi_siemens_pe_dir_positive", "INTEGER"),
    ("dwi_ge_b_value", "INTEGER"),
    ("dwi_ge_n_directions", "INTEGER"),
    ("dwi_philips_b_value", "REAL"),
]

_CACHE_COLUMNS = [
    ("dwi_b_value", "REAL"),
    ("dwi_b_values_csv", "TEXT"),
    ("dwi_pe_direction", "TEXT"),
    ("dwi_n_directions", "INTEGER"),
]


def _needs_migration(conn: Connection) -> bool:
    inspector = inspect(conn)
    tables = inspector.get_table_names()
    if "mri_series_details" not in tables and "series_classification_cache" not in tables:
        return False
    if "mri_series_details" in tables:
        mri_cols = {c["name"] for c in inspector.get_columns("mri_series_details")}
        if "dwi_siemens_b_value" not in mri_cols:
            return True
    if "series_classification_cache" in tables:
        cache_cols = {c["name"] for c in inspector.get_columns("series_classification_cache")}
        if "dwi_b_value" not in cache_cols:
            return True
    return False


def run_migration(engine: Engine, dry_run: bool = False) -> dict:
    results: dict = {
        "success": False,
        "already_migrated": False,
        "changes_made": [],
        "elapsed_seconds": 0.0,
    }

    start_time = time.time()

    with engine.begin() as conn:
        if not _needs_migration(conn):
            results["success"] = True
            results["already_migrated"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results

        if dry_run:
            logger.info("DRY RUN: Would add DWI enrichment columns")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results

        inspector = inspect(conn)
        tables = inspector.get_table_names()

        if "mri_series_details" in tables:
            existing = {c["name"] for c in inspector.get_columns("mri_series_details")}
            for col_name, col_type in _MRI_COLUMNS:
                if col_name not in existing:
                    conn.execute(text(
                        f"ALTER TABLE mri_series_details ADD COLUMN {col_name} {col_type}"
                    ))
                    results["changes_made"].append(f"mri_series_details.{col_name} {col_type}")
                    logger.info("Added mri_series_details.%s %s", col_name, col_type)

        if "series_classification_cache" in tables:
            existing = {c["name"] for c in inspector.get_columns("series_classification_cache")}
            for col_name, col_type in _CACHE_COLUMNS:
                if col_name not in existing:
                    conn.execute(text(
                        f"ALTER TABLE series_classification_cache ADD COLUMN {col_name} {col_type}"
                    ))
                    results["changes_made"].append(f"series_classification_cache.{col_name} {col_type}")
                    logger.info("Added series_classification_cache.%s %s", col_name, col_type)

        results["success"] = True
        results["elapsed_seconds"] = time.time() - start_time
        logger.info(
            "DWI enrichment migration completed: %d columns added (%.1fs)",
            len(results["changes_made"]),
            results["elapsed_seconds"],
        )

    return results
