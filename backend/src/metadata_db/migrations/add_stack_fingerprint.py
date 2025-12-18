"""
Migration to add stack_fingerprint table.

This table stores flattened, feature-rich representations of SeriesStacks
designed for classification algorithms. It normalizes physics parameters
across modalities and aggregates text fields into searchable blobs.

The stack_fingerprint table has a 1:1 relationship with series_stack
via series_stack_id foreign key with CASCADE delete.

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
    return "stack_fingerprint" not in inspector.get_table_names()


def migrate(engine: Engine, dry_run: bool = False) -> dict:
    """
    Add stack_fingerprint table to metadata database.
    
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
            logger.info("stack_fingerprint migration not needed (table already exists)")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results
        
        if dry_run:
            logger.info("DRY RUN: Would create stack_fingerprint table")
            results["success"] = True
            results["elapsed_seconds"] = time.time() - start_time
            return results
        
        logger.info("Starting stack_fingerprint table migration...")
        
        # Step 1: Create the stack_fingerprint table
        logger.info("Creating table: stack_fingerprint")
        conn.execute(text("""
            CREATE TABLE stack_fingerprint (
                -- Primary key
                fingerprint_id SERIAL PRIMARY KEY,
                
                -- 1:1 relationship with series_stack
                series_stack_id INTEGER NOT NULL UNIQUE 
                    REFERENCES series_stack(series_stack_id) ON DELETE CASCADE,
                
                -- General metadata
                modality VARCHAR(16) NOT NULL,
                manufacturer TEXT,
                manufacturer_model TEXT,
                
                -- Text features
                stack_sequence_name TEXT,
                text_search_blob TEXT,
                contrast_search_blob TEXT,
                
                -- Geometry & dimensions
                stack_orientation VARCHAR(32),
                fov_x REAL,
                fov_y REAL,
                aspect_ratio REAL,
                
                -- General series features
                image_type TEXT,
                scanning_sequence TEXT,
                sequence_variant TEXT,
                scan_options TEXT,
                
                -- MR specific features
                mr_te REAL,
                mr_tr REAL,
                mr_ti REAL,
                mr_flip_angle REAL,
                mr_echo_train_length INTEGER,
                mr_echo_number TEXT,
                mr_acquisition_type VARCHAR(2),
                mr_angio_flag VARCHAR(1),
                mr_b1rms TEXT,
                mr_diffusion_b_value TEXT,
                mr_parallel_acquisition_technique TEXT,
                mr_temporal_position_identifier TEXT,
                mr_phase_contrast VARCHAR(1),
                
                -- CT specific features
                ct_kvp REAL,
                ct_exposure_time REAL,
                ct_tube_current REAL,
                ct_convolution_kernel TEXT,
                ct_revolution_time REAL,
                ct_filter_type TEXT,
                ct_is_calcium_score BOOLEAN,
                
                -- PET specific features
                pet_bed_index INTEGER,
                pet_frame_type TEXT,
                pet_tracer TEXT,
                pet_reconstruction_method TEXT,
                pet_suv_type TEXT,
                pet_units VARCHAR(32),
                pet_units_type TEXT,
                pet_series_type TEXT,
                pet_corrected_image TEXT,
                pet_counts_source TEXT,
                pet_is_attenuation_corrected BOOLEAN,
                pet_radionuclide_total_dose REAL,
                pet_radionuclide_half_life REAL,
                
                -- Aggregates
                stack_n_instances INTEGER,
                
                -- Timestamps
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """))
        results["changes_made"].append("Created table stack_fingerprint")
        
        # Step 2: Create indexes for common query patterns
        logger.info("Creating indexes on stack_fingerprint")
        
        # Index on modality for filtering
        conn.execute(text("""
            CREATE INDEX idx_stack_fingerprint_modality
            ON stack_fingerprint(modality);
        """))
        results["changes_made"].append("Created index idx_stack_fingerprint_modality")
        
        # Index on manufacturer for grouping/filtering
        conn.execute(text("""
            CREATE INDEX idx_stack_fingerprint_manufacturer
            ON stack_fingerprint(manufacturer);
        """))
        results["changes_made"].append("Created index idx_stack_fingerprint_manufacturer")
        
        # GIN index on text_search_blob for full-text search (if contains text)
        conn.execute(text("""
            CREATE INDEX idx_stack_fingerprint_text_search
            ON stack_fingerprint USING gin(to_tsvector('english', COALESCE(text_search_blob, '')));
        """))
        results["changes_made"].append("Created GIN index idx_stack_fingerprint_text_search")
        
        # Partial index for MR series
        conn.execute(text("""
            CREATE INDEX idx_stack_fingerprint_mr
            ON stack_fingerprint(mr_te, mr_tr, mr_ti)
            WHERE modality = 'MR';
        """))
        results["changes_made"].append("Created partial index idx_stack_fingerprint_mr")
        
        # Partial index for CT series  
        conn.execute(text("""
            CREATE INDEX idx_stack_fingerprint_ct
            ON stack_fingerprint(ct_kvp, ct_tube_current)
            WHERE modality = 'CT';
        """))
        results["changes_made"].append("Created partial index idx_stack_fingerprint_ct")
        
        # Partial index for PET series
        conn.execute(text("""
            CREATE INDEX idx_stack_fingerprint_pet
            ON stack_fingerprint(pet_tracer, pet_is_attenuation_corrected)
            WHERE modality = 'PT';
        """))
        results["changes_made"].append("Created partial index idx_stack_fingerprint_pet")
        
        conn.commit()
        
        logger.info("stack_fingerprint migration completed successfully")
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
            "table_exists": bool,
            "row_count": int | None,
            "indexes": list[str]
        }
    """
    status = {
        "migrated": False,
        "table_exists": False,
        "row_count": None,
        "indexes": [],
    }
    
    with engine.connect() as conn:
        inspector = inspect(conn)
        status["table_exists"] = "stack_fingerprint" in inspector.get_table_names()
        status["migrated"] = status["table_exists"]
        
        if status["table_exists"]:
            # Get row count
            result = conn.execute(text("SELECT COUNT(*) FROM stack_fingerprint"))
            status["row_count"] = result.scalar()
            
            # Get indexes
            indexes = inspector.get_indexes("stack_fingerprint")
            status["indexes"] = [idx["name"] for idx in indexes]
    
    return status
