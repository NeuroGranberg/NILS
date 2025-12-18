"""Tests for stack_key nullable migration."""

import pytest
from sqlalchemy import inspect, text

from src.metadata_db.migrations.migrate_stack_key_nullable import (
    check_migration_status,
    run_migration,
)


def test_migration_makes_stack_key_nullable(metadata_engine):
    """Test that migration makes stack_key nullable."""
    # Run migration
    results = run_migration(metadata_engine, dry_run=False)
    
    assert results["success"]
    assert "Changed stack_key to nullable" in results["changes_made"]
    
    # Verify stack_key is now nullable
    with metadata_engine.connect() as conn:
        columns = inspect(conn).get_columns("series_stack")
        stack_key_col = next(c for c in columns if c["name"] == "stack_key")
        assert stack_key_col["nullable"] is True


def test_migration_removes_unique_constraint(metadata_engine):
    """Test that migration removes uq_series_stack_key constraint."""
    # Run migration
    results = run_migration(metadata_engine, dry_run=False)
    
    assert results["success"]
    
    # Verify constraint is removed
    with metadata_engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM pg_constraint
            WHERE conname = 'uq_series_stack_key'
        """))
        count = result.scalar()
        assert count == 0


def test_migration_status_check(metadata_engine):
    """Test migration status check."""
    # Before migration
    status_before = check_migration_status(metadata_engine)
    
    # Run migration
    run_migration(metadata_engine, dry_run=False)
    
    # After migration
    status_after = check_migration_status(metadata_engine)
    assert status_after["migrated"] is True
    assert status_after["stack_key_nullable"] is True
    assert status_after["constraint_exists"] is False


def test_migration_idempotent(metadata_engine):
    """Test that migration can be run multiple times safely."""
    # Run migration twice
    results1 = run_migration(metadata_engine, dry_run=False)
    results2 = run_migration(metadata_engine, dry_run=False)
    
    assert results1["success"]
    assert results2["success"]
    assert results2["already_migrated"] is True


def test_can_insert_null_stack_key_after_migration(metadata_engine):
    """Test that we can insert NULL stack_key after migration."""
    # Run migration
    run_migration(metadata_engine, dry_run=False)
    
    # Insert a record with NULL stack_key
    with metadata_engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO series_stack (
                series_id, stack_modality, stack_index, stack_key, stack_n_instances
            ) VALUES (
                1, 'MR', 0, NULL, 100
            )
        """))
        conn.commit()
        
        # Verify it was inserted
        result = conn.execute(text("""
            SELECT stack_key FROM series_stack WHERE series_id = 1
        """))
        row = result.fetchone()
        assert row[0] is None  # NULL value
        
        # Clean up
        conn.execute(text("DELETE FROM series_stack WHERE series_id = 1"))
        conn.commit()
