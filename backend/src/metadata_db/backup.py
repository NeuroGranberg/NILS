"""Compatibility wrappers around the shared backup management utilities."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

from metadata_db.config import get_backup_settings

from backup.manager import BackupError, DatabaseKey, PostgresBackupManager, get_backup_config

logger = logging.getLogger(__name__)

# Tables added by migrations that may not exist in older backups.
# These must be dropped BEFORE pg_restore to avoid FK dependency errors.
# Order matters: drop tables with FKs first, then tables they reference.
MIGRATION_TABLES = [
    "stack_fingerprint",  # Has FK to series_stack
]


class MetadataBackupManager(PostgresBackupManager):
    """Backward-compatible manager dedicated to the metadata database."""

    def __init__(self) -> None:
        self.backup_settings = get_backup_settings()
        super().__init__(get_backup_config(DatabaseKey.METADATA))

    def create_backup(self, directory: str | Path | None = None, *, note: str | None = None) -> Path:
        return super().create_backup(directory=directory, note=note)

    def _drop_migration_tables(self) -> None:
        """Drop tables created by migrations to allow clean pg_restore.
        
        When restoring an older backup, pg_restore --clean can fail because
        newer tables (created by migrations) have FK dependencies on tables
        that pg_restore wants to drop and recreate.
        
        Solution: Drop these migration tables first, restore, then re-apply migrations.
        """
        from .session import SessionLocal
        from sqlalchemy import text
        
        logger.info("Dropping migration tables before restore...")
        
        with SessionLocal() as session:
            for table_name in MIGRATION_TABLES:
                try:
                    # Use CASCADE to handle any remaining dependencies
                    session.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                    logger.info("Dropped table: %s", table_name)
                except Exception as exc:
                    logger.warning("Failed to drop table %s: %s", table_name, exc)
            session.commit()

    def restore(self, dump_path: str | Path | None = None) -> Path:
        """Restore metadata database and apply migrations for schema compatibility.
        
        This ensures that restored backups (potentially from older versions)
        are automatically brought up-to-date with the current schema via migrations.
        
        The restore process:
        1. Drop tables created by migrations (to avoid FK conflicts)
        2. Run pg_restore --clean --if-exists
        3. Re-apply migrations to bring schema up to date
        
        Args:
            dump_path: Path to backup file (or None for latest)
            
        Returns:
            Path to the restored backup file
        """
        # Step 1: Drop migration tables to avoid FK dependency errors
        self._drop_migration_tables()
        
        def post_restore_migrations():
            """Apply any pending migrations to restored database."""
            from .lifecycle import ensure_schema
            
            logger.info("Applying migrations to restored database...")
            try:
                ensure_schema()
                logger.info("Migrations applied successfully - database schema is current")
            except Exception as exc:
                logger.error("Migration failed after restore: %s", exc)
                raise
        
        # Step 2 & 3: Restore and apply migrations
        return super().restore(dump_path, post_restore_hook=post_restore_migrations)

    def latest_backup(self) -> Path | None:
        return super().latest_backup()

    def auto_restore_if_empty(self, is_empty: bool) -> bool:
        if not is_empty or not self.backup_settings.auto_restore:
            return False
        try:
            latest = self.ensure_backup_path(None)
        except BackupError:
            return False
        try:
            super().restore(latest)
            return True
        except BackupError:
            return False

    def list_backups(self) -> Iterable[Path]:
        return super().list_backups()

    def clear_backups(self) -> None:
        for dump in list(self.list_backups()):
            try:
                Path(dump).unlink()
            except OSError:
                pass
