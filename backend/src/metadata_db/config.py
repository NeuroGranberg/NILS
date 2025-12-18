"""Settings for the metadata database and backup workflow."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel


DEFAULT_DB_URL = "postgresql+psycopg://postgres:postgres@localhost:5433/neurotoolkit_metadata"


class MetadataDatabaseSettings(BaseModel):
    url: str = DEFAULT_DB_URL
    echo: bool = False
    pool_size: int = 20  # Increased from 5 for better concurrent DICOM loading
    max_overflow: int = 30  # Allow burst connections
    pool_pre_ping: bool = True  # Verify connections before use


class MetadataBackupSettings(BaseModel):
    enabled: bool = True
    directory: Path = Path("resource/backups/metadata")
    auto_restore: bool = False
    verify_dumps: bool = True


@lru_cache
def get_settings() -> MetadataDatabaseSettings:
    return MetadataDatabaseSettings(
        url=os.getenv("METADATA_DATABASE_URL", DEFAULT_DB_URL),
        echo=os.getenv("METADATA_DB_ECHO", "false").lower() == "true",
        pool_size=int(os.getenv("METADATA_DB_POOL_SIZE", "20")),
        max_overflow=int(os.getenv("METADATA_DB_MAX_OVERFLOW", "30")),
        pool_pre_ping=os.getenv("METADATA_DB_POOL_PRE_PING", "true").lower() == "true",
    )


@lru_cache
def get_backup_settings() -> MetadataBackupSettings:
    directory_env = os.getenv("METADATA_BACKUP_DIR")
    directory = Path(directory_env) if directory_env else MetadataBackupSettings().directory
    return MetadataBackupSettings(
        enabled=os.getenv("METADATA_BACKUP_ENABLED", "true").lower() == "true",
        directory=directory,
        auto_restore=os.getenv("METADATA_AUTO_RESTORE", "false").lower() == "true",
        verify_dumps=os.getenv("METADATA_BACKUP_VERIFY", "true").lower() == "true",
    )
