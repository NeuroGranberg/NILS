"""Database configuration via Pydantic settings."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel


class DatabaseSettings(BaseModel):
    url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/neurotoolkit"
    echo: bool = False
    pool_size: int = 5


class DatabaseBackupSettings(BaseModel):
    enabled: bool = True
    directory: Path = Path("resource/backups/application")
    verify_dumps: bool = True


@lru_cache
def get_settings() -> DatabaseSettings:
    return DatabaseSettings(
        url=os.getenv("DATABASE_URL", DatabaseSettings().url),
        echo=os.getenv("DATABASE_ECHO", "false").lower() == "true",
        pool_size=int(os.getenv("DATABASE_POOL_SIZE", "5")),
    )


@lru_cache
def get_backup_settings() -> DatabaseBackupSettings:
    directory_env = os.getenv("DATABASE_BACKUP_DIR")
    directory = Path(directory_env) if directory_env else DatabaseBackupSettings().directory
    return DatabaseBackupSettings(
        enabled=os.getenv("DATABASE_BACKUP_ENABLED", "true").lower() == "true",
        directory=directory,
        verify_dumps=os.getenv("DATABASE_BACKUP_VERIFY", "true").lower() == "true",
    )
