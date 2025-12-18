"""Database and metadata backup/restore API routes."""
from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, Response, status
from fastapi.responses import Response as FastAPIResponse
from pydantic import BaseModel

from backup.manager import PostgresBackupManager, BackupError, get_backup_config
from metadata_db.backup import MetadataBackupManager
from db.backup import ApplicationBackupManager
from jobs.service import job_service
from jobs.models import JobDTO

# Import models from api/models
from api.models.database import (
    BackupInfo,
    CreateDatabaseBackupPayload,
    DeleteDatabaseBackupPayload,
    RestoreDatabaseBackupPayload,
    DatabaseKey,
)

# Import serializers
from api.utils.serializers import serialize_job as _serialize_job

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["backups"])

# Constants
RESTORE_STAGE = "restore"
DATABASE_LABELS = {
    DatabaseKey.METADATA: "Metadata",
    DatabaseKey.APPLICATION: "Application",
}


def _get_backup_manager(database: DatabaseKey) -> PostgresBackupManager:
    """Get backup manager for specified database."""
    if database is DatabaseKey.METADATA:
        # Preserve existing behaviour that reuses the singleton metadata manager for auto-restore.
        return MetadataBackupManager()
    elif database is DatabaseKey.APPLICATION:
        # Use application backup manager with post-restore migrations
        return ApplicationBackupManager()
    return PostgresBackupManager(get_backup_config(database))


def _read_backup_metadata(path: Path) -> dict | None:
    """Read backup metadata JSON file."""
    metadata_path = Path(f"{path}.json")
    if not metadata_path.exists():
        return None
    try:
        return json.loads(metadata_path.read_text())
    except json.JSONDecodeError:
        return None


def _build_backup_info(path: Path, database: DatabaseKey) -> BackupInfo:
    """Build BackupInfo from backup file path."""
    stat = path.stat()
    metadata = _read_backup_metadata(path)
    created_at = None
    size_bytes = stat.st_size
    note_value: Optional[str] = None
    if metadata:
        try:
            created_at = dt.datetime.fromisoformat(metadata.get("created_at"))
        except (TypeError, ValueError):
            created_at = None
        size_bytes = metadata.get("size_bytes", size_bytes)
        note_raw = metadata.get("note")
        if isinstance(note_raw, str) and note_raw.strip():
            note_value = note_raw.strip()
    if created_at is None:
        created_at = dt.datetime.fromtimestamp(stat.st_mtime, tz=dt.timezone.utc)
    elif created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=dt.timezone.utc)
    return BackupInfo(
        database=database.value,
        database_label=DATABASE_LABELS[database],
        filename=path.name,
        path=str(path.resolve()),
        created_at=created_at,
        size_bytes=size_bytes,
        note=note_value,
    )


def _is_client_backup_error(detail: str) -> bool:
    """Determine if backup error is client-caused (400) vs server (500)."""
    detail_lower = detail.lower()
    client_patterns = [
        "not found",
        "does not exist",
        "invalid",
        "must be",
        "cannot",
        "failed to parse",
    ]
    return any(pattern in detail_lower for pattern in client_patterns)


def _run_database_restore(job_id: int, database_value: str, path_str: str) -> None:
    """Background task to run database restore."""
    database = DatabaseKey(database_value)
    manager = _get_backup_manager(database)
    try:
        job_service.mark_running(job_id)
        manager.restore(Path(path_str))
        job_service.mark_completed(job_id)
    except BackupError as exc:
        logger.exception("Database restore failed for %s", path_str)
        job_service.mark_failed(job_id, str(exc))
    except Exception as exc:  # pragma: no cover - unexpected failure
        logger.exception("Unexpected error during database restore for %s", path_str)
        job_service.mark_failed(job_id, str(exc))


def _start_restore_job(
    database: DatabaseKey,
    path: str | Path | None,
    submit_restore: Optional[callable] = None,
) -> tuple[JobDTO, Path]:
    """Start a background restore job."""
    from api.server import _restore_executor  # Import executor from server
    
    manager = _get_backup_manager(database)
    target = manager.ensure_backup_path(path)

    job = job_service.create_job(
        stage=RESTORE_STAGE,
        config={
            "database": database.value,
            "path": str(target),
        },
        name=f"Restore {database.value} database",
    )

    submit_fn = submit_restore or _restore_executor.submit
    submit_fn(_run_database_restore, job.id, database.value, str(target))
    return job, target


# Response model for restore endpoint
class RestoreJobResponse(BaseModel):
    """Response for database restore request."""
    job: dict
    backup: BackupInfo


@router.get("/database/backups", response_model=list[BackupInfo])
def list_database_backups(database: DatabaseKey | None = Query(default=None)):
    """List all database backups, optionally filtered by database."""
    keys = [database] if database else list(DatabaseKey)
    records: list[BackupInfo] = []
    for key in keys:
        manager = _get_backup_manager(key)
        for path in manager.list_backups():
            try:
                records.append(_build_backup_info(path, key))
            except FileNotFoundError:
                continue
    records.sort(key=lambda info: info.created_at, reverse=True)
    return records


@router.post("/database/backups", response_model=BackupInfo)
def create_database_backup(payload: CreateDatabaseBackupPayload):
    """Create a new database backup."""
    manager = _get_backup_manager(payload.database)
    try:
        kwargs: dict[str, Any] = {}
        if payload.directory is not None:
            kwargs["directory"] = payload.directory
        if payload.note is not None:
            kwargs["note"] = payload.note
        path = manager.create_backup(**kwargs)
    except BackupError as exc:
        detail = str(exc)
        status_code = 400 if _is_client_backup_error(detail) else 500
        raise HTTPException(status_code=status_code, detail=detail)
    return _build_backup_info(path, payload.database)


@router.delete("/database/backups", status_code=status.HTTP_204_NO_CONTENT)
def delete_database_backup(payload: DeleteDatabaseBackupPayload):
    """Delete a database backup."""
    manager = _get_backup_manager(payload.database)
    try:
        manager.delete_backup(payload.path)
    except BackupError as exc:
        detail = str(exc)
        status_code = 400 if _is_client_backup_error(detail) else 500
        raise HTTPException(status_code=status_code, detail=detail)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/database/restore",
    response_model=RestoreJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def restore_database_backup(payload: RestoreDatabaseBackupPayload):
    """Restore a database from backup (asynchronous)."""
    from fastapi import Request
    
    # Get app state for executor - will be injected properly when router is registered
    try:
        job, target = _start_restore_job(
            payload.database,
            payload.path,
            None,  # Will use _restore_executor default
        )
    except BackupError as exc:
        detail = str(exc)
        status_code = 400 if _is_client_backup_error(detail) else 500
        raise HTTPException(status_code=status_code, detail=detail)
    latest_job = job_service.get_job(job.id) or job
    return RestoreJobResponse(
        job=_serialize_job(latest_job) or {},
        backup=_build_backup_info(target, payload.database),
    )


@router.get("/metadata/backups", response_model=list[BackupInfo])
def list_metadata_backups():
    """List all metadata database backups."""
    manager = _get_backup_manager(DatabaseKey.METADATA)
    return [_build_backup_info(path, DatabaseKey.METADATA) for path in manager.list_backups()]


@router.post("/metadata/backups", response_model=BackupInfo)
def create_metadata_backup():
    """Create a new metadata database backup."""
    manager = _get_backup_manager(DatabaseKey.METADATA)
    try:
        path = manager.create_backup()
    except BackupError as exc:
        detail = str(exc)
        status_code = 400 if _is_client_backup_error(detail) else 500
        raise HTTPException(status_code=status_code, detail=detail)
    return _build_backup_info(path, DatabaseKey.METADATA)


@router.post(
    "/metadata/restore",
    response_model=RestoreJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def restore_metadata_backup(payload: RestoreDatabaseBackupPayload):
    """Restore metadata database from backup (asynchronous)."""
    try:
        job, target = _start_restore_job(
            DatabaseKey.METADATA,
            payload.path,
            None,
        )
    except BackupError as exc:
        detail = str(exc)
        status_code = 400 if _is_client_backup_error(detail) else 500
        raise HTTPException(status_code=status_code, detail=detail)
    latest_job = job_service.get_job(job.id) or job
    return RestoreJobResponse(
        job=_serialize_job(latest_job) or {},
        backup=_build_backup_info(target, DatabaseKey.METADATA),
    )
