"""Pydantic schemas for database backup/restore operations."""

from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from pydantic import BaseModel

from backup.manager import DatabaseKey


class BackupInfo(BaseModel):
    database: str
    database_label: str
    filename: str
    path: str
    size_bytes: int
    created_at: dt.datetime
    note: Optional[str] = None


class RestoreBackupPayload(BaseModel):
    path: str | None = None


class CreateDatabaseBackupPayload(BaseModel):
    database: DatabaseKey
    directory: str | None = None
    note: Optional[str] = None


class RestoreDatabaseBackupPayload(BaseModel):
    database: DatabaseKey
    path: str | None = None


class DeleteDatabaseBackupPayload(BaseModel):
    database: DatabaseKey
    path: str


class RestoreJobResponse(BaseModel):
    job: dict[str, Any]
    backup: BackupInfo


class MetadataTableColumnInfo(BaseModel):
    name: str
    label: str
    type: str
    searchable: bool
    orderable: bool


class MetadataTableInfo(BaseModel):
    name: str
    label: str
    row_count: int
    columns: list[MetadataTableColumnInfo]
    category: str = "other"
    description: str = ""


class TableCategoryInfo(BaseModel):
    """Category grouping for tables."""
    id: str
    label: str
    description: str
    table_names: list[str]


class DatabaseSummary(BaseModel):
    database: str
    database_label: str
    tables: dict[str, int]


# DataTables server-side processing models
class DataTablesOrder(BaseModel):
    """DataTables column ordering specification."""
    column: int
    dir: str


class DataTablesColumn(BaseModel):
    """DataTables column definition."""
    data: str
    name: str | None = None
    searchable: bool = True
    orderable: bool = True


class DataTablesSearch(BaseModel):
    """DataTables search specification."""
    value: str | None = None
    regex: bool = False


class DataTablesRequest(BaseModel):
    """DataTables server-side request payload."""
    draw: int = 0
    start: int = 0
    length: int = 50
    order: list[DataTablesOrder] = []
    columns: list[DataTablesColumn]
    search: DataTablesSearch | None = None


class DataTablesResponse(BaseModel):
    """DataTables server-side response."""
    draw: int
    recordsTotal: int
    recordsFiltered: int
    data: list[dict[str, Any]]
