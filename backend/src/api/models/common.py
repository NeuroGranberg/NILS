"""Common Pydantic schemas shared across routes."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class DataTablesOrder(BaseModel):
    column: int
    dir: str


class DataTablesColumn(BaseModel):
    data: str
    name: str | None = None
    searchable: bool = True
    orderable: bool = True


class DataTablesSearch(BaseModel):
    value: str | None = None
    regex: bool = False


class DataTablesRequest(BaseModel):
    draw: int = 0
    start: int = Field(default=0, ge=0, le=1000000)
    length: int = Field(default=50, ge=1, le=500)
    order: list[DataTablesOrder] = []
    columns: list[DataTablesColumn] = []
    search: DataTablesSearch | None = None


class DataTablesResponse(BaseModel):
    draw: int
    recordsTotal: int
    recordsFiltered: int
    data: list[dict[str, Any]]
