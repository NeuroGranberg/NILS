"""Import logic for disease_types table (manual-only upsert with disease lookup)."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from metadata_db import schema

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class DiseaseSummary(BaseModel):
    id: int
    name: str
    code: str | None = None


class DiseaseTypeDetail(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    disease_type_id: int = Field(alias="diseaseTypeId")
    disease_id: int = Field(alias="diseaseId")
    type_name: str = Field(alias="typeName")
    description: str | None = None
    sort_order: int | None = Field(default=None, alias="sortOrder")


class DiseaseTypeFieldsResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    diseases: list[DiseaseSummary]


class DiseaseTypeUpsertPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    disease_type_id: int | None = Field(default=None, alias="diseaseTypeId")
    disease_id: int = Field(alias="diseaseId")
    type_name: str = Field(alias="typeName")
    description: str | None = None
    sort_order: int | None = Field(default=None, alias="sortOrder")
    dry_run: bool = Field(default=False, alias="dryRun")


class DiseaseTypeUpsertResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    disease_type_id: int = Field(alias="diseaseTypeId")
    inserted: bool
    updated: bool


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_disease_type_fields_response(engine: Engine) -> DiseaseTypeFieldsResponse:
    with Session(engine) as session:
        rows = session.execute(
            select(schema.Disease).order_by(schema.Disease.disease_name)
        ).scalars().all()
        diseases = [
            DiseaseSummary(id=d.disease_id, name=d.disease_name, code=d.disease_code)
            for d in rows
        ]
    return DiseaseTypeFieldsResponse(diseases=diseases)


def get_disease_type_by_id(engine: Engine, disease_type_id: int) -> DiseaseTypeDetail | None:
    with Session(engine) as session:
        record = session.execute(
            select(schema.DiseaseType).where(schema.DiseaseType.disease_type_id == disease_type_id)
        ).scalar_one_or_none()
        if record is None:
            return None
        return DiseaseTypeDetail(
            diseaseTypeId=record.disease_type_id,
            diseaseId=record.disease_id,
            typeName=record.type_name,
            description=record.description,
            sortOrder=record.sort_order,
        )


def upsert_disease_type(
    engine: Engine, payload: DiseaseTypeUpsertPayload,
) -> DiseaseTypeUpsertResult:
    with Session(engine) as session:
        # Validate disease exists
        disease = session.execute(
            select(schema.Disease).where(schema.Disease.disease_id == payload.disease_id)
        ).scalar_one_or_none()
        if disease is None:
            raise ValueError(f"Disease with id {payload.disease_id} not found")

        existing: schema.DiseaseType | None = None
        if payload.disease_type_id is not None:
            existing = session.execute(
                select(schema.DiseaseType).where(
                    schema.DiseaseType.disease_type_id == payload.disease_type_id
                )
            ).scalar_one_or_none()

        # Also match by (disease_id, type_name) if no ID match
        if existing is None:
            existing = session.execute(
                select(schema.DiseaseType).where(
                    schema.DiseaseType.disease_id == payload.disease_id,
                    schema.DiseaseType.type_name == payload.type_name,
                )
            ).scalar_one_or_none()

        if existing is not None:
            # UPDATE
            updates: list[str] = []
            params: dict[str, Any] = {"_id": existing.disease_type_id}
            if payload.disease_id != existing.disease_id:
                updates.append("disease_id = :disease_id")
                params["disease_id"] = payload.disease_id
            if payload.type_name != existing.type_name:
                updates.append("type_name = :type_name")
                params["type_name"] = payload.type_name
            if payload.description is not None and payload.description != existing.description:
                updates.append("description = :description")
                params["description"] = payload.description
            if payload.sort_order is not None and payload.sort_order != existing.sort_order:
                updates.append("sort_order = :sort_order")
                params["sort_order"] = payload.sort_order

            if updates:
                sql = f"UPDATE disease_types SET {', '.join(updates)} WHERE disease_type_id = :_id"
                session.execute(text(sql), params)

            if not payload.dry_run:
                session.commit()
            else:
                session.rollback()

            return DiseaseTypeUpsertResult(
                diseaseTypeId=existing.disease_type_id,
                inserted=False,
                updated=bool(updates),
            )
        else:
            # INSERT
            cols = ["disease_id", "type_name"]
            vals: dict[str, Any] = {
                "disease_id": payload.disease_id,
                "type_name": payload.type_name,
            }
            if payload.disease_type_id is not None:
                cols.insert(0, "disease_type_id")
                vals["disease_type_id"] = payload.disease_type_id
            if payload.description is not None:
                cols.append("description")
                vals["description"] = payload.description
            if payload.sort_order is not None:
                cols.append("sort_order")
                vals["sort_order"] = payload.sort_order

            col_list = ", ".join(cols)
            val_list = ", ".join(f":{c}" for c in cols)
            result = session.execute(
                text(f"INSERT INTO disease_types ({col_list}) VALUES ({val_list}) RETURNING disease_type_id"),
                vals,
            )
            new_id = result.scalar()

            if not payload.dry_run:
                session.commit()
            else:
                session.rollback()

            return DiseaseTypeUpsertResult(
                diseaseTypeId=new_id if new_id else payload.disease_type_id or 0,
                inserted=True,
                updated=False,
            )
