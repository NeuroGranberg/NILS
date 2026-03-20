"""Import logic for diseases table (fields, preview, apply, lookup)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from metadata_db import schema

from .shared import (
    FieldDefinition,
    FieldMapping,
    coerce_value,
    iter_csv_rows,
)

logger = logging.getLogger(__name__)

DISEASE_FIELDS: tuple[FieldDefinition, ...] = (
    FieldDefinition(name="disease_id", label="ID", required=False, default_parser="int", parsers=("int",)),
    FieldDefinition(name="disease_name", label="Disease Name", required=True),
    FieldDefinition(name="disease_code", label="Disease Code"),
    FieldDefinition(name="description", label="Description"),
)

DISEASE_FIELD_MAP = {f.name: f for f in DISEASE_FIELDS}

VALUE_COLUMNS: tuple[str, ...] = ("disease_name", "disease_code", "description")


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class DiseaseImportOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    skip_blank_updates: bool = Field(default=True, alias="skipBlankUpdates")


class DiseaseImportPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    file_token: str | None = Field(default=None, alias="fileToken")
    file_path: str | None = Field(default=None, alias="filePath")
    fields: dict[str, FieldMapping] = Field(alias="fields")
    options: DiseaseImportOptions = Field(default_factory=DiseaseImportOptions)
    dry_run: bool = Field(default=False, alias="dryRun")

    @model_validator(mode="after")
    def _validate_fields(self) -> "DiseaseImportPayload":
        mappings = self.fields or {}
        required = [f.name for f in DISEASE_FIELDS if f.required and f.name not in mappings]
        if required:
            raise ValueError(f"Disease import requires mappings for: {', '.join(required)}")
        unknown = sorted(set(mappings) - set(DISEASE_FIELD_MAP))
        if unknown:
            raise ValueError(f"Unknown disease field mappings: {', '.join(unknown)}")
        return self


class DiseasePreviewRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    disease: dict[str, Any]
    existing: bool = False
    existing_disease: dict[str, Any] | None = Field(default=None, alias="existingDisease")


class DiseaseImportPreview(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    total_rows: int = Field(alias="totalRows")
    processed_rows: int = Field(alias="processedRows")
    skipped_rows: int = Field(alias="skippedRows")
    warnings: list[str]
    rows: list[DiseasePreviewRow]


class DiseaseImportResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    diseases_inserted: int = Field(alias="diseasesInserted")
    diseases_updated: int = Field(alias="diseasesUpdated")


class DiseaseFieldsResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    disease_fields: list[dict[str, Any]] = Field(alias="diseaseFields")


class DiseaseDetailResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    disease_id: int = Field(alias="diseaseId")
    disease_name: str = Field(alias="diseaseName")
    disease_code: str | None = Field(default=None, alias="diseaseCode")
    description: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _serialize_record(record: schema.Disease) -> dict[str, Any]:
    return {
        "disease_id": record.disease_id,
        "disease_name": record.disease_name,
        "disease_code": record.disease_code,
        "description": record.description,
    }


def _load_existing_by_id(session: Session) -> dict[int, dict[str, Any]]:
    records = session.scalars(select(schema.Disease)).all()
    return {r.disease_id: _serialize_record(r) for r in records}


def _load_existing_by_name(session: Session) -> dict[str, dict[str, Any]]:
    records = session.scalars(select(schema.Disease)).all()
    return {r.disease_name.lower(): _serialize_record(r) for r in records}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_fields_response() -> DiseaseFieldsResponse:
    fields = [
        {
            "name": f.name,
            "label": f.label,
            "required": f.required,
            "parsers": list(f.parsers),
            "defaultParser": f.default_parser,
        }
        for f in DISEASE_FIELDS
    ]
    return DiseaseFieldsResponse(diseaseFields=fields)


def get_disease_by_id(engine: Engine, disease_id: int) -> dict[str, Any] | None:
    with Session(engine) as session:
        record = session.execute(
            select(schema.Disease).where(schema.Disease.disease_id == disease_id)
        ).scalar_one_or_none()
        if record is None:
            return None
        return _serialize_record(record)


def preview_disease_import(
    *,
    engine: Engine,
    path: Path,
    config: DiseaseImportPayload,
    preview_limit: int = 50,
) -> DiseaseImportPreview:
    warnings: list[str] = []
    rows: list[DiseasePreviewRow] = []
    total = 0
    skipped = 0
    field_mappings = config.fields

    with Session(engine) as session:
        existing_by_id = _load_existing_by_id(session)
        existing_by_name = _load_existing_by_name(session)

    for row_number, (row, aliases) in enumerate(iter_csv_rows(path), start=1):
        total += 1
        if len(rows) >= preview_limit:
            continue

        parsed: dict[str, Any] = {}
        for field in DISEASE_FIELDS:
            mapping = field_mappings.get(field.name)
            if not mapping:
                continue
            value = coerce_value(
                field=field,
                mapping=mapping,
                row=row,
                aliases=aliases,
                warnings=warnings,
                row_number=row_number,
            )
            parsed[field.name] = value

        disease_name = parsed.get("disease_name")
        if not disease_name:
            warnings.append(f"Row {row_number}: missing disease_name, skipped")
            skipped += 1
            continue

        disease_id = parsed.get("disease_id")
        existing: dict[str, Any] | None = None
        if disease_id is not None:
            existing = existing_by_id.get(int(disease_id))
        if existing is None:
            existing = existing_by_name.get(str(disease_name).lower())

        rows.append(DiseasePreviewRow(
            disease=parsed,
            existing=existing is not None,
            existingDisease=existing,
        ))

    return DiseaseImportPreview(
        totalRows=total,
        processedRows=len(rows),
        skippedRows=skipped,
        warnings=warnings,
        rows=rows,
    )


def apply_disease_import(
    *,
    engine: Engine,
    path: Path,
    config: DiseaseImportPayload,
) -> DiseaseImportResult:
    warnings: list[str] = []
    field_mappings = config.fields
    skip_blanks = config.options.skip_blank_updates
    inserted = 0
    updated = 0

    with Session(engine) as session:
        existing_by_id = _load_existing_by_id(session)
        existing_by_name = _load_existing_by_name(session)

        for row_number, (row, aliases) in enumerate(iter_csv_rows(path), start=1):
            parsed: dict[str, Any] = {}
            for field in DISEASE_FIELDS:
                mapping = field_mappings.get(field.name)
                if not mapping:
                    continue
                value = coerce_value(
                    field=field,
                    mapping=mapping,
                    row=row,
                    aliases=aliases,
                    warnings=warnings,
                    row_number=row_number,
                )
                parsed[field.name] = value

            disease_name = parsed.get("disease_name")
            if not disease_name:
                continue

            disease_id = parsed.get("disease_id")
            existing: dict[str, Any] | None = None
            existing_id: int | None = None
            if disease_id is not None:
                existing_id = int(disease_id)
                existing = existing_by_id.get(existing_id)
            if existing is None:
                existing = existing_by_name.get(str(disease_name).lower())
                if existing:
                    existing_id = existing["disease_id"]

            if existing is not None and existing_id is not None:
                set_clauses: list[str] = []
                params: dict[str, Any] = {"_id": existing_id}
                for col in VALUE_COLUMNS:
                    new_val = parsed.get(col)
                    if skip_blanks and (new_val is None or (isinstance(new_val, str) and new_val.strip() == "")):
                        continue
                    old_val = existing.get(col)
                    if new_val == old_val:
                        continue
                    set_clauses.append(f"{col} = :{col}")
                    params[col] = new_val
                if set_clauses:
                    sql = f"UPDATE diseases SET {', '.join(set_clauses)} WHERE disease_id = :_id"
                    session.execute(text(sql), params)
                    updated += 1
                    existing_by_id[existing_id] = {**existing, **{k: v for k, v in parsed.items() if v is not None}}
            else:
                cols = ["disease_name"]
                vals: dict[str, Any] = {"disease_name": disease_name}
                for col in VALUE_COLUMNS:
                    if col == "disease_name":
                        continue
                    v = parsed.get(col)
                    if v is not None:
                        cols.append(col)
                        vals[col] = v
                if disease_id is not None:
                    cols.insert(0, "disease_id")
                    vals["disease_id"] = int(disease_id)

                col_list = ", ".join(cols)
                val_list = ", ".join(f":{c}" for c in cols)
                sql = f"INSERT INTO diseases ({col_list}) VALUES ({val_list}) RETURNING disease_id"
                row_result = session.execute(text(sql), vals)
                new_id = int(disease_id) if disease_id else None
                if new_id is None:
                    returned = row_result.scalar()
                    new_id = int(returned) if returned is not None else None
                if new_id is not None:
                    new_record = {**vals, "disease_id": new_id}
                    existing_by_id[new_id] = new_record
                    existing_by_name[str(disease_name).lower()] = new_record
                inserted += 1

        if not config.dry_run:
            session.commit()
        else:
            session.rollback()

    return DiseaseImportResult(diseasesInserted=inserted, diseasesUpdated=updated)
