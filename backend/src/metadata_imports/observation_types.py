"""Import logic for observation_types table (fields, preview, apply, lookup)."""

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

OBSERVATION_TYPE_FIELDS: tuple[FieldDefinition, ...] = (
    FieldDefinition(name="observation_type_id", label="ID", required=False, default_parser="int", parsers=("int",)),
    FieldDefinition(name="category", label="Category", required=True),
    FieldDefinition(name="name", label="Name", required=True),
    FieldDefinition(name="description", label="Description"),
    FieldDefinition(name="unit", label="Unit"),
    FieldDefinition(name="value_type", label="Value Type"),
    FieldDefinition(name="min_value", label="Min Value", default_parser="float", parsers=("float",)),
    FieldDefinition(name="max_value", label="Max Value", default_parser="float", parsers=("float",)),
    FieldDefinition(name="is_active", label="Active", default_parser="bool", parsers=("bool",)),
    FieldDefinition(name="is_primary", label="Primary", default_parser="bool", parsers=("bool",)),
)

OBSERVATION_TYPE_FIELD_MAP = {d.name: d for d in OBSERVATION_TYPE_FIELDS}

VALUE_COLUMNS: tuple[str, ...] = (
    "category",
    "name",
    "description",
    "unit",
    "value_type",
    "min_value",
    "max_value",
    "is_active",
    "is_primary",
)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ObservationTypeImportOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    skip_blank_updates: bool = Field(default=True, alias="skipBlankUpdates")


class ObservationTypeImportPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    file_token: str | None = Field(default=None, alias="fileToken")
    file_path: str | None = Field(default=None, alias="filePath")
    fields: dict[str, FieldMapping] = Field(alias="fields")
    options: ObservationTypeImportOptions = Field(default_factory=ObservationTypeImportOptions)
    dry_run: bool = Field(default=False, alias="dryRun")

    @model_validator(mode="after")
    def _validate_fields(self) -> "ObservationTypeImportPayload":
        mappings = self.fields or {}
        required = [f.name for f in OBSERVATION_TYPE_FIELDS if f.required and f.name not in mappings]
        if required:
            raise ValueError(f"Observation type import requires mappings for: {', '.join(required)}")
        unknown = sorted(set(mappings) - set(OBSERVATION_TYPE_FIELD_MAP))
        if unknown:
            raise ValueError(f"Unknown observation type field mappings: {', '.join(unknown)}")
        return self


class ObservationTypePreviewRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    observation_type: dict[str, Any] = Field(alias="observationType")
    existing: bool = False
    existing_observation_type: dict[str, Any] | None = Field(default=None, alias="existingObservationType")


class ObservationTypeImportPreview(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    total_rows: int = Field(alias="totalRows")
    processed_rows: int = Field(alias="processedRows")
    skipped_rows: int = Field(alias="skippedRows")
    warnings: list[str]
    rows: list[ObservationTypePreviewRow]


class ObservationTypeImportResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    types_inserted: int = Field(alias="typesInserted")
    types_updated: int = Field(alias="typesUpdated")


class ObservationTypeFieldsResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    observation_type_fields: list[dict[str, Any]] = Field(alias="observationTypeFields")


class ObservationTypeDetailResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    observation_type_id: int = Field(alias="observationTypeId")
    category: str
    name: str
    description: str | None = None
    unit: str | None = None
    value_type: str | None = Field(default=None, alias="valueType")
    min_value: float | None = Field(default=None, alias="minValue")
    max_value: float | None = Field(default=None, alias="maxValue")
    is_active: bool = Field(alias="isActive")
    is_primary: bool = Field(alias="isPrimary")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _serialize_record(record: schema.ObservationType) -> dict[str, Any]:
    return {
        "observation_type_id": record.observation_type_id,
        "category": record.category,
        "name": record.name,
        "description": record.description,
        "unit": record.unit,
        "value_type": record.value_type,
        "min_value": record.min_value,
        "max_value": record.max_value,
        "is_active": bool(record.is_active) if record.is_active is not None else True,
        "is_primary": bool(record.is_primary) if record.is_primary is not None else False,
    }


def _load_existing_map(session: Session) -> dict[int, dict[str, Any]]:
    records = session.scalars(select(schema.ObservationType)).all()
    return {r.observation_type_id: _serialize_record(r) for r in records}


def _load_name_map(session: Session) -> dict[tuple[str, str], dict[str, Any]]:
    """Build (category_lower, name_lower) -> serialized record map for matching by name."""
    records = session.scalars(select(schema.ObservationType)).all()
    return {
        (r.category.lower(), r.name.lower()): _serialize_record(r)
        for r in records
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_fields_response() -> ObservationTypeFieldsResponse:
    fields = [
        {
            "name": f.name,
            "label": f.label,
            "required": f.required,
            "parsers": list(f.parsers),
            "defaultParser": f.default_parser,
        }
        for f in OBSERVATION_TYPE_FIELDS
    ]
    return ObservationTypeFieldsResponse(observationTypeFields=fields)


def get_observation_type_by_id(engine: Engine, observation_type_id: int) -> dict[str, Any] | None:
    with Session(engine) as session:
        record = session.execute(
            select(schema.ObservationType).where(
                schema.ObservationType.observation_type_id == observation_type_id
            )
        ).scalar_one_or_none()
        if record is None:
            return None
        return _serialize_record(record)


def preview_observation_type_import(
    *,
    engine: Engine,
    path: Path,
    config: ObservationTypeImportPayload,
    preview_limit: int = 50,
) -> ObservationTypeImportPreview:
    warnings: list[str] = []
    rows: list[ObservationTypePreviewRow] = []
    total = 0
    skipped = 0
    field_mappings = config.fields

    with Session(engine) as session:
        existing_by_id = _load_existing_map(session)
        existing_by_name = _load_name_map(session)

    for row_number, (row, aliases) in enumerate(iter_csv_rows(path), start=1):
        total += 1
        if len(rows) >= preview_limit:
            continue

        parsed: dict[str, Any] = {}
        for field in OBSERVATION_TYPE_FIELDS:
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

        category = parsed.get("category")
        name = parsed.get("name")
        if not category or not name:
            warnings.append(f"Row {row_number}: missing category or name, skipped")
            skipped += 1
            continue

        obs_id = parsed.get("observation_type_id")
        existing: dict[str, Any] | None = None
        if obs_id is not None:
            existing = existing_by_id.get(int(obs_id))
        if existing is None:
            key = (str(category).lower(), str(name).lower())
            existing = existing_by_name.get(key)

        rows.append(ObservationTypePreviewRow(
            observationType=parsed,
            existing=existing is not None,
            existingObservationType=existing,
        ))

    return ObservationTypeImportPreview(
        totalRows=total,
        processedRows=len(rows),
        skippedRows=skipped,
        warnings=warnings,
        rows=rows,
    )


def apply_observation_type_import(
    *,
    engine: Engine,
    path: Path,
    config: ObservationTypeImportPayload,
) -> ObservationTypeImportResult:
    warnings: list[str] = []
    field_mappings = config.fields
    skip_blanks = config.options.skip_blank_updates
    inserted = 0
    updated = 0

    with Session(engine) as session:
        existing_by_id = _load_existing_map(session)
        existing_by_name = _load_name_map(session)

        for row_number, (row, aliases) in enumerate(iter_csv_rows(path), start=1):
            parsed: dict[str, Any] = {}
            for field in OBSERVATION_TYPE_FIELDS:
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

            category = parsed.get("category")
            name = parsed.get("name")
            if not category or not name:
                continue

            obs_id = parsed.get("observation_type_id")
            existing: dict[str, Any] | None = None
            existing_id: int | None = None
            if obs_id is not None:
                existing_id = int(obs_id)
                existing = existing_by_id.get(existing_id)
            if existing is None:
                key = (str(category).lower(), str(name).lower())
                existing = existing_by_name.get(key)
                if existing:
                    existing_id = existing["observation_type_id"]

            if existing is not None and existing_id is not None:
                # UPDATE
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
                    sql = f"UPDATE observation_types SET {', '.join(set_clauses)} WHERE observation_type_id = :_id"
                    session.execute(text(sql), params)
                    updated += 1
                    existing_by_id[existing_id] = {**existing, **{k: v for k, v in parsed.items() if v is not None}}
            else:
                # INSERT
                cols = ["category", "name"]
                vals: dict[str, Any] = {"category": category, "name": name}
                for col in VALUE_COLUMNS:
                    if col in ("category", "name"):
                        continue
                    v = parsed.get(col)
                    if v is not None:
                        cols.append(col)
                        vals[col] = v
                if obs_id is not None:
                    cols.insert(0, "observation_type_id")
                    vals["observation_type_id"] = int(obs_id)

                col_list = ", ".join(cols)
                val_list = ", ".join(f":{c}" for c in cols)
                sql = f"INSERT INTO observation_types ({col_list}) VALUES ({val_list}) RETURNING observation_type_id"
                row_result = session.execute(text(sql), vals)
                new_id = int(obs_id) if obs_id else None
                if new_id is None:
                    returned = row_result.scalar()
                    new_id = int(returned) if returned is not None else None
                if new_id is not None:
                    new_record = {**vals, "observation_type_id": new_id}
                    existing_by_id[new_id] = new_record
                    existing_by_name[(str(category).lower(), str(name).lower())] = new_record
                inserted += 1

        if not config.dry_run:
            session.commit()
        else:
            session.rollback()

    return ObservationTypeImportResult(typesInserted=inserted, typesUpdated=updated)
