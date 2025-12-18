from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import func, select, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

from metadata_db import schema

from .shared import FieldDefinition, FieldMapping, coerce_value, copy_rows, iter_csv_rows

_coerce_value = coerce_value
_iter_csv_rows = iter_csv_rows
_copy_rows = copy_rows


COHORT_FIELDS: tuple[FieldDefinition, ...] = (
    FieldDefinition(name="name", label="Cohort Name", required=True),
    FieldDefinition(name="owner", label="Owner"),
    FieldDefinition(name="path", label="Path"),
    FieldDefinition(name="description", label="Description"),
    FieldDefinition(
        name="cohort_id",
        label="Cohort ID (optional)",
        required=False,
        default_parser="int",
        parsers=("int", "string"),
    ),
)

COHORT_FIELD_MAP = {definition.name: definition for definition in COHORT_FIELDS}
PRIMARY_COHORT_FIELDS = {"name"}
COHORT_VALUE_COLUMNS: tuple[str, ...] = ("owner", "path", "description")


class CohortImportOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    skip_blank_updates: bool = Field(default=True, alias="skipBlankUpdates")


class CohortImportPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    file_token: str | None = Field(default=None, alias="fileToken")
    file_path: str | None = Field(default=None, alias="filePath")
    cohort_fields: dict[str, FieldMapping] = Field(alias="cohortFields")
    options: CohortImportOptions = Field(default_factory=CohortImportOptions)
    dry_run: bool = Field(default=False, alias="dryRun")

    @model_validator(mode="after")
    def _validate_fields(self) -> "CohortImportPayload":
        mappings = self.cohort_fields or {}
        missing = [field for field in PRIMARY_COHORT_FIELDS if field not in mappings]
        if missing:
            raise ValueError(f"Cohort import requires mappings for: {', '.join(sorted(missing))}")
        unknown = sorted(set(mappings) - set(COHORT_FIELD_MAP))
        if unknown:
            raise ValueError(f"Unknown cohort field mappings: {', '.join(unknown)}")
        return self


def _normalize_cohort_name(value: str) -> str:
    normalized = value.strip().lower()
    if not normalized:
        raise ValueError("Cohort name cannot be blank")
    return normalized


def _normalize_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value if text_value else None


def _normalize_required_string(value: Any, field_label: str) -> str:
    normalized = _normalize_optional_string(value)
    if not normalized:
        raise ValueError(f"{field_label} is required")
    return normalized


@dataclass
class ParsedCohortRow:
    normalized_name: str
    values: dict[str, Any]
    provided_id: int | None = None


class CohortImportFieldsResponse(BaseModel):
    cohort_fields: list[dict[str, Any]] = Field(alias="cohortFields")


class CohortImportPreviewRow(BaseModel):
    cohort: dict[str, Any]
    existing: bool = False
    existing_cohort: dict[str, Any] | None = Field(default=None, alias="existingCohort")


class CohortImportPreview(BaseModel):
    total_rows: int = Field(alias="totalRows")
    processed_rows: int = Field(alias="processedRows")
    skipped_rows: int = Field(alias="skippedRows")
    warnings: list[str]
    rows: list[CohortImportPreviewRow]


class CohortImportResult(BaseModel):
    cohorts_inserted: int = Field(alias="cohortsInserted")
    cohorts_updated: int = Field(alias="cohortsUpdated")


def build_cohort_fields_response() -> CohortImportFieldsResponse:
    payload = [
        {
            "name": field.name,
            "label": field.label,
            "required": field.required,
            "parsers": list(field.parsers),
            "defaultParser": field.default_parser,
        }
        for field in COHORT_FIELDS
    ]
    return CohortImportFieldsResponse(cohortFields=payload)


def _parse_cohort_rows(
    *,
    path: Path,
    config: CohortImportPayload,
    preview_limit: int | None = None,
) -> tuple[list[ParsedCohortRow], list[str], int, int]:
    rows: list[ParsedCohortRow] = []
    warnings: list[str] = []
    total_rows = 0
    skipped_rows = 0
    mappings = config.cohort_fields

    name_mapping = mappings.get("name")
    if name_mapping is None:
        warnings.append("Cohort Name mapping missing; all rows skipped")
        return [], warnings, 0, 0

    id_mapping = mappings.get("cohort_id")

    for row_number, (row, aliases) in enumerate(_iter_csv_rows(path), start=1):
        total_rows += 1
        if preview_limit is not None and len(rows) >= preview_limit:
            continue

        raw_name = _coerce_value(
            field=COHORT_FIELD_MAP["name"],
            mapping=name_mapping,
            row=row,
            aliases=aliases,
            warnings=warnings,
            row_number=row_number,
        )
        if raw_name in (None, ""):
            warnings.append(f"Row {row_number}: missing cohort name, skipped")
            skipped_rows += 1
            continue

        original_name = str(raw_name).strip()

        try:
            normalized_name = _normalize_cohort_name(str(raw_name))
        except ValueError as exc:
            warnings.append(f"Row {row_number}: {exc}")
            skipped_rows += 1
            continue

        values: dict[str, Any] = {"name": normalized_name, "_display_name": original_name}
        provided_id: int | None = None

        if id_mapping is not None:
            raw_id = _coerce_value(
                field=COHORT_FIELD_MAP["cohort_id"],
                mapping=id_mapping,
                row=row,
                aliases=aliases,
                warnings=warnings,
                row_number=row_number,
            )
            if raw_id not in (None, ""):
                try:
                    provided_id = int(str(raw_id))
                    values["cohort_id"] = provided_id
                except ValueError:
                    warnings.append(f"Row {row_number}: invalid cohort id '{raw_id}', ignored")

        missing_required = False
        for field in COHORT_FIELDS:
            if field.name in ("name", "cohort_id"):
                continue
            mapping = mappings.get(field.name)
            if not mapping:
                continue
            value = _coerce_value(
                field=field,
                mapping=mapping,
                row=row,
                aliases=aliases,
                warnings=warnings,
                row_number=row_number,
            )
            normalized_value = _normalize_optional_string(value)
            values[field.name] = normalized_value
            if field.required and normalized_value in (None, ""):
                warnings.append(f"Row {row_number}: missing value for required field '{field.label}', skipped")
                skipped_rows += 1
                missing_required = True
                break

        if missing_required:
            continue

        rows.append(
            ParsedCohortRow(
                normalized_name=normalized_name,
                values=values,
                provided_id=provided_id,
            )
        )

    return rows, warnings, total_rows, skipped_rows


def preview_cohort_import(
    *,
    engine: Engine,
    path: Path,
    config: CohortImportPayload,
    limit: int = 25,
) -> CohortImportPreview:
    rows, warnings, total_rows, skipped_rows = _parse_cohort_rows(path=path, config=config, preview_limit=limit)

    existing_map: dict[str, dict[str, Any]] = {}
    name_keys = [row.normalized_name for row in rows]
    if name_keys:
        with Session(engine) as session:
            existing_map = _load_existing_cohort_map_by_names(session, name_keys)

    preview_rows: list[CohortImportPreviewRow] = []
    for row in rows:
        existing = existing_map.get(row.normalized_name)
        combined = dict(existing or {})
        combined.update(row.values)
        if existing and "cohort_id" not in combined:
            combined["cohort_id"] = existing.get("cohort_id")
        elif not existing and row.provided_id is not None:
            combined.setdefault("cohort_id", row.provided_id)
        if existing and existing.get("name"):
            combined["name"] = existing["name"]
        elif combined.get("_display_name"):
            combined["name"] = combined["_display_name"]
        combined.pop("_display_name", None)
        preview_rows.append(
            CohortImportPreviewRow(
                cohort=combined,
                existing=existing is not None,
                existingCohort=existing,
            )
        )

    return CohortImportPreview(
        totalRows=total_rows,
        processedRows=len(rows),
        skippedRows=skipped_rows,
        warnings=warnings,
        rows=preview_rows,
    )


def apply_cohort_import(
    *,
    engine: Engine,
    path: Path,
    config: CohortImportPayload,
) -> CohortImportResult:
    rows, _, _, _ = _parse_cohort_rows(path=path, config=config, preview_limit=None)
    if not rows:
        return CohortImportResult(cohortsInserted=0, cohortsUpdated=0)

    cohort_data: dict[str, dict[str, Any]] = {}
    for row in rows:
        cohort_data[row.normalized_name] = {
            key: value
            for key, value in row.values.items()
            if key not in {"cohort_id"}
        }

    name_keys = sorted(cohort_data.keys())
    existing_map: dict[str, dict[str, Any]] = {}
    with Session(engine) as session:
        if name_keys:
            existing_map = _load_existing_cohort_map_by_names(session, name_keys)

    existing_names = set(existing_map.keys())
    cohorts_inserted = sum(1 for name in name_keys if name not in existing_names)
    cohorts_updated = len(name_keys) - cohorts_inserted

    conn = engine.connect()
    trans = conn.begin()
    try:
        _stage_and_merge_cohorts(
            conn,
            cohort_data,
            skip_blank_updates=config.options.skip_blank_updates,
            existing_map=existing_map,
        )
    except Exception:
        trans.rollback()
        conn.close()
        raise
    else:
        if config.dry_run:
            trans.rollback()
        else:
            trans.commit()
        conn.close()

    return CohortImportResult(cohortsInserted=cohorts_inserted, cohortsUpdated=cohorts_updated)


def _stage_and_merge_cohorts(
    conn: Connection,
    data: dict[str, dict[str, Any]],
    *,
    skip_blank_updates: bool,
    existing_map: dict[str, dict[str, Any]],
) -> None:
    conn.execute(text("DROP TABLE IF EXISTS cohort_stage"))
    conn.execute(
        text(
            """
            CREATE TEMP TABLE cohort_stage (
                name TEXT PRIMARY KEY,
                owner TEXT,
                path TEXT,
                description TEXT
            )
            """
        )
    )

    stage_rows = []
    for normalized_name, values in data.items():
        existing_record = existing_map.get(normalized_name)
        display_name = values.get("_display_name")
        if existing_record:
            display_name = existing_record.get("name") or display_name or normalized_name
        else:
            display_name = display_name or normalized_name

        owner_input = _normalize_optional_string(values.get("owner"))
        path_input = _normalize_optional_string(values.get("path"))
        description_input = _normalize_optional_string(values.get("description"))

        if existing_record is None:
            owner_stage = owner_input if owner_input is not None else ""
            path_stage = path_input if path_input is not None else ""
            description_stage = description_input
        else:
            owner_stage = owner_input
            path_stage = path_input
            description_stage = description_input

            if owner_stage is None:
                owner_stage = existing_record.get("owner") or ""
            if path_stage is None:
                path_stage = existing_record.get("path") or ""
            if skip_blank_updates and description_stage is None:
                # signal to keep existing value by leaving as None
                description_stage = None

        stage_rows.append(
            (
                display_name,
                owner_stage,
                path_stage,
                description_stage,
            )
        )

    _copy_rows(
        conn,
        table="cohort_stage",
        columns=("name", "owner", "path", "description"),
        rows=stage_rows,
    )

    if conn.dialect.name == "sqlite":
        _merge_cohorts_sqlite(conn, skip_blank_updates=skip_blank_updates)
        return

    if skip_blank_updates:
        owner_set = "CASE WHEN EXCLUDED.owner IS NULL THEN cohort.owner ELSE EXCLUDED.owner END"
        path_set = "CASE WHEN EXCLUDED.path IS NULL THEN cohort.path ELSE EXCLUDED.path END"
        description_set = (
            "CASE WHEN EXCLUDED.description IS NULL THEN cohort.description ELSE EXCLUDED.description END"
        )
    else:
        owner_set = "EXCLUDED.owner"
        path_set = "EXCLUDED.path"
        description_set = "EXCLUDED.description"

    conn.execute(
        text(
            f"""
            INSERT INTO cohort (
                name,
                owner,
                path,
                description,
                is_active
            )
            SELECT
                name,
                owner,
                path,
                description,
                1
            FROM cohort_stage
            ON CONFLICT (name) DO UPDATE SET
                owner = {owner_set},
                path = {path_set},
                description = {description_set}
            """
        )
    )


def _merge_cohorts_sqlite(conn: Connection, *, skip_blank_updates: bool) -> None:
    rows = conn.execute(
        text(
            """
            SELECT name, owner, path, description
            FROM cohort_stage
            """
        )
    ).mappings().all()

    for row in rows:
        existing = conn.execute(
            text("SELECT cohort_id, owner, path, description FROM cohort WHERE name = :name"),
            {"name": row["name"]},
        ).mappings().first()

        if existing:
            updates: dict[str, Any] = {"name": row["name"]}

            owner_value = row.get("owner")
            if owner_value is not None:
                updates["owner"] = owner_value
            elif not skip_blank_updates:
                updates["owner"] = ""

            path_value = row.get("path")
            if path_value is not None:
                updates["path"] = path_value
            elif not skip_blank_updates:
                updates["path"] = ""

            description_value = row.get("description")
            if description_value is not None or not skip_blank_updates:
                updates["description"] = description_value

            if len(updates) > 1:
                set_clause = ", ".join(f"{column} = :{column}" for column in updates.keys())
                updates["name"] = row["name"]
                conn.execute(text(f"UPDATE cohort SET {set_clause} WHERE name = :name"), updates)
        else:
            insert_params = {
                "name": row["name"],
                "owner": row.get("owner") if row.get("owner") is not None else "",
                "path": row.get("path") if row.get("path") is not None else "",
                "description": row.get("description"),
                "is_active": 1,
            }
            placeholders = ", ".join(insert_params.keys())
            values_clause = ", ".join(f":{key}" for key in insert_params)
            conn.execute(
                text(f"INSERT INTO cohort ({placeholders}) VALUES ({values_clause})"),
                insert_params,
            )


def _load_existing_cohort_map_by_names(session: Session, names: Sequence[str]) -> dict[str, dict[str, Any]]:
    if not names:
        return {}
    unique_names = sorted(set(names))
    records = (
        session.execute(
            select(
                schema.Cohort.cohort_id,
                schema.Cohort.name,
                schema.Cohort.owner,
                schema.Cohort.path,
                schema.Cohort.description,
                schema.Cohort.is_active,
            ).where(func.lower(schema.Cohort.name).in_(unique_names))
        ).mappings().all()
    )

    result: dict[str, dict[str, Any]] = {}
    for record in records:
        normalized = _normalize_cohort_name(record["name"] or "")
        result[normalized] = {
            "cohort_id": record["cohort_id"],
            "name": record["name"],
            "owner": record["owner"],
            "path": record["path"],
            "description": record["description"],
            "is_active": bool(record["is_active"]) if record["is_active"] is not None else None,
        }
    return result


def _load_existing_cohort_map(session: Session, cohort_ids: Sequence[int]) -> dict[int, dict[str, Any]]:
    records = (
        session.execute(
            select(
                schema.Cohort.cohort_id,
                schema.Cohort.name,
                schema.Cohort.owner,
                schema.Cohort.path,
                schema.Cohort.description,
                schema.Cohort.is_active,
            ).where(schema.Cohort.cohort_id.in_(cohort_ids))
        ).mappings().all()
    )

    return {
        record["cohort_id"]: {
            "cohort_id": record["cohort_id"],
            "name": record["name"],
            "owner": record["owner"],
            "path": record["path"],
            "description": record["description"],
            "is_active": bool(record["is_active"]) if record["is_active"] is not None else None,
        }
        for record in records
    }


def get_existing_cohort_by_id(cohort_id: int, *, engine: Engine) -> dict[str, Any] | None:
    with Session(engine) as session:
        data = _load_existing_cohort_map(session, [cohort_id])
        return data.get(cohort_id)


def get_existing_cohort_by_name(name: str, *, engine: Engine) -> dict[str, Any] | None:
    normalized = _normalize_cohort_name(name)
    with Session(engine) as session:
        records = (
            session.execute(
                select(
                    schema.Cohort.cohort_id,
                    schema.Cohort.name,
                    schema.Cohort.owner,
                    schema.Cohort.path,
                    schema.Cohort.description,
                    schema.Cohort.is_active,
                ).where(func.lower(schema.Cohort.name) == normalized)
            ).mappings().all()
        )
    if not records:
        return None
    record = records[0]
    return {
        "cohort_id": record["cohort_id"],
        "name": record["name"],
        "owner": record["owner"],
        "path": record["path"],
        "description": record["description"],
        "is_active": bool(record["is_active"]) if record["is_active"] is not None else None,
    }


def upsert_metadata_cohort(
    *,
    engine: Engine,
    name: str,
    owner: str,
    path: str,
    description: str | None,
    is_active: bool | None = True,
) -> dict[str, Any]:
    normalized_name = _normalize_cohort_name(name)
    normalized_owner = _normalize_required_string(owner, "Owner")
    normalized_path = _normalize_required_string(path, "Path")
    normalized_description = _normalize_optional_string(description)

    with Session(engine) as session:
        record = session.execute(
            select(schema.Cohort).where(func.lower(schema.Cohort.name) == normalized_name)
        ).scalar_one_or_none()

        if record is None:
            record = schema.Cohort(
                name=normalized_name,
                owner=normalized_owner,
                path=normalized_path,
                description=normalized_description,
                is_active=1 if (is_active is None or is_active) else 0,
            )
            session.add(record)
        else:
            record.owner = normalized_owner
            record.path = normalized_path
            record.description = normalized_description
            if is_active is not None:
                record.is_active = 1 if is_active else 0

        session.commit()
        session.refresh(record)

    return {
        "cohort_id": record.cohort_id,
        "name": record.name,
        "owner": record.owner,
        "path": record.path,
        "description": record.description,
        "is_active": bool(record.is_active) if record.is_active is not None else None,
    }
