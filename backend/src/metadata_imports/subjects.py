from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Literal, Sequence

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

from metadata_db import schema

from .shared import (
    FieldDefinition,
    FieldMapping,
    coerce_value,
    copy_rows,
    iter_csv_rows,
    normalize_birth_date as shared_normalize_birth_date,
)
from .subject_cohorts import apply_subject_cohorts


SUBJECT_FIELDS: tuple[FieldDefinition, ...] = (
    FieldDefinition(name="subject_code", label="Subject Code", required=True),
    FieldDefinition(name="patient_name", label="Patient Name"),
    FieldDefinition(name="patient_birth_date", label="Birth Date", default_parser="date", parsers=("date",)),
    FieldDefinition(name="patient_sex", label="Sex/Gender"),
    FieldDefinition(name="ethnic_group", label="Ethnic Group"),
    FieldDefinition(name="occupation", label="Occupation"),
    FieldDefinition(name="additional_patient_history", label="Additional History"),
)


COHORT_FIELDS: tuple[FieldDefinition, ...] = (
    FieldDefinition(name="name", label="Cohort Name", required=True),
    FieldDefinition(name="owner", label="Owner"),
    FieldDefinition(name="path", label="Path"),
    FieldDefinition(name="description", label="Description"),
)


SUBJECT_FIELD_MAP = {definition.name: definition for definition in SUBJECT_FIELDS}
COHORT_FIELD_MAP = {definition.name: definition for definition in COHORT_FIELDS}

SUBJECT_VALUE_COLUMNS: tuple[str, ...] = (
    "patient_name",
    "patient_birth_date",
    "patient_sex",
    "ethnic_group",
    "occupation",
    "additional_patient_history",
)

COHORT_VALUE_COLUMNS: tuple[str, ...] = (
    "owner",
    "path",
    "description",
    "is_active",
)

normalize_birth_date = shared_normalize_birth_date
_coerce_value = coerce_value
_iter_csv_rows = iter_csv_rows
_copy_rows = copy_rows


class SubjectImportOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    skip_blank_updates: bool = Field(default=True, alias="skipBlankUpdates")


class CohortImportConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    enabled: bool = Field(default=False, alias="enabled")
    assign_subjects: bool = Field(default=True, alias="assignSubjects")
    membership_mode: Literal["append", "replace"] = Field(default="append", alias="membershipMode")
    name: FieldMapping | None = None
    owner: FieldMapping | None = None
    path: FieldMapping | None = None
    description: FieldMapping | None = None
    is_active: FieldMapping | None = Field(default=None, alias="isActive")

    @model_validator(mode="after")
    def _validate_required(self) -> "CohortImportConfig":
        if not self.enabled:
            return self
        missing: list[str] = []
        for field in COHORT_FIELDS:
            if field.required and getattr(self, field.name, None) is None:
                missing.append(field.name)
        if missing:
            raise ValueError(f"Cohort import requires mappings for: {', '.join(missing)}")
        return self


class IdentifierImportConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id_type_id: int | None = Field(default=None, alias="idTypeId")
    id_type_name: str | None = Field(default=None, alias="idTypeName")
    value: FieldMapping = Field(alias="value")

    @model_validator(mode="after")
    def _validate_identifier(self) -> "IdentifierImportConfig":
        if self.id_type_id is None and not self.id_type_name:
            raise ValueError("Identifier mapping requires idTypeId or idTypeName")
        return self


class SubjectImportPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    file_token: str | None = Field(default=None, alias="fileToken")
    file_path: str | None = Field(default=None, alias="filePath")
    subject_fields: dict[str, FieldMapping] = Field(alias="subjectFields")
    cohort: CohortImportConfig | None = None
    identifiers: list[IdentifierImportConfig] = Field(default_factory=list)
    options: SubjectImportOptions = Field(default_factory=SubjectImportOptions)
    dry_run: bool = Field(default=False, alias="dryRun")

    @model_validator(mode="after")
    def _validate_subject_fields(self) -> "SubjectImportPayload":
        mappings = self.subject_fields or {}
        missing = [field.name for field in SUBJECT_FIELDS if field.required and field.name not in mappings]
        if missing:
            raise ValueError(f"Subject import requires mappings for: {', '.join(missing)}")
        unknown = sorted(set(mappings) - set(SUBJECT_FIELD_MAP))
        if unknown:
            raise ValueError(f"Unknown subject field mappings: {', '.join(unknown)}")
        return self


@dataclass
class IdentifierValue:
    id_type_id: int
    subject_code: str
    other_identifier: str


@dataclass
class ParsedSubjectRow:
    subject_code: str
    subject_values: dict[str, Any]
    cohort_values: dict[str, Any] | None
    cohort_name: str | None
    identifiers: list[IdentifierValue]


class SubjectImportPreviewRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    subject: dict[str, Any]
    cohort: dict[str, Any] | None = None
    identifiers: list[dict[str, Any]] = Field(default_factory=list)
    existing: bool = False
    existing_subject: dict[str, Any] | None = Field(default=None, alias="existingSubject")
    existing: bool = False


class SubjectImportPreview(BaseModel):
    total_rows: int = Field(alias="totalRows")
    processed_rows: int = Field(alias="processedRows")
    skipped_rows: int = Field(alias="skippedRows")
    warnings: list[str]
    rows: list[SubjectImportPreviewRow]


class SubjectImportResult(BaseModel):
    subjects_inserted: int = Field(alias="subjectsInserted")
    subjects_updated: int = Field(alias="subjectsUpdated")
    cohorts_inserted: int = Field(alias="cohortsInserted")
    cohorts_updated: int = Field(alias="cohortsUpdated")
    identifiers_inserted: int = Field(alias="identifiersInserted")
    identifiers_skipped: int = Field(alias="identifiersSkipped")


class SubjectImportFieldsResponse(BaseModel):
    subject_fields: list[dict[str, Any]] = Field(alias="subjectFields")
    cohort_fields: list[dict[str, Any]] = Field(alias="cohortFields")
    identifier_fields: list[dict[str, Any]] = Field(alias="identifierFields")
    id_types: list[dict[str, Any]] = Field(alias="idTypes")


def _serialize_subject_record(record: schema.Subject) -> dict[str, Any]:
    birth_date = record.patient_birth_date
    if birth_date is None:
        birth_date_value = None
    elif isinstance(birth_date, dt.date):
        birth_date_value = birth_date.isoformat()
    elif hasattr(birth_date, "isoformat"):
        birth_date_value = birth_date.isoformat()
    else:
        birth_date_value = str(birth_date)

    return {
        "subject_code": record.subject_code,
        "patient_name": record.patient_name,
        "patient_birth_date": birth_date_value,
        "patient_sex": record.patient_sex,
        "ethnic_group": record.ethnic_group,
        "occupation": record.occupation,
        "additional_patient_history": record.additional_patient_history,
        "is_active": bool(record.is_active) if record.is_active is not None else None,
    }


def _load_existing_subject_map(session: Session, subject_codes: Sequence[str]) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    if not subject_codes:
        return mapping
    unique_codes = sorted(set(subject_codes))
    for chunk in _chunked(unique_codes, 500):
        subjects = session.scalars(
            select(schema.Subject).where(schema.Subject.subject_code.in_(chunk))
        ).all()
        for subject in subjects:
            mapping[subject.subject_code] = _serialize_subject_record(subject)
    return mapping


def get_existing_subject(session: Session, subject_code: str) -> dict[str, Any] | None:
    subject = session.execute(
        select(schema.Subject).where(schema.Subject.subject_code == subject_code)
    ).scalar_one_or_none()
    if subject is None:
        return None
    return _serialize_subject_record(subject)


def _parse_rows(
    *,
    path: Path,
    config: SubjectImportPayload,
    id_types_by_name: dict[str, int],
    id_types_by_id: dict[int, str],
    preview_limit: int | None = None,
) -> tuple[list[ParsedSubjectRow], list[str], int, int]:
    rows: list[ParsedSubjectRow] = []
    warnings: list[str] = []
    total_rows = 0
    skipped_rows = 0
    subject_mappings = config.subject_fields
    cohort_config = config.cohort if config.cohort and config.cohort.enabled else None
    identifier_configs = config.identifiers or []

    for row_number, (row, aliases) in enumerate(_iter_csv_rows(path), start=1):
        total_rows += 1
        if preview_limit is not None and len(rows) >= preview_limit:
            continue

        subject_code_mapping = subject_mappings["subject_code"]
        subject_code_value = _coerce_value(
            field=SUBJECT_FIELD_MAP["subject_code"],
            mapping=subject_code_mapping,
            row=row,
            aliases=aliases,
            warnings=warnings,
            row_number=row_number,
        )
        if not subject_code_value:
            warnings.append(f"Row {row_number}: missing subject code, skipped")
            skipped_rows += 1
            continue

        subject_values: dict[str, Any] = {"subject_code": subject_code_value}
        for field in SUBJECT_FIELDS:
            if field.name == "subject_code":
                continue
            mapping = subject_mappings.get(field.name)
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
            subject_values[field.name] = value

        cohort_values: dict[str, Any] | None = None
        cohort_name: str | None = None
        if cohort_config:
            cohort_values = {}
            for field in COHORT_FIELDS:
                mapping = getattr(cohort_config, field.name)
                if not isinstance(mapping, FieldMapping):
                    continue
                value = _coerce_value(
                    field=field,
                    mapping=mapping,
                    row=row,
                    aliases=aliases,
                    warnings=warnings,
                    row_number=row_number,
                )
                cohort_values[field.name] = value
            cohort_name = cohort_values.get("name")
            if cohort_name is None:
                warnings.append(f"Row {row_number}: cohort mapping enabled but cohort name empty")

        identifier_values: list[IdentifierValue] = []
        for identifier in identifier_configs:
            id_type_id = identifier.id_type_id
            if id_type_id is None and identifier.id_type_name:
                id_type_id = id_types_by_name.get(identifier.id_type_name.lower())
            if id_type_id is None:
                warnings.append(
                    f"Row {row_number}: identifier type '{identifier.id_type_name or identifier.id_type_id}' unknown"
                )
                continue
            if id_type_id not in id_types_by_id:
                warnings.append(f"Row {row_number}: identifier type id {id_type_id} unknown")
                continue
            value = _coerce_value(
                field=FieldDefinition(name="other_identifier", label="Other Identifier"),
                mapping=identifier.value,
                row=row,
                aliases=aliases,
                warnings=warnings,
                row_number=row_number,
            )
            if value is None or value == "":
                continue
            identifier_values.append(
                IdentifierValue(
                    id_type_id=id_type_id,
                    subject_code=subject_code_value,
                    other_identifier=str(value),
                )
            )

        rows.append(
            ParsedSubjectRow(
                subject_code=subject_code_value,
                subject_values=subject_values,
                cohort_values=cohort_values if cohort_name else None,
                cohort_name=cohort_name,
                identifiers=identifier_values,
            )
        )

    return rows, warnings, total_rows, skipped_rows


def preview_subject_import(
    *,
    engine: Engine,
    path: Path,
    config: SubjectImportPayload,
    id_types: Sequence[schema.IdType],
    limit: int = 25,
) -> SubjectImportPreview:
    id_types_by_name = {record.id_type_name.lower(): record.id_type_id for record in id_types}
    id_types_by_id = {record.id_type_id: record.id_type_name for record in id_types}

    rows, warnings, total_rows, skipped_rows = _parse_rows(
        path=path,
        config=config,
        id_types_by_name=id_types_by_name,
        id_types_by_id=id_types_by_id,
        preview_limit=limit,
    )

    subject_codes = [parsed.subject_code for parsed in rows]
    existing_subject_map: dict[str, dict[str, Any]] = {}
    if subject_codes:
        with Session(engine) as session:
            existing_subject_map = _load_existing_subject_map(session, subject_codes)

    preview_rows: list[SubjectImportPreviewRow] = []
    for parsed in rows:
        existing_payload = existing_subject_map.get(parsed.subject_code)
        combined_subject = dict(existing_payload or {})
        combined_subject.update(parsed.subject_values)
        preview_rows.append(
            SubjectImportPreviewRow(
                subject=combined_subject,
                cohort=parsed.cohort_values,
                identifiers=[
                    {
                        "subject_code": ident.subject_code,
                        "id_type_id": ident.id_type_id,
                        "other_identifier": ident.other_identifier,
                    }
                    for ident in parsed.identifiers
                ],
                existing=existing_payload is not None,
                existing_subject=existing_payload,
            )
        )

    return SubjectImportPreview(
        totalRows=total_rows,
        processedRows=len(rows),
        skippedRows=skipped_rows,
        warnings=warnings,
        rows=preview_rows,
    )


def _chunked(items: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def apply_subject_import(
    *,
    engine: Engine,
    path: Path,
    config: SubjectImportPayload,
    id_types: Sequence[schema.IdType],
) -> SubjectImportResult:
    id_types_by_name = {record.id_type_name.lower(): record.id_type_id for record in id_types}
    id_types_by_id = {record.id_type_id: record.id_type_name for record in id_types}

    rows, _, total_rows, skipped_rows = _parse_rows(
        path=path,
        config=config,
        id_types_by_name=id_types_by_name,
        id_types_by_id=id_types_by_id,
        preview_limit=None,
    )

    subject_data: dict[str, dict[str, Any]] = {}
    cohort_data: dict[str, dict[str, Any]] = {}
    subject_cohort_map: set[tuple[str, str]] = set()
    identifier_records: set[tuple[str, int, str]] = set()

    assign_subjects = bool(config.cohort and config.cohort.enabled and config.cohort.assign_subjects)

    for parsed in rows:
        subject_data[parsed.subject_code] = dict(parsed.subject_values)
        if parsed.cohort_values and parsed.cohort_name:
            cohort_data[parsed.cohort_name] = dict(parsed.cohort_values)
            if assign_subjects:
                subject_cohort_map.add((parsed.subject_code, parsed.cohort_name))
        for ident in parsed.identifiers:
            identifier_records.add((ident.subject_code, ident.id_type_id, ident.other_identifier))

    subject_codes = sorted(subject_data.keys())
    cohort_names = sorted(cohort_data.keys())

    existing_subject_codes: set[str] = set()
    existing_cohort_names: set[str] = set()
    with Session(engine) as session:
        if subject_codes:
            existing_subject_codes = set(_load_existing_subject_map(session, subject_codes).keys())

        if cohort_names:
            for chunk in _chunked(cohort_names, 500):
                existing_cohort_names.update(
                    session.scalars(select(schema.Cohort.name).where(schema.Cohort.name.in_(chunk))).all()
                )

    subjects_inserted = max(0, len(subject_codes) - len(existing_subject_codes))
    subjects_updated = len(subject_codes) - subjects_inserted
    cohorts_inserted = max(0, len(cohort_names) - len(existing_cohort_names))
    cohorts_updated = len(cohort_names) - cohorts_inserted

    if not subject_data and not cohort_data and not identifier_records:
        return SubjectImportResult(
            subjectsInserted=0,
            subjectsUpdated=0,
            cohortsInserted=0,
            cohortsUpdated=0,
            identifiersInserted=0,
            identifiersSkipped=0,
        )

    membership_mode = config.cohort.membership_mode if (config.cohort and config.cohort.enabled) else "append"

    conn = engine.connect()
    trans = conn.begin()
    try:
        _run_import_transaction(
            conn=conn,
            subject_data=subject_data,
            cohort_data=cohort_data,
            subject_cohort_map=subject_cohort_map,
            identifier_records=identifier_records,
            skip_blank_updates=config.options.skip_blank_updates,
            assign_subjects=assign_subjects,
            membership_mode=membership_mode,
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

    identifiers_inserted = len(identifier_records)

    return SubjectImportResult(
        subjectsInserted=subjects_inserted,
        subjectsUpdated=subjects_updated,
        cohortsInserted=cohorts_inserted,
        cohortsUpdated=cohorts_updated,
        identifiersInserted=identifiers_inserted,
        identifiersSkipped=0,
    )


def _run_import_transaction(
    *,
    conn: Connection,
    subject_data: dict[str, dict[str, Any]],
    cohort_data: dict[str, dict[str, Any]],
    subject_cohort_map: set[tuple[str, str]],
    identifier_records: set[tuple[str, int, str]],
    skip_blank_updates: bool,
    assign_subjects: bool,
    membership_mode: Literal["append", "replace"],
) -> None:
    if subject_data:
        _stage_and_merge_subjects(conn, subject_data, skip_blank_updates=skip_blank_updates)
    if cohort_data:
        _stage_and_merge_cohorts(conn, cohort_data)
    if assign_subjects and subject_cohort_map:
        apply_subject_cohorts(conn, subject_cohort_map, membership_mode=membership_mode)
    if identifier_records:
        _apply_identifiers(conn, identifier_records)


def _stage_and_merge_subjects(conn: Connection, data: dict[str, dict[str, Any]], *, skip_blank_updates: bool) -> None:
    conn.execute(text("DROP TABLE IF EXISTS subject_stage"))
    conn.execute(
        text(
            """
            CREATE TEMP TABLE subject_stage (
                subject_code TEXT PRIMARY KEY,
                patient_name TEXT,
                patient_birth_date TEXT,
                patient_sex TEXT,
                ethnic_group TEXT,
                occupation TEXT,
                additional_patient_history TEXT,
                is_active INTEGER
            )
            """
        )
    )

    stage_rows = []
    for subject_code, values in data.items():
        stage_rows.append(
            (
                subject_code,
                values.get("patient_name"),
                values.get("patient_birth_date"),
                values.get("patient_sex"),
                values.get("ethnic_group"),
                values.get("occupation"),
                values.get("additional_patient_history"),
                values.get("is_active"),
            )
        )

    _copy_rows(
        conn,
        table="subject_stage",
        columns=(
            "subject_code",
            "patient_name",
            "patient_birth_date",
            "patient_sex",
            "ethnic_group",
            "occupation",
            "additional_patient_history",
            "is_active",
        ),
        rows=stage_rows,
    )

    update_parts: list[str] = []
    for column in (
        "patient_name",
        "patient_birth_date",
        "patient_sex",
        "ethnic_group",
        "occupation",
        "additional_patient_history",
        "is_active",
    ):
        if skip_blank_updates:
            update_parts.append(f"{column} = COALESCE(EXCLUDED.{column}, subject.{column})")
        else:
            update_parts.append(f"{column} = EXCLUDED.{column}")

    update_clause = ", ".join(update_parts)

    if conn.dialect.name == "sqlite":
        _merge_subjects_sqlite(conn, skip_blank_updates=skip_blank_updates)
        return

    conn.execute(
        text(
            f"""
            INSERT INTO subject (
                subject_code,
                patient_name,
                patient_birth_date,
                patient_sex,
                ethnic_group,
                occupation,
                additional_patient_history,
                is_active
            )
            SELECT
                subject_code,
                patient_name,
                CAST(patient_birth_date AS DATE),
                patient_sex,
                ethnic_group,
                occupation,
                additional_patient_history,
                COALESCE(is_active, 1)
            FROM subject_stage
            ON CONFLICT (subject_code) DO UPDATE SET
                {update_clause}
            """
        )
    )


def _stage_and_merge_cohorts(conn: Connection, data: dict[str, dict[str, Any]]) -> None:
    conn.execute(text("DROP TABLE IF EXISTS cohort_stage"))
    conn.execute(
        text(
            """
            CREATE TEMP TABLE cohort_stage (
                name TEXT PRIMARY KEY,
                owner TEXT,
                path TEXT,
                description TEXT,
                is_active INTEGER
            )
            """
        )
    )

    stage_rows = []
    for cohort_name, values in data.items():
        stage_rows.append(
            (
                cohort_name,
                values.get("owner"),
                values.get("path"),
                values.get("description"),
                values.get("is_active"),
            )
        )

    _copy_rows(
        conn,
        table="cohort_stage",
        columns=("name", "owner", "path", "description", "is_active"),
        rows=stage_rows,
    )

    if conn.dialect.name == "sqlite":
        _merge_cohorts_sqlite(conn)
        return

    conn.execute(
        text(
            """
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
                COALESCE(is_active, 1)
            FROM cohort_stage
            ON CONFLICT (name) DO UPDATE SET
                owner = EXCLUDED.owner,
                path = EXCLUDED.path,
                description = EXCLUDED.description,
                is_active = COALESCE(EXCLUDED.is_active, cohort.is_active)
            """
        )
    )


def _merge_subjects_sqlite(conn: Connection, *, skip_blank_updates: bool) -> None:
    rows = conn.execute(
        text(
            """
            SELECT subject_code, patient_name, patient_birth_date, patient_sex,
                   ethnic_group, occupation, additional_patient_history,
                   is_active
            FROM subject_stage
            """
        )
    ).mappings().all()

    for row in rows:
        params = dict(row)
        params.setdefault("is_active", 1)
        existing = conn.execute(
            text("SELECT subject_id FROM subject WHERE subject_code = :subject_code"),
            {"subject_code": row["subject_code"]},
        ).mappings().first()

        if existing:
            updates: dict[str, Any] = {}
            for column in SUBJECT_VALUE_COLUMNS:
                if column == "is_active":
                    value = row.get(column, None)
                else:
                    value = row.get(column)
                if value is None and skip_blank_updates:
                    continue
                updates[column] = value
            if updates:
                set_clause = ", ".join(f"{column} = :{column}" for column in updates)
                params = dict(updates)
                params["subject_code"] = row["subject_code"]
                conn.execute(
                    text(f"UPDATE subject SET {set_clause} WHERE subject_code = :subject_code"),
                    params,
                )
        else:
            insert_params = {
                "subject_code": row["subject_code"],
                "patient_name": row.get("patient_name"),
                "patient_birth_date": row.get("patient_birth_date"),
                "patient_sex": row.get("patient_sex"),
                "ethnic_group": row.get("ethnic_group"),
                "occupation": row.get("occupation"),
                "additional_patient_history": row.get("additional_patient_history"),
                "is_active": row.get("is_active") if row.get("is_active") is not None else 1,
            }
            conn.execute(
                text(
                    """
                    INSERT INTO subject (
                        subject_code,
                        patient_name,
                        patient_birth_date,
                        patient_sex,
                        ethnic_group,
                        occupation,
                        additional_patient_history,
                        is_active
                    ) VALUES (
                        :subject_code,
                        :patient_name,
                        :patient_birth_date,
                        :patient_sex,
                        :ethnic_group,
                        :occupation,
                        :additional_patient_history,
                        :is_active
                    )
                    """
                ),
                insert_params,
            )


def _merge_cohorts_sqlite(conn: Connection) -> None:
    rows = conn.execute(
        text(
            """
            SELECT name, owner, path, description, is_active
            FROM cohort_stage
            """
        )
    ).mappings().all()

    for row in rows:
        existing = conn.execute(
            text("SELECT cohort_id FROM cohort WHERE name = :name"),
            {"name": row["name"]},
        ).mappings().first()

        if existing:
            updates: dict[str, Any] = {}
            for column in COHORT_VALUE_COLUMNS:
                value = row.get(column)
                if value is None and column == "is_active":
                    continue
                updates[column] = value
            if updates:
                set_clause = ", ".join(f"{column} = :{column}" for column in updates)
                params = dict(updates)
                params["name"] = row["name"]
                conn.execute(text(f"UPDATE cohort SET {set_clause} WHERE name = :name"), params)
        else:
            insert_params = {
                "name": row["name"],
                "owner": row.get("owner"),
                "path": row.get("path"),
                "description": row.get("description"),
                "is_active": row.get("is_active") if row.get("is_active") is not None else 1,
            }
            conn.execute(
                text(
                    """
                    INSERT INTO cohort (
                        name,
                        owner,
                        path,
                        description,
                        is_active
                    ) VALUES (
                        :name,
                        :owner,
                        :path,
                        :description,
                        :is_active
                    )
                    """
                ),
                insert_params,
            )
def _apply_identifiers(conn: Connection, identifier_records: set[tuple[str, int, str]]) -> None:
    conn.execute(text("DROP TABLE IF EXISTS identifier_stage"))
    conn.execute(
        text(
            """
            CREATE TEMP TABLE identifier_stage (
                subject_code TEXT,
                id_type_id INTEGER,
                other_identifier TEXT
            )
            """
        )
    )

    stage_rows = list(identifier_records)

    _copy_rows(
        conn,
        table="identifier_stage",
        columns=("subject_code", "id_type_id", "other_identifier"),
        rows=stage_rows,
    )

    conn.execute(
        text(
            """
            INSERT INTO subject_other_identifiers (subject_id, id_type_id, other_identifier)
            SELECT s.subject_id, st.id_type_id, st.other_identifier
            FROM identifier_stage st
            JOIN subject s ON s.subject_code = st.subject_code
            WHERE NOT EXISTS (
                SELECT 1
                FROM subject_other_identifiers existing
                WHERE existing.subject_id = s.subject_id
                  AND existing.id_type_id = st.id_type_id
                  AND existing.other_identifier = st.other_identifier
            )
            """
        )
    )
def build_fields_response(id_types: Sequence[schema.IdType]) -> SubjectImportFieldsResponse:
    subject_fields_payload = [
        {
            "name": field.name,
            "label": field.label,
            "required": field.required,
            "parsers": list(field.parsers),
            "defaultParser": field.default_parser,
        }
        for field in SUBJECT_FIELDS
    ]

    cohort_fields_payload = [
        {
            "name": field.name,
            "label": field.label,
            "required": field.required,
            "parsers": list(field.parsers),
            "defaultParser": field.default_parser,
        }
        for field in COHORT_FIELDS
    ]

    identifier_fields_payload = [
        {
            "name": "other_identifier",
            "label": "Other Identifier",
            "parsers": ["string"],
            "defaultParser": "string",
        }
    ]

    id_types_payload = [
        {"id": record.id_type_id, "name": record.id_type_name, "description": record.description}
        for record in id_types
    ]

    return SubjectImportFieldsResponse(
        subjectFields=subject_fields_payload,
        cohortFields=cohort_fields_payload,
        identifierFields=identifier_fields_payload,
        idTypes=id_types_payload,
    )
