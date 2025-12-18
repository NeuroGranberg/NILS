from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Literal, Sequence

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import delete, func, select, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

from metadata_db import schema

from .shared import FieldDefinition, FieldMapping, coerce_value, copy_rows, iter_csv_rows


SUBJECT_FIELD = FieldDefinition(name="subject_code", label="Subject Code", required=True)


@dataclass
class ParsedSubjectCohortRow:
    row_number: int
    subject_code: str
    cohort_name: str


class SubjectCohortImportOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    membership_mode: Literal["append", "replace"] = Field(default="append", alias="membershipMode")


class SubjectCohortImportPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    file_token: str | None = Field(default=None, alias="fileToken")
    file_path: str | None = Field(default=None, alias="filePath")
    subject_field: FieldMapping = Field(alias="subjectField")
    static_cohort_name: str = Field(alias="staticCohortName")
    options: SubjectCohortImportOptions = Field(default_factory=SubjectCohortImportOptions)
    dry_run: bool = Field(default=False, alias="dryRun")

    @model_validator(mode="after")
    def _validate(self) -> "SubjectCohortImportPayload":
        static_name = (self.static_cohort_name or "").strip()
        if not static_name:
            raise ValueError("staticCohortName is required")
        self.static_cohort_name = static_name
        if not self.subject_field:
            raise ValueError("subjectField mapping is required")
        return self


class SubjectCohortImportPreviewRow(BaseModel):
    subjectCode: str
    cohortName: str
    subjectExists: bool
    cohortExists: bool
    alreadyMember: bool


class SubjectCohortImportPreview(BaseModel):
    totalRows: int
    processedRows: int
    skippedRows: int
    warnings: list[str]
    rows: list[SubjectCohortImportPreviewRow]


class SubjectCohortImportResult(BaseModel):
    membershipsInserted: int
    membershipsExisting: int
    subjectsMissing: int
    cohortsMissing: int
    rowsSkipped: int
    warnings: list[str]


class SubjectCohortMembership(BaseModel):
    subjectCode: str
    cohortId: int
    cohortName: str
    owner: str | None = None
    path: str | None = None
    description: str | None = None
    notes: str | None = None
    createdAt: str | None = None
    updatedAt: str | None = None


class SubjectCohortMetadataCohort(BaseModel):
    cohortId: int
    name: str
    owner: str | None = None
    path: str | None = None
    description: str | None = None
    isActive: bool | None = None
    createdAt: str | None = None
    updatedAt: str | None = None


def apply_subject_cohorts(
    conn: Connection,
    subject_cohort_map: Iterable[tuple[str, str]],
    *,
    membership_mode: Literal["append", "replace"],
) -> None:
    conn.execute(text("DROP TABLE IF EXISTS subject_cohort_stage"))
    conn.execute(
        text(
            """
            CREATE TEMP TABLE subject_cohort_stage (
                subject_code TEXT,
                cohort_name TEXT
            )
            """
        )
    )

    rows = list(subject_cohort_map)

    copy_rows(
        conn,
        table="subject_cohort_stage",
        columns=("subject_code", "cohort_name"),
        rows=rows,
    )

    if membership_mode == "replace":
        conn.execute(
            text(
                """
                DELETE FROM subject_cohorts
                WHERE subject_id IN (
                    SELECT s.subject_id
                    FROM subject_cohort_stage scs
                    JOIN subject s ON s.subject_code = scs.subject_code
                )
                """
            )
        )

    conn.execute(
        text(
            """
            INSERT INTO subject_cohorts (subject_id, cohort_id)
            SELECT s.subject_id, c.cohort_id
            FROM subject_cohort_stage scs
            JOIN subject s ON s.subject_code = scs.subject_code
            JOIN cohort c ON c.name = scs.cohort_name
            WHERE NOT EXISTS (
                SELECT 1
                FROM subject_cohorts existing
                WHERE existing.subject_id = s.subject_id AND existing.cohort_id = c.cohort_id
            )
            """
        )
    )


def _sanitize_value(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _parse_rows(
    *,
    path: Path,
    config: SubjectCohortImportPayload,
    preview_limit: int | None = None,
) -> tuple[list[ParsedSubjectCohortRow], list[str], int, int]:
    rows: list[ParsedSubjectCohortRow] = []
    warnings: list[str] = []
    total_rows = 0
    skipped_rows = 0
    static_cohort = config.static_cohort_name

    for row_number, (row, aliases) in enumerate(iter_csv_rows(path), start=1):
        total_rows += 1
        if preview_limit is not None and len(rows) >= preview_limit:
            continue

        subject_value = coerce_value(
            field=SUBJECT_FIELD,
            mapping=config.subject_field,
            row=row,
            aliases=aliases,
            warnings=warnings,
            row_number=row_number,
        )
        subject_code = _sanitize_value(subject_value)
        if not subject_code:
            warnings.append(f"Row {row_number}: missing subject code, skipped")
            skipped_rows += 1
            continue

        cohort_name = static_cohort

        rows.append(
            ParsedSubjectCohortRow(
                row_number=row_number,
                subject_code=subject_code,
                cohort_name=cohort_name,
            )
        )

    return rows, warnings, total_rows, skipped_rows


def _load_subject_map(session: Session, subject_codes: Sequence[str]) -> dict[str, int]:
    if not subject_codes:
        return {}
    unique_codes = sorted(set(subject_codes))
    records = (
        session.execute(
            select(schema.Subject.subject_code, schema.Subject.subject_id).where(
                schema.Subject.subject_code.in_(unique_codes)
            )
        )
        .mappings()
        .all()
    )
    return {record["subject_code"]: record["subject_id"] for record in records}


def _load_cohort_map(session: Session, cohort_names: Sequence[str]) -> dict[str, int]:
    if not cohort_names:
        return {}
    unique_names = sorted(set(cohort_names))
    records = (
        session.execute(
            select(schema.Cohort.name, schema.Cohort.cohort_id).where(schema.Cohort.name.in_(unique_names))
        )
        .mappings()
        .all()
    )
    return {record["name"]: record["cohort_id"] for record in records}


def _load_existing_memberships(
    session: Session,
    subject_ids: Sequence[int],
    cohort_ids: Sequence[int],
) -> set[tuple[str, str]]:
    if not subject_ids or not cohort_ids:
        return set()

    records = (
        session.execute(
            select(schema.Subject.subject_code, schema.Cohort.name)
            .select_from(schema.SubjectCohort)
            .join(schema.Subject, schema.Subject.subject_id == schema.SubjectCohort.subject_id)
            .join(schema.Cohort, schema.Cohort.cohort_id == schema.SubjectCohort.cohort_id)
            .where(schema.Subject.subject_id.in_(subject_ids))
            .where(schema.Cohort.cohort_id.in_(cohort_ids))
        )
        .mappings()
        .all()
    )
    return {(record["subject_code"], record["name"]) for record in records}


def preview_subject_cohort_import(
    *,
    engine: Engine,
    path: Path,
    config: SubjectCohortImportPayload,
    limit: int = 100,
) -> SubjectCohortImportPreview:
    rows, warnings, total_rows, skipped_rows = _parse_rows(path=path, config=config, preview_limit=limit)

    if not rows:
        return SubjectCohortImportPreview(
            totalRows=total_rows,
            processedRows=len(rows),
            skippedRows=skipped_rows,
            warnings=warnings,
            rows=[],
        )

    subject_codes = [row.subject_code for row in rows]
    cohort_names = [row.cohort_name for row in rows]

    with Session(engine) as session:
        subject_map = _load_subject_map(session, subject_codes)
        cohort_map = _load_cohort_map(session, cohort_names)
        existing_pairs = _load_existing_memberships(
            session,
            list(subject_map.values()),
            list(cohort_map.values()),
        )

    preview_rows: list[SubjectCohortImportPreviewRow] = []
    for row in rows:
        subject_exists = row.subject_code in subject_map
        cohort_exists = row.cohort_name in cohort_map
        already_member = (row.subject_code, row.cohort_name) in existing_pairs if subject_exists and cohort_exists else False
        preview_rows.append(
            SubjectCohortImportPreviewRow(
                subjectCode=row.subject_code,
                cohortName=row.cohort_name,
                subjectExists=subject_exists,
                cohortExists=cohort_exists,
                alreadyMember=already_member,
            )
        )

    return SubjectCohortImportPreview(
        totalRows=total_rows,
        processedRows=len(rows),
        skippedRows=skipped_rows,
        warnings=warnings,
        rows=preview_rows,
    )


def apply_subject_cohort_import(
    *,
    engine: Engine,
    path: Path,
    config: SubjectCohortImportPayload,
) -> SubjectCohortImportResult:
    rows, warnings, total_rows, skipped_rows = _parse_rows(path=path, config=config, preview_limit=None)

    if not rows:
        return SubjectCohortImportResult(
            membershipsInserted=0,
            membershipsExisting=0,
            subjectsMissing=0,
            cohortsMissing=0,
            rowsSkipped=skipped_rows,
            warnings=warnings,
        )

    subject_codes = [row.subject_code for row in rows]
    cohort_names = [row.cohort_name for row in rows]

    with Session(engine) as session:
        subject_map = _load_subject_map(session, subject_codes)
        cohort_map = _load_cohort_map(session, cohort_names)
        existing_pairs = _load_existing_memberships(
            session,
            list(subject_map.values()),
            list(cohort_map.values()),
        )

    missing_subjects = set(code for code in subject_codes if code not in subject_map)
    missing_cohorts = set(name for name in cohort_names if name not in cohort_map)

    valid_pairs: list[tuple[str, str]] = []
    for row in rows:
        if row.subject_code not in subject_map:
            continue
        if row.cohort_name not in cohort_map:
            continue
        valid_pairs.append((row.subject_code, row.cohort_name))

    if not valid_pairs:
        warnings.append("No valid subject/cohort pairs to apply")
        return SubjectCohortImportResult(
            membershipsInserted=0,
            membershipsExisting=0,
            subjectsMissing=len(missing_subjects),
            cohortsMissing=len(missing_cohorts),
            rowsSkipped=skipped_rows,
            warnings=warnings,
        )

    unique_pairs = sorted(set(valid_pairs))
    if config.options.membership_mode == "replace":
        membership_payload = unique_pairs
        inserted_count = len(unique_pairs)
        existing_count = len([pair for pair in unique_pairs if pair in existing_pairs])
    else:
        new_pairs = [pair for pair in unique_pairs if pair not in existing_pairs]
        membership_payload = new_pairs
        inserted_count = len(new_pairs)
        existing_count = len(unique_pairs) - len(new_pairs)

    if config.options.membership_mode == "append" and not membership_payload:
        warnings.append("No new subject/cohort memberships to insert")

    if membership_payload:
        conn = engine.connect()
        trans = conn.begin()
        try:
            apply_subject_cohorts(
                conn,
                membership_payload,
                membership_mode=config.options.membership_mode,
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
    if config.options.membership_mode == "replace" and not membership_payload:
        warnings.append("No subject/cohort pairs provided for replacement")

    return SubjectCohortImportResult(
        membershipsInserted=inserted_count,
        membershipsExisting=existing_count,
        subjectsMissing=len(missing_subjects),
        cohortsMissing=len(missing_cohorts),
        rowsSkipped=skipped_rows,
        warnings=warnings,
    )


def get_subject_cohort_memberships(subject_code: str, *, engine: Engine) -> tuple[list[SubjectCohortMembership], bool]:
    with Session(engine) as session:
        subject = (
            session.execute(select(schema.Subject.subject_id).where(schema.Subject.subject_code == subject_code))
            .scalars()
            .first()
        )
        if subject is None:
            return [], False

        records = (
            session.execute(
                select(
                    schema.Cohort.cohort_id,
                    schema.Cohort.name,
                    schema.Cohort.owner,
                    schema.Cohort.path,
                    schema.Cohort.description,
                    schema.SubjectCohort.notes,
                    schema.SubjectCohort.created_at,
                    schema.SubjectCohort.updated_at,
                )
                .join(schema.SubjectCohort, schema.Cohort.cohort_id == schema.SubjectCohort.cohort_id)
                .where(schema.SubjectCohort.subject_id == subject)
                .order_by(func.lower(schema.Cohort.name))
            )
            .mappings()
            .all()
        )

    memberships: list[SubjectCohortMembership] = []
    for record in records:
        memberships.append(
            SubjectCohortMembership(
                subjectCode=subject_code,
                cohortId=record["cohort_id"],
                cohortName=record["name"],
                owner=record.get("owner"),
                path=record.get("path"),
                description=record.get("description"),
                notes=record.get("notes"),
                createdAt=record.get("created_at"),
                updatedAt=record.get("updated_at"),
            )
        )
    return memberships, True


def delete_subject_cohort_membership(
    *,
    engine: Engine,
    subject_code: str,
    cohort_id: int | None = None,
    cohort_name: str | None = None,
) -> Literal["deleted", "subject_not_found", "cohort_not_found", "membership_not_found"]:
    if cohort_id is None and not cohort_name:
        raise ValueError("Provide cohortId or cohortName for deletion")

    with Session(engine) as session:
        subject = (
            session.execute(select(schema.Subject.subject_id).where(schema.Subject.subject_code == subject_code))
            .scalars()
            .first()
        )
        if subject is None:
            return "subject_not_found"

        target_cohort_id = cohort_id
        if target_cohort_id is None:
            target_cohort_id = (
                session.execute(select(schema.Cohort.cohort_id).where(schema.Cohort.name == cohort_name))
                .scalars()
                .first()
            )
        if target_cohort_id is None:
            return "cohort_not_found"

        result = session.execute(
            delete(schema.SubjectCohort).where(
                schema.SubjectCohort.subject_id == subject,
                schema.SubjectCohort.cohort_id == target_cohort_id,
            )
        )
        if result.rowcount and result.rowcount > 0:
            session.commit()
            return "deleted"
        session.rollback()
        return "membership_not_found"


class SubjectCohortImportFieldsResponse(BaseModel):
    subjectField: dict[str, Any]


def build_subject_cohort_fields_response() -> SubjectCohortImportFieldsResponse:
    subject_payload = {
        "name": SUBJECT_FIELD.name,
        "label": SUBJECT_FIELD.label,
        "required": SUBJECT_FIELD.required,
        "parsers": list(SUBJECT_FIELD.parsers),
        "defaultParser": SUBJECT_FIELD.default_parser,
    }
    return SubjectCohortImportFieldsResponse(subjectField=subject_payload)


def list_subject_cohort_metadata_cohorts(*, engine: Engine) -> list[SubjectCohortMetadataCohort]:
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
                    schema.Cohort.created_at,
                    schema.Cohort.updated_at,
                ).order_by(func.lower(schema.Cohort.name))
            )
            .mappings()
            .all()
        )

    def _to_iso(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        try:
            return value.isoformat()
        except AttributeError:  # pragma: no cover - defensive
            return str(value)

    cohorts: list[SubjectCohortMetadataCohort] = []
    for record in records:
        created_at = record.get("created_at")
        updated_at = record.get("updated_at")
        cohorts.append(
            SubjectCohortMetadataCohort(
                cohortId=record["cohort_id"],
                name=record["name"],
                owner=record.get("owner"),
                path=record.get("path"),
                description=record.get("description"),
                isActive=(bool(record.get("is_active")) if record.get("is_active") is not None else None),
                createdAt=_to_iso(created_at),
                updatedAt=_to_iso(updated_at),
            )
        )

    return cohorts
