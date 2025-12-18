from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import delete, func, select, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

from metadata_db import schema

from .shared import FieldDefinition, FieldMapping, coerce_value, copy_rows, iter_csv_rows


SUBJECT_FIELD = FieldDefinition(name="subject_code", label="Subject Code", required=True)
IDENTIFIER_VALUE_FIELD = FieldDefinition(name="other_identifier", label="Identifier Value", required=True)


class SubjectIdentifierImportOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    mode: Literal["append", "replace"] = Field(default="append", alias="mode")


class SubjectIdentifierImportPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    file_token: str | None = Field(default=None, alias="fileToken")
    file_path: str | None = Field(default=None, alias="filePath")
    subject_field: FieldMapping = Field(alias="subjectField")
    identifier_field: FieldMapping = Field(alias="identifierField")
    static_id_type_id: int = Field(alias="staticIdTypeId")
    options: SubjectIdentifierImportOptions = Field(default_factory=SubjectIdentifierImportOptions)
    dry_run: bool = Field(default=False, alias="dryRun")

    @model_validator(mode="after")
    def _validate(self) -> "SubjectIdentifierImportPayload":
        if not self.subject_field:
            raise ValueError("subjectField mapping is required")
        if not self.identifier_field:
            raise ValueError("identifierField mapping is required")
        if self.static_id_type_id is None:
            raise ValueError("staticIdTypeId is required")
        if self.static_id_type_id <= 0:
            raise ValueError("staticIdTypeId must be positive")
        return self


class SubjectIdentifierImportPreviewRow(BaseModel):
    subjectCode: str
    idTypeId: int | None
    idTypeName: str | None
    identifierValue: str | None
    subjectExists: bool
    idTypeExists: bool
    existingValue: bool


class SubjectIdentifierImportPreview(BaseModel):
    totalRows: int
    processedRows: int
    skippedRows: int
    identifiersInserted: int
    identifiersSkipped: int
    warnings: list[str]
    rows: list[SubjectIdentifierImportPreviewRow]


class SubjectIdentifierImportResult(BaseModel):
    identifiersInserted: int
    identifiersUpdated: int
    identifiersSkipped: int
    subjectsMissing: int
    idTypesMissing: int
    rowsSkipped: int
    warnings: list[str]


class SubjectIdentifierDetail(BaseModel):
    idTypeId: int
    idTypeName: str
    description: str | None = None
    identifierValue: str | None = None
    updatedAt: str | None = None


class SubjectIdentifierDetailResponse(BaseModel):
    subjectCode: str
    subjectExists: bool
    identifiers: list[SubjectIdentifierDetail]


class UpsertSubjectIdentifierPayload(BaseModel):
    subjectCode: str
    idTypeId: int
    identifierValue: str


class DeleteSubjectIdentifierPayload(BaseModel):
    subjectCode: str
    idTypeId: int


@dataclass
class ParsedIdentifierRow:
    row_number: int
    subject_code: str
    identifier_value: str | None


def _sanitize(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _lookup_id_types(engine: Engine) -> tuple[dict[int, schema.IdType], dict[str, schema.IdType]]:
    with Session(engine) as session:
        rows = (
            session.execute(select(schema.IdType).order_by(schema.IdType.id_type_name))
            .scalars()
            .all()
        )
    by_id = {row.id_type_id: row for row in rows}
    by_name = {row.id_type_name.lower(): row for row in rows}
    return by_id, by_name


def _parse_rows(
    *,
    path: Path,
    config: SubjectIdentifierImportPayload,
    engine: Engine,
    preview_limit: int | None = None,
) -> tuple[list[ParsedIdentifierRow], list[str], int, int]:
    rows: list[ParsedIdentifierRow] = []
    warnings: list[str] = []
    total_rows = 0
    skipped_rows = 0
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
        subject_code = _sanitize(subject_value)
        if not subject_code:
            warnings.append(f"Row {row_number}: missing subject code, skipped")
            skipped_rows += 1
            continue

        identifier_value = coerce_value(
            field=IDENTIFIER_VALUE_FIELD,
            mapping=config.identifier_field,
            row=row,
            aliases=aliases,
            warnings=warnings,
            row_number=row_number,
        )
        identifier_value = _sanitize(identifier_value)
        if not identifier_value:
            warnings.append(f"Row {row_number}: missing identifier value, skipped")
            skipped_rows += 1
            continue

        rows.append(
            ParsedIdentifierRow(
                row_number=row_number,
                subject_code=subject_code,
                identifier_value=identifier_value,
            )
        )

    return rows, warnings, total_rows, skipped_rows



def _subject_exists(session: Session, subject_code: str) -> bool:
    return (
        session.execute(
            select(func.count()).select_from(schema.Subject).where(schema.Subject.subject_code == subject_code)
        ).scalar_one()
        > 0
    )


def preview_subject_identifier_import(
    *,
    engine: Engine,
    path: Path,
    config: SubjectIdentifierImportPayload,
    preview_limit: int = 100,
) -> SubjectIdentifierImportPreview:
    rows, warnings, total_rows, skipped_rows = _parse_rows(
        path=path,
        config=config,
        engine=engine,
        preview_limit=preview_limit,
    )
    identifiers_inserted = 0
    identifiers_skipped = 0
    processed_rows = 0

    by_id, _ = _lookup_id_types(engine)
    selected_id_type = by_id.get(config.static_id_type_id)
    if selected_id_type is None:
        raise ValueError("Identifier type not found")

    with Session(engine) as session:
        preview_rows: list[SubjectIdentifierImportPreviewRow] = []
        for entry in rows:
            subject_exists = _subject_exists(session, entry.subject_code)

            existing_value = False
            if subject_exists:
                existing_value = (
                    session.execute(
                        select(func.count())
                        .select_from(schema.SubjectOtherIdentifier)
                        .join(
                            schema.Subject,
                            schema.Subject.subject_id == schema.SubjectOtherIdentifier.subject_id,
                        )
                        .where(schema.Subject.subject_code == entry.subject_code)
                        .where(schema.SubjectOtherIdentifier.id_type_id == selected_id_type.id_type_id)
                        .where(schema.SubjectOtherIdentifier.other_identifier == entry.identifier_value)
                    ).scalar_one()
                ) > 0

            if subject_exists and entry.identifier_value:
                identifiers_inserted += 1
            else:
                identifiers_skipped += 1

            processed_rows += 1
            preview_rows.append(
                SubjectIdentifierImportPreviewRow(
                    subjectCode=entry.subject_code,
                    idTypeId=selected_id_type.id_type_id,
                    idTypeName=selected_id_type.id_type_name,
                    identifierValue=entry.identifier_value,
                    subjectExists=subject_exists,
                    idTypeExists=True,
                    existingValue=existing_value,
                )
            )

    return SubjectIdentifierImportPreview(
        totalRows=total_rows,
        processedRows=processed_rows,
        skippedRows=skipped_rows,
        identifiersInserted=identifiers_inserted,
        identifiersSkipped=identifiers_skipped,
        warnings=warnings,
        rows=preview_rows,
    )


def apply_subject_identifier_import(
    *,
    engine: Engine,
    path: Path,
    config: SubjectIdentifierImportPayload,
) -> SubjectIdentifierImportResult:
    rows, warnings, total_rows, skipped_rows = _parse_rows(path=path, config=config, engine=engine)
    by_id, _ = _lookup_id_types(engine)
    selected_id_type = by_id.get(config.static_id_type_id)
    if selected_id_type is None:
        raise ValueError("Identifier type not found")

    identifier_records: set[tuple[str, int, str]] = set()
    subjects_missing = 0
    id_types_missing = 0

    with Session(engine) as session:
        for entry in rows:
            subject_exists = _subject_exists(session, entry.subject_code)

            if not subject_exists:
                subjects_missing += 1

            if not subject_exists or not entry.identifier_value:
                continue

            identifier_records.add((entry.subject_code, selected_id_type.id_type_id, entry.identifier_value))

    inserted, updated = _apply_identifiers(engine, identifier_records, config)

    return SubjectIdentifierImportResult(
        identifiersInserted=inserted,
        identifiersUpdated=updated,
        identifiersSkipped=total_rows - inserted - updated,
        subjectsMissing=subjects_missing,
        idTypesMissing=id_types_missing,
        rowsSkipped=skipped_rows,
        warnings=warnings,
    )


def _apply_identifiers(
    engine: Engine,
    identifier_records: set[tuple[str, int, str]],
    config: SubjectIdentifierImportPayload,
) -> tuple[int, int]:
    if not identifier_records:
        return 0, 0

    conn = engine.connect()
    trans = conn.begin()
    update_count = 0
    try:
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

        copy_rows(
            conn,
            table="identifier_stage",
            columns=("subject_code", "id_type_id", "other_identifier"),
            rows=list(identifier_records),
        )

        if config.options.mode == "replace" and config.static_id_type_id:
            conn.execute(
                text(
                    """
                    DELETE FROM subject_other_identifiers
                    WHERE id_type_id = :id_type_id
                    """
                ),
                {"id_type_id": config.static_id_type_id},
            )

        update_count = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM subject_other_identifiers soi
                JOIN subject s ON s.subject_id = soi.subject_id
                JOIN identifier_stage st
                  ON st.subject_code = s.subject_code
                 AND st.id_type_id = soi.id_type_id
                WHERE soi.other_identifier <> st.other_identifier
                """
            )
        ).scalar_one()

        if update_count:
            conn.execute(
                text(
                    """
                    DELETE FROM subject_other_identifiers
                    WHERE subject_other_identifier_id IN (
                        SELECT soi.subject_other_identifier_id
                        FROM subject_other_identifiers soi
                        JOIN subject s ON s.subject_id = soi.subject_id
                        JOIN identifier_stage st
                          ON st.subject_code = s.subject_code
                         AND st.id_type_id = soi.id_type_id
                        WHERE soi.other_identifier <> st.other_identifier
                    )
                    """
                )
            )

        inserted = conn.execute(
            text(
                """
                INSERT INTO subject_other_identifiers (subject_id, id_type_id, other_identifier)
                SELECT s.subject_id, st.id_type_id, st.other_identifier
                FROM identifier_stage st
                JOIN subject s ON s.subject_code = st.subject_code
                LEFT JOIN subject_other_identifiers existing
                    ON existing.subject_id = s.subject_id
                    AND existing.id_type_id = st.id_type_id
                WHERE existing.subject_other_identifier_id IS NULL
                """
            )
        ).rowcount

        if config.dry_run:
            trans.rollback()
        else:
            trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()

    return inserted or 0, int(update_count or 0)


class SubjectIdentifierImportFieldsResponse(BaseModel):
    subjectField: dict[str, Any]
    identifierField: dict[str, Any]
    idTypes: list[dict[str, Any]]


def build_subject_identifier_fields_response(*, engine: Engine) -> SubjectIdentifierImportFieldsResponse:
    by_id, _ = _lookup_id_types(engine)
    subject_payload = {
        "name": SUBJECT_FIELD.name,
        "label": SUBJECT_FIELD.label,
        "required": SUBJECT_FIELD.required,
    }
    identifier_payload = {
        "name": IDENTIFIER_VALUE_FIELD.name,
        "label": IDENTIFIER_VALUE_FIELD.label,
        "required": IDENTIFIER_VALUE_FIELD.required,
    }
    id_types_payload = [
        {"id": record.id_type_id, "name": record.id_type_name, "description": record.description}
        for record in by_id.values()
    ]
    return SubjectIdentifierImportFieldsResponse(
        subjectField=subject_payload,
        identifierField=identifier_payload,
        idTypes=id_types_payload,
    )


def get_subject_identifier_detail(subject_code: str, *, engine: Engine) -> SubjectIdentifierDetailResponse:
    by_id, _ = _lookup_id_types(engine)
    identifiers_map = {key: None for key in by_id.keys()}

    with Session(engine) as session:
        subject = session.execute(
            select(schema.Subject).where(schema.Subject.subject_code == subject_code)
        ).scalar_one_or_none()
        if subject is None:
            return SubjectIdentifierDetailResponse(
                subjectCode=subject_code,
                subjectExists=False,
                identifiers=[
                    SubjectIdentifierDetail(
                        idTypeId=id_type_id,
                        idTypeName=record.id_type_name,
                        description=record.description,
                    )
                    for id_type_id, record in by_id.items()
                ],
            )

        rows = session.execute(
            select(schema.SubjectOtherIdentifier)
            .where(schema.SubjectOtherIdentifier.subject_id == subject.subject_id)
        ).scalars()

        for row in rows:
            identifiers_map[row.id_type_id] = row

    details = []
    for id_type_id, record in by_id.items():
        existing = identifiers_map.get(id_type_id)
        details.append(
            SubjectIdentifierDetail(
                idTypeId=id_type_id,
                idTypeName=record.id_type_name,
                description=record.description,
                identifierValue=getattr(existing, "other_identifier", None),
                updatedAt=getattr(existing, "updated_at", None),
            )
        )

    return SubjectIdentifierDetailResponse(
        subjectCode=subject_code,
        subjectExists=True,
        identifiers=details,
    )


def upsert_subject_identifier(
    payload: UpsertSubjectIdentifierPayload,
    *,
    engine: Engine,
) -> SubjectIdentifierDetail:
    subject_code = payload.subjectCode.strip()
    if not subject_code:
        raise ValueError("subjectCode is required")
    identifier_value = payload.identifierValue.strip()
    if not identifier_value:
        raise ValueError("identifierValue is required")

    with Session(engine) as session:
        subject = session.execute(
            select(schema.Subject).where(schema.Subject.subject_code == subject_code)
        ).scalar_one_or_none()
        if subject is None:
            raise ValueError("Subject not found")

        id_type = session.get(schema.IdType, payload.idTypeId)
        if id_type is None:
            raise ValueError("Identifier type not found")

        id_type_id = id_type.id_type_id
        id_type_name = id_type.id_type_name
        id_type_description = id_type.description

        existing = session.execute(
            select(schema.SubjectOtherIdentifier)
            .where(schema.SubjectOtherIdentifier.subject_id == subject.subject_id)
            .where(schema.SubjectOtherIdentifier.id_type_id == payload.idTypeId)
        ).scalar_one_or_none()

        if existing is None:
            existing = schema.SubjectOtherIdentifier(
                subject_id=subject.subject_id,
                id_type_id=payload.idTypeId,
                other_identifier=identifier_value,
            )
            session.add(existing)
        else:
            existing.other_identifier = identifier_value

        session.commit()

    return SubjectIdentifierDetail(
        idTypeId=id_type_id,
        idTypeName=id_type_name,
        description=id_type_description,
        identifierValue=identifier_value,
    )


def delete_subject_identifier(payload: DeleteSubjectIdentifierPayload, *, engine: Engine) -> None:
    subject_code = payload.subjectCode.strip()
    if not subject_code:
        raise ValueError("subjectCode is required")

    with Session(engine) as session:
        subject = session.execute(
            select(schema.Subject).where(schema.Subject.subject_code == subject_code)
        ).scalar_one_or_none()
        if subject is None:
            raise ValueError("Subject not found")

        deleted = session.execute(
            delete(schema.SubjectOtherIdentifier)
            .where(schema.SubjectOtherIdentifier.subject_id == subject.subject_id)
            .where(schema.SubjectOtherIdentifier.id_type_id == payload.idTypeId)
        ).rowcount

        if deleted == 0:
            raise ValueError("Identifier not found for subject")

        session.commit()
