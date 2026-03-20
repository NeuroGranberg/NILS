"""Import logic for subject_diseases table (CSV upsert + cohort bulk assign)."""

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

# Observation type IDs for auto-linking
DIAGNOSIS_OBS_TYPE_ID = 8
ONSET_OBS_TYPE_ID = 9

SUBJECT_DISEASE_FIELDS: tuple[FieldDefinition, ...] = (
    FieldDefinition(name="subject_identifier", label="Subject Identifier", required=True),
    FieldDefinition(name="diagnosis_notes", label="Diagnosis Notes"),
    FieldDefinition(name="family_history", label="Family History"),
    FieldDefinition(name="is_active", label="Active", default_parser="bool", parsers=("bool",)),
)

FIELD_MAP = {f.name: f for f in SUBJECT_DISEASE_FIELDS}


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class DiseaseSummary(BaseModel):
    id: int
    name: str
    code: str | None = None


class CohortSummary(BaseModel):
    id: int
    name: str
    subject_count: int = Field(alias="subjectCount")


class IdTypeSummary(BaseModel):
    id: int
    name: str


class SubjectDiseaseFieldDefinition(BaseModel):
    name: str
    label: str
    required: bool = False
    parsers: list[str]
    default_parser: str = Field(alias="defaultParser")


class SubjectDiseaseFieldsResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    fields: list[SubjectDiseaseFieldDefinition]
    diseases: list[DiseaseSummary]
    cohorts: list[CohortSummary]
    id_types: list[IdTypeSummary] = Field(alias="idTypes")


class SubjectDiseaseImportOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    skip_blank_updates: bool = Field(default=True, alias="skipBlankUpdates")


class SubjectDiseaseImportPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    file_token: str | None = Field(default=None, alias="fileToken")
    file_path: str | None = Field(default=None, alias="filePath")
    disease_id: int = Field(alias="diseaseId")
    subject_identifier_type: str = Field(alias="subjectIdentifierType")
    fields: dict[str, FieldMapping] = Field(alias="fields")
    options: SubjectDiseaseImportOptions = Field(default_factory=SubjectDiseaseImportOptions)
    dry_run: bool = Field(default=False, alias="dryRun")

    @model_validator(mode="after")
    def _validate_fields(self) -> "SubjectDiseaseImportPayload":
        mappings = self.fields or {}
        all_known = set(FIELD_MAP)
        unknown = sorted(set(mappings) - all_known)
        if unknown:
            raise ValueError(f"Unknown field mappings: {', '.join(unknown)}")
        for f in SUBJECT_DISEASE_FIELDS:
            if f.required and f.name not in mappings:
                raise ValueError(f"Subject disease import requires mapping for: {f.name}")
        return self


class SubjectDiseasePreviewRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    subject_identifier: str = Field(alias="subjectIdentifier")
    resolved_subject_code: str | None = Field(default=None, alias="resolvedSubjectCode")
    subject_found: bool = Field(alias="subjectFound")
    diagnosis_notes: str | None = Field(default=None, alias="diagnosisNotes")
    family_history: str | None = Field(default=None, alias="familyHistory")
    has_diagnosis_event: bool = Field(default=False, alias="hasDiagnosisEvent")
    has_onset_event: bool = Field(default=False, alias="hasOnsetEvent")
    existing: bool = False


class SubjectDiseaseImportPreview(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    total_rows: int = Field(alias="totalRows")
    processed_rows: int = Field(alias="processedRows")
    skipped_rows: int = Field(alias="skippedRows")
    warnings: list[str]
    rows: list[SubjectDiseasePreviewRow]


class SubjectDiseaseImportResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    inserted: int
    updated: int
    subjects_missing: int = Field(alias="subjectsMissing")
    diagnosis_events_linked: int = Field(alias="diagnosisEventsLinked")
    onset_events_linked: int = Field(alias="onsetEventsLinked")


class CohortAssignPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    disease_id: int = Field(alias="diseaseId")
    cohort_id: int = Field(alias="cohortId")
    dry_run: bool = Field(default=False, alias="dryRun")


class CohortAssignResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    inserted: int
    skipped: int
    diagnosis_events_linked: int = Field(alias="diagnosisEventsLinked")
    onset_events_linked: int = Field(alias="onsetEventsLinked")
    total_subjects: int = Field(alias="totalSubjects")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_subject_map_by_code(session: Session) -> dict[str, int]:
    rows = session.execute(select(schema.Subject.subject_code, schema.Subject.subject_id)).all()
    return {code.lower(): sid for code, sid in rows}


def _build_subject_map_by_identifier(session: Session, id_type_id: int) -> dict[str, int]:
    rows = session.execute(
        select(schema.SubjectOtherIdentifier.other_identifier, schema.SubjectOtherIdentifier.subject_id)
        .where(schema.SubjectOtherIdentifier.id_type_id == id_type_id)
    ).all()
    return {ident.lower(): sid for ident, sid in rows}


def _build_subject_code_map(session: Session) -> dict[int, str]:
    rows = session.execute(select(schema.Subject.subject_id, schema.Subject.subject_code)).all()
    return {sid: code for sid, code in rows}


def _build_event_map(session: Session, observation_type_id: int) -> dict[int, int]:
    """Build {subject_id: event_id} for the given observation type.
    If multiple events exist per subject, pick the earliest."""
    rows = session.execute(
        text(
            "SELECT subject_id, event_id FROM event "
            "WHERE observation_type_id = :oid "
            "ORDER BY subject_id, event_date"
        ),
        {"oid": observation_type_id},
    ).all()
    result: dict[int, int] = {}
    for sid, eid in rows:
        if sid not in result:
            result[sid] = eid
    return result


def _load_existing_subject_diseases(
    session: Session, disease_id: int,
) -> dict[int, dict[str, Any]]:
    rows = session.execute(
        select(schema.SubjectDisease).where(schema.SubjectDisease.disease_id == disease_id)
    ).scalars().all()
    return {
        r.subject_id: {
            "subject_disease_id": r.subject_disease_id,
            "diagnosis_notes": r.diagnosis_notes,
            "family_history": r.family_history,
            "onset_event_id": r.onset_event_id,
            "diagnosis_event_id": r.diagnosis_event_id,
            "is_active": r.is_active,
        }
        for r in rows
    }


def _upsert_subject_disease(
    session: Session,
    *,
    subject_id: int,
    disease_id: int,
    existing: dict[str, Any] | None,
    diagnosis_event_id: int | None,
    onset_event_id: int | None,
    diagnosis_notes: str | None = None,
    family_history: str | None = None,
    is_active: Any = None,
    skip_blanks: bool = True,
) -> tuple[bool, bool, bool, bool]:
    """Returns (inserted, updated, diag_linked, onset_linked)."""
    diag_linked = False
    onset_linked = False

    if existing is not None:
        sd_id = existing["subject_disease_id"]
        updates: list[str] = []
        params: dict[str, Any] = {"_id": sd_id}

        if diagnosis_event_id and existing.get("diagnosis_event_id") != diagnosis_event_id:
            updates.append("diagnosis_event_id = :diagnosis_event_id")
            params["diagnosis_event_id"] = diagnosis_event_id
            diag_linked = True
        if onset_event_id and existing.get("onset_event_id") != onset_event_id:
            updates.append("onset_event_id = :onset_event_id")
            params["onset_event_id"] = onset_event_id
            onset_linked = True
        if diagnosis_notes is not None and not (skip_blanks and diagnosis_notes.strip() == ""):
            if diagnosis_notes != existing.get("diagnosis_notes"):
                updates.append("diagnosis_notes = :diagnosis_notes")
                params["diagnosis_notes"] = diagnosis_notes
        if family_history is not None and not (skip_blanks and family_history.strip() == ""):
            if family_history != existing.get("family_history"):
                updates.append("family_history = :family_history")
                params["family_history"] = family_history
        if is_active is not None:
            active_val = 1 if is_active else 0
            if active_val != existing.get("is_active"):
                updates.append("is_active = :is_active")
                params["is_active"] = active_val

        if updates:
            sql = f"UPDATE subject_diseases SET {', '.join(updates)} WHERE subject_disease_id = :_id"
            session.execute(text(sql), params)
            return False, True, diag_linked, onset_linked
        return False, False, False, False
    else:
        cols = ["subject_id", "disease_id", "is_active"]
        vals: dict[str, Any] = {
            "subject_id": subject_id,
            "disease_id": disease_id,
            "is_active": (1 if is_active else 0) if is_active is not None else 1,
        }
        if diagnosis_event_id:
            cols.append("diagnosis_event_id")
            vals["diagnosis_event_id"] = diagnosis_event_id
            diag_linked = True
        if onset_event_id:
            cols.append("onset_event_id")
            vals["onset_event_id"] = onset_event_id
            onset_linked = True
        if diagnosis_notes:
            cols.append("diagnosis_notes")
            vals["diagnosis_notes"] = diagnosis_notes
        if family_history:
            cols.append("family_history")
            vals["family_history"] = family_history

        col_list = ", ".join(cols)
        val_list = ", ".join(f":{c}" for c in cols)
        session.execute(text(f"INSERT INTO subject_diseases ({col_list}) VALUES ({val_list})"), vals)
        return True, False, diag_linked, onset_linked


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_subject_disease_fields_response(engine: Engine) -> SubjectDiseaseFieldsResponse:
    field_defs = [
        SubjectDiseaseFieldDefinition(
            name=f.name, label=f.label, required=f.required,
            parsers=list(f.parsers), defaultParser=f.default_parser,
        )
        for f in SUBJECT_DISEASE_FIELDS
    ]

    with Session(engine) as session:
        disease_rows = session.execute(
            select(schema.Disease).order_by(schema.Disease.disease_name)
        ).scalars().all()
        diseases = [
            DiseaseSummary(id=d.disease_id, name=d.disease_name, code=d.disease_code)
            for d in disease_rows
        ]

        cohort_rows = session.execute(text(
            "SELECT c.cohort_id, c.name, COUNT(sc.subject_id) AS cnt "
            "FROM cohort c LEFT JOIN subject_cohorts sc ON sc.cohort_id = c.cohort_id "
            "GROUP BY c.cohort_id, c.name ORDER BY c.name"
        )).all()
        cohorts = [
            CohortSummary(id=r[0], name=r[1], subjectCount=r[2])
            for r in cohort_rows
        ]

        id_rows = session.execute(
            select(schema.IdType).order_by(schema.IdType.id_type_name)
        ).scalars().all()
        id_types = [IdTypeSummary(id=it.id_type_id, name=it.id_type_name) for it in id_rows]

    return SubjectDiseaseFieldsResponse(
        fields=field_defs, diseases=diseases, cohorts=cohorts, idTypes=id_types,
    )


def preview_subject_disease_import(
    *,
    engine: Engine,
    path: Path,
    config: SubjectDiseaseImportPayload,
    preview_limit: int = 50,
) -> SubjectDiseaseImportPreview:
    warnings: list[str] = []
    rows: list[SubjectDiseasePreviewRow] = []
    total = 0
    skipped = 0
    field_mappings = config.fields
    disease_id = config.disease_id
    id_type = config.subject_identifier_type

    with Session(engine) as session:
        disease = session.execute(
            select(schema.Disease).where(schema.Disease.disease_id == disease_id)
        ).scalar_one_or_none()
        if disease is None:
            raise ValueError(f"Disease {disease_id} not found")

        if id_type == "subject_code":
            subject_map = _build_subject_map_by_code(session)
        else:
            subject_map = _build_subject_map_by_identifier(session, int(id_type))

        subject_code_map = _build_subject_code_map(session)
        existing = _load_existing_subject_diseases(session, disease_id)
        diag_events = _build_event_map(session, DIAGNOSIS_OBS_TYPE_ID)
        onset_events = _build_event_map(session, ONSET_OBS_TYPE_ID)

    for row_number, (row, aliases) in enumerate(iter_csv_rows(path), start=1):
        total += 1
        if len(rows) >= preview_limit:
            continue

        ident_mapping = field_mappings.get("subject_identifier")
        if not ident_mapping:
            warnings.append(f"Row {row_number}: no subject_identifier mapping")
            skipped += 1
            continue
        ident_value = coerce_value(
            field=FIELD_MAP["subject_identifier"], mapping=ident_mapping,
            row=row, aliases=aliases, warnings=warnings, row_number=row_number,
        )
        if not ident_value:
            warnings.append(f"Row {row_number}: missing subject identifier, skipped")
            skipped += 1
            continue
        ident_str = str(ident_value).strip()

        subject_id = subject_map.get(ident_str.lower())
        subject_found = subject_id is not None
        resolved_code = subject_code_map.get(subject_id) if subject_id else None

        notes_val = None
        notes_mapping = field_mappings.get("diagnosis_notes")
        if notes_mapping:
            notes_val = coerce_value(
                field=FIELD_MAP["diagnosis_notes"], mapping=notes_mapping,
                row=row, aliases=aliases, warnings=warnings, row_number=row_number,
            )

        fh_val = None
        fh_mapping = field_mappings.get("family_history")
        if fh_mapping:
            fh_val = coerce_value(
                field=FIELD_MAP["family_history"], mapping=fh_mapping,
                row=row, aliases=aliases, warnings=warnings, row_number=row_number,
            )

        has_diag = subject_id is not None and subject_id in diag_events
        has_onset = subject_id is not None and subject_id in onset_events
        is_existing = subject_id is not None and subject_id in existing

        rows.append(SubjectDiseasePreviewRow(
            subjectIdentifier=ident_str,
            resolvedSubjectCode=resolved_code,
            subjectFound=subject_found,
            diagnosisNotes=str(notes_val) if notes_val else None,
            familyHistory=str(fh_val) if fh_val else None,
            hasDiagnosisEvent=has_diag,
            hasOnsetEvent=has_onset,
            existing=is_existing,
        ))

    return SubjectDiseaseImportPreview(
        totalRows=total, processedRows=len(rows), skippedRows=skipped,
        warnings=warnings, rows=rows,
    )


def apply_subject_disease_import(
    *,
    engine: Engine,
    path: Path,
    config: SubjectDiseaseImportPayload,
) -> SubjectDiseaseImportResult:
    warnings: list[str] = []
    field_mappings = config.fields
    disease_id = config.disease_id
    id_type = config.subject_identifier_type
    skip_blanks = config.options.skip_blank_updates

    total_inserted = 0
    total_updated = 0
    subjects_missing = 0
    total_diag_linked = 0
    total_onset_linked = 0

    with Session(engine) as session:
        disease = session.execute(
            select(schema.Disease).where(schema.Disease.disease_id == disease_id)
        ).scalar_one_or_none()
        if disease is None:
            raise ValueError(f"Disease {disease_id} not found")

        if id_type == "subject_code":
            subject_map = _build_subject_map_by_code(session)
        else:
            subject_map = _build_subject_map_by_identifier(session, int(id_type))

        existing = _load_existing_subject_diseases(session, disease_id)
        diag_events = _build_event_map(session, DIAGNOSIS_OBS_TYPE_ID)
        onset_events = _build_event_map(session, ONSET_OBS_TYPE_ID)

        for row_number, (row, aliases) in enumerate(iter_csv_rows(path), start=1):
            ident_mapping = field_mappings.get("subject_identifier")
            if not ident_mapping:
                continue
            ident_value = coerce_value(
                field=FIELD_MAP["subject_identifier"], mapping=ident_mapping,
                row=row, aliases=aliases, warnings=warnings, row_number=row_number,
            )
            if not ident_value:
                continue
            ident_str = str(ident_value).strip()

            subject_id = subject_map.get(ident_str.lower())
            if subject_id is None:
                subjects_missing += 1
                continue

            notes_val = None
            notes_mapping = field_mappings.get("diagnosis_notes")
            if notes_mapping:
                notes_val = coerce_value(
                    field=FIELD_MAP["diagnosis_notes"], mapping=notes_mapping,
                    row=row, aliases=aliases, warnings=warnings, row_number=row_number,
                )

            fh_val = None
            fh_mapping = field_mappings.get("family_history")
            if fh_mapping:
                fh_val = coerce_value(
                    field=FIELD_MAP["family_history"], mapping=fh_mapping,
                    row=row, aliases=aliases, warnings=warnings, row_number=row_number,
                )

            active_val = None
            active_mapping = field_mappings.get("is_active")
            if active_mapping:
                active_val = coerce_value(
                    field=FIELD_MAP["is_active"], mapping=active_mapping,
                    row=row, aliases=aliases, warnings=warnings, row_number=row_number,
                )

            inserted, updated, dl, ol = _upsert_subject_disease(
                session,
                subject_id=subject_id,
                disease_id=disease_id,
                existing=existing.get(subject_id),
                diagnosis_event_id=diag_events.get(subject_id),
                onset_event_id=onset_events.get(subject_id),
                diagnosis_notes=str(notes_val) if notes_val else None,
                family_history=str(fh_val) if fh_val else None,
                is_active=active_val,
                skip_blanks=skip_blanks,
            )
            if inserted:
                total_inserted += 1
                existing[subject_id] = {"subject_disease_id": -1}
            if updated:
                total_updated += 1
            if dl:
                total_diag_linked += 1
            if ol:
                total_onset_linked += 1

        if not config.dry_run:
            session.commit()
        else:
            session.rollback()

    return SubjectDiseaseImportResult(
        inserted=total_inserted, updated=total_updated,
        subjectsMissing=subjects_missing,
        diagnosisEventsLinked=total_diag_linked,
        onsetEventsLinked=total_onset_linked,
    )


def apply_cohort_assign(
    engine: Engine, payload: CohortAssignPayload,
) -> CohortAssignResult:
    with Session(engine) as session:
        disease = session.execute(
            select(schema.Disease).where(schema.Disease.disease_id == payload.disease_id)
        ).scalar_one_or_none()
        if disease is None:
            raise ValueError(f"Disease {payload.disease_id} not found")

        cohort = session.execute(
            select(schema.Cohort).where(schema.Cohort.cohort_id == payload.cohort_id)
        ).scalar_one_or_none()
        if cohort is None:
            raise ValueError(f"Cohort {payload.cohort_id} not found")

        # Get all subjects in the cohort
        subject_ids = [
            r[0] for r in session.execute(
                text("SELECT subject_id FROM subject_cohorts WHERE cohort_id = :cid"),
                {"cid": payload.cohort_id},
            ).all()
        ]

        existing = _load_existing_subject_diseases(session, payload.disease_id)
        diag_events = _build_event_map(session, DIAGNOSIS_OBS_TYPE_ID)
        onset_events = _build_event_map(session, ONSET_OBS_TYPE_ID)

        total_inserted = 0
        total_skipped = 0
        total_diag_linked = 0
        total_onset_linked = 0

        for subject_id in subject_ids:
            inserted, updated, dl, ol = _upsert_subject_disease(
                session,
                subject_id=subject_id,
                disease_id=payload.disease_id,
                existing=existing.get(subject_id),
                diagnosis_event_id=diag_events.get(subject_id),
                onset_event_id=onset_events.get(subject_id),
            )
            if inserted:
                total_inserted += 1
                existing[subject_id] = {"subject_disease_id": -1}
            elif updated:
                total_inserted += 0  # count as skip if only event linking
            if dl:
                total_diag_linked += 1
            if ol:
                total_onset_linked += 1
            if not inserted and not updated:
                total_skipped += 1

        if not payload.dry_run:
            session.commit()
        else:
            session.rollback()

    return CohortAssignResult(
        inserted=total_inserted,
        skipped=total_skipped,
        diagnosisEventsLinked=total_diag_linked,
        onsetEventsLinked=total_onset_linked,
        totalSubjects=len(subject_ids),
    )
