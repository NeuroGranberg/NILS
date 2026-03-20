"""Import logic for subject_disease_types table (CSV + manual upsert with fuzzy disease-type matching)."""

from __future__ import annotations

import logging
import re
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
    parse_date,
)

logger = logging.getLogger(__name__)

SP_TRANSITION_OBS_TYPE_ID = 10

SUBJECT_DISEASE_TYPE_FIELDS: tuple[FieldDefinition, ...] = (
    FieldDefinition(name="subject_identifier", label="Subject Identifier", required=True),
    FieldDefinition(name="disease_type", label="Disease Type / Subtype", required=True),
    FieldDefinition(name="assignment_date", label="Assignment Date", required=False, default_parser="date", parsers=("date",)),
    FieldDefinition(name="notes", label="Notes"),
)

FIELD_MAP = {f.name: f for f in SUBJECT_DISEASE_TYPE_FIELDS}


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class DiseaseSummary(BaseModel):
    id: int
    name: str
    code: str | None = None


class DiseaseTypeSummary(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: int
    disease_id: int = Field(alias="diseaseId")
    name: str
    description: str | None = None
    aliases: list[str] = []


class IdTypeSummary(BaseModel):
    id: int
    name: str


class SDTFieldDefinition(BaseModel):
    name: str
    label: str
    required: bool = False
    parsers: list[str]
    default_parser: str = Field(alias="defaultParser")


class SDTFieldsResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    fields: list[SDTFieldDefinition]
    diseases: list[DiseaseSummary]
    disease_types: list[DiseaseTypeSummary] = Field(alias="diseaseTypes")
    id_types: list[IdTypeSummary] = Field(alias="idTypes")


class SDTImportPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    file_token: str | None = Field(default=None, alias="fileToken")
    file_path: str | None = Field(default=None, alias="filePath")
    disease_id: int = Field(alias="diseaseId")
    subject_identifier_type: str = Field(alias="subjectIdentifierType")
    fields: dict[str, FieldMapping] = Field(alias="fields")
    dry_run: bool = Field(default=False, alias="dryRun")

    @model_validator(mode="after")
    def _validate_fields(self) -> "SDTImportPayload":
        mappings = self.fields or {}
        unknown = sorted(set(mappings) - set(FIELD_MAP))
        if unknown:
            raise ValueError(f"Unknown field mappings: {', '.join(unknown)}")
        for f in SUBJECT_DISEASE_TYPE_FIELDS:
            if f.required and f.name not in mappings:
                raise ValueError(f"Missing required mapping: {f.name}")
        return self


class SDTManualPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    subject_disease_type_id: int | None = Field(default=None, alias="subjectDiseaseTypeId")
    subject_identifier: str = Field(alias="subjectIdentifier")
    subject_identifier_type: str = Field(alias="subjectIdentifierType")
    disease_id: int = Field(alias="diseaseId")
    disease_type_id: int = Field(alias="diseaseTypeId")
    assignment_date: str | None = Field(default=None, alias="assignmentDate")
    notes: str | None = None
    dry_run: bool = Field(default=False, alias="dryRun")


class SDTPreviewRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    subject_identifier: str = Field(alias="subjectIdentifier")
    resolved_subject_code: str | None = Field(default=None, alias="resolvedSubjectCode")
    subject_found: bool = Field(alias="subjectFound")
    disease_type_input: str = Field(alias="diseaseTypeInput")
    resolved_disease_type: str | None = Field(default=None, alias="resolvedDiseaseType")
    disease_type_found: bool = Field(alias="diseaseTypeFound")
    assignment_date: str | None = Field(default=None, alias="assignmentDate")
    has_transition_event: bool = Field(default=False, alias="hasTransitionEvent")
    notes: str | None = None
    existing: bool = False


class SDTImportPreview(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    total_rows: int = Field(alias="totalRows")
    processed_rows: int = Field(alias="processedRows")
    skipped_rows: int = Field(alias="skippedRows")
    warnings: list[str]
    rows: list[SDTPreviewRow]


class SDTImportResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    inserted: int
    updated: int
    subjects_missing: int = Field(alias="subjectsMissing")
    types_unresolved: int = Field(alias="typesUnresolved")
    transition_events_linked: int = Field(alias="transitionEventsLinked")


class SDTManualResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    subject_disease_type_id: int = Field(alias="subjectDiseaseTypeId")
    inserted: bool
    updated: bool


class SDTDetailResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    subject_disease_type_id: int = Field(alias="subjectDiseaseTypeId")
    subject_disease_id: int = Field(alias="subjectDiseaseId")
    disease_type_id: int = Field(alias="diseaseTypeId")
    disease_type_name: str = Field(alias="diseaseTypeName")
    assignment_date: str = Field(alias="assignmentDate")
    transition_event_id: int | None = Field(default=None, alias="transitionEventId")
    notes: str | None = None
    is_active: int = Field(alias="isActive")


# ---------------------------------------------------------------------------
# Fuzzy disease type matching
# ---------------------------------------------------------------------------

def _build_alias_map(disease_types: list[schema.DiseaseType]) -> dict[str, int]:
    """Build a case-insensitive alias->disease_type_id map.

    Matches are tried in order:
      1. Exact ID
      2. Exact type_name
      3. type_name without hyphens/spaces
      4. First word of type_name
      5. Abbreviation (first letter of each word in description)
      6. Well-known short forms extracted from type_name
    """
    alias_map: dict[str, int] = {}

    for dt in disease_types:
        dtid = dt.disease_type_id
        name = dt.type_name
        name_lower = name.lower().strip()

        # ID as string
        alias_map[str(dtid)] = dtid

        # Exact name
        alias_map[name_lower] = dtid

        # Name without hyphens, dashes, spaces
        collapsed = re.sub(r"[\s\-_]+", "", name_lower)
        if collapsed not in alias_map:
            alias_map[collapsed] = dtid

        # First word (e.g. "Limb" from "Limb-onset")
        first_word = re.split(r"[\s\-_]+", name_lower)[0]
        if first_word and first_word not in alias_map:
            alias_map[first_word] = dtid

        # For multi-word uppercase names like RRMS, SPMS — partial prefix
        # e.g. "RR" for "RRMS", "SP" for "SPMS", "PP" for "PPMS"
        if name.isupper() and len(name) >= 3:
            for prefix_len in range(2, len(name)):
                prefix = name_lower[:prefix_len]
                if prefix not in alias_map:
                    alias_map[prefix] = dtid

        # Description-based: grab key adjectives and multi-word phrases
        if dt.description:
            desc_lower = dt.description.lower()
            desc_words = desc_lower.split()
            skip_words = {"disease", "multiple", "sclerosis", "onset", "negative", "positive",
                          "als", "parkinson", "alzheimer", "nmosd", "mogad"}
            for w in desc_words:
                clean = re.sub(r"[^a-z]", "", w)
                if len(clean) >= 4 and clean not in alias_map and clean not in skip_words:
                    alias_map[clean] = dtid

            # Multi-word descriptive phrases: "secondary progressive", "relapsing-remitting"
            # Take qualifier words before disease-name words
            qualifier_parts: list[str] = []
            for w in desc_words:
                clean = re.sub(r"[^a-z\-]", "", w.lower())
                if clean in skip_words or clean in {"disease's", "igg"}:
                    break
                if len(clean) >= 2:
                    qualifier_parts.append(clean)
            if len(qualifier_parts) >= 2:
                phrase = " ".join(qualifier_parts)
                if phrase not in alias_map:
                    alias_map[phrase] = dtid
                # Also collapsed (no spaces/hyphens)
                collapsed_phrase = re.sub(r"[\s\-]+", "", phrase)
                if collapsed_phrase not in alias_map:
                    alias_map[collapsed_phrase] = dtid
                # Hyphen variant: "relapsing-remitting"
                hyphen_phrase = "-".join(qualifier_parts)
                if hyphen_phrase not in alias_map:
                    alias_map[hyphen_phrase] = dtid

    return alias_map


def _resolve_disease_type(
    raw_value: str,
    alias_map: dict[str, int],
    type_name_by_id: dict[int, str],
) -> tuple[int | None, str | None]:
    """Resolve a raw string to (disease_type_id, type_name) or (None, None)."""
    val = raw_value.strip()
    if not val:
        return None, None

    val_lower = val.lower()

    # 1. Direct lookup
    if val_lower in alias_map:
        dtid = alias_map[val_lower]
        return dtid, type_name_by_id.get(dtid)

    # 2. Collapsed (remove hyphens/spaces)
    collapsed = re.sub(r"[\s\-_]+", "", val_lower)
    if collapsed in alias_map:
        dtid = alias_map[collapsed]
        return dtid, type_name_by_id.get(dtid)

    # 3. Try as integer ID
    try:
        int_val = int(val)
        if int_val in type_name_by_id:
            return int_val, type_name_by_id[int_val]
    except ValueError:
        pass

    return None, None


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


def _build_subject_disease_map(session: Session, disease_id: int) -> dict[int, int]:
    """subject_id -> subject_disease_id for the given disease."""
    rows = session.execute(
        text("SELECT subject_id, subject_disease_id FROM subject_diseases WHERE disease_id = :did"),
        {"did": disease_id},
    ).all()
    return {sid: sdid for sid, sdid in rows}


def _build_transition_event_map(session: Session) -> dict[int, int]:
    """subject_id -> event_id for SP Transition events (earliest per subject)."""
    rows = session.execute(
        text(
            "SELECT subject_id, event_id FROM event "
            "WHERE observation_type_id = :oid ORDER BY subject_id, event_date"
        ),
        {"oid": SP_TRANSITION_OBS_TYPE_ID},
    ).all()
    result: dict[int, int] = {}
    for sid, eid in rows:
        if sid not in result:
            result[sid] = eid
    return result


def _load_existing_sdt(
    session: Session,
    subject_disease_ids: set[int],
) -> dict[tuple[int, int], dict[str, Any]]:
    """(subject_disease_id, disease_type_id) -> row dict."""
    if not subject_disease_ids:
        return {}
    placeholders = ", ".join(str(i) for i in subject_disease_ids)
    rows = session.execute(
        text(
            f"SELECT subject_disease_type_id, subject_disease_id, disease_type_id, "
            f"assignment_date, transition_event_id, notes, is_active "
            f"FROM subject_disease_types WHERE subject_disease_id IN ({placeholders})"
        )
    ).all()
    return {
        (r[1], r[2]): {
            "subject_disease_type_id": r[0],
            "assignment_date": str(r[3]) if r[3] else None,
            "transition_event_id": r[4],
            "notes": r[5],
            "is_active": r[6],
        }
        for r in rows
    }


def _load_disease_types_for_disease(session: Session, disease_id: int) -> list[schema.DiseaseType]:
    return list(
        session.execute(
            select(schema.DiseaseType)
            .where(schema.DiseaseType.disease_id == disease_id)
            .order_by(schema.DiseaseType.sort_order)
        ).scalars().all()
    )


def _is_spms_type(disease_type_id: int, type_name_by_id: dict[int, str]) -> bool:
    name = type_name_by_id.get(disease_type_id, "")
    return name.upper() in ("SPMS", "SP")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_sdt_fields_response(engine: Engine) -> SDTFieldsResponse:
    field_defs = [
        SDTFieldDefinition(
            name=f.name, label=f.label, required=f.required,
            parsers=list(f.parsers), defaultParser=f.default_parser,
        )
        for f in SUBJECT_DISEASE_TYPE_FIELDS
    ]

    with Session(engine) as session:
        disease_rows = session.execute(
            select(schema.Disease).order_by(schema.Disease.disease_name)
        ).scalars().all()

        # Only include diseases that have subtypes
        dt_rows = session.execute(
            select(schema.DiseaseType).order_by(schema.DiseaseType.disease_id, schema.DiseaseType.sort_order)
        ).scalars().all()

        diseases_with_types = {dt.disease_id for dt in dt_rows}
        diseases = [
            DiseaseSummary(id=d.disease_id, name=d.disease_name, code=d.disease_code)
            for d in disease_rows if d.disease_id in diseases_with_types
        ]

        alias_map = _build_alias_map(dt_rows)
        # Build per-type alias list for UI display
        reverse: dict[int, list[str]] = {}
        for alias, dtid in alias_map.items():
            if alias != str(dtid):
                reverse.setdefault(dtid, []).append(alias)

        disease_types = [
            DiseaseTypeSummary(
                id=dt.disease_type_id,
                diseaseId=dt.disease_id,
                name=dt.type_name,
                description=dt.description,
                aliases=sorted(set(reverse.get(dt.disease_type_id, [])))[:8],
            )
            for dt in dt_rows
        ]

        id_rows = session.execute(
            select(schema.IdType).order_by(schema.IdType.id_type_name)
        ).scalars().all()
        id_types = [IdTypeSummary(id=it.id_type_id, name=it.id_type_name) for it in id_rows]

    return SDTFieldsResponse(
        fields=field_defs, diseases=diseases, diseaseTypes=disease_types, idTypes=id_types,
    )


def preview_sdt_import(
    *,
    engine: Engine,
    path: Path,
    config: SDTImportPayload,
    preview_limit: int = 50,
) -> SDTImportPreview:
    warnings: list[str] = []
    rows: list[SDTPreviewRow] = []
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

        dtypes = _load_disease_types_for_disease(session, disease_id)
        if not dtypes:
            raise ValueError(f"Disease '{disease.disease_name}' has no subtypes defined")

        alias_map = _build_alias_map(dtypes)
        type_name_by_id = {dt.disease_type_id: dt.type_name for dt in dtypes}

        if id_type == "subject_code":
            subject_map = _build_subject_map_by_code(session)
        else:
            subject_map = _build_subject_map_by_identifier(session, int(id_type))

        subject_code_map = _build_subject_code_map(session)
        sd_map = _build_subject_disease_map(session, disease_id)
        transition_events = _build_transition_event_map(session)
        existing_sdt = _load_existing_sdt(session, set(sd_map.values()))

    for row_number, (row, aliases) in enumerate(iter_csv_rows(path), start=1):
        total += 1
        if len(rows) >= preview_limit:
            continue

        # Subject identifier
        ident_val = coerce_value(
            field=FIELD_MAP["subject_identifier"],
            mapping=field_mappings.get("subject_identifier"),
            row=row, aliases=aliases, warnings=warnings, row_number=row_number,
        )
        if not ident_val:
            warnings.append(f"Row {row_number}: missing subject identifier, skipped")
            skipped += 1
            continue
        ident_str = str(ident_val).strip()

        # Disease type
        dtype_val = coerce_value(
            field=FIELD_MAP["disease_type"],
            mapping=field_mappings.get("disease_type"),
            row=row, aliases=aliases, warnings=warnings, row_number=row_number,
        )
        dtype_input = str(dtype_val).strip() if dtype_val else ""
        resolved_dtid, resolved_dtname = _resolve_disease_type(dtype_input, alias_map, type_name_by_id)

        # Assignment date
        date_mapping = field_mappings.get("assignment_date")
        date_val = coerce_value(
            field=FIELD_MAP["assignment_date"], mapping=date_mapping,
            row=row, aliases=aliases, warnings=warnings, row_number=row_number,
        ) if date_mapping else None

        # Notes
        notes_mapping = field_mappings.get("notes")
        notes_val = coerce_value(
            field=FIELD_MAP["notes"], mapping=notes_mapping,
            row=row, aliases=aliases, warnings=warnings, row_number=row_number,
        ) if notes_mapping else None

        subject_id = subject_map.get(ident_str.lower())
        subject_found = subject_id is not None
        resolved_code = subject_code_map.get(subject_id) if subject_id else None

        has_transition = False
        is_existing = False
        if subject_id and resolved_dtid:
            sd_id = sd_map.get(subject_id)
            if sd_id:
                is_existing = (sd_id, resolved_dtid) in existing_sdt
                if _is_spms_type(resolved_dtid, type_name_by_id):
                    has_transition = subject_id in transition_events

        rows.append(SDTPreviewRow(
            subjectIdentifier=ident_str,
            resolvedSubjectCode=resolved_code,
            subjectFound=subject_found,
            diseaseTypeInput=dtype_input,
            resolvedDiseaseType=resolved_dtname,
            diseaseTypeFound=resolved_dtid is not None,
            assignmentDate=str(date_val) if date_val else None,
            hasTransitionEvent=has_transition,
            notes=str(notes_val) if notes_val else None,
            existing=is_existing,
        ))

    return SDTImportPreview(
        totalRows=total, processedRows=len(rows), skippedRows=skipped,
        warnings=warnings, rows=rows,
    )


def apply_sdt_import(
    *,
    engine: Engine,
    path: Path,
    config: SDTImportPayload,
) -> SDTImportResult:
    warnings: list[str] = []
    field_mappings = config.fields
    disease_id = config.disease_id
    id_type = config.subject_identifier_type

    total_inserted = 0
    total_updated = 0
    subjects_missing = 0
    types_unresolved = 0
    transition_linked = 0

    with Session(engine) as session:
        disease = session.execute(
            select(schema.Disease).where(schema.Disease.disease_id == disease_id)
        ).scalar_one_or_none()
        if disease is None:
            raise ValueError(f"Disease {disease_id} not found")

        dtypes = _load_disease_types_for_disease(session, disease_id)
        if not dtypes:
            raise ValueError(f"Disease '{disease.disease_name}' has no subtypes defined")

        alias_map = _build_alias_map(dtypes)
        type_name_by_id = {dt.disease_type_id: dt.type_name for dt in dtypes}

        if id_type == "subject_code":
            subject_map = _build_subject_map_by_code(session)
        else:
            subject_map = _build_subject_map_by_identifier(session, int(id_type))

        sd_map = _build_subject_disease_map(session, disease_id)
        transition_events = _build_transition_event_map(session)
        existing_sdt = _load_existing_sdt(session, set(sd_map.values()))

        for row_number, (row, aliases) in enumerate(iter_csv_rows(path), start=1):
            ident_val = coerce_value(
                field=FIELD_MAP["subject_identifier"],
                mapping=field_mappings.get("subject_identifier"),
                row=row, aliases=aliases, warnings=warnings, row_number=row_number,
            )
            if not ident_val:
                continue
            ident_str = str(ident_val).strip()

            subject_id = subject_map.get(ident_str.lower())
            if subject_id is None:
                subjects_missing += 1
                continue

            sd_id = sd_map.get(subject_id)
            if sd_id is None:
                subjects_missing += 1
                warnings.append(f"Row {row_number}: subject '{ident_str}' has no subject_disease record for this disease, skipped")
                continue

            dtype_val = coerce_value(
                field=FIELD_MAP["disease_type"],
                mapping=field_mappings.get("disease_type"),
                row=row, aliases=aliases, warnings=warnings, row_number=row_number,
            )
            dtype_input = str(dtype_val).strip() if dtype_val else ""
            resolved_dtid, _ = _resolve_disease_type(dtype_input, alias_map, type_name_by_id)
            if resolved_dtid is None:
                types_unresolved += 1
                warnings.append(f"Row {row_number}: cannot resolve disease type '{dtype_input}'")
                continue

            date_mapping = field_mappings.get("assignment_date")
            date_val = coerce_value(
                field=FIELD_MAP["assignment_date"], mapping=date_mapping,
                row=row, aliases=aliases, warnings=warnings, row_number=row_number,
            ) if date_mapping else None
            date_str = str(date_val) if date_val else None

            notes_mapping = field_mappings.get("notes")
            notes_val = coerce_value(
                field=FIELD_MAP["notes"], mapping=notes_mapping,
                row=row, aliases=aliases, warnings=warnings, row_number=row_number,
            ) if notes_mapping else None
            notes_str = str(notes_val).strip() if notes_val else None

            # Determine transition_event_id for SPMS
            trans_eid: int | None = None
            if _is_spms_type(resolved_dtid, type_name_by_id):
                trans_eid = transition_events.get(subject_id)

            key = (sd_id, resolved_dtid)
            existing = existing_sdt.get(key)

            if existing is not None:
                # UPDATE
                updates: list[str] = []
                params: dict[str, Any] = {"_id": existing["subject_disease_type_id"]}
                if date_str != existing.get("assignment_date"):
                    updates.append("assignment_date = :assignment_date")
                    params["assignment_date"] = date_str
                if notes_str and notes_str != existing.get("notes"):
                    updates.append("notes = :notes")
                    params["notes"] = notes_str
                if trans_eid and trans_eid != existing.get("transition_event_id"):
                    updates.append("transition_event_id = :transition_event_id")
                    params["transition_event_id"] = trans_eid
                    transition_linked += 1
                if updates:
                    sql = f"UPDATE subject_disease_types SET {', '.join(updates)} WHERE subject_disease_type_id = :_id"
                    session.execute(text(sql), params)
                    total_updated += 1
            else:
                # INSERT
                cols = ["subject_disease_id", "disease_type_id", "is_active"]
                vals: dict[str, Any] = {
                    "subject_disease_id": sd_id,
                    "disease_type_id": resolved_dtid,
                    "is_active": 1,
                }
                if date_str:
                    cols.append("assignment_date")
                    vals["assignment_date"] = date_str
                if trans_eid:
                    cols.append("transition_event_id")
                    vals["transition_event_id"] = trans_eid
                    transition_linked += 1
                if notes_str:
                    cols.append("notes")
                    vals["notes"] = notes_str

                col_list = ", ".join(cols)
                val_list = ", ".join(f":{c}" for c in cols)
                session.execute(text(f"INSERT INTO subject_disease_types ({col_list}) VALUES ({val_list})"), vals)
                total_inserted += 1
                existing_sdt[key] = {"subject_disease_type_id": -1}

        if not config.dry_run:
            session.commit()
        else:
            session.rollback()

    return SDTImportResult(
        inserted=total_inserted, updated=total_updated,
        subjectsMissing=subjects_missing, typesUnresolved=types_unresolved,
        transitionEventsLinked=transition_linked,
    )


def get_sdt_by_id(engine: Engine, sdt_id: int) -> SDTDetailResponse | None:
    with Session(engine) as session:
        row = session.execute(
            text(
                "SELECT sdt.subject_disease_type_id, sdt.subject_disease_id, sdt.disease_type_id, "
                "dt.type_name, sdt.assignment_date, sdt.transition_event_id, sdt.notes, sdt.is_active "
                "FROM subject_disease_types sdt "
                "JOIN disease_types dt ON dt.disease_type_id = sdt.disease_type_id "
                "WHERE sdt.subject_disease_type_id = :id"
            ),
            {"id": sdt_id},
        ).one_or_none()
        if row is None:
            return None
        return SDTDetailResponse(
            subjectDiseaseTypeId=row[0], subjectDiseaseId=row[1],
            diseaseTypeId=row[2], diseaseTypeName=row[3],
            assignmentDate=str(row[4]), transitionEventId=row[5],
            notes=row[6], isActive=row[7],
        )


def upsert_sdt_manual(engine: Engine, payload: SDTManualPayload) -> SDTManualResult:
    with Session(engine) as session:
        # Resolve subject
        if payload.subject_identifier_type == "subject_code":
            subject_map = _build_subject_map_by_code(session)
        else:
            subject_map = _build_subject_map_by_identifier(session, int(payload.subject_identifier_type))

        subject_id = subject_map.get(payload.subject_identifier.strip().lower())
        if subject_id is None:
            raise ValueError(f"Subject '{payload.subject_identifier}' not found")

        # Get subject_disease_id
        sd_id = session.execute(
            text("SELECT subject_disease_id FROM subject_diseases WHERE subject_id = :sid AND disease_id = :did"),
            {"sid": subject_id, "did": payload.disease_id},
        ).scalar_one_or_none()
        if sd_id is None:
            raise ValueError(
                f"Subject '{payload.subject_identifier}' does not have a subject_disease record "
                f"for disease_id={payload.disease_id}. Assign the disease first."
            )

        # Validate disease_type_id belongs to this disease
        dt = session.execute(
            select(schema.DiseaseType).where(
                schema.DiseaseType.disease_type_id == payload.disease_type_id,
                schema.DiseaseType.disease_id == payload.disease_id,
            )
        ).scalar_one_or_none()
        if dt is None:
            raise ValueError(f"Disease type {payload.disease_type_id} does not belong to disease {payload.disease_id}")

        date_str = parse_date(payload.assignment_date) if payload.assignment_date and payload.assignment_date.strip() else None

        # SP Transition event auto-link
        type_name_by_id = {dt.disease_type_id: dt.type_name}
        trans_eid: int | None = None
        if _is_spms_type(payload.disease_type_id, type_name_by_id):
            trans_eid = session.execute(
                text("SELECT event_id FROM event WHERE subject_id = :sid AND observation_type_id = :oid ORDER BY event_date LIMIT 1"),
                {"sid": subject_id, "oid": SP_TRANSITION_OBS_TYPE_ID},
            ).scalar_one_or_none()

        # Check existing by ID or by (subject_disease_id, disease_type_id)
        existing: Any = None
        if payload.subject_disease_type_id is not None:
            existing = session.execute(
                text("SELECT * FROM subject_disease_types WHERE subject_disease_type_id = :id"),
                {"id": payload.subject_disease_type_id},
            ).one_or_none()
        if existing is None:
            existing = session.execute(
                text(
                    "SELECT * FROM subject_disease_types "
                    "WHERE subject_disease_id = :sdid AND disease_type_id = :dtid"
                ),
                {"sdid": sd_id, "dtid": payload.disease_type_id},
            ).one_or_none()

        if existing is not None:
            ex_id = existing[0]
            updates: list[str] = []
            params: dict[str, Any] = {"_id": ex_id}
            existing_date = str(existing[3]) if existing[3] else None
            if date_str and date_str != existing_date:
                updates.append("assignment_date = :assignment_date")
                params["assignment_date"] = date_str
            if payload.notes is not None and payload.notes != existing[5]:
                updates.append("notes = :notes")
                params["notes"] = payload.notes
            if trans_eid and trans_eid != existing[4]:
                updates.append("transition_event_id = :transition_event_id")
                params["transition_event_id"] = trans_eid
            if updates:
                sql = f"UPDATE subject_disease_types SET {', '.join(updates)} WHERE subject_disease_type_id = :_id"
                session.execute(text(sql), params)

            if not payload.dry_run:
                session.commit()
            else:
                session.rollback()

            return SDTManualResult(subjectDiseaseTypeId=ex_id, inserted=False, updated=bool(updates))
        else:
            cols = ["subject_disease_id", "disease_type_id", "is_active"]
            vals: dict[str, Any] = {
                "subject_disease_id": sd_id,
                "disease_type_id": payload.disease_type_id,
                "is_active": 1,
            }
            if date_str:
                cols.append("assignment_date")
                vals["assignment_date"] = date_str
            if trans_eid:
                cols.append("transition_event_id")
                vals["transition_event_id"] = trans_eid
            if payload.notes:
                cols.append("notes")
                vals["notes"] = payload.notes

            col_list = ", ".join(cols)
            val_list = ", ".join(f":{c}" for c in cols)
            result = session.execute(
                text(f"INSERT INTO subject_disease_types ({col_list}) VALUES ({val_list}) RETURNING subject_disease_type_id"),
                vals,
            )
            new_id = result.scalar()

            if not payload.dry_run:
                session.commit()
            else:
                session.rollback()

            return SDTManualResult(subjectDiseaseTypeId=new_id or 0, inserted=True, updated=False)
