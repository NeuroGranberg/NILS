"""Import logic for event table with linked measure upsert."""

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

# ---------------------------------------------------------------------------
# Field definitions
# ---------------------------------------------------------------------------

EVENT_FIELDS: tuple[FieldDefinition, ...] = (
    FieldDefinition(name="subject_identifier", label="Subject Identifier", required=True),
    FieldDefinition(name="event_date", label="Event Date", required=True, default_parser="date", parsers=("date",)),
    FieldDefinition(name="event_time", label="Event Time"),
    FieldDefinition(name="notes", label="Notes"),
)

MEASURE_FIELDS: tuple[FieldDefinition, ...] = (
    FieldDefinition(name="value", label="Value", required=True),
)

EVENT_FIELD_MAP = {f.name: f for f in EVENT_FIELDS}
MEASURE_FIELD_MAP = {f.name: f for f in MEASURE_FIELDS}

VALUE_TYPE_TO_TABLE: dict[str, str] = {
    "Numeric": "numeric_measures",
    "Text": "text_measures",
    "Boolean": "boolean_measures",
    "JSON": "json_measures",
}

VALUE_TYPE_TO_COLUMN: dict[str, str] = {
    "Numeric": "numeric_value",
    "Text": "text_value",
    "Boolean": "boolean_value",
    "JSON": "json_value",
}

VALUE_TYPE_TO_PARSER: dict[str, str] = {
    "Numeric": "float",
    "Text": "string",
    "Boolean": "bool",
    "JSON": "string",
}


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class EventImportOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    skip_blank_updates: bool = Field(default=True, alias="skipBlankUpdates")


class EventImportPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    file_token: str | None = Field(default=None, alias="fileToken")
    file_path: str | None = Field(default=None, alias="filePath")
    observation_type_id: int = Field(alias="observationTypeId")
    subject_identifier_type: str = Field(alias="subjectIdentifierType")
    fields: dict[str, FieldMapping] = Field(alias="fields")
    options: EventImportOptions = Field(default_factory=EventImportOptions)
    dry_run: bool = Field(default=False, alias="dryRun")

    @model_validator(mode="after")
    def _validate_fields(self) -> "EventImportPayload":
        mappings = self.fields or {}
        all_known = set(EVENT_FIELD_MAP) | set(MEASURE_FIELD_MAP)
        unknown = sorted(set(mappings) - all_known)
        if unknown:
            raise ValueError(f"Unknown field mappings: {', '.join(unknown)}")
        for f in EVENT_FIELDS:
            if f.required and f.name not in mappings:
                raise ValueError(f"Event import requires mapping for: {f.name}")
        return self


class EventImportPreviewRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    subject_identifier: str = Field(alias="subjectIdentifier")
    resolved_subject_code: str | None = Field(default=None, alias="resolvedSubjectCode")
    subject_found: bool = Field(alias="subjectFound")
    event_date: str = Field(alias="eventDate")
    event_time: str | None = Field(default=None, alias="eventTime")
    notes: str | None = None
    value: Any | None = None
    existing: bool = False
    existing_event: dict[str, Any] | None = Field(default=None, alias="existingEvent")


class EventImportPreview(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    total_rows: int = Field(alias="totalRows")
    processed_rows: int = Field(alias="processedRows")
    skipped_rows: int = Field(alias="skippedRows")
    warnings: list[str]
    rows: list[EventImportPreviewRow]


class EventImportResult(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    events_inserted: int = Field(alias="eventsInserted")
    events_updated: int = Field(alias="eventsUpdated")
    measures_inserted: int = Field(alias="measuresInserted")
    measures_updated: int = Field(alias="measuresUpdated")
    subjects_missing: int = Field(alias="subjectsMissing")


class ObservationTypeSummary(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: int
    category: str
    name: str
    value_type: str | None = Field(default=None, alias="valueType")
    unit: str | None = None
    min_value: float | None = Field(default=None, alias="minValue")
    max_value: float | None = Field(default=None, alias="maxValue")


class IdTypeSummary(BaseModel):
    id: int
    name: str


class EventImportFieldDefinition(BaseModel):
    name: str
    label: str
    required: bool = False
    parsers: list[str]
    default_parser: str = Field(alias="defaultParser")


class EventImportFieldsResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    event_fields: list[EventImportFieldDefinition] = Field(alias="eventFields")
    measure_fields: list[EventImportFieldDefinition] = Field(alias="measureFields")
    observation_types: list[ObservationTypeSummary] = Field(alias="observationTypes")
    id_types: list[IdTypeSummary] = Field(alias="idTypes")


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


def _load_existing_events(
    session: Session, observation_type_id: int
) -> dict[tuple[int, str], dict[str, Any]]:
    """Load existing events for the given observation type.
    Returns {(subject_id, date_str): event_dict}."""
    rows = session.execute(
        select(schema.Event).where(schema.Event.observation_type_id == observation_type_id)
    ).scalars().all()
    result: dict[tuple[int, str], dict[str, Any]] = {}
    for e in rows:
        key = (e.subject_id, str(e.event_date))
        result[key] = {
            "event_id": e.event_id,
            "subject_id": e.subject_id,
            "observation_type_id": e.observation_type_id,
            "event_date": str(e.event_date),
            "event_time": str(e.event_time) if e.event_time else None,
            "notes": e.notes,
        }
    return result


def _resolve_subject_id(
    identifier: str,
    identifier_type: str,
    subject_map: dict[str, int],
) -> int | None:
    return subject_map.get(identifier.strip().lower())


def _get_observation_type(session: Session, obs_type_id: int) -> schema.ObservationType | None:
    return session.execute(
        select(schema.ObservationType).where(schema.ObservationType.observation_type_id == obs_type_id)
    ).scalar_one_or_none()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_event_fields_response(engine: Engine) -> EventImportFieldsResponse:
    event_defs = [
        EventImportFieldDefinition(
            name=f.name, label=f.label, required=f.required,
            parsers=list(f.parsers), defaultParser=f.default_parser,
        )
        for f in EVENT_FIELDS
    ]
    measure_defs = [
        EventImportFieldDefinition(
            name=f.name, label=f.label, required=f.required,
            parsers=list(f.parsers), defaultParser=f.default_parser,
        )
        for f in MEASURE_FIELDS
    ]

    with Session(engine) as session:
        obs_rows = session.execute(
            select(schema.ObservationType)
            .where(schema.ObservationType.is_active == 1)
            .order_by(schema.ObservationType.category, schema.ObservationType.name)
        ).scalars().all()
        obs_types = [
            ObservationTypeSummary(
                id=o.observation_type_id, category=o.category, name=o.name,
                valueType=o.value_type, unit=o.unit,
                minValue=o.min_value, maxValue=o.max_value,
            )
            for o in obs_rows
        ]

        id_rows = session.execute(
            select(schema.IdType).order_by(schema.IdType.id_type_name)
        ).scalars().all()
        id_types = [IdTypeSummary(id=it.id_type_id, name=it.id_type_name) for it in id_rows]

    return EventImportFieldsResponse(
        eventFields=event_defs,
        measureFields=measure_defs,
        observationTypes=obs_types,
        idTypes=id_types,
    )


def preview_event_import(
    *,
    engine: Engine,
    path: Path,
    config: EventImportPayload,
    preview_limit: int = 50,
) -> EventImportPreview:
    warnings: list[str] = []
    rows: list[EventImportPreviewRow] = []
    total = 0
    skipped = 0
    field_mappings = config.fields
    obs_type_id = config.observation_type_id
    id_type = config.subject_identifier_type

    with Session(engine) as session:
        obs_type = _get_observation_type(session, obs_type_id)
        if obs_type is None:
            raise ValueError(f"Observation type {obs_type_id} not found")

        value_type = obs_type.value_type
        needs_value = value_type is not None and value_type in VALUE_TYPE_TO_TABLE

        if id_type == "subject_code":
            subject_map = _build_subject_map_by_code(session)
        else:
            try:
                id_type_id = int(id_type)
            except ValueError:
                raise ValueError(f"Invalid subject identifier type: {id_type}")
            subject_map = _build_subject_map_by_identifier(session, id_type_id)

        subject_code_map = _build_subject_code_map(session)
        existing_events = _load_existing_events(session, obs_type_id)

    for row_number, (row, aliases) in enumerate(iter_csv_rows(path), start=1):
        total += 1
        if len(rows) >= preview_limit:
            continue

        # Parse subject identifier
        ident_mapping = field_mappings.get("subject_identifier")
        if not ident_mapping:
            warnings.append(f"Row {row_number}: no subject_identifier mapping")
            skipped += 1
            continue
        ident_value = coerce_value(
            field=EVENT_FIELD_MAP["subject_identifier"], mapping=ident_mapping,
            row=row, aliases=aliases, warnings=warnings, row_number=row_number,
        )
        if not ident_value:
            warnings.append(f"Row {row_number}: missing subject identifier, skipped")
            skipped += 1
            continue
        ident_str = str(ident_value).strip()

        # Parse event_date
        date_mapping = field_mappings.get("event_date")
        if not date_mapping:
            warnings.append(f"Row {row_number}: no event_date mapping")
            skipped += 1
            continue
        date_value = coerce_value(
            field=EVENT_FIELD_MAP["event_date"], mapping=date_mapping,
            row=row, aliases=aliases, warnings=warnings, row_number=row_number,
        )
        if not date_value:
            warnings.append(f"Row {row_number}: missing event date, skipped")
            skipped += 1
            continue

        # Parse optional fields
        time_value = None
        time_mapping = field_mappings.get("event_time")
        if time_mapping:
            time_value = coerce_value(
                field=EVENT_FIELD_MAP["event_time"], mapping=time_mapping,
                row=row, aliases=aliases, warnings=warnings, row_number=row_number,
            )

        notes_value = None
        notes_mapping = field_mappings.get("notes")
        if notes_mapping:
            notes_value = coerce_value(
                field=EVENT_FIELD_MAP["notes"], mapping=notes_mapping,
                row=row, aliases=aliases, warnings=warnings, row_number=row_number,
            )

        # Parse measure value if needed
        measure_value = None
        if needs_value:
            val_mapping = field_mappings.get("value")
            if val_mapping:
                parser_name = VALUE_TYPE_TO_PARSER.get(value_type, "string")
                value_field = FieldDefinition(
                    name="value", label="Value", required=True,
                    default_parser=parser_name, parsers=(parser_name, "string"),
                )
                measure_value = coerce_value(
                    field=value_field, mapping=val_mapping,
                    row=row, aliases=aliases, warnings=warnings, row_number=row_number,
                )
            if measure_value is None:
                warnings.append(f"Row {row_number}: missing measurement value, skipped")
                skipped += 1
                continue

        # Resolve subject
        subject_id = _resolve_subject_id(ident_str, id_type, subject_map)
        subject_found = subject_id is not None
        resolved_code = subject_code_map.get(subject_id) if subject_id else None

        # Check existing event
        existing = None
        if subject_id:
            existing = existing_events.get((subject_id, str(date_value)))

        rows.append(EventImportPreviewRow(
            subjectIdentifier=ident_str,
            resolvedSubjectCode=resolved_code,
            subjectFound=subject_found,
            eventDate=str(date_value),
            eventTime=str(time_value) if time_value else None,
            notes=str(notes_value) if notes_value else None,
            value=measure_value,
            existing=existing is not None,
            existingEvent=existing,
        ))

    return EventImportPreview(
        totalRows=total, processedRows=len(rows), skippedRows=skipped,
        warnings=warnings, rows=rows,
    )


def apply_event_import(
    *,
    engine: Engine,
    path: Path,
    config: EventImportPayload,
) -> EventImportResult:
    warnings: list[str] = []
    field_mappings = config.fields
    obs_type_id = config.observation_type_id
    id_type = config.subject_identifier_type
    skip_blanks = config.options.skip_blank_updates

    events_inserted = 0
    events_updated = 0
    measures_inserted = 0
    measures_updated = 0
    subjects_missing = 0

    with Session(engine) as session:
        obs_type = _get_observation_type(session, obs_type_id)
        if obs_type is None:
            raise ValueError(f"Observation type {obs_type_id} not found")

        value_type = obs_type.value_type
        needs_value = value_type is not None and value_type in VALUE_TYPE_TO_TABLE
        measure_table = VALUE_TYPE_TO_TABLE.get(value_type) if needs_value else None
        measure_col = VALUE_TYPE_TO_COLUMN.get(value_type) if needs_value else None

        if id_type == "subject_code":
            subject_map = _build_subject_map_by_code(session)
        else:
            try:
                id_type_id = int(id_type)
            except ValueError:
                raise ValueError(f"Invalid subject identifier type: {id_type}")
            subject_map = _build_subject_map_by_identifier(session, id_type_id)

        existing_events = _load_existing_events(session, obs_type_id)

        for row_number, (row, aliases) in enumerate(iter_csv_rows(path), start=1):
            # Parse subject identifier
            ident_mapping = field_mappings.get("subject_identifier")
            if not ident_mapping:
                continue
            ident_value = coerce_value(
                field=EVENT_FIELD_MAP["subject_identifier"], mapping=ident_mapping,
                row=row, aliases=aliases, warnings=warnings, row_number=row_number,
            )
            if not ident_value:
                continue
            ident_str = str(ident_value).strip()

            # Resolve subject
            subject_id = _resolve_subject_id(ident_str, id_type, subject_map)
            if subject_id is None:
                subjects_missing += 1
                continue

            # Parse event_date
            date_mapping = field_mappings.get("event_date")
            if not date_mapping:
                continue
            date_value = coerce_value(
                field=EVENT_FIELD_MAP["event_date"], mapping=date_mapping,
                row=row, aliases=aliases, warnings=warnings, row_number=row_number,
            )
            if not date_value:
                continue

            # Parse optional fields
            time_value = None
            time_mapping = field_mappings.get("event_time")
            if time_mapping:
                time_value = coerce_value(
                    field=EVENT_FIELD_MAP["event_time"], mapping=time_mapping,
                    row=row, aliases=aliases, warnings=warnings, row_number=row_number,
                )

            notes_value = None
            notes_mapping = field_mappings.get("notes")
            if notes_mapping:
                notes_value = coerce_value(
                    field=EVENT_FIELD_MAP["notes"], mapping=notes_mapping,
                    row=row, aliases=aliases, warnings=warnings, row_number=row_number,
                )

            # Parse measure value if needed
            measure_value = None
            if needs_value:
                val_mapping = field_mappings.get("value")
                if val_mapping:
                    parser_name = VALUE_TYPE_TO_PARSER.get(value_type, "string")
                    value_field = FieldDefinition(
                        name="value", label="Value", required=True,
                        default_parser=parser_name, parsers=(parser_name, "string"),
                    )
                    measure_value = coerce_value(
                        field=value_field, mapping=val_mapping,
                        row=row, aliases=aliases, warnings=warnings, row_number=row_number,
                    )
                if measure_value is None:
                    continue

            # Upsert event
            date_str = str(date_value)
            event_key = (subject_id, date_str)
            existing = existing_events.get(event_key)

            if existing is not None:
                # UPDATE event
                event_id = existing["event_id"]
                update_parts: list[str] = []
                params: dict[str, Any] = {"_eid": event_id}
                if time_value is not None:
                    update_parts.append("event_time = :event_time")
                    params["event_time"] = str(time_value)
                if notes_value is not None and not (skip_blanks and str(notes_value).strip() == ""):
                    update_parts.append("notes = :notes")
                    params["notes"] = str(notes_value)
                if update_parts:
                    sql = f"UPDATE event SET {', '.join(update_parts)} WHERE event_id = :_eid"
                    session.execute(text(sql), params)
                    events_updated += 1
            else:
                # INSERT event
                cols = ["subject_id", "observation_type_id", "event_date"]
                vals: dict[str, Any] = {
                    "subject_id": subject_id,
                    "observation_type_id": obs_type_id,
                    "event_date": date_str,
                }
                if time_value is not None:
                    cols.append("event_time")
                    vals["event_time"] = str(time_value)
                if notes_value is not None:
                    cols.append("notes")
                    vals["notes"] = str(notes_value)

                col_list = ", ".join(cols)
                val_list = ", ".join(f":{c}" for c in cols)
                sql = f"INSERT INTO event ({col_list}) VALUES ({val_list}) RETURNING event_id"
                result = session.execute(text(sql), vals)
                event_id = result.scalar()
                events_inserted += 1

                existing_events[event_key] = {
                    "event_id": event_id,
                    "subject_id": subject_id,
                    "observation_type_id": obs_type_id,
                    "event_date": date_str,
                }

            # Upsert measure if needed
            if needs_value and measure_value is not None and measure_table and measure_col:
                obs_unit = obs_type.unit

                existing_measure = session.execute(text(
                    f"SELECT measure_id FROM {measure_table} "
                    f"WHERE subject_id = :sid AND observation_type_id = :oid AND event_id = :eid"
                ), {"sid": subject_id, "oid": obs_type_id, "eid": event_id}).scalar()

                if existing_measure is not None:
                    sql = f"UPDATE {measure_table} SET {measure_col} = :val WHERE measure_id = :_mid"
                    session.execute(text(sql), {"val": measure_value, "_mid": existing_measure})
                    measures_updated += 1
                else:
                    m_cols = ["subject_id", "observation_type_id", measure_col, "event_id"]
                    m_vals: dict[str, Any] = {
                        "subject_id": subject_id,
                        "observation_type_id": obs_type_id,
                        measure_col: measure_value,
                        "event_id": event_id,
                    }
                    if obs_unit:
                        m_cols.append("unit")
                        m_vals["unit"] = obs_unit

                    m_col_list = ", ".join(m_cols)
                    m_val_list = ", ".join(f":{c}" for c in m_cols)
                    sql = f"INSERT INTO {measure_table} ({m_col_list}) VALUES ({m_val_list})"
                    session.execute(text(sql), m_vals)
                    measures_inserted += 1

        if not config.dry_run:
            session.commit()
        else:
            session.rollback()

    return EventImportResult(
        eventsInserted=events_inserted, eventsUpdated=events_updated,
        measuresInserted=measures_inserted, measuresUpdated=measures_updated,
        subjectsMissing=subjects_missing,
    )
