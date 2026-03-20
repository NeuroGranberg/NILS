"""Metadata import API routes (subjects, cohorts, subject-cohorts, identifiers)."""
from __future__ import annotations

import csv
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException

from metadata_db.session import SessionLocal as MetadataSessionLocal, engine as metadata_engine

# Subject imports
from metadata_imports.subjects import (
    SubjectImportFieldsResponse,
    SubjectImportPayload,
    SubjectImportPreview,
    SubjectImportResult,
    apply_subject_import,
    build_fields_response,
    preview_subject_import,
)

# Cohort imports
from metadata_imports.cohorts import (
    CohortImportFieldsResponse,
    CohortImportPayload,
    CohortImportPreview,
    CohortImportResult,
    apply_cohort_import,
    build_cohort_fields_response,
    preview_cohort_import,
)

# Subject-cohort imports
from metadata_imports.subject_cohorts import (
    SubjectCohortImportFieldsResponse,
    SubjectCohortImportPayload,
    SubjectCohortImportPreview,
    SubjectCohortImportResult,
    apply_subject_cohort_import,
    build_subject_cohort_fields_response,
    preview_subject_cohort_import,
)

# Event imports
from metadata_imports.events import (
    EventImportFieldsResponse,
    EventImportPayload,
    EventImportPreview,
    EventImportResult,
    apply_event_import,
    build_event_fields_response,
    preview_event_import,
)

# Subject disease imports
from metadata_imports.subject_diseases import (
    SubjectDiseaseFieldsResponse,
    SubjectDiseaseImportPayload,
    SubjectDiseaseImportPreview,
    SubjectDiseaseImportResult,
    CohortAssignPayload,
    CohortAssignResult,
    apply_subject_disease_import,
    build_subject_disease_fields_response,
    preview_subject_disease_import,
    apply_cohort_assign,
)

# Subject disease type imports
from metadata_imports.subject_disease_types import (
    SDTFieldsResponse,
    SDTImportPayload,
    SDTImportPreview,
    SDTImportResult,
    SDTManualPayload,
    SDTManualResult,
    SDTDetailResponse,
    apply_sdt_import,
    build_sdt_fields_response,
    get_sdt_by_id,
    preview_sdt_import,
    upsert_sdt_manual,
)

# Disease type imports
from metadata_imports.disease_types import (
    DiseaseTypeDetail,
    DiseaseTypeFieldsResponse,
    DiseaseTypeUpsertPayload,
    DiseaseTypeUpsertResult,
    build_disease_type_fields_response,
    get_disease_type_by_id,
    upsert_disease_type,
)

# Disease imports
from metadata_imports.diseases import (
    DiseaseDetailResponse,
    DiseaseFieldsResponse,
    DiseaseImportPayload,
    DiseaseImportPreview,
    DiseaseImportResult,
    apply_disease_import,
    build_fields_response as build_disease_fields_response,
    get_disease_by_id,
    preview_disease_import,
)

# Observation type imports
from metadata_imports.observation_types import (
    ObservationTypeDetailResponse,
    ObservationTypeFieldsResponse,
    ObservationTypeImportPayload,
    ObservationTypeImportPreview,
    ObservationTypeImportResult,
    apply_observation_type_import,
    build_fields_response as build_observation_type_fields_response,
    get_observation_type_by_id,
    preview_observation_type_import,
)

# Subject identifier imports
from metadata_imports.subject_identifiers import (
    SubjectIdentifierImportFieldsResponse,
    SubjectIdentifierImportPayload,
    SubjectIdentifierImportPreview,
    SubjectIdentifierImportResult,
    apply_subject_identifier_import,
    build_subject_identifier_fields_response,
    preview_subject_identifier_import,
)

# ID types
from metadata_imports.id_types import get_id_type_models

# CSV utilities
from api.utils.csv import csv_file_path, sanitize_csv_token, read_csv_metadata, write_csv_metadata

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/metadata/imports", tags=["imports"])


def _get_metadata_engine():
    """Get metadata database engine."""
    session_kwargs = getattr(MetadataSessionLocal, "kw", None)
    if isinstance(session_kwargs, dict):
        bound = session_kwargs.get("bind")
        if bound is not None:
            return bound
    return metadata_engine


def _extract_csv_columns(path: Path) -> list[str]:
    """Extract column headers from CSV file."""
    if not path.exists():
        raise HTTPException(status_code=404, detail="CSV file not found")
    with path.open("r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return []
    return [column.strip() for column in header if column is not None]


def _resolve_import_csv(
    payload: SubjectImportPayload | CohortImportPayload | SubjectCohortImportPayload | SubjectIdentifierImportPayload | ObservationTypeImportPayload | EventImportPayload | DiseaseImportPayload | SubjectDiseaseImportPayload | SDTImportPayload,
) -> tuple[Path, list[str]]:
    """Resolve CSV file path and columns from payload."""
    if payload.file_token:
        token = sanitize_csv_token(payload.file_token)
        csv_path = csv_file_path(token)
        if not csv_path.exists():
            raise HTTPException(status_code=404, detail="Uploaded CSV not found")
        metadata = read_csv_metadata(token) or {}
        columns = metadata.get("columns") or _extract_csv_columns(csv_path)
        filename = metadata.get("filename") or csv_path.name
        if not metadata.get("columns"):
            write_csv_metadata(token, {"filename": filename, "columns": columns})
        return csv_path, columns
    if payload.file_path:
        csv_path = Path(payload.file_path).expanduser().resolve()
        if not csv_path.exists():
            raise HTTPException(status_code=404, detail="CSV file not found")
        columns = _extract_csv_columns(csv_path)
        return csv_path, columns
    raise HTTPException(status_code=400, detail="CSV import requires fileToken or filePath")


def _validate_mapping_columns(columns: list[str], payload: SubjectImportPayload) -> None:
    """Validate subject import mapping columns exist in CSV."""
    available = {column: column for column in columns}
    missing_subject_columns: list[str] = []
    for mapping in payload.subject_fields.values():
        if mapping.column and mapping.column not in available:
            missing_subject_columns.append(mapping.column)
    if missing_subject_columns:
        raise HTTPException(
            status_code=400,
            detail=f"Subject field columns not found in CSV: {', '.join(sorted(set(missing_subject_columns)))}",
        )

    if payload.cohort and payload.cohort.enabled:
        for field_name in ("name", "owner", "path", "description", "is_active"):
            mapping = getattr(payload.cohort, field_name, None)
            column_value = getattr(mapping, "column", None)
            if column_value and column_value not in available:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cohort field column '{column_value}' not found in CSV",
                )

    for identifier in payload.identifiers or []:
        if identifier.value.column and identifier.value.column not in available:
            raise HTTPException(
                status_code=400,
                detail=f"Identifier column '{identifier.value.column}' not found in CSV",
            )


def _validate_cohort_mapping_columns(columns: list[str], payload: CohortImportPayload) -> None:
    """Validate cohort import mapping columns exist in CSV."""
    available = {column: column for column in columns}
    missing_columns: list[str] = []
    for mapping in payload.cohort_fields.values():
        if mapping.column and mapping.column not in available:
            missing_columns.append(mapping.column)
    if missing_columns:
        raise HTTPException(
            status_code=400,
            detail=f"Cohort field columns not found in CSV: {', '.join(sorted(set(missing_columns)))}",
        )


def _validate_subject_cohort_mapping_columns(columns: list[str], payload: SubjectCohortImportPayload) -> None:
    """Validate subject-cohort import mapping columns exist in CSV."""
    available = {column: column for column in columns}
    subject_column = payload.subject_field.column
    if subject_column and subject_column not in available:
        raise HTTPException(
            status_code=400,
            detail=f"Subject field column '{subject_column}' not found in CSV",
        )


def _validate_subject_identifier_columns(columns: list[str], payload: SubjectIdentifierImportPayload) -> None:
    """Validate subject identifier import columns exist in CSV."""
    available = set(columns)
    subject_column = payload.subject_field.column
    if subject_column and subject_column not in available:
        raise HTTPException(status_code=400, detail=f"Subject column '{subject_column}' not found in CSV")
    identifier_column = payload.identifier_field.column
    if identifier_column and identifier_column not in available:
        raise HTTPException(status_code=400, detail=f"Identifier column '{identifier_column}' not found in CSV")


# =============================================================================
# Subject Import Endpoints
# =============================================================================

@router.get(
    "/subjects/fields",
    response_model=SubjectImportFieldsResponse,
)
def get_subject_import_fields():
    """Get subject import field definitions and available ID types."""
    id_types = get_id_type_models(engine=_get_metadata_engine())
    return build_fields_response(id_types)


@router.post(
    "/subjects/preview",
    response_model=SubjectImportPreview,
)
def preview_subject_import_endpoint(payload: SubjectImportPayload):
    """Preview subject import from CSV."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_mapping_columns(columns, payload)
    id_types = get_id_type_models(engine=_get_metadata_engine())
    preview = preview_subject_import(
        engine=_get_metadata_engine(),
        path=csv_path,
        config=payload,
        id_types=id_types,
    )
    return preview


@router.post(
    "/subjects/apply",
    response_model=SubjectImportResult,
)
def apply_subject_import_endpoint(payload: SubjectImportPayload):
    """Apply subject import from CSV."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_mapping_columns(columns, payload)
    id_types = get_id_type_models(engine=_get_metadata_engine())
    try:
        result = apply_subject_import(
            engine=_get_metadata_engine(),
            path=csv_path,
            config=payload,
            id_types=id_types,
        )
    except Exception as exc:
        logger.exception("Subject import failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result


# =============================================================================
# Cohort Import Endpoints
# =============================================================================

@router.get(
    "/cohorts/fields",
    response_model=CohortImportFieldsResponse,
)
def get_cohort_import_fields():
    """Get cohort import field definitions."""
    return build_cohort_fields_response()


@router.post(
    "/cohorts/preview",
    response_model=CohortImportPreview,
)
def preview_cohort_import_endpoint(payload: CohortImportPayload):
    """Preview cohort import from CSV."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_cohort_mapping_columns(columns, payload)
    preview = preview_cohort_import(
        engine=_get_metadata_engine(),
        path=csv_path,
        config=payload,
    )
    return preview


@router.post(
    "/cohorts/apply",
    response_model=CohortImportResult,
)
def apply_cohort_import_endpoint(payload: CohortImportPayload):
    """Apply cohort import from CSV."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_cohort_mapping_columns(columns, payload)
    try:
        result = apply_cohort_import(
            engine=_get_metadata_engine(),
            path=csv_path,
            config=payload,
        )
    except Exception as exc:
        logger.exception("Cohort import failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result


# =============================================================================
# Subject-Cohort Import Endpoints
# =============================================================================

@router.get(
    "/subject-cohorts/fields",
    response_model=SubjectCohortImportFieldsResponse,
)
def get_subject_cohort_import_fields():
    """Get subject-cohort import field definitions."""
    return build_subject_cohort_fields_response()


@router.post(
    "/subject-cohorts/preview",
    response_model=SubjectCohortImportPreview,
)
def preview_subject_cohort_import_endpoint(payload: SubjectCohortImportPayload):
    """Preview subject-cohort import from CSV."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_subject_cohort_mapping_columns(columns, payload)
    preview = preview_subject_cohort_import(
        engine=_get_metadata_engine(),
        path=csv_path,
        config=payload,
    )
    return preview


@router.post(
    "/subject-cohorts/apply",
    response_model=SubjectCohortImportResult,
)
def apply_subject_cohort_import_endpoint(payload: SubjectCohortImportPayload):
    """Apply subject-cohort import from CSV."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_subject_cohort_mapping_columns(columns, payload)
    try:
        result = apply_subject_cohort_import(
            engine=_get_metadata_engine(),
            path=csv_path,
            config=payload,
        )
    except Exception as exc:
        logger.exception("Subject-cohort import failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result


# =============================================================================
# Subject Identifier Import Endpoints
# =============================================================================

@router.get(
    "/subject-other-identifiers/fields",
    response_model=SubjectIdentifierImportFieldsResponse,
)
def get_subject_identifier_import_fields():
    """Get subject identifier import field definitions."""
    return build_subject_identifier_fields_response(engine=_get_metadata_engine())


@router.post(
    "/subject-other-identifiers/preview",
    response_model=SubjectIdentifierImportPreview,
)
def preview_subject_identifier_import_endpoint(payload: SubjectIdentifierImportPayload):
    """Preview subject identifier import from CSV."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_subject_identifier_columns(columns, payload)
    try:
        preview = preview_subject_identifier_import(
            engine=_get_metadata_engine(),
            path=csv_path,
            config=payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return preview


@router.post(
    "/subject-other-identifiers/apply",
    response_model=SubjectIdentifierImportResult,
)
def apply_subject_identifier_import_endpoint(payload: SubjectIdentifierImportPayload):
    """Apply subject identifier import from CSV."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_subject_identifier_columns(columns, payload)
    try:
        result = apply_subject_identifier_import(
            engine=_get_metadata_engine(),
            path=csv_path,
            config=payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Subject identifier import failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result


# =============================================================================
# Observation Type Import Endpoints
# =============================================================================

def _validate_observation_type_columns(columns: list[str], payload: ObservationTypeImportPayload) -> None:
    """Validate observation type import mapping columns exist in CSV."""
    available = set(columns)
    missing = [m.column for m in payload.fields.values() if m.column and m.column not in available]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Observation type field columns not found in CSV: {', '.join(sorted(set(missing)))}",
        )


@router.get(
    "/observation-types/fields",
    response_model=ObservationTypeFieldsResponse,
)
def get_observation_type_import_fields():
    """Get observation type import field definitions."""
    return build_observation_type_fields_response()


@router.get(
    "/observation-types/{observation_type_id}",
    response_model=ObservationTypeDetailResponse,
)
def get_observation_type_detail(observation_type_id: int):
    """Get a single observation type by ID."""
    record = get_observation_type_by_id(_get_metadata_engine(), observation_type_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Observation type not found")
    return ObservationTypeDetailResponse(
        observationTypeId=record["observation_type_id"],
        category=record["category"],
        name=record["name"],
        description=record.get("description"),
        unit=record.get("unit"),
        valueType=record.get("value_type"),
        minValue=record.get("min_value"),
        maxValue=record.get("max_value"),
        isActive=record.get("is_active", True),
        isPrimary=record.get("is_primary", False),
    )


@router.post(
    "/observation-types/preview",
    response_model=ObservationTypeImportPreview,
)
def preview_observation_type_import_endpoint(payload: ObservationTypeImportPayload):
    """Preview observation type import from CSV."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_observation_type_columns(columns, payload)
    preview = preview_observation_type_import(
        engine=_get_metadata_engine(),
        path=csv_path,
        config=payload,
    )
    return preview


@router.post(
    "/observation-types/apply",
    response_model=ObservationTypeImportResult,
)
def apply_observation_type_import_endpoint(payload: ObservationTypeImportPayload):
    """Apply observation type import from CSV."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_observation_type_columns(columns, payload)
    try:
        result = apply_observation_type_import(
            engine=_get_metadata_engine(),
            path=csv_path,
            config=payload,
        )
    except Exception as exc:
        logger.exception("Observation type import failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result


# =============================================================================
# Event Import Endpoints
# =============================================================================

def _validate_event_columns(columns: list[str], payload: EventImportPayload) -> None:
    available = set(columns)
    missing = [m.column for m in payload.fields.values() if m.column and m.column not in available]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Event field columns not found in CSV: {', '.join(sorted(set(missing)))}",
        )


@router.get("/events/fields", response_model=EventImportFieldsResponse)
def get_event_import_fields():
    """Get event import field definitions, observation types, and id types."""
    return build_event_fields_response(_get_metadata_engine())


@router.post("/events/preview", response_model=EventImportPreview)
def preview_event_import_endpoint(payload: EventImportPayload):
    """Preview event import from CSV."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_event_columns(columns, payload)
    try:
        preview = preview_event_import(
            engine=_get_metadata_engine(),
            path=csv_path,
            config=payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return preview


@router.post("/events/apply", response_model=EventImportResult)
def apply_event_import_endpoint(payload: EventImportPayload):
    """Apply event import from CSV (upsert events + measures)."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_event_columns(columns, payload)
    try:
        result = apply_event_import(
            engine=_get_metadata_engine(),
            path=csv_path,
            config=payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Event import failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result


# =============================================================================
# Disease Import Endpoints
# =============================================================================

def _validate_disease_columns(columns: list[str], payload: DiseaseImportPayload) -> None:
    available = set(columns)
    missing = [m.column for m in payload.fields.values() if m.column and m.column not in available]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Disease field columns not found in CSV: {', '.join(sorted(set(missing)))}",
        )


@router.get("/diseases/fields", response_model=DiseaseFieldsResponse)
def get_disease_import_fields():
    """Get disease import field definitions."""
    return build_disease_fields_response()


@router.get("/diseases/{disease_id}", response_model=DiseaseDetailResponse)
def get_disease_detail(disease_id: int):
    """Get a single disease by ID for manual lookup."""
    record = get_disease_by_id(_get_metadata_engine(), disease_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Disease not found")
    return DiseaseDetailResponse(
        diseaseId=record["disease_id"],
        diseaseName=record["disease_name"],
        diseaseCode=record.get("disease_code"),
        description=record.get("description"),
    )


@router.post("/diseases/preview", response_model=DiseaseImportPreview)
def preview_disease_import_endpoint(payload: DiseaseImportPayload):
    """Preview disease import from CSV."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_disease_columns(columns, payload)
    preview = preview_disease_import(
        engine=_get_metadata_engine(),
        path=csv_path,
        config=payload,
    )
    return preview


@router.post("/diseases/apply", response_model=DiseaseImportResult)
def apply_disease_import_endpoint(payload: DiseaseImportPayload):
    """Apply disease import from CSV."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_disease_columns(columns, payload)
    try:
        result = apply_disease_import(
            engine=_get_metadata_engine(),
            path=csv_path,
            config=payload,
        )
    except Exception as exc:
        logger.exception("Disease import failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result


# =============================================================================
# Disease Type Endpoints
# =============================================================================

@router.get("/disease-types/fields", response_model=DiseaseTypeFieldsResponse)
def get_disease_type_fields():
    """Get disease type field config including list of diseases for dropdown."""
    return build_disease_type_fields_response(_get_metadata_engine())


@router.get("/disease-types/{disease_type_id}", response_model=DiseaseTypeDetail)
def get_disease_type_detail(disease_type_id: int):
    """Get a single disease type by ID for manual lookup."""
    record = get_disease_type_by_id(_get_metadata_engine(), disease_type_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Disease type not found")
    return record


@router.post("/disease-types/upsert", response_model=DiseaseTypeUpsertResult)
def upsert_disease_type_endpoint(payload: DiseaseTypeUpsertPayload):
    """Create or update a disease type."""
    try:
        return upsert_disease_type(_get_metadata_engine(), payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Disease type upsert failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# =============================================================================
# Subject Disease Endpoints
# =============================================================================

def _validate_subject_disease_columns(columns: list[str], payload: SubjectDiseaseImportPayload) -> None:
    available = set(columns)
    missing = [m.column for m in payload.fields.values() if m.column and m.column not in available]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Subject disease field columns not found in CSV: {', '.join(sorted(set(missing)))}",
        )


@router.get("/subject-diseases/fields", response_model=SubjectDiseaseFieldsResponse)
def get_subject_disease_fields():
    """Get subject disease import config including diseases, cohorts, and id types."""
    return build_subject_disease_fields_response(_get_metadata_engine())


@router.post("/subject-diseases/preview", response_model=SubjectDiseaseImportPreview)
def preview_subject_disease_import_endpoint(payload: SubjectDiseaseImportPayload):
    """Preview subject disease CSV import."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_subject_disease_columns(columns, payload)
    try:
        return preview_subject_disease_import(
            engine=_get_metadata_engine(), path=csv_path, config=payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/subject-diseases/apply", response_model=SubjectDiseaseImportResult)
def apply_subject_disease_import_endpoint(payload: SubjectDiseaseImportPayload):
    """Apply subject disease CSV import (upsert + auto-link events)."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_subject_disease_columns(columns, payload)
    try:
        return apply_subject_disease_import(
            engine=_get_metadata_engine(), path=csv_path, config=payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Subject disease import failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/subject-diseases/cohort-assign", response_model=CohortAssignResult)
def cohort_assign_endpoint(payload: CohortAssignPayload):
    """Assign all subjects of a cohort to a disease."""
    try:
        return apply_cohort_assign(_get_metadata_engine(), payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Cohort disease assign failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# =============================================================================
# Subject Disease Type Endpoints
# =============================================================================

def _validate_sdt_columns(columns: list[str], payload: SDTImportPayload) -> None:
    available = set(columns)
    missing = [m.column for m in payload.fields.values() if m.column and m.column not in available]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"SDT field columns not found in CSV: {', '.join(sorted(set(missing)))}",
        )


@router.get("/subject-disease-types/fields", response_model=SDTFieldsResponse)
def get_sdt_fields():
    """Get subject disease type import config (diseases with subtypes, disease types, id types)."""
    return build_sdt_fields_response(_get_metadata_engine())


@router.get("/subject-disease-types/{sdt_id}", response_model=SDTDetailResponse)
def get_sdt_detail(sdt_id: int):
    """Get a single subject_disease_type record by ID."""
    result = get_sdt_by_id(_get_metadata_engine(), sdt_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Subject disease type {sdt_id} not found")
    return result


@router.post("/subject-disease-types/preview", response_model=SDTImportPreview)
def preview_sdt_import_endpoint(payload: SDTImportPayload):
    """Preview subject disease type CSV import with fuzzy type matching."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_sdt_columns(columns, payload)
    try:
        return preview_sdt_import(engine=_get_metadata_engine(), path=csv_path, config=payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/subject-disease-types/apply", response_model=SDTImportResult)
def apply_sdt_import_endpoint(payload: SDTImportPayload):
    """Apply subject disease type CSV import."""
    csv_path, columns = _resolve_import_csv(payload)
    _validate_sdt_columns(columns, payload)
    try:
        return apply_sdt_import(engine=_get_metadata_engine(), path=csv_path, config=payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Subject disease type import failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/subject-disease-types/upsert", response_model=SDTManualResult)
def upsert_sdt_endpoint(payload: SDTManualPayload):
    """Manual upsert of a single subject disease type record."""
    try:
        return upsert_sdt_manual(_get_metadata_engine(), payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Subject disease type upsert failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
