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
    payload: SubjectImportPayload | CohortImportPayload | SubjectCohortImportPayload | SubjectIdentifierImportPayload,
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
