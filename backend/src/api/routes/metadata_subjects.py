"""Subject metadata API routes."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Response, status

from metadata_db.session import SessionLocal as MetadataSessionLocal, engine as metadata_engine
from metadata_imports.subjects import get_existing_subject
from metadata_imports.subject_identifiers import (
    SubjectIdentifierDetailResponse,
    SubjectIdentifierDetail,
    UpsertSubjectIdentifierPayload,
    DeleteSubjectIdentifierPayload,
    get_subject_identifier_detail,
    upsert_subject_identifier,
    delete_subject_identifier,
)

from api.models.metadata import SubjectDetailResponse

router = APIRouter(prefix="/api/metadata", tags=["metadata-subjects"])


def _get_metadata_engine():
    """Get metadata database engine."""
    session_kwargs = getattr(MetadataSessionLocal, "kw", None)
    if isinstance(session_kwargs, dict):
        bound = session_kwargs.get("bind")
        if bound is not None:
            return bound
    return metadata_engine


@router.get(
    "/subjects/{subject_code}",
    response_model=SubjectDetailResponse,
)
def get_subject_detail(subject_code: str):
    """Get subject details by subject code."""
    with MetadataSessionLocal() as session:
        record = get_existing_subject(session, subject_code)

    if record is None:
        raise HTTPException(status_code=404, detail="Subject not found")

    return SubjectDetailResponse(
        subjectCode=record["subject_code"],
        patientName=record.get("patient_name"),
        patientBirthDate=record.get("patient_birth_date"),
        patientSex=record.get("patient_sex"),
        ethnicGroup=record.get("ethnic_group"),
        occupation=record.get("occupation"),
        additionalPatientHistory=record.get("additional_patient_history"),
        isActive=(bool(record.get("is_active")) if record.get("is_active") is not None else None),
    )


@router.get(
    "/subject-other-identifiers/{subject_code}",
    response_model=SubjectIdentifierDetailResponse,
)
def get_subject_identifiers_endpoint(subject_code: str):
    """Get all identifiers for a subject."""
    return get_subject_identifier_detail(subject_code, engine=_get_metadata_engine())


@router.post(
    "/subject-other-identifiers",
    response_model=SubjectIdentifierDetail,
)
def upsert_subject_identifier_endpoint(payload: UpsertSubjectIdentifierPayload):
    """Create or update a subject identifier."""
    try:
        return upsert_subject_identifier(payload, engine=_get_metadata_engine())
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc


@router.delete(
    "/subject-other-identifiers",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_subject_identifier_endpoint(payload: DeleteSubjectIdentifierPayload):
    """Delete a subject identifier."""
    try:
        delete_subject_identifier(payload, engine=_get_metadata_engine())
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)
