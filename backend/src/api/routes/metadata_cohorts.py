"""Metadata cohort API routes (separate from pipeline cohorts)."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Response, status

from metadata_db.session import SessionLocal as MetadataSessionLocal, engine as metadata_engine
from metadata_imports.cohorts import (
    get_existing_cohort_by_id,
    get_existing_cohort_by_name,
    upsert_metadata_cohort,
)
from metadata_imports.subject_cohorts import (
    SubjectCohortMetadataCohort,
    delete_subject_cohort_membership,
    get_subject_cohort_memberships,
    list_subject_cohort_metadata_cohorts,
)

from api.models.metadata import (
    CohortDetailResponse,
    UpsertMetadataCohortPayload,
    CreateMetadataCohortPayload,
    SubjectCohortMembershipResponse,
    SubjectCohortMembershipsResponse,
    DeleteSubjectCohortMembershipPayload,
)

router = APIRouter(prefix="/api/metadata", tags=["metadata-cohorts"])


def _get_metadata_engine():
    """Get metadata database engine."""
    session_kwargs = getattr(MetadataSessionLocal, "kw", None)
    if isinstance(session_kwargs, dict):
        bound = session_kwargs.get("bind")
        if bound is not None:
            return bound
    return metadata_engine


@router.get(
    "/cohorts",
    response_model=list[SubjectCohortMetadataCohort],
)
def list_metadata_cohorts_endpoint():
    """List all metadata cohorts."""
    return list_subject_cohort_metadata_cohorts(engine=_get_metadata_engine())


@router.get(
    "/cohorts/{cohort_id}",
    response_model=CohortDetailResponse,
)
def get_cohort_detail(cohort_id: int):
    """Get metadata cohort by ID."""
    record = get_existing_cohort_by_id(cohort_id, engine=_get_metadata_engine())
    if record is None:
        raise HTTPException(status_code=404, detail="Cohort not found")
    return CohortDetailResponse(
        cohortId=record["cohort_id"],
        name=record["name"],
        owner=record.get("owner"),
        path=record.get("path"),
        description=record.get("description"),
        isActive=(bool(record.get("is_active")) if record.get("is_active") is not None else None),
    )


@router.get(
    "/cohorts/by-name/{cohort_name}",
    response_model=CohortDetailResponse,
)
def get_cohort_detail_by_name(cohort_name: str):
    """Get metadata cohort by name."""
    record = get_existing_cohort_by_name(cohort_name, engine=_get_metadata_engine())
    if record is None:
        raise HTTPException(status_code=404, detail="Cohort not found")
    return CohortDetailResponse(
        cohortId=record["cohort_id"],
        name=record["name"],
        owner=record.get("owner"),
        path=record.get("path"),
        description=record.get("description"),
        isActive=(bool(record.get("is_active")) if record.get("is_active") is not None else None),
    )


@router.post(
    "/cohorts",
    response_model=CohortDetailResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_metadata_cohort_endpoint(payload: CreateMetadataCohortPayload):
    """Create a new metadata cohort."""
    try:
        record = upsert_metadata_cohort(
            engine=_get_metadata_engine(),
            name=payload.name,
            owner=payload.owner,
            path=payload.path,
            description=payload.description,
            is_active=payload.isActive,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return CohortDetailResponse(
        cohortId=record["cohort_id"],
        name=record["name"],
        owner=record.get("owner"),
        path=record.get("path"),
        description=record.get("description"),
        isActive=record.get("is_active"),
    )


@router.put(
    "/cohorts/{cohort_name}",
    response_model=CohortDetailResponse,
)
def upsert_metadata_cohort_endpoint(cohort_name: str, payload: UpsertMetadataCohortPayload):
    """Create or update a metadata cohort by name."""
    try:
        record = upsert_metadata_cohort(
            engine=_get_metadata_engine(),
            name=cohort_name,
            owner=payload.owner,
            path=payload.path,
            description=payload.description,
            is_active=payload.isActive,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return CohortDetailResponse(
        cohortId=record["cohort_id"],
        name=record["name"],
        owner=record.get("owner"),
        path=record.get("path"),
        description=record.get("description"),
        isActive=record.get("is_active"),
    )


@router.get(
    "/subject-cohorts/{subject_code}",
    response_model=SubjectCohortMembershipsResponse,
)
def get_subject_cohort_memberships_endpoint(subject_code: str):
    """Get all cohort memberships for a subject."""
    memberships, subject_exists = get_subject_cohort_memberships(subject_code, engine=_get_metadata_engine())
    if not subject_exists:
        raise HTTPException(status_code=404, detail="Subject not found")
    payload = [
        SubjectCohortMembershipResponse(
            subjectCode=membership.subjectCode,
            cohortId=membership.cohortId,
            cohortName=membership.cohortName,
            owner=membership.owner,
            path=membership.path,
            description=membership.description,
            notes=membership.notes,
            createdAt=membership.createdAt,
            updatedAt=membership.updatedAt,
        )
        for membership in memberships
    ]
    return SubjectCohortMembershipsResponse(subjectCode=subject_code, memberships=payload)


@router.delete(
    "/subject-cohorts",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_subject_cohort_membership_endpoint(payload: DeleteSubjectCohortMembershipPayload):
    """Delete a subject's cohort membership."""
    outcome = delete_subject_cohort_membership(
        engine=_get_metadata_engine(),
        subject_code=payload.subjectCode,
        cohort_id=payload.cohortId,
        cohort_name=payload.cohortName,
    )
    if outcome == "subject_not_found":
        raise HTTPException(status_code=404, detail="Subject not found")
    if outcome == "cohort_not_found":
        raise HTTPException(status_code=404, detail="Cohort not found")
    if outcome == "membership_not_found":
        raise HTTPException(status_code=404, detail="Subject is not a member of the cohort")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
