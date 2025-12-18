"""Pydantic schemas for cohort management.

Note: Metadata-related cohort models (SubjectDetailResponse, CohortDetailResponse, etc.)
have been moved to api/models/metadata.py. This file re-exports them for backward compatibility.
"""

from __future__ import annotations

# Re-export metadata models for backward compatibility
from api.models.metadata import (
    SubjectDetailResponse,
    CohortDetailResponse,
    UpsertMetadataCohortPayload,
    CreateMetadataCohortPayload,
    SubjectCohortMembershipResponse,
    SubjectCohortMembershipsResponse,
    DeleteSubjectCohortMembershipPayload,
)

__all__ = [
    "SubjectDetailResponse",
    "CohortDetailResponse",
    "UpsertMetadataCohortPayload",
    "CreateMetadataCohortPayload",
    "SubjectCohortMembershipResponse",
    "SubjectCohortMembershipsResponse",
    "DeleteSubjectCohortMembershipPayload",
]
