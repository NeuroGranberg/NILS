"""Pydantic schemas for metadata operations (subjects, cohorts, memberships)."""

from __future__ import annotations

from pydantic import BaseModel, model_validator


class SubjectDetailResponse(BaseModel):
    """Response for subject detail endpoint."""
    subjectCode: str
    patientName: str | None = None
    patientBirthDate: str | None = None
    patientSex: str | None = None
    ethnicGroup: str | None = None
    occupation: str | None = None
    additionalPatientHistory: str | None = None
    isActive: bool | None = None


class CohortDetailResponse(BaseModel):
    """Response for cohort detail endpoint."""
    cohortId: int
    name: str
    owner: str | None = None
    path: str | None = None
    description: str | None = None
    isActive: bool | None = None


class UpsertMetadataCohortPayload(BaseModel):
    """Payload for upserting a metadata cohort."""
    owner: str
    path: str
    description: str | None = None
    isActive: bool | None = True

    @model_validator(mode="after")
    def _trim_fields(self) -> "UpsertMetadataCohortPayload":
        self.owner = (self.owner or "").strip()
        self.path = (self.path or "").strip()
        if self.description is not None:
            description = self.description.strip()
            self.description = description or None
        return self


class CreateMetadataCohortPayload(UpsertMetadataCohortPayload):
    """Payload for creating a new metadata cohort."""
    name: str

    @model_validator(mode="after")
    def _trim_name(self) -> "CreateMetadataCohortPayload":
        self.name = (self.name or "").strip()
        if not self.name:
            raise ValueError("Cohort name is required")
        return self


class SubjectCohortMembershipResponse(BaseModel):
    """Response for a single subject-cohort membership."""
    subjectCode: str
    cohortId: int
    cohortName: str
    owner: str | None = None
    path: str | None = None
    description: str | None = None
    notes: str | None = None
    createdAt: str | None = None
    updatedAt: str | None = None


class SubjectCohortMembershipsResponse(BaseModel):
    """Response for subject's cohort memberships list."""
    subjectCode: str
    memberships: list[SubjectCohortMembershipResponse]


class DeleteSubjectCohortMembershipPayload(BaseModel):
    """Payload for deleting a subject-cohort membership."""
    subjectCode: str
    cohortId: int | None = None
    cohortName: str | None = None

    @model_validator(mode="after")
    def _validate_cohort(self) -> "DeleteSubjectCohortMembershipPayload":
        if self.cohortId is None and (not self.cohortName or not self.cohortName.strip()):
            raise ValueError("Provide cohortId or cohortName")
        if self.cohortName is not None:
            name = self.cohortName.strip()
            self.cohortName = name or None
        return self


# ID Type payloads
class CreateIdTypePayload(BaseModel):
    """Payload for creating a new ID type."""
    name: str
    description: str | None = None


class UpdateIdTypePayload(BaseModel):
    """Payload for updating an ID type."""
    name: str
    description: str | None = None
