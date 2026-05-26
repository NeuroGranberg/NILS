"""REST endpoints for per-cohort classification keyword overrides."""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, status

from cohorts import keyword_override_service
from cohorts.models import (
    AxisViewDTO,
    BucketUpdatePayload,
    BucketViewDTO,
    CohortKeywordConfigDTO,
)

logger = logging.getLogger(__name__)


router = APIRouter(tags=["cohort-keywords"])


# -----------------------------------------------------------------------------
# Global catalog (no cohort context)
# -----------------------------------------------------------------------------


@router.get(
    "/api/classification-config/catalog",
    response_model=list[AxisViewDTO],
    summary="Global editable-keyword catalog (defaults only).",
)
def get_global_catalog() -> list[AxisViewDTO]:
    return keyword_override_service.get_global_catalog()


# -----------------------------------------------------------------------------
# Cohort-scoped configuration
# -----------------------------------------------------------------------------


@router.get(
    "/api/cohorts/{cohort_id}/classification-config",
    response_model=CohortKeywordConfigDTO,
    summary="Fetch a cohort's effective keyword configuration (defaults + deltas).",
)
def get_cohort_classification_config(cohort_id: int) -> CohortKeywordConfigDTO:
    try:
        return keyword_override_service.get_config(cohort_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.put(
    "/api/cohorts/{cohort_id}/classification-config/bucket",
    response_model=BucketViewDTO,
    summary="Upsert the delta for a single keyword bucket.",
)
def upsert_cohort_bucket(
    cohort_id: int, payload: BucketUpdatePayload
) -> BucketViewDTO:
    try:
        return keyword_override_service.update_bucket(
            cohort_id=cohort_id,
            axis=payload.axis,
            bucket_path=payload.bucket_path,
            added=payload.added,
            removed=payload.removed,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.delete(
    "/api/cohorts/{cohort_id}/classification-config/bucket",
    response_model=BucketViewDTO,
    summary="Reset a single bucket to its global defaults.",
)
def reset_cohort_bucket(
    cohort_id: int,
    axis: str = Query(..., description="Axis identifier, e.g. 'base'."),
    bucket_path: str = Query(
        ...,
        description="Dotted bucket path, e.g. 'bases.T1w.keywords'.",
    ),
) -> BucketViewDTO:
    try:
        return keyword_override_service.reset_bucket(
            cohort_id, axis, bucket_path
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.delete(
    "/api/cohorts/{cohort_id}/classification-config",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Reset all keyword overrides for a cohort.",
)
def reset_cohort_all(cohort_id: int) -> None:
    try:
        keyword_override_service.reset_all(cohort_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
