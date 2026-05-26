"""API routes for the cohort-wide Body Part QC.

Mirrors the shape of ``cohort_main_qc.py``. M1 ships:

- ``GET /api/cohorts/{cohort_id}/body-part-qc``
- Stub endpoints for the rest of the surface (return HTTP 501 until the
  matching milestone lands).

See spec at
``/home/nima/.factory/specs/2026-04-26-body-part-qc-cohort-module-v1.md``.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from cohorts.models import (
    BodyPartCandidateDTO,
    BodyPartCategoriesPayload,
    BodyPartChangesPageDTO,
    BodyPartCommitPayload,
    BodyPartDestagePayload,
    BodyPartOverridePayload,
    BodyPartSamplesPayload,
    BodyPartSeedPayload,
    BodyPartSessionPickDTO,
    BodyPartSessionResetPayload,
    BodyPartStateDTO,
    BodyPartTrainingSampleDTO,
)
from qc.body_part.service import CohortBodyPartQCService

logger = logging.getLogger(__name__)

router = APIRouter(tags=["cohort-body-part-qc"])


def _service() -> CohortBodyPartQCService:
    return CohortBodyPartQCService()


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


@router.get(
    "/api/cohorts/{cohort_id}/body-part-qc",
    response_model=BodyPartStateDTO,
    summary="Get the current cohort Body Part QC state.",
)
def get_state(cohort_id: int) -> BodyPartStateDTO:
    try:
        return _service().get_state(cohort_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


# ---------------------------------------------------------------------------
# Setup: categories + sample seeding + sample edits
# ---------------------------------------------------------------------------


@router.put(
    "/api/cohorts/{cohort_id}/body-part-qc/categories",
    response_model=BodyPartStateDTO,
    summary="Replace the Body Part QC categories for this cohort.",
)
def update_categories(
    cohort_id: int, payload: BodyPartCategoriesPayload
) -> BodyPartStateDTO:
    try:
        return _service().update_categories(cohort_id, payload.categories)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post(
    "/api/cohorts/{cohort_id}/body-part-qc/seed",
    response_model=list[BodyPartCandidateDTO],
    summary="Generate zero-shot sample candidates for one category.",
)
def seed_candidates(
    cohort_id: int, payload: BodyPartSeedPayload
) -> list[BodyPartCandidateDTO]:
    try:
        return _service().seed(
            cohort_id=cohort_id,
            category=payload.category,
            n_target=payload.n_target,
            n_keyword_prior=payload.n_keyword_prior,
            n_zero_shot=payload.n_zero_shot,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))


@router.post(
    "/api/cohorts/{cohort_id}/body-part-qc/samples",
    response_model=BodyPartStateDTO,
    summary="Apply approve/remove/replace/move ops to the labeled training set.",
)
def update_samples(
    cohort_id: int, payload: BodyPartSamplesPayload
) -> BodyPartStateDTO:
    try:
        return _service().update_samples(cohort_id, payload.ops)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))


@router.get(
    "/api/cohorts/{cohort_id}/body-part-qc/samples",
    response_model=list[BodyPartTrainingSampleDTO],
    summary="Hydrated list of approved training samples (newest first).",
)
def list_samples(
    cohort_id: int,
    label: Optional[str] = Query(default=None),
) -> list[BodyPartTrainingSampleDTO]:
    try:
        return _service().list_samples(cohort_id, label=label)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


# ---------------------------------------------------------------------------
# Apply + Restore-Previous
# ---------------------------------------------------------------------------


@router.post(
    "/api/cohorts/{cohort_id}/body-part-qc/apply",
    response_model=BodyPartStateDTO,
    summary="Run cohort-wide inference, capture diff vs prior body_part, "
            "rotate snapshot, and write tokens.",
)
def apply(cohort_id: int) -> BodyPartStateDTO:
    try:
        return _service().apply(cohort_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))
    except Exception as exc:  # pragma: no cover
        logger.exception(
            "Body Part QC apply failed for cohort_id=%s", cohort_id
        )
        raise HTTPException(status_code=500, detail=str(exc))


@router.post(
    "/api/cohorts/{cohort_id}/body-part-qc/restore-previous",
    response_model=BodyPartStateDTO,
    summary="Restore the previous Body Part QC snapshot. Single-level undo.",
)
def restore_previous(cohort_id: int) -> BodyPartStateDTO:
    try:
        return _service().restore_previous(cohort_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))


class BodyPartResetResponse(BaseModel):
    tokens_removed: int
    had_state: bool


@router.post(
    "/api/cohorts/{cohort_id}/body-part-qc/reset",
    response_model=BodyPartResetResponse,
    summary="Wipe Body Part QC state. Embeddings and classifier preserved.",
)
def reset(cohort_id: int) -> BodyPartResetResponse:
    try:
        result = _service().reset(cohort_id)
        return BodyPartResetResponse(**result)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


class SelectModelPayload(BaseModel):
    model_id: Optional[int] = None


@router.post(
    "/api/cohorts/{cohort_id}/body-part-qc/select-model",
    response_model=BodyPartStateDTO,
    summary="Select a global model for this cohort's Apply. "
            "Pass model_id=null to clear.",
)
def select_model(
    cohort_id: int, payload: SelectModelPayload,
) -> BodyPartStateDTO:
    try:
        return _service().select_model(cohort_id, payload.model_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post(
    "/api/cohorts/{cohort_id}/body-part-qc/commit",
    response_model=BodyPartStateDTO,
    summary="Commit staged Body Part QC picks to the metadata DB. "
            "Accepts optional stack_ids or min_confidence for partial commit.",
)
def commit(
    cohort_id: int,
    payload: BodyPartCommitPayload = BodyPartCommitPayload(),
) -> BodyPartStateDTO:
    """Write picks to ``series_classification_cache.body_part``.

    - Empty body → full commit (back-compat).
    - ``stack_ids`` → partial commit of those stacks only.
    - ``min_confidence`` → partial commit of all picks ≥ threshold.
    """
    try:
        return _service().commit(
            cohort_id,
            stack_ids=payload.stack_ids,
            min_confidence=payload.min_confidence,
            from_label=payload.from_label,
            to_label=payload.to_label,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:  # pragma: no cover
        logger.exception(
            "Body Part QC commit failed for cohort_id=%s", cohort_id,
        )
        raise HTTPException(status_code=500, detail=str(exc))


@router.post(
    "/api/cohorts/{cohort_id}/body-part-qc/destage",
    response_model=BodyPartStateDTO,
    summary="Remove stacks from staging without writing to the metadata DB.",
)
def destage(
    cohort_id: int, payload: BodyPartDestagePayload,
) -> BodyPartStateDTO:
    try:
        return _service().destage(cohort_id, payload.stack_ids)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # pragma: no cover
        logger.exception(
            "Body Part QC destage failed for cohort_id=%s", cohort_id,
        )
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Diff / Changes pane
# ---------------------------------------------------------------------------


@router.get(
    "/api/cohorts/{cohort_id}/body-part-qc/changes",
    response_model=BodyPartChangesPageDTO,
    summary="Paginated list of stacks whose body_part changed in the latest run.",
)
def get_changes(
    cohort_id: int,
    from_label: Optional[str] = Query(default=None, alias="from"),
    to_label: Optional[str] = Query(default=None, alias="to"),
    prior_source: Optional[str] = Query(default=None),
    min_confidence: Optional[float] = Query(default=None, ge=0, le=1),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=500),
) -> BodyPartChangesPageDTO:
    try:
        return _service().get_changes(
            cohort_id,
            from_label=from_label,
            to_label=to_label,
            prior_source=prior_source,
            min_confidence=min_confidence,
            offset=offset,
            limit=limit,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))


# ---------------------------------------------------------------------------
# Per-session: override + reset
# Sessions are identified by (subject_id, session_date) in the payload.
# ---------------------------------------------------------------------------


@router.post(
    "/api/cohorts/{cohort_id}/body-part-qc/session-override",
    response_model=BodyPartSessionPickDTO,
    summary="Manually override the predicted body_part for one stack within a "
            "session. Session = (subject_id, session_date).",
)
def override_stack(
    cohort_id: int, payload: BodyPartOverridePayload,
) -> BodyPartSessionPickDTO:
    try:
        return _service().override_stack(
            cohort_id=cohort_id,
            subject_id=payload.subject_id,
            session_date=payload.session_date,
            stack_id=payload.stack_id,
            label=payload.label,
            note=payload.note,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post(
    "/api/cohorts/{cohort_id}/body-part-qc/session-reset",
    response_model=BodyPartSessionPickDTO,
    summary="Re-run Body Part QC inference for one session, clearing overrides.",
)
def reset_session(
    cohort_id: int, payload: BodyPartSessionResetPayload,
) -> BodyPartSessionPickDTO:
    try:
        return _service().reset_session(
            cohort_id=cohort_id,
            subject_id=payload.subject_id,
            session_date=payload.session_date,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))
