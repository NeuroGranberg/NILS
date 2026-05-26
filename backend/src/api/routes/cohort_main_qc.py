"""API routes for the Cohort-wide Main Acquisition QC.

See spec at
``/home/nima/.factory/specs/2026-04-25-cohort-main-acquisition-qc-auto-pick-heatmap-rev-3-2d-3d-dixon-family-aware.md``.

Endpoints:
- GET    /api/cohorts/{cohort_id}/main-qc
- POST   /api/cohorts/{cohort_id}/main-qc/apply
- POST   /api/cohorts/{cohort_id}/main-qc/restore-previous
- POST   /api/cohorts/{cohort_id}/main-qc/session-pick
- POST   /api/cohorts/{cohort_id}/main-qc/session-reset
- POST   /api/cohorts/{cohort_id}/main-qc/session-acknowledge

Sessions are identified by ``(subject_id, session_date)`` in the request
body, since one calendar visit can span multiple StudyInstanceUIDs.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, status

from cohorts.models import (
    MainQCSessionAcknowledgePayload,
    MainQCSessionPickDTO,
    MainQCSessionPickPayload,
    MainQCSessionResetPayload,
    MainQCStateDTO,
)
from qc.cohort_main_qc_service import CohortMainQCService


logger = logging.getLogger(__name__)


router = APIRouter(tags=["cohort-main-qc"])


def _service() -> CohortMainQCService:
    return CohortMainQCService()


@router.get(
    "/api/cohorts/{cohort_id}/main-qc",
    response_model=MainQCStateDTO,
    summary="Get the current cohort Main QC state (auto-pick heatmap data).",
)
def get_state(cohort_id: int) -> MainQCStateDTO:
    try:
        return _service().get_state(cohort_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post(
    "/api/cohorts/{cohort_id}/main-qc/apply",
    response_model=MainQCStateDTO,
    summary="Run cohort-wide auto-pick. Wipes existing main_acquisition tokens "
            "for this cohort and replaces them with algorithm picks. Rotates "
            "the previous snapshot.",
)
def apply(cohort_id: int) -> MainQCStateDTO:
    try:
        return _service().apply(cohort_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:  # pragma: no cover
        logger.exception("Cohort Main QC apply failed for cohort_id=%s", cohort_id)
        raise HTTPException(status_code=500, detail=str(exc))


@router.post(
    "/api/cohorts/{cohort_id}/main-qc/restore-previous",
    response_model=MainQCStateDTO,
    summary="Restore the previous Main QC snapshot. Single-level undo.",
)
def restore_previous(cohort_id: int) -> MainQCStateDTO:
    try:
        return _service().restore_previous(cohort_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post(
    "/api/cohorts/{cohort_id}/main-qc/session-pick",
    response_model=MainQCSessionPickDTO,
    summary="Manually override the auto-pick for a session+axis. "
            "Session is identified by (subject_id, session_date).",
)
def update_session_pick(
    cohort_id: int,
    payload: MainQCSessionPickPayload,
) -> MainQCSessionPickDTO:
    try:
        return _service().update_session_pick(
            cohort_id=cohort_id,
            subject_id=payload.subject_id,
            session_date=payload.session_date,
            axis=payload.axis,
            stack_ids=payload.stack_ids,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post(
    "/api/cohorts/{cohort_id}/main-qc/session-reset",
    response_model=MainQCSessionPickDTO,
    summary="Reset a session+axis pick back to the algorithm's choice "
            "(re-run only this session).",
)
def reset_session(
    cohort_id: int,
    payload: MainQCSessionResetPayload,
) -> MainQCSessionPickDTO:
    try:
        return _service().reset_session_to_auto(
            cohort_id=cohort_id,
            subject_id=payload.subject_id,
            session_date=payload.session_date,
            axis=payload.axis,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post(
    "/api/cohorts/{cohort_id}/main-qc/session-acknowledge",
    response_model=MainQCSessionPickDTO,
    summary="Acknowledge the auto-pick for a session+axis ('In NILS we trust'). "
            "Records a durable ack that survives cohort Apply.",
)
def acknowledge_session(
    cohort_id: int,
    payload: MainQCSessionAcknowledgePayload,
) -> MainQCSessionPickDTO:
    try:
        return _service().acknowledge_session_pick(
            cohort_id=cohort_id,
            subject_id=payload.subject_id,
            session_date=payload.session_date,
            axis=payload.axis,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
