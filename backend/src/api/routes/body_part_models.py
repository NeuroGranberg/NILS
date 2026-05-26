"""API routes for global Body Part QC — sample pool + model registry.

Endpoints:
    GET    /api/body-part-qc/pool/summary
    POST   /api/body-part-qc/pool/push
    GET    /api/body-part-qc/models
    POST   /api/body-part-qc/models/train
    GET    /api/body-part-qc/models/{model_id}
    POST   /api/body-part-qc/models/{model_id}/set-default
    DELETE /api/body-part-qc/models/{model_id}
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from qc.body_part.global_service import (
    BodyPartGlobalService,
    ModelDTO,
    PoolSummaryDTO,
    PushToPoolResult,
    TrainModelPayload,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["body-part-qc-global"])


def _service() -> BodyPartGlobalService:
    return BodyPartGlobalService()


# ---------------------------------------------------------------------------
# Sample Pool
# ---------------------------------------------------------------------------


@router.get(
    "/api/body-part-qc/pool/summary",
    response_model=PoolSummaryDTO,
    summary="Get per-label sample counts from the global pool.",
)
def get_pool_summary() -> PoolSummaryDTO:
    return _service().get_pool_summary()


class PushPayload(BaseModel):
    cohort_id: int


@router.post(
    "/api/body-part-qc/pool/push",
    response_model=PushToPoolResult,
    summary="Push a cohort's training samples into the global pool.",
)
def push_to_pool(payload: PushPayload) -> PushToPoolResult:
    try:
        return _service().push_to_pool(payload.cohort_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


# ---------------------------------------------------------------------------
# Model Registry
# ---------------------------------------------------------------------------


@router.get(
    "/api/body-part-qc/models",
    response_model=list[ModelDTO],
    summary="List all registered body-part models.",
)
def list_models() -> list[ModelDTO]:
    return _service().list_models()


@router.post(
    "/api/body-part-qc/models/train",
    response_model=ModelDTO,
    summary="Train a new model from the global pool with optional label remap.",
)
def train_model(payload: TrainModelPayload) -> ModelDTO:
    try:
        return _service().train_model(payload)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Model training failed")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get(
    "/api/body-part-qc/models/{model_id}",
    response_model=ModelDTO,
    summary="Get a single model by ID.",
)
def get_model(model_id: int) -> ModelDTO:
    try:
        return _service().get_model(model_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post(
    "/api/body-part-qc/models/{model_id}/set-default",
    response_model=ModelDTO,
    summary="Mark a model as the default.",
)
def set_default_model(model_id: int) -> ModelDTO:
    try:
        return _service().set_default_model(model_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.delete(
    "/api/body-part-qc/models/{model_id}",
    summary="Delete a model (fails if cohorts reference it).",
)
def delete_model(model_id: int):
    try:
        _service().delete_model(model_id)
        return {"ok": True}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
