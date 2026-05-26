"""Global Body Part QC service — sample pool + model registry.

Manages the shared labeled dataset and the trained-model registry.
Cohorts select a model from this registry for their Apply step.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import func, select, text

from cohorts.models import (
    BodyPartGlobalSample,
    BodyPartModel,
    CohortBodyPartQCState,
)
from cohorts.repository import get_cohort
from db.session import session_scope
from qc.body_part import worker_client
from qc.body_part.preprocess import central_slice_indices

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DTOs (kept simple — Pydantic models for API responses)
# ---------------------------------------------------------------------------

from pydantic import BaseModel


class PoolSummaryDTO(BaseModel):
    by_label: dict[str, int]
    total: int


class GlobalSampleDTO(BaseModel):
    id: int
    stack_id: int
    slice_index: int
    label: str
    orientation: str
    source_cohort_name: str
    contributed_at: str


class ModelDTO(BaseModel):
    id: int
    name: str
    description: Optional[str]
    classes: list[str]
    label_remap: dict[str, str]
    accuracy: Optional[float]
    n_samples: int
    trained_at: str
    is_default: bool
    meta: dict


class TrainModelPayload(BaseModel):
    name: str
    classes: list[str]
    label_remap: dict[str, str] = {}
    description: Optional[str] = None
    estimator_kind: str = "logreg"   # "logreg" | "rf" | "svm"
    use_pca: bool = True             # False → skip PCA entirely
    pca_components: Optional[int] = None  # None + use_pca=True → auto_tune picks


class PushToPoolResult(BaseModel):
    inserted: int
    updated: int
    total_pool_size: int


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class BodyPartGlobalService:
    """Manages the global sample pool and model registry."""

    # --- Sample Pool ---

    def get_pool_summary(self) -> PoolSummaryDTO:
        """Return per-label sample counts from the global pool."""
        with session_scope() as db:
            rows = db.execute(
                select(
                    BodyPartGlobalSample.label,
                    func.count(BodyPartGlobalSample.id),
                ).group_by(BodyPartGlobalSample.label)
            ).all()
            by_label = {label: count for label, count in rows}
            total = sum(by_label.values())
        return PoolSummaryDTO(by_label=by_label, total=total)

    def push_to_pool(self, cohort_id: int) -> PushToPoolResult:
        """Copy training samples from a cohort's QC state into the global pool.

        Upserts by (stack_id, slice_index) — existing entries get updated.
        """
        with session_scope() as db:
            cohort = get_cohort(db, cohort_id)
            if cohort is None:
                raise LookupError(f"Cohort {cohort_id} not found")
            cohort_name = cohort.name

            state = db.get(CohortBodyPartQCState, cohort_id)
            if state is None or not state.training_samples:
                raise ValueError("No training samples to push.")

            samples = list(state.training_samples)

        inserted = 0
        updated = 0
        now = datetime.now(timezone.utc)

        with session_scope() as db:
            for s in samples:
                stack_id = int(s["stack_id"])
                slice_index = int(s["slice_index"])
                label = str(s["label"])
                orientation = str(s.get("orientation", "unknown"))
                contributed_at = s.get("approved_at") or now.isoformat()

                existing = db.execute(
                    select(BodyPartGlobalSample).where(
                        BodyPartGlobalSample.stack_id == stack_id,
                        BodyPartGlobalSample.slice_index == slice_index,
                    )
                ).scalar_one_or_none()

                if existing:
                    existing.label = label
                    existing.orientation = orientation
                    existing.source_cohort_name = cohort_name
                    existing.contributed_at = contributed_at
                    updated += 1
                else:
                    db.add(BodyPartGlobalSample(
                        stack_id=stack_id,
                        slice_index=slice_index,
                        label=label,
                        orientation=orientation,
                        source_cohort_name=cohort_name,
                        contributed_at=contributed_at,
                    ))
                    inserted += 1

            db.flush()
            total = db.scalar(
                select(func.count(BodyPartGlobalSample.id))
            ) or 0

        return PushToPoolResult(
            inserted=inserted, updated=updated, total_pool_size=total,
        )

    def list_pool_samples(
        self, *, label: Optional[str] = None, limit: int = 200, offset: int = 0,
    ) -> list[GlobalSampleDTO]:
        """List global samples with optional label filter."""
        with session_scope() as db:
            stmt = select(BodyPartGlobalSample).order_by(
                BodyPartGlobalSample.contributed_at.desc()
            )
            if label:
                stmt = stmt.where(BodyPartGlobalSample.label == label)
            stmt = stmt.offset(offset).limit(limit)
            rows = db.scalars(stmt).all()
            return [
                GlobalSampleDTO(
                    id=r.id,
                    stack_id=r.stack_id,
                    slice_index=r.slice_index,
                    label=r.label,
                    orientation=r.orientation,
                    source_cohort_name=r.source_cohort_name,
                    contributed_at=str(r.contributed_at),
                )
                for r in rows
            ]

    # --- Model Registry ---

    def list_models(self) -> list[ModelDTO]:
        """Return all registered models, newest first."""
        with session_scope() as db:
            rows = db.scalars(
                select(BodyPartModel).order_by(BodyPartModel.trained_at.desc())
            ).all()
            return [self._model_to_dto(m) for m in rows]

    def get_model(self, model_id: int) -> ModelDTO:
        """Return a single model by ID."""
        with session_scope() as db:
            model = db.get(BodyPartModel, model_id)
            if model is None:
                raise LookupError(f"Model {model_id} not found")
            return self._model_to_dto(model)

    def train_model(self, payload: TrainModelPayload) -> ModelDTO:
        """Train a new model from the global pool.

        Steps:
            1. Load global samples, apply label_remap + target_classes filter.
            2. Embed all (if not cached).
            3. Call worker /train with remap + artifact_name.
            4. Register in body_part_model.
        """
        if len(payload.classes) < 2:
            raise ValueError("Need at least 2 target classes.")

        # 1. Load global samples and compute effective labels.
        with session_scope() as db:
            all_samples = db.scalars(select(BodyPartGlobalSample)).all()
            if not all_samples:
                raise ValueError("Global sample pool is empty.")

            # Apply remap + filter
            effective_samples: list[dict[str, Any]] = []
            for s in all_samples:
                effective_label = payload.label_remap.get(s.label, s.label)
                if effective_label not in payload.classes:
                    continue
                effective_samples.append({
                    "stack_id": s.stack_id,
                    "slice_index": s.slice_index,
                    "label": effective_label,
                    "original_label": s.label,
                })

        if not effective_samples:
            raise ValueError(
                "No global samples match the target classes after remap."
            )

        # Check class balance
        from collections import Counter
        class_counts = Counter(s["label"] for s in effective_samples)
        if len(class_counts) < 2:
            raise ValueError(
                f"Only one class after remap: {list(class_counts.keys())}. "
                "Need at least 2."
            )
        too_few = [c for c, n in class_counts.items() if n < 5]
        if too_few:
            raise ValueError(
                f"Classes with fewer than 5 samples: {too_few} "
                f"(counts: {dict(class_counts)})"
            )

        # 2. Embed — build embed items (3 central slices per stack).
        stack_ids = list({s["stack_id"] for s in effective_samples})
        num_slices_map = self._fetch_slices_count(stack_ids)

        embed_items: list[dict[str, int]] = []
        for s in effective_samples:
            sid = s["stack_id"]
            n_total = num_slices_map.get(sid, 0)
            if n_total > 0:
                for idx in central_slice_indices(n_total, n=3):
                    embed_items.append({"stack_id": sid, "slice_index": idx})
            else:
                embed_items.append({
                    "stack_id": sid, "slice_index": s["slice_index"],
                })

        worker_client.embed(embed_items, mode="concat")

        # 3. Train via worker with remap.
        artifact_name = f"model_{uuid.uuid4().hex[:12]}_body_part_qc.joblib"
        train_samples = [
            {"stack_id": s["stack_id"], "slice_index": s["slice_index"],
             "label": s["original_label"]}
            for s in effective_samples
        ]

        result = worker_client.train(
            cohort_id=0,
            samples=train_samples,
            mode="concat",
            n_train_slices=3,
            auto_tune=True,
            estimator_kind=payload.estimator_kind,
            use_pca=payload.use_pca,
            pca_components=payload.pca_components,
            num_slices_map=num_slices_map,
            label_remap=payload.label_remap or None,
            target_classes=payload.classes,
            artifact_name=artifact_name,
        )

        # 4. Register in DB.
        classifier_meta = result.get("classifier_meta", {})
        accuracy = classifier_meta.get("accuracy")
        n_samples = classifier_meta.get("n_samples", len(effective_samples))

        with session_scope() as db:
            model = BodyPartModel(
                name=payload.name,
                description=payload.description,
                classes=payload.classes,
                label_remap=payload.label_remap or {},
                artifact_path=result["classifier_path"],
                artifact_sha256=result["classifier_sha256"],
                meta=classifier_meta,
                accuracy=accuracy,
                n_samples=n_samples,
                trained_at=datetime.now(timezone.utc),
                is_default=False,
            )
            db.add(model)
            db.flush()
            db.refresh(model)
            return self._model_to_dto(model)

    def set_default_model(self, model_id: int) -> ModelDTO:
        """Mark a model as default (unmarks any previous default)."""
        with session_scope() as db:
            model = db.get(BodyPartModel, model_id)
            if model is None:
                raise LookupError(f"Model {model_id} not found")
            # Clear previous default
            db.execute(
                text("UPDATE body_part_model SET is_default = FALSE WHERE is_default = TRUE")
            )
            model.is_default = True
            db.flush()
            db.refresh(model)
            return self._model_to_dto(model)

    def delete_model(self, model_id: int) -> None:
        """Delete a model. Fails if any cohort is using it."""
        with session_scope() as db:
            model = db.get(BodyPartModel, model_id)
            if model is None:
                raise LookupError(f"Model {model_id} not found")
            # Check references
            referencing = db.scalar(
                select(func.count(CohortBodyPartQCState.cohort_id)).where(
                    CohortBodyPartQCState.selected_model_id == model_id,
                )
            )
            if referencing and referencing > 0:
                raise ValueError(
                    f"Cannot delete model {model_id}: {referencing} cohort(s) "
                    "still reference it. Change their selection first."
                )
            db.delete(model)

    # --- Helpers ---

    @staticmethod
    def _model_to_dto(model: BodyPartModel) -> ModelDTO:
        return ModelDTO(
            id=model.id,
            name=model.name,
            description=model.description,
            classes=model.classes or [],
            label_remap=model.label_remap or {},
            accuracy=model.accuracy,
            n_samples=model.n_samples,
            trained_at=str(model.trained_at),
            is_default=model.is_default,
            meta=model.meta or {},
        )

    @staticmethod
    def _fetch_slices_count(stack_ids: list[int]) -> dict[int, int]:
        """Read num_slices per stack from the metadata DB."""
        if not stack_ids:
            return {}
        from metadata_db.session import SessionLocal as MetadataSessionLocal
        out: dict[int, int] = {}
        sql = text(
            "SELECT series_stack_id, slices_count "
            "FROM series_classification_cache "
            "WHERE series_stack_id = ANY(:ids)"
        )
        CHUNK = 2000
        for start in range(0, len(stack_ids), CHUNK):
            chunk = stack_ids[start:start + CHUNK]
            with MetadataSessionLocal() as mdb:
                rows = mdb.execute(sql, {"ids": chunk}).fetchall()
                for r in rows:
                    ns = r.slices_count
                    if ns and ns > 0:
                        out[int(r.series_stack_id)] = int(ns)
        return out
