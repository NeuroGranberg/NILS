"""Cohort inference for Body Part QC.

For each input stack the worker:

1. Determines the 3 central slice indices (or fewer for very thin stacks).
2. Reads the per-encoder embeddings from ``stack_embedding_cache`` and
   concatenates them in encoder-chain order (matching what the
   classifier was trained on).
3. Runs ``classifier.predict_proba`` on each slice and **averages the
   probabilities**. The argmax of the average is the predicted label;
   the max-avg-probability is the per-stack confidence.
4. Tags ``needs_check=True`` when the confidence falls below the
   ``manual_review_below`` threshold.

The classifier is loaded lazily from the joblib artefact via a
process-wide LRU cache keyed by (path, sha256) so repeat ``/infer``
calls don't re-read from disk.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Callable, Optional, Sequence

import numpy as np
from sqlalchemy.orm import Session

from cohorts.models import StackEmbeddingCache
from qc.body_part.embedding_cache import _bytes_to_embedding
from qc.body_part.preprocess import central_slice_indices, orientation_slice_indices
from qc.body_part.train import EncoderSpec

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Defaults (mirrored on the backend service)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class InferConfig:
    n_slices: int = 3
    manual_review_below: float = 0.70


DEFAULT_INFER_CONFIG = InferConfig()


# ---------------------------------------------------------------------------
# Classifier loader (lazy import + path/sha cache)
# ---------------------------------------------------------------------------


class ClassifierLoader:
    """Loads a joblib pipeline once per (path, sha256). Tests can pass a
    custom loader that bypasses joblib entirely."""

    def __init__(self, joblib_load: Optional[Callable[[str], object]] = None) -> None:
        self._joblib_load = joblib_load
        self._cache: dict[tuple[str, str], object] = {}

    def load(self, path: str, sha256: str) -> object:
        key = (path, sha256)
        if key in self._cache:
            return self._cache[key]
        if self._joblib_load is None:
            import joblib  # type: ignore
            est = joblib.load(path)
        else:
            est = self._joblib_load(path)
        self._cache[key] = est
        return est


_default_loader = ClassifierLoader()


# ---------------------------------------------------------------------------
# Stack predictions
# ---------------------------------------------------------------------------


@dataclass
class StackPrediction:
    stack_id: int
    label: Optional[str]
    confidence: float
    probs: dict[str, float]
    needs_check: bool
    n_slices_used: int
    reasoning: Optional[dict] = None


def _gather_slice_embedding(
    db: Session, stack_id: int, slice_index: int,
    encoder_chain: Sequence[EncoderSpec],
) -> Optional[np.ndarray]:
    """Concatenate cached embeddings for one slice. Returns ``None`` if
    any encoder row is missing."""
    blocks: list[np.ndarray] = []
    for enc in encoder_chain:
        row = db.get(StackEmbeddingCache, (
            stack_id, slice_index, enc.name, enc.version,
        ))
        if row is None or row.dim != enc.dim:
            return None
        blocks.append(_bytes_to_embedding(row.embedding, enc.dim))
    return np.concatenate(blocks, axis=0).astype(np.float32)


def _axial_compose(
    probs_per_slice: np.ndarray,
    est_classes: list[str],
) -> tuple[str, float, dict[str, float], dict]:
    """Orientation-aware aggregation for axial stacks.

    Checks if the top slices predict Brain and bottom slices predict
    Spine — that spatial pattern *is* the Brain-Neck signal. Falls back
    to plain mean when no clear top/bottom split exists.
    """
    n = probs_per_slice.shape[0]
    class_idx = {c: i for i, c in enumerate(est_classes)}

    # Need at least 4 slices to split into top/bottom groups.
    brain_i = class_idx.get("Brain")
    spine_i = class_idx.get("Spine")
    brain_neck_i = class_idx.get("Brain-Neck")

    if n >= 4 and brain_i is not None and spine_i is not None and brain_neck_i is not None:
        # Split: first 40% = top, last 40% = bottom
        n_top = max(1, n * 2 // 5)
        n_bot = max(1, n * 2 // 5)
        top_avg = probs_per_slice[:n_top].mean(axis=0)
        bot_avg = probs_per_slice[-n_bot:].mean(axis=0)

        top_label = est_classes[int(np.argmax(top_avg))]
        bot_label = est_classes[int(np.argmax(bot_avg))]

        if top_label == "Brain" and bot_label == "Spine":
            # Classic Brain-Neck pattern: brain above, spine below.
            conf = min(float(top_avg[brain_i]), float(bot_avg[spine_i]))
            # Build probs that reflect this composition decision.
            probs = {c: 0.0 for c in est_classes}
            probs["Brain-Neck"] = conf
            probs["Brain"] = float(top_avg[brain_i]) * (1.0 - conf)
            probs["Spine"] = float(bot_avg[spine_i]) * (1.0 - conf)
            # Normalize
            total = sum(probs.values())
            if total > 0:
                probs = {c: v / total for c, v in probs.items()}
            reasoning = {
                "aggregation": "axial_compose",
                "top_label": top_label,
                "top_brain_prob": round(float(top_avg[brain_i]), 3),
                "bot_label": bot_label,
                "bot_spine_prob": round(float(bot_avg[spine_i]), 3),
            }
            return "Brain-Neck", conf, probs, reasoning

    # No clear split — plain mean.
    avg = probs_per_slice.mean(axis=0)
    best_idx = int(np.argmax(avg))
    label = est_classes[best_idx]
    confidence = float(avg[best_idx])
    probs = {str(c): float(avg[i]) for i, c in enumerate(est_classes)}
    return label, confidence, probs, {"aggregation": "axial_mean"}


def _weighted_center_avg(
    probs_per_slice: np.ndarray,
    est_classes: list[str],
) -> tuple[str, float, dict[str, float], dict]:
    """Center-weighted average for sagittal/coronal stacks.

    The middle slice gets 2× weight (it's closest to the midline and
    most informative). Edge slices get 1× weight.
    """
    n = probs_per_slice.shape[0]
    weights = np.ones(n, dtype=np.float32)
    mid = n // 2
    weights[mid] = 2.0
    weights /= weights.sum()
    avg = (probs_per_slice * weights[:, None]).sum(axis=0)

    best_idx = int(np.argmax(avg))
    label = est_classes[best_idx]
    confidence = float(avg[best_idx])
    probs = {str(c): float(avg[i]) for i, c in enumerate(est_classes)}
    reasoning = {
        "aggregation": "center_weighted",
        "center_weight": 2.0,
    }
    return label, confidence, probs, reasoning


def predict_stack(
    *,
    estimator: object,
    classes: list[str],
    db: Session,
    stack_id: int,
    num_slices: int,
    encoder_chain: Sequence[EncoderSpec],
    orientation: Optional[str] = None,
    config: InferConfig = DEFAULT_INFER_CONFIG,
) -> StackPrediction:
    """Run inference on a single stack using orientation-aware slicing.

    - **Axial**: 5 spread slices + composition logic (detects brain-top /
      spine-bottom → Brain-Neck).
    - **Sagittal / Coronal**: 3 midline slices, center-weighted average.
    - **Unknown**: 3 central slices, plain average (legacy behavior).
    """
    slice_idxs = orientation_slice_indices(
        num_slices, orientation, n_default=config.n_slices,
    )
    embs: list[np.ndarray] = []
    for idx in slice_idxs:
        v = _gather_slice_embedding(db, stack_id, idx, encoder_chain)
        if v is not None:
            embs.append(v)

    if not embs:
        return StackPrediction(
            stack_id=stack_id, label=None, confidence=0.0,
            probs={c: 0.0 for c in classes},
            needs_check=True, n_slices_used=0,
        )

    X = np.stack(embs, axis=0)
    probs_per_slice = estimator.predict_proba(X)  # (S, K)
    est_classes = list(getattr(estimator, "classes_", classes))
    if len(est_classes) != probs_per_slice.shape[1]:
        est_classes = classes[: probs_per_slice.shape[1]]

    ori = (orientation or "").strip().lower()
    if ori == "axial":
        label, confidence, probs, reasoning = _axial_compose(
            probs_per_slice, est_classes,
        )
    elif ori in ("sagittal", "coronal"):
        label, confidence, probs, reasoning = _weighted_center_avg(
            probs_per_slice, est_classes,
        )
    else:
        avg = probs_per_slice.mean(axis=0)
        best_idx = int(np.argmax(avg))
        label = est_classes[best_idx]
        confidence = float(avg[best_idx])
        probs = {str(c): float(avg[i]) for i, c in enumerate(est_classes)}
        reasoning = {"aggregation": "mean"}

    return StackPrediction(
        stack_id=stack_id,
        label=label,
        confidence=confidence,
        probs=probs,
        needs_check=confidence < config.manual_review_below,
        n_slices_used=len(embs),
        reasoning=reasoning,
    )


# ---------------------------------------------------------------------------
# Top-level cohort inference
# ---------------------------------------------------------------------------


@dataclass
class StackInferenceRequest:
    """One stack the caller wants a prediction for."""
    stack_id: int
    num_slices: int
    orientation: Optional[str] = None


def run_inference(
    *,
    db: Session,
    classifier_path: str,
    classifier_sha256: str,
    classes: list[str],
    encoder_chain: Sequence[EncoderSpec],
    items: Sequence[StackInferenceRequest],
    config: InferConfig = DEFAULT_INFER_CONFIG,
    loader: Optional[ClassifierLoader] = None,
) -> list[StackPrediction]:
    """Predict labels for all ``items``. Loads the classifier once.

    Passing ``loader=None`` resolves :data:`_default_loader` from the
    module namespace at call time, so tests can monkeypatch it.
    """
    if not items:
        return []
    if loader is None:
        loader = _default_loader
    estimator = loader.load(classifier_path, classifier_sha256)

    out: list[StackPrediction] = []
    for it in items:
        out.append(predict_stack(
            estimator=estimator,
            classes=classes,
            db=db,
            stack_id=it.stack_id,
            num_slices=it.num_slices,
            encoder_chain=encoder_chain,
            orientation=it.orientation,
            config=config,
        ))
    return out
