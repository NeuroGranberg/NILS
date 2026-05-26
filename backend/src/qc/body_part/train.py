"""Classifier trainer for Body Part QC.

Pipeline (per model):

1. **Assemble features** — for each training sample read its
   per-encoder embedding(s) from ``stack_embedding_cache`` and concat
   them in encoder-chain order.
2. **Optional PCA reduction** (default 128 components, capped at the
   CV-safe min(N_train_fold, D)). Skipped when ``pca_components=None``.
3. **Standardize + classify** with one of:
   - Multi-class Logistic Regression (default, balanced, L2, lbfgs)
   - Random Forest (balanced, probability output)
   - SVM with Platt calibration (CalibratedClassifierCV wrapping RBF SVC)
4. **Persist** the fitted Pipeline as a joblib artefact, plus a meta
   JSON describing the encoder chain, sample counts, accuracy,
   estimator kind, and dim_in / pca_components for inference.

Heavy deps (sklearn, joblib) are lazy-imported so unit tests can mock
the estimator factory and stay torch/sklearn-free.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional, Protocol, Sequence

import numpy as np
from sqlalchemy.orm import Session

from cohorts.models import StackEmbeddingCache
from qc.body_part.embedding_cache import CacheKey, _bytes_to_embedding

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


ESTIMATOR_KINDS = ("logreg", "rf", "svm")


@dataclass(frozen=True)
class TrainConfig:
    """Tunable knobs. Defaults are conservative for ~100-500 samples/class."""
    pca_components: Optional[int] = 128   # None → skip PCA
    pca_floor: int = 16            # never reduce below this
    logreg_C: float = 1.0
    random_state: int = 0
    test_split_min_per_class: int = 5  # if any class has <N → skip eval split
    n_train_slices: int = 3        # embed 3 central slices per stack, average
    auto_tune: bool = False        # cross-validated PCA/C sweep
    estimator_kind: str = "logreg"  # "logreg" | "rf" | "svm"


DEFAULT_TRAIN_CONFIG = TrainConfig()


# ---------------------------------------------------------------------------
# Encoder chain descriptor (passed in by caller; mirrors the response of
# ``EncoderRegistry.encoders_for_mode(mode)``).
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EncoderSpec:
    name: str
    version: str
    dim: int


# ---------------------------------------------------------------------------
# Estimator factory protocol — lets tests inject fakes
# ---------------------------------------------------------------------------


class EstimatorFactory(Protocol):
    def __call__(
        self, *, n_components: Optional[int], C: float, random_state: int,
        estimator_kind: str, estimator_hyperparams: Optional[dict],
    ) -> object:
        ...


def _default_estimator_factory(
    *, n_components: Optional[int], C: float, random_state: int,
    estimator_kind: str = "logreg",
    estimator_hyperparams: Optional[dict] = None,
) -> object:
    """Build the sklearn pipeline. Imported lazily.

    ``n_components=None`` skips PCA entirely.
    ``estimator_kind`` selects the classifier head.
    """
    from sklearn.decomposition import PCA
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    steps: list[tuple[str, object]] = [
        ("scale", StandardScaler(with_mean=True, with_std=True)),
    ]
    if n_components is not None:
        steps.append(("pca", PCA(n_components=n_components, random_state=random_state)))

    hp = estimator_hyperparams or {}

    if estimator_kind == "rf":
        from sklearn.ensemble import RandomForestClassifier
        steps.append(("clf", RandomForestClassifier(
            n_estimators=hp.get("n_estimators", 300),
            max_depth=hp.get("max_depth", None),
            class_weight="balanced",
            random_state=random_state,
            n_jobs=-1,
        )))
    elif estimator_kind == "svm":
        from sklearn.calibration import CalibratedClassifierCV
        from sklearn.svm import SVC
        steps.append(("clf", CalibratedClassifierCV(
            SVC(
                C=hp.get("C", C),
                kernel="rbf",
                gamma="scale",
                class_weight="balanced",
                random_state=random_state,
            ),
            method="sigmoid",
            cv=3,
        )))
    else:  # "logreg" (default)
        steps.append(("clf", LogisticRegression(
            C=C,
            class_weight="balanced",
            max_iter=2000,
            solver="lbfgs",
            random_state=random_state,
        )))

    return Pipeline(steps)


# ---------------------------------------------------------------------------
# Feature assembly
# ---------------------------------------------------------------------------


@dataclass
class TrainingSample:
    stack_id: int
    slice_index: int
    label: str


def _read_slice_embedding(
    db: Session, stack_id: int, slice_index: int,
    encoder_chain: Sequence[EncoderSpec],
) -> Optional[np.ndarray]:
    """Read and concat cached embeddings for one (stack, slice) pair.
    Returns None if any encoder row is missing."""
    blocks: list[np.ndarray] = []
    for enc in encoder_chain:
        row = db.get(StackEmbeddingCache, (
            stack_id, slice_index, enc.name, enc.version,
        ))
        if row is None or row.dim != enc.dim:
            return None
        blocks.append(_bytes_to_embedding(row.embedding, enc.dim))
    return np.concatenate(blocks, axis=0)


def assemble_features(
    db: Session,
    samples: Sequence[TrainingSample],
    encoder_chain: Sequence[EncoderSpec],
    *,
    n_slices: int = 1,
    num_slices_map: Optional[dict[int, int]] = None,
) -> tuple[np.ndarray, np.ndarray, list[TrainingSample]]:
    """Read cached embeddings for every (sample × encoder) pair and
    concat them in encoder-chain order.

    When ``n_slices > 1``, reads the N central slices for each stack and
    **averages** their embeddings. This matches inference behaviour and
    provides a more robust feature vector. ``num_slices_map`` maps
    stack_id → total slice count (needed to compute central indices);
    if missing, falls back to single-slice mode.

    Samples missing **any** encoder row are silently dropped (the caller
    is responsible for embedding them via ``/embed`` before calling
    ``train``). Returns ``(X, y, kept_samples)``.
    """
    from qc.body_part.preprocess import central_slice_indices

    if not samples or not encoder_chain:
        return (
            np.zeros((0, 0), dtype=np.float32),
            np.zeros((0,), dtype=object),
            [],
        )

    X_rows: list[np.ndarray] = []
    y_rows: list[str] = []
    kept: list[TrainingSample] = []

    for sample in samples:
        if n_slices > 1 and num_slices_map and sample.stack_id in num_slices_map:
            total = num_slices_map[sample.stack_id]
            slice_idxs = central_slice_indices(total, n=n_slices)
        else:
            slice_idxs = [sample.slice_index]

        slice_embs: list[np.ndarray] = []
        for idx in slice_idxs:
            emb = _read_slice_embedding(db, sample.stack_id, idx, encoder_chain)
            if emb is not None:
                slice_embs.append(emb)

        if not slice_embs:
            continue

        # Average across slices (single slice → no-op).
        avg = np.stack(slice_embs, axis=0).mean(axis=0)
        X_rows.append(avg)
        y_rows.append(sample.label)
        kept.append(sample)

    if not X_rows:
        return (
            np.zeros((0, 0), dtype=np.float32),
            np.zeros((0,), dtype=object),
            [],
        )
    X = np.stack(X_rows, axis=0).astype(np.float32)
    y = np.asarray(y_rows, dtype=object)
    return X, y, kept


# ---------------------------------------------------------------------------
# Cross-validated accuracy (small-budget friendly)
# ---------------------------------------------------------------------------


def _safe_eval_accuracy(
    estimator: object, X: np.ndarray, y: np.ndarray,
    *, min_per_class: int, random_state: int,
) -> Optional[float]:
    """Return cross-validated accuracy as a quality signal.

    Strategy:
        - If every class has ≥ ``min_per_class`` samples, run 5-fold
          stratified CV and return mean accuracy.
        - Otherwise (very small budget), single stratified 80/20 split.
        - As last resort, resubstitution accuracy.

    Returns ``None`` if sklearn is unavailable.
    """
    try:
        from sklearn.base import clone
        from sklearn.model_selection import (
            StratifiedKFold,
            cross_val_score,
            train_test_split,
        )
    except Exception:
        return None

    classes, counts = np.unique(y, return_counts=True)
    if len(classes) < 2:
        return None

    if int(counts.min()) >= max(min_per_class, 5):
        # 5-fold stratified CV — more reliable than a single split.
        n_folds = min(5, int(counts.min()))
        cv = StratifiedKFold(
            n_splits=n_folds, shuffle=True, random_state=random_state,
        )
        scores = cross_val_score(
            clone(estimator), X, y, cv=cv, scoring="accuracy",
        )
        return float(scores.mean())

    if int(counts.min()) >= 2:
        X_tr, X_te, y_tr, y_te = train_test_split(
            X, y, test_size=0.2, stratify=y, random_state=random_state,
        )
        eval_est = clone(estimator).fit(X_tr, y_tr)
        return float((eval_est.predict(X_te) == y_te).mean())

    # Resubstitution fallback.
    return float((estimator.predict(X) == y).mean())


# ---------------------------------------------------------------------------
# Auto-tuning: cross-validated sweep over PCA components and C
# ---------------------------------------------------------------------------


def _auto_tune_hyperparams(
    X: np.ndarray, y: np.ndarray,
    *, random_state: int, pca_floor: int,
    estimator_kind: str = "logreg",
) -> tuple[Optional[int], float, dict]:
    """Grid search over PCA components and classifier hyperparameters.

    Returns ``(best_pca, best_C, tune_report)`` where ``best_pca`` may be
    ``None`` (skip PCA), and ``tune_report`` contains every tried
    combination and its mean CV accuracy.

    The PCA candidate list is capped at the **CV-fold training size**
    (not the full N) to avoid ``n_components > n_samples`` crashes
    inside sklearn's cross_val_score.
    """
    from sklearn.model_selection import StratifiedKFold, cross_val_score

    N, D = X.shape
    classes, counts = np.unique(y, return_counts=True)
    n_folds = min(5, int(counts.min()))

    # CV-safe upper: each fold trains on (n_folds-1)/n_folds of N.
    cv_fold_train = N * (n_folds - 1) // n_folds
    upper = min(cv_fold_train, D)

    cv = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=random_state)

    best_score = -1.0
    best_pca: Optional[int] = None
    best_hp: dict = {}
    results: list[dict] = []

    if estimator_kind == "rf":
        # RF: skip PCA, sweep n_estimators and max_depth.
        pca_candidates: list[Optional[int]] = [None]
        hp_grid = [
            {"n_estimators": n, "max_depth": d}
            for n in [100, 300, 600]
            for d in [None, 10, 20]
        ]
    elif estimator_kind == "svm":
        # SVM: optional PCA (smaller set), sweep C.
        pca_candidates = [None] + sorted({
            c for c in [64, 128, 256]
            if pca_floor <= c <= upper
        })
        hp_grid = [{"C": c} for c in [0.1, 1.0, 10.0]]
    else:
        # LogReg: optional PCA, sweep C.
        pca_candidates = [None] + sorted({
            c for c in [64, 128, 256, 512, D]
            if pca_floor <= c <= upper
        })
        hp_grid = [{"C": c} for c in [0.01, 0.1, 1.0, 10.0, 100.0]]

    for pca_n in pca_candidates:
        for hp in hp_grid:
            est = _default_estimator_factory(
                n_components=pca_n,
                C=hp.get("C", 1.0),
                random_state=random_state,
                estimator_kind=estimator_kind,
                estimator_hyperparams=hp,
            )
            try:
                scores = cross_val_score(est, X, y, cv=cv, scoring="accuracy")
            except ValueError:
                # Skip this combination if it's invalid (e.g. PCA > fold size)
                continue
            mean_acc = float(scores.mean())
            entry = {
                "pca": pca_n, **hp,
                "mean_cv_acc": round(mean_acc, 4),
                "std_cv_acc": round(float(scores.std()), 4),
            }
            results.append(entry)
            logger.info(
                "auto_tune [%s]: PCA=%s %s → CV acc=%.4f ± %.4f",
                estimator_kind, pca_n, hp, mean_acc, float(scores.std()),
            )
            if mean_acc > best_score:
                best_score = mean_acc
                best_pca = pca_n
                best_hp = hp

    if not results:
        # Fallback: no PCA, default hyperparams
        best_pca = None
        best_hp = {"C": 1.0}

    report = {
        "grid": results,
        "best_pca": best_pca,
        "best_hp": best_hp,
        "best_cv_accuracy": round(best_score, 4),
        "estimator_kind": estimator_kind,
    }
    logger.info(
        "auto_tune [%s]: best PCA=%s hp=%s acc=%.4f",
        estimator_kind, best_pca, best_hp, best_score,
    )
    return best_pca, best_hp.get("C", 1.0), report


# ---------------------------------------------------------------------------
# Top-level trainer
# ---------------------------------------------------------------------------


@dataclass
class TrainResult:
    classifier_path: str
    classifier_sha256: str
    classifier_meta: dict


def train_classifier(
    *,
    db: Session,
    cohort_id: int = 0,
    samples: Sequence[TrainingSample],
    encoder_chain: Sequence[EncoderSpec],
    artefacts_dir: str | os.PathLike,
    config: TrainConfig = DEFAULT_TRAIN_CONFIG,
    estimator_factory: EstimatorFactory = _default_estimator_factory,
    persist: Optional[Callable[[object, Path], None]] = None,
    num_slices_map: Optional[dict[int, int]] = None,
    label_remap: Optional[dict[str, str]] = None,
    target_classes: Optional[list[str]] = None,
    artifact_name: Optional[str] = None,
) -> TrainResult:
    """Train the linear probe end-to-end.

    Args:
        db: open SQLAlchemy session against the application DB.
        cohort_id: used in the artefact filename so each cohort gets
            its own model file (ignored when ``artifact_name`` is set).
        samples: approved training set.
        encoder_chain: ordered encoders whose cached embeddings will be
            concatenated to form X. Must match what was used to populate
            the cache.
        artefacts_dir: directory to write the joblib file into. Created
            if missing.
        config: tuning knobs (PCA, regularisation, RNG).
        estimator_factory: returns an unfit sklearn-compatible pipeline.
            Tests inject a deterministic fake.
        persist: optional override for joblib.dump. Tests inject a stub
            that writes a small marker file.
        num_slices_map: ``{stack_id: total_slice_count}`` — needed when
            ``config.n_train_slices > 1`` to compute central indices.
        label_remap: optional mapping to fold labels before training,
            e.g. ``{"Brain-Neck": "Brain"}``. Applied before
            ``target_classes`` filter.
        target_classes: if set, only keep samples whose (remapped) label
            is in this list. Samples with other labels are dropped.
        artifact_name: custom filename for the .joblib artifact. If None,
            defaults to ``cohort_{cohort_id}_body_part_qc.joblib``.

    Returns:
        :class:`TrainResult` with the on-disk path, sha256, and meta
        JSON for ``classifier_meta``.

    Raises:
        ValueError: empty samples, single-class set, or feature width
            inconsistent with the encoder chain.
    """
    if not samples:
        raise ValueError("At least one training sample is required.")
    if not encoder_chain:
        raise ValueError("encoder_chain must not be empty.")

    # --- Label remap + class filter ---
    effective_remap = label_remap or {}
    if effective_remap or target_classes:
        remapped: list[TrainingSample] = []
        for s in samples:
            new_label = effective_remap.get(s.label, s.label)
            if target_classes and new_label not in target_classes:
                continue
            remapped.append(TrainingSample(
                stack_id=s.stack_id,
                slice_index=s.slice_index,
                label=new_label,
            ))
        samples = remapped
        if not samples:
            raise ValueError(
                "No samples remain after applying label_remap/target_classes filter."
            )

    expected_dim = sum(e.dim for e in encoder_chain)
    if expected_dim <= 0:
        raise ValueError("Encoder chain reports zero total dimension.")

    X, y, kept = assemble_features(
        db, samples, encoder_chain,
        n_slices=config.n_train_slices,
        num_slices_map=num_slices_map,
    )
    if X.shape[0] == 0:
        raise ValueError(
            "No samples have all required encoder embeddings cached. "
            "Run /embed first.",
        )
    if X.shape[1] != expected_dim:
        raise ValueError(
            f"Feature width {X.shape[1]} does not match encoder chain "
            f"total dim {expected_dim}.",
        )
    if len(set(y.tolist())) < 2:
        raise ValueError(
            "Training requires at least 2 classes; got only "
            f"{set(y.tolist())}.",
        )

    if X.shape[0] < 2 or X.shape[1] < 2:
        raise ValueError(
            f"Need at least 2 samples and 2 features; got "
            f"X.shape={X.shape}",
        )

    upper = min(X.shape[0], X.shape[1])
    tune_report: Optional[dict] = None
    logreg_C = config.logreg_C
    estimator_kind = config.estimator_kind
    estimator_hp: Optional[dict] = None

    if config.auto_tune and X.shape[0] >= 20:
        # Cross-validated hyperparameter sweep.
        best_pca, best_C, tune_report = _auto_tune_hyperparams(
            X, y,
            random_state=config.random_state,
            pca_floor=config.pca_floor,
            estimator_kind=estimator_kind,
        )
        n_components = best_pca
        logreg_C = best_C
        estimator_hp = tune_report.get("best_hp")
        logger.info(
            "auto_tune [%s] selected PCA=%s C=%.2f (CV acc=%.4f)",
            estimator_kind, n_components, logreg_C,
            tune_report["best_cv_accuracy"],
        )
    else:
        # Fixed hyperparameters from config.
        if config.pca_components is None:
            n_components = None
        else:
            n_components = min(config.pca_components, upper)
            n_components = max(min(config.pca_floor, upper), n_components, 2)
            n_components = min(n_components, upper)

    estimator = estimator_factory(
        n_components=n_components,
        C=logreg_C,
        random_state=config.random_state,
        estimator_kind=estimator_kind,
        estimator_hyperparams=estimator_hp,
    )
    estimator.fit(X, y)

    accuracy = _safe_eval_accuracy(
        estimator, X, y,
        min_per_class=config.test_split_min_per_class,
        random_state=config.random_state,
    )

    # Persist
    artefacts_dir = Path(artefacts_dir)
    artefacts_dir.mkdir(parents=True, exist_ok=True)
    fname = artifact_name or f"cohort_{cohort_id}_body_part_qc.joblib"
    artefact_path = artefacts_dir / fname

    if persist is None:
        try:
            import joblib  # type: ignore
            joblib.dump(estimator, artefact_path)
        except Exception as e:  # pragma: no cover
            raise RuntimeError(f"Failed to persist classifier: {e}") from e
    else:
        persist(estimator, artefact_path)

    sha = _sha256_of_file(artefact_path)

    classes_sorted = sorted({s.label for s in kept})
    per_class_counts = {c: int((y == c).sum()) for c in classes_sorted}

    meta: dict = {
        "cohort_id": int(cohort_id),
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "encoder_chain": [
            {"name": e.name, "version": e.version, "dim": e.dim}
            for e in encoder_chain
        ],
        "dim_in": int(X.shape[1]),
        "pca_components": int(n_components) if n_components is not None else None,
        "estimator_kind": estimator_kind,
        "logreg_C": float(logreg_C),
        "random_state": int(config.random_state),
        "n_samples": int(X.shape[0]),
        "n_train_slices": int(config.n_train_slices),
        "auto_tuned": config.auto_tune,
        "classes": classes_sorted,
        "per_class_counts": per_class_counts,
        "accuracy": accuracy,
        "label_remap": effective_remap or {},
        "target_classes": target_classes,
    }
    if tune_report is not None:
        meta["tune_report"] = tune_report
    return TrainResult(
        classifier_path=str(artefact_path),
        classifier_sha256=sha,
        classifier_meta=meta,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(64 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
