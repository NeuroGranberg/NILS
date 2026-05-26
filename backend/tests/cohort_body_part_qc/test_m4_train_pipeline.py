"""M4 — trainer unit tests with a deterministic fake estimator.

The real sklearn pipeline isn't installed in the dev venv. We inject a
dummy estimator (a 1-NN-style centroid classifier) plus a stub persister
that just writes the meta to disk, so we can validate the orchestration
without sklearn.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from cohorts.models import Base, StackEmbeddingCache
from qc.body_part.embedding_cache import CacheKey, write_many
from qc.body_part.train import (
    EncoderSpec,
    TrainConfig,
    TrainingSample,
    assemble_features,
    train_classifier,
)


# ---------------------------------------------------------------------------
# A deterministic fake estimator (centroid-based) — no sklearn needed.
# ---------------------------------------------------------------------------


class FakeCentroidEstimator:
    def __init__(self, *, n_components: int, C: float, random_state: int):
        self.n_components = int(n_components)
        self.C = float(C)
        self.random_state = int(random_state)
        self.classes_: list[str] | None = None
        self.centroids_: np.ndarray | None = None

    def fit(self, X, y):
        labels = sorted(set(y.tolist()))
        cents = []
        for lab in labels:
            mask = y == lab
            cents.append(X[mask].mean(axis=0))
        self.classes_ = labels
        self.centroids_ = np.stack(cents, axis=0)
        return self

    def predict(self, X):
        # Cosine-similarity to centroids.
        cents = self.centroids_
        # Defensive: handle zero centroids (shouldn't happen here).
        c_norm = cents / np.linalg.norm(cents, axis=1, keepdims=True).clip(min=1e-12)
        x_norm = X / np.linalg.norm(X, axis=1, keepdims=True).clip(min=1e-12)
        sims = x_norm @ c_norm.T
        idx = sims.argmax(axis=1)
        return np.asarray([self.classes_[i] for i in idx], dtype=object)

    def predict_proba(self, X):
        cents = self.centroids_
        c_norm = cents / np.linalg.norm(cents, axis=1, keepdims=True).clip(min=1e-12)
        x_norm = X / np.linalg.norm(X, axis=1, keepdims=True).clip(min=1e-12)
        sims = x_norm @ c_norm.T
        # Softmax for a probability-like view.
        exp = np.exp(sims - sims.max(axis=1, keepdims=True))
        return exp / exp.sum(axis=1, keepdims=True)


def _fake_estimator_factory(*, n_components, C, random_state,
                            estimator_kind="logreg", estimator_hyperparams=None):
    return FakeCentroidEstimator(
        n_components=n_components, C=C, random_state=random_state,
    )


def _fake_persist(estimator, path: Path) -> None:
    """Stub persister — writes a tiny marker so sha256 has something to hash."""
    path.write_bytes(b"FAKE_MODEL\n" + repr(estimator.classes_).encode())


# ---------------------------------------------------------------------------
# DB fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def db():
    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(
        engine,
        tables=[t for t in Base.metadata.tables.values()
                if t.name == "stack_embedding_cache"],
    )
    SessionLocal = sessionmaker(
        bind=engine, autoflush=False, autocommit=False,
        expire_on_commit=False, future=True,
    )

    @contextmanager
    def _scope():
        s = SessionLocal()
        try:
            yield s
            s.commit()
        except Exception:
            s.rollback()
            raise
        finally:
            s.close()

    yield {"engine": engine, "SessionLocal": SessionLocal, "scope": _scope}


# ---------------------------------------------------------------------------
# Helpers — populate the cache with deterministic embeddings
# ---------------------------------------------------------------------------


BC = EncoderSpec(name="biomedclip", version="v1.0.0", dim=4)
SL = EncoderSpec(name="siglip2", version="v1.0.0", dim=2)


def _seed_cache(scope, samples, encoder_chain, *, label_to_vec):
    """Seed the cache with embeddings derived from each sample's label."""
    items = []
    for s in samples:
        for enc in encoder_chain:
            v = label_to_vec(s.label, enc)
            items.append((CacheKey(
                series_stack_id=s.stack_id, slice_index=s.slice_index,
                encoder_name=enc.name, encoder_version=enc.version,
            ), v))
    with scope() as db:
        write_many(db, items)


# ---------------------------------------------------------------------------
# assemble_features
# ---------------------------------------------------------------------------


def test_assemble_features_concats_in_chain_order(db):
    samples = [
        TrainingSample(1, 0, "Brain"),
        TrainingSample(2, 0, "Spine"),
    ]
    def vec(label, enc):
        if enc.name == "biomedclip":
            return np.array(
                [1.0, 0.0, 0.0, 0.0] if label == "Brain"
                else [0.0, 1.0, 0.0, 0.0],
                dtype=np.float32,
            )
        return np.array(
            [1.0, 0.0] if label == "Brain" else [0.0, 1.0],
            dtype=np.float32,
        )
    _seed_cache(db["scope"], samples, [BC, SL], label_to_vec=vec)

    with db["scope"]() as s:
        X, y, kept = assemble_features(s, samples, [BC, SL])
    assert X.shape == (2, 6)
    assert list(y) == ["Brain", "Spine"]
    assert kept == samples
    np.testing.assert_array_equal(
        X[0], np.array([1, 0, 0, 0, 1, 0], dtype=np.float32),
    )
    np.testing.assert_array_equal(
        X[1], np.array([0, 1, 0, 0, 0, 1], dtype=np.float32),
    )


def test_assemble_features_drops_samples_missing_any_encoder(db):
    samples = [
        TrainingSample(1, 0, "Brain"),
        TrainingSample(2, 0, "Spine"),
    ]
    # Only seed BiomedCLIP for sample 1; sample 2 has both.
    items = [
        (CacheKey(1, 0, "biomedclip", "v1.0.0"),
         np.array([1, 0, 0, 0], dtype=np.float32)),
        (CacheKey(2, 0, "biomedclip", "v1.0.0"),
         np.array([0, 1, 0, 0], dtype=np.float32)),
        (CacheKey(2, 0, "siglip2", "v1.0.0"),
         np.array([0, 1], dtype=np.float32)),
    ]
    with db["scope"]() as s:
        write_many(s, items)
    with db["scope"]() as s:
        X, y, kept = assemble_features(s, samples, [BC, SL])
    assert kept == [samples[1]]
    assert list(y) == ["Spine"]
    assert X.shape == (1, 6)


def test_assemble_features_empty_inputs(db):
    with db["scope"]() as s:
        X, y, kept = assemble_features(s, [], [BC, SL])
    assert X.shape == (0, 0) and list(y) == [] and kept == []


# ---------------------------------------------------------------------------
# train_classifier
# ---------------------------------------------------------------------------


def _build_two_class_dataset(scope, *, n_per_class=20):
    samples = []
    for i in range(n_per_class):
        samples.append(TrainingSample(100 + i, 0, "Brain"))
    for i in range(n_per_class):
        samples.append(TrainingSample(200 + i, 0, "Spine"))

    def vec(label, enc):
        rng = np.random.default_rng(hash((label, enc.name)) & 0xFFFF)
        base = np.zeros(enc.dim, dtype=np.float32)
        if label == "Brain":
            base[0] = 1.0
        else:
            base[1 % enc.dim] = 1.0
        base += rng.normal(0, 0.05, enc.dim).astype(np.float32)
        return base / max(np.linalg.norm(base), 1e-12)

    _seed_cache(scope, samples, [BC, SL], label_to_vec=vec)
    return samples


def test_train_classifier_writes_artefact_and_meta(db, tmp_path):
    samples = _build_two_class_dataset(db["scope"])
    with db["scope"]() as s:
        result = train_classifier(
            db=s,
            cohort_id=42,
            samples=samples,
            encoder_chain=[BC, SL],
            artefacts_dir=tmp_path,
            estimator_factory=_fake_estimator_factory,
            persist=_fake_persist,
        )
    p = Path(result.classifier_path)
    assert p.exists() and p.name == "cohort_42_body_part_qc.joblib"
    assert len(result.classifier_sha256) == 64

    meta = result.classifier_meta
    assert meta["cohort_id"] == 42
    assert meta["dim_in"] == 6
    assert meta["pca_components"] >= 2
    assert meta["pca_components"] <= 6
    assert meta["n_samples"] == len(samples)
    assert meta["classes"] == ["Brain", "Spine"]
    assert meta["per_class_counts"] == {"Brain": 20, "Spine": 20}
    assert "trained_at" in meta
    assert isinstance(meta["accuracy"], (float, type(None)))


def test_train_classifier_rejects_empty_samples(db, tmp_path):
    with db["scope"]() as s:
        with pytest.raises(ValueError, match="At least one"):
            train_classifier(
                db=s, cohort_id=1, samples=[],
                encoder_chain=[BC, SL], artefacts_dir=tmp_path,
                estimator_factory=_fake_estimator_factory,
                persist=_fake_persist,
            )


def test_train_classifier_rejects_single_class(db, tmp_path):
    samples = [TrainingSample(i, 0, "Brain") for i in range(10)]
    def vec(label, enc):
        return np.array([1.0] + [0.0] * (enc.dim - 1), dtype=np.float32)
    _seed_cache(db["scope"], samples, [BC, SL], label_to_vec=vec)

    with db["scope"]() as s:
        with pytest.raises(ValueError, match="at least 2 classes"):
            train_classifier(
                db=s, cohort_id=1, samples=samples,
                encoder_chain=[BC, SL], artefacts_dir=tmp_path,
                estimator_factory=_fake_estimator_factory,
                persist=_fake_persist,
            )


def test_train_classifier_rejects_uncached_samples(db, tmp_path):
    # Samples not present in the cache → assemble_features yields nothing.
    samples = [
        TrainingSample(900, 0, "Brain"),
        TrainingSample(901, 0, "Spine"),
    ]
    with db["scope"]() as s:
        with pytest.raises(ValueError, match="No samples have all required"):
            train_classifier(
                db=s, cohort_id=1, samples=samples,
                encoder_chain=[BC, SL], artefacts_dir=tmp_path,
                estimator_factory=_fake_estimator_factory,
                persist=_fake_persist,
            )


def test_train_classifier_rejects_empty_encoder_chain(db, tmp_path):
    with db["scope"]() as s:
        with pytest.raises(ValueError, match="encoder_chain"):
            train_classifier(
                db=s, cohort_id=1,
                samples=[TrainingSample(1, 0, "Brain")],
                encoder_chain=[], artefacts_dir=tmp_path,
                estimator_factory=_fake_estimator_factory,
                persist=_fake_persist,
            )


def test_train_classifier_pca_clamps_to_feature_dim(db, tmp_path):
    samples = _build_two_class_dataset(db["scope"], n_per_class=10)
    cfg = TrainConfig(pca_components=4096)
    with db["scope"]() as s:
        result = train_classifier(
            db=s, cohort_id=7, samples=samples,
            encoder_chain=[BC, SL], artefacts_dir=tmp_path,
            config=cfg,
            estimator_factory=_fake_estimator_factory,
            persist=_fake_persist,
        )
    # Total dim = 6, n=20 → PCA clamped down.
    assert result.classifier_meta["pca_components"] <= 6


def test_train_classifier_meta_mentions_encoder_chain(db, tmp_path):
    samples = _build_two_class_dataset(db["scope"], n_per_class=8)
    with db["scope"]() as s:
        result = train_classifier(
            db=s, cohort_id=5, samples=samples,
            encoder_chain=[BC, SL], artefacts_dir=tmp_path,
            estimator_factory=_fake_estimator_factory,
            persist=_fake_persist,
        )
    chain = result.classifier_meta["encoder_chain"]
    assert chain == [
        {"name": "biomedclip", "version": "v1.0.0", "dim": 4},
        {"name": "siglip2", "version": "v1.0.0", "dim": 2},
    ]
