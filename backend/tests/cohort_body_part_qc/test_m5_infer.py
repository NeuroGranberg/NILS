"""M5 — inference unit tests.

Uses a small DB-backed cache and a deterministic fake classifier
exposing ``predict_proba`` and ``classes_`` so we don't need sklearn.
"""

from __future__ import annotations

from contextlib import contextmanager

import numpy as np
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from cohorts.models import Base
from qc.body_part.embedding_cache import CacheKey, write_many
from qc.body_part.infer import (
    ClassifierLoader,
    InferConfig,
    StackInferenceRequest,
    predict_stack,
    run_inference,
)
from qc.body_part.train import EncoderSpec


BC = EncoderSpec(name="biomedclip", version="v1.0.0", dim=4)
SL = EncoderSpec(name="siglip2", version="v1.0.0", dim=2)


# ---------------------------------------------------------------------------
# Fake classifier: returns probs proportional to per-class centroid sims.
# ---------------------------------------------------------------------------


class FakeClassifier:
    """Deterministic 'classifier' — returns argmax along configured axes
    so tests can assert label outcomes."""

    def __init__(self, classes: list[str], centroids: np.ndarray) -> None:
        self.classes_ = list(classes)
        self.centroids_ = centroids.astype(np.float32)

    def predict_proba(self, X):
        c_norm = self.centroids_ / np.linalg.norm(
            self.centroids_, axis=1, keepdims=True,
        ).clip(min=1e-12)
        x_norm = X / np.linalg.norm(X, axis=1, keepdims=True).clip(min=1e-12)
        sims = x_norm @ c_norm.T
        # softmax with high temperature for sharp probs.
        sims *= 10.0
        sims -= sims.max(axis=1, keepdims=True)
        exp = np.exp(sims)
        return exp / exp.sum(axis=1, keepdims=True)


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

    yield {"engine": engine, "scope": _scope, "SessionLocal": SessionLocal}


def _seed_stack_embeddings(scope, *, stack_id, slice_indices, vec_for):
    items = []
    for idx in slice_indices:
        for enc in (BC, SL):
            v = vec_for(stack_id, idx, enc)
            items.append((CacheKey(
                series_stack_id=stack_id, slice_index=idx,
                encoder_name=enc.name, encoder_version=enc.version,
            ), v))
    with scope() as db:
        write_many(db, items)


def _brain_centroid_classifier():
    # 6-D feature: BiomedCLIP[0]=brain, [1]=spine; SigLIP2[0]=brain, [1]=spine
    centroids = np.array([
        [1, 0, 0, 0, 1, 0],   # Brain
        [0, 1, 0, 0, 0, 1],   # Spine
    ], dtype=np.float32)
    return FakeClassifier(["Brain", "Spine"], centroids)


# ---------------------------------------------------------------------------
# predict_stack
# ---------------------------------------------------------------------------


def test_predict_stack_uses_three_central_slices(db):
    # 11-slice stack → central slices = {4, 5, 6}.
    def vec_for(_sid, _idx, enc):
        v = np.zeros(enc.dim, dtype=np.float32)
        v[0] = 1.0  # all 'brain'
        return v
    _seed_stack_embeddings(db["scope"], stack_id=1,
                           slice_indices=range(11), vec_for=vec_for)

    clf = _brain_centroid_classifier()
    with db["scope"]() as s:
        pred = predict_stack(
            estimator=clf, classes=["Brain", "Spine"],
            db=s, stack_id=1, num_slices=11,
            encoder_chain=[BC, SL],
        )
    assert pred.label == "Brain"
    assert pred.n_slices_used == 3
    assert pred.confidence > 0.9
    assert pred.needs_check is False
    assert set(pred.probs.keys()) == {"Brain", "Spine"}


def test_predict_stack_handles_thin_stack(db):
    # 1-slice stack → only [0].
    def vec_for(_sid, _idx, enc):
        v = np.zeros(enc.dim, dtype=np.float32)
        v[0] = 1.0
        return v
    _seed_stack_embeddings(db["scope"], stack_id=2,
                           slice_indices=[0], vec_for=vec_for)

    clf = _brain_centroid_classifier()
    with db["scope"]() as s:
        pred = predict_stack(
            estimator=clf, classes=["Brain", "Spine"],
            db=s, stack_id=2, num_slices=1, encoder_chain=[BC, SL],
        )
    assert pred.label == "Brain"
    assert pred.n_slices_used == 1


def test_predict_stack_low_confidence_flags_needs_check(db):
    # All-zero embeddings → uniform probs ≈ 0.5 → below default 0.70.
    def vec_for(_sid, _idx, enc):
        return np.zeros(enc.dim, dtype=np.float32)
    _seed_stack_embeddings(db["scope"], stack_id=3,
                           slice_indices=range(7), vec_for=vec_for)

    clf = _brain_centroid_classifier()
    with db["scope"]() as s:
        pred = predict_stack(
            estimator=clf, classes=["Brain", "Spine"],
            db=s, stack_id=3, num_slices=7, encoder_chain=[BC, SL],
        )
    assert pred.confidence == pytest.approx(0.5)
    assert pred.needs_check is True


def test_predict_stack_returns_none_when_no_embeddings(db):
    # No cache entries for this stack at all.
    clf = _brain_centroid_classifier()
    with db["scope"]() as s:
        pred = predict_stack(
            estimator=clf, classes=["Brain", "Spine"],
            db=s, stack_id=99, num_slices=10, encoder_chain=[BC, SL],
        )
    assert pred.label is None
    assert pred.confidence == 0.0
    assert pred.needs_check is True
    assert pred.n_slices_used == 0
    assert pred.probs == {"Brain": 0.0, "Spine": 0.0}


def test_predict_stack_skips_missing_slices(db):
    # 11-slice stack, but only the centre slice has embeddings.
    def vec_for(_sid, _idx, enc):
        v = np.zeros(enc.dim, dtype=np.float32)
        v[0] = 1.0
        return v
    _seed_stack_embeddings(db["scope"], stack_id=4,
                           slice_indices=[5], vec_for=vec_for)

    clf = _brain_centroid_classifier()
    with db["scope"]() as s:
        pred = predict_stack(
            estimator=clf, classes=["Brain", "Spine"],
            db=s, stack_id=4, num_slices=11, encoder_chain=[BC, SL],
        )
    assert pred.n_slices_used == 1
    assert pred.label == "Brain"


def test_predict_stack_averages_disagreeing_slices(db):
    # Slice 4: brain, slices 5+6: spine. Average should lean spine but with
    # lower confidence than a single-class stack.
    def vec_for(_sid, idx, enc):
        v = np.zeros(enc.dim, dtype=np.float32)
        v[0 if idx == 4 else 1 % enc.dim] = 1.0
        return v
    _seed_stack_embeddings(db["scope"], stack_id=5,
                           slice_indices=[4, 5, 6], vec_for=vec_for)

    clf = _brain_centroid_classifier()
    with db["scope"]() as s:
        pred = predict_stack(
            estimator=clf, classes=["Brain", "Spine"],
            db=s, stack_id=5, num_slices=11, encoder_chain=[BC, SL],
        )
    # 1 brain + 2 spine slices → spine wins.
    assert pred.label == "Spine"
    # Confidence should be lower than a unanimous stack (~0.66 not ~0.99)
    assert pred.confidence < 0.95


# ---------------------------------------------------------------------------
# run_inference + ClassifierLoader
# ---------------------------------------------------------------------------


def test_run_inference_uses_cached_loader(db):
    def vec_for(_sid, _idx, enc):
        v = np.zeros(enc.dim, dtype=np.float32)
        v[0] = 1.0
        return v
    for sid in (10, 11, 12):
        _seed_stack_embeddings(
            db["scope"], stack_id=sid,
            slice_indices=range(7), vec_for=vec_for,
        )

    load_calls = {"n": 0}
    def fake_load(path):
        load_calls["n"] += 1
        return _brain_centroid_classifier()
    loader = ClassifierLoader(joblib_load=fake_load)

    items = [StackInferenceRequest(stack_id=s, num_slices=7) for s in (10, 11, 12)]
    with db["scope"]() as s:
        preds = run_inference(
            db=s, classifier_path="/fake/m.joblib",
            classifier_sha256="abc", classes=["Brain", "Spine"],
            encoder_chain=[BC, SL], items=items, loader=loader,
        )
    assert [p.label for p in preds] == ["Brain", "Brain", "Brain"]
    assert load_calls["n"] == 1  # cached after first load


def test_run_inference_empty_returns_empty(db):
    loader = ClassifierLoader(joblib_load=lambda _: None)
    with db["scope"]() as s:
        out = run_inference(
            db=s, classifier_path="/x", classifier_sha256="x",
            classes=["Brain"], encoder_chain=[BC, SL],
            items=[], loader=loader,
        )
    assert out == []


def test_classifier_loader_keys_on_path_and_sha():
    calls = {"n": 0}
    def fake_load(path):
        calls["n"] += 1
        return ("loaded", path)
    loader = ClassifierLoader(joblib_load=fake_load)
    a = loader.load("/p.joblib", "sha1")
    b = loader.load("/p.joblib", "sha1")
    c = loader.load("/p.joblib", "sha2")  # different sha → reload
    assert a is b
    assert a is not c
    assert calls["n"] == 2


def test_predict_stack_respects_threshold_override(db):
    def vec_for(_sid, _idx, enc):
        v = np.zeros(enc.dim, dtype=np.float32)
        v[0] = 1.0
        return v
    _seed_stack_embeddings(db["scope"], stack_id=6,
                           slice_indices=range(7), vec_for=vec_for)
    clf = _brain_centroid_classifier()

    with db["scope"]() as s:
        # Default threshold (0.70) — high-confidence prediction is fine.
        pred = predict_stack(
            estimator=clf, classes=["Brain", "Spine"],
            db=s, stack_id=6, num_slices=7, encoder_chain=[BC, SL],
        )
        assert pred.needs_check is False
        # An impossibly-strict threshold flips it to needs_check.
        pred2 = predict_stack(
            estimator=clf, classes=["Brain", "Spine"],
            db=s, stack_id=6, num_slices=7, encoder_chain=[BC, SL],
            config=InferConfig(n_slices=3, manual_review_below=1.5),
        )
        assert pred2.needs_check is True


def test_predict_stack_n_slices_override(db):
    def vec_for(_sid, _idx, enc):
        v = np.zeros(enc.dim, dtype=np.float32)
        v[0] = 1.0
        return v
    _seed_stack_embeddings(db["scope"], stack_id=7,
                           slice_indices=range(11), vec_for=vec_for)
    clf = _brain_centroid_classifier()
    with db["scope"]() as s:
        pred = predict_stack(
            estimator=clf, classes=["Brain", "Spine"],
            db=s, stack_id=7, num_slices=11, encoder_chain=[BC, SL],
            config=InferConfig(n_slices=5, manual_review_below=0.7),
        )
    assert pred.n_slices_used == 5
