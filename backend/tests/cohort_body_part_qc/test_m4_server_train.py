"""M4 — worker /train endpoint test."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from cohorts.models import Base
from qc.body_part import server as server_mod
from qc.body_part import embedding_cache as ec_mod
from qc.body_part.embedding_cache import CacheKey, write_many
from qc.body_part.encoders import BaseEncoder, EncoderInfo, EncoderRegistry


class FakeBC(BaseEncoder):
    info = EncoderInfo(name="biomedclip", version="v1.0.0", dim_image=4)

    def encode_images(self, images):
        return np.zeros((len(images), 4), dtype=np.float32)


class FakeSL(BaseEncoder):
    info = EncoderInfo(name="siglip2", version="v1.0.0", dim_image=2)

    def encode_images(self, images):
        return np.zeros((len(images), 2), dtype=np.float32)


@pytest.fixture
def client(monkeypatch, tmp_path):
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

    monkeypatch.setattr(ec_mod, "session_scope", _scope)
    monkeypatch.setattr(server_mod, "session_scope", _scope)

    EncoderRegistry.set_for_testing(biomedclip=FakeBC(), siglip2=FakeSL())

    # Redirect artefact writes to tmp_path.
    monkeypatch.setattr(server_mod, "ARTEFACTS_DIR", str(tmp_path))

    # Inject deterministic estimator factory + persister so we don't need sklearn.
    from tests.cohort_body_part_qc.test_m4_train_pipeline import (
        _fake_estimator_factory,
        _fake_persist,
    )

    # Patch train_classifier to use the fakes by default.
    import qc.body_part.server as srv
    real_train = srv.train_classifier
    def patched_train(*args, **kwargs):
        kwargs.setdefault("estimator_factory", _fake_estimator_factory)
        kwargs.setdefault("persist", _fake_persist)
        return real_train(*args, **kwargs)
    monkeypatch.setattr(srv, "train_classifier", patched_train)

    yield TestClient(server_mod.app), _scope
    EncoderRegistry.reset()


def _seed_cache(scope, samples, encoder_chain, *, label_to_vec):
    items = []
    for s in samples:
        for enc in encoder_chain:
            v = label_to_vec(s["label"], enc)
            items.append((CacheKey(
                series_stack_id=s["stack_id"], slice_index=s["slice_index"],
                encoder_name=enc["name"], encoder_version=enc["version"],
            ), v))
    with scope() as db:
        write_many(db, items)


def test_train_returns_artefact_and_meta(client):
    c, scope = client
    encoders = [
        {"name": "biomedclip", "version": "v1.0.0", "dim": 4},
        {"name": "siglip2", "version": "v1.0.0", "dim": 2},
    ]
    samples = []
    for i in range(8):
        samples.append({"stack_id": 100 + i, "slice_index": 0, "label": "Brain"})
    for i in range(8):
        samples.append({"stack_id": 200 + i, "slice_index": 0, "label": "Spine"})

    def vec(label, enc):
        v = np.zeros(enc["dim"], dtype=np.float32)
        v[0 if label == "Brain" else 1 % enc["dim"]] = 1.0
        return v

    _seed_cache(scope, samples, encoders, label_to_vec=vec)

    r = c.post("/train", json={
        "cohort_id": 1, "samples": samples, "mode": "concat",
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["classifier_path"].endswith("cohort_1_body_part_qc.joblib")
    assert len(body["classifier_sha256"]) == 64
    assert body["classifier_meta"]["classes"] == ["Brain", "Spine"]
    assert body["classifier_meta"]["per_class_counts"] == {"Brain": 8, "Spine": 8}


def test_train_409_when_samples_uncached(client):
    c, _ = client
    samples = [
        {"stack_id": 999, "slice_index": 0, "label": "Brain"},
        {"stack_id": 998, "slice_index": 0, "label": "Spine"},
    ]
    r = c.post("/train", json={
        "cohort_id": 1, "samples": samples, "mode": "concat",
    })
    assert r.status_code == 409
    assert "embedded" in r.json()["detail"].lower() or \
           "cached" in r.json()["detail"].lower()


def test_train_400_on_empty_samples(client):
    c, _ = client
    r = c.post("/train", json={"cohort_id": 1, "samples": [], "mode": "concat"})
    assert r.status_code == 400


def test_train_400_on_unknown_mode(client):
    c, _ = client
    r = c.post("/train", json={
        "cohort_id": 1,
        "samples": [{"stack_id": 1, "slice_index": 0, "label": "Brain"}],
        "mode": "xyz",
    })
    assert r.status_code == 400


def test_train_409_on_single_class(client):
    c, scope = client
    encoders = [
        {"name": "biomedclip", "version": "v1.0.0", "dim": 4},
        {"name": "siglip2", "version": "v1.0.0", "dim": 2},
    ]
    samples = [
        {"stack_id": 100 + i, "slice_index": 0, "label": "Brain"}
        for i in range(5)
    ]

    def vec(label, enc):
        v = np.zeros(enc["dim"], dtype=np.float32)
        v[0] = 1.0
        return v

    _seed_cache(scope, samples, encoders, label_to_vec=vec)
    r = c.post("/train", json={
        "cohort_id": 1, "samples": samples, "mode": "concat",
    })
    assert r.status_code == 409
