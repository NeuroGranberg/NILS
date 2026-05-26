"""M5 — worker /infer endpoint test."""

from __future__ import annotations

from contextlib import contextmanager

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


@pytest.fixture
def client(monkeypatch):
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

    # Inject a fake classifier loader so we don't need joblib.
    from tests.cohort_body_part_qc.test_m5_infer import (
        _brain_centroid_classifier,
    )

    class FakeLoader:
        def load(self, path, sha):
            return _brain_centroid_classifier()
    monkeypatch.setattr(
        server_mod, "_default_loader", FakeLoader(), raising=False,
    )
    # The endpoint reads the loader from the module namespace, so make
    # sure ``run_inference`` uses ours.
    import qc.body_part.infer as infer_mod
    monkeypatch.setattr(infer_mod, "_default_loader", FakeLoader())

    yield TestClient(server_mod.app), _scope


def _seed(scope, stack_id, n_slices, label):
    items = []
    for idx in range(n_slices):
        for enc in [("biomedclip", "v1.0.0", 4), ("siglip2", "v1.0.0", 2)]:
            v = np.zeros(enc[2], dtype=np.float32)
            v[0 if label == "Brain" else 1 % enc[2]] = 1.0
            items.append((CacheKey(stack_id, idx, enc[0], enc[1]), v))
    with scope() as db:
        write_many(db, items)


def _payload(items):
    return {
        "classifier_path": "/fake/m.joblib",
        "classifier_sha256": "abc",
        "classes": ["Brain", "Spine"],
        "encoder_chain": [
            {"name": "biomedclip", "version": "v1.0.0", "dim": 4},
            {"name": "siglip2", "version": "v1.0.0", "dim": 2},
        ],
        "items": items,
    }


def test_infer_returns_predictions(client):
    c, scope = client
    _seed(scope, stack_id=1, n_slices=11, label="Brain")
    _seed(scope, stack_id=2, n_slices=11, label="Spine")

    r = c.post("/infer", json=_payload([
        {"stack_id": 1, "num_slices": 11},
        {"stack_id": 2, "num_slices": 11},
    ]))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["n_items"] == 2
    assert body["n_predicted"] == 2
    by_id = {r["stack_id"]: r for r in body["results"]}
    assert by_id[1]["label"] == "Brain"
    assert by_id[2]["label"] == "Spine"
    assert by_id[1]["n_slices_used"] == 3


def test_infer_flags_low_confidence(client):
    c, scope = client
    # Ambiguous embeddings → low-confidence flag.
    items = []
    for idx in range(7):
        for enc in [("biomedclip", "v1.0.0", 4), ("siglip2", "v1.0.0", 2)]:
            v = np.zeros(enc[2], dtype=np.float32)  # all-zero → uniform softmax
            items.append((CacheKey(50, idx, enc[0], enc[1]), v))
    with scope() as db:
        write_many(db, items)

    r = c.post("/infer", json=_payload([{"stack_id": 50, "num_slices": 7}]))
    body = r.json()
    assert body["n_low_conf"] == 1
    assert body["results"][0]["needs_check"] is True


def test_infer_handles_missing_embeddings(client):
    c, _ = client
    r = c.post("/infer", json=_payload([{"stack_id": 9999, "num_slices": 5}]))
    body = r.json()
    assert body["n_no_embeddings"] == 1
    assert body["results"][0]["label"] is None
    assert body["results"][0]["n_slices_used"] == 0


def test_infer_empty_items(client):
    c, _ = client
    r = c.post("/infer", json=_payload([]))
    assert r.status_code == 200
    assert r.json()["n_items"] == 0


def test_infer_threshold_override(client):
    c, scope = client
    _seed(scope, stack_id=1, n_slices=11, label="Brain")

    payload = _payload([{"stack_id": 1, "num_slices": 11}])
    payload["manual_review_below"] = 1.5  # impossibly strict
    r = c.post("/infer", json=payload)
    assert r.json()["results"][0]["needs_check"] is True
