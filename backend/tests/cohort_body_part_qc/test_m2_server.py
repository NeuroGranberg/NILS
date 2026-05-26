"""M2 — worker server tests: /health and /embed via FastAPI TestClient.

Both encoders are replaced with fakes; ``_resolve_paths`` is monkeypatched
so we don't need real DICOM files; ``_load_and_preprocess`` is overridden
to return synthetic arrays. The point is to validate the wiring contract,
not the encoders or pixel loader (those have their own tests).
"""

from __future__ import annotations

from contextlib import contextmanager

import numpy as np
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from cohorts.models import Base, StackEmbeddingCache

from qc.body_part import embedding_cache as ec_mod
from qc.body_part import server as server_mod
from qc.body_part.encoders import (
    BaseEncoder,
    EncoderInfo,
    EncoderRegistry,
)


class FakeBC(BaseEncoder):
    info = EncoderInfo(
        name="biomedclip", version="v1.0.0", dim_image=4, dim_text=4,
    )
    def encode_images(self, images):
        if not images:
            return np.zeros((0, 4), dtype=np.float32)
        out = np.array(
            [[float(arr.mean()), 0.0, 0.0, 1.0] for arr in images],
            dtype=np.float32,
        )
        norms = np.linalg.norm(out, axis=1, keepdims=True).clip(min=1e-12)
        return out / norms
    def encode_texts(self, prompts):
        return np.zeros((len(prompts), 4), dtype=np.float32)


class FakeSL(BaseEncoder):
    info = EncoderInfo(
        name="siglip2", version="v1.0.0", dim_image=2, dim_text=None,
    )
    def encode_images(self, images):
        if not images:
            return np.zeros((0, 2), dtype=np.float32)
        out = np.array([[float(arr.std()), 1.0] for arr in images], dtype=np.float32)
        norms = np.linalg.norm(out, axis=1, keepdims=True).clip(min=1e-12)
        return out / norms


# ---------------------------------------------------------------------------
# Shared fixture: in-mem app DB + fake encoders + path/load shims
# ---------------------------------------------------------------------------


@pytest.fixture
def client(monkeypatch):
    # Share one connection across all sessions so the in-memory schema is
    # visible to every `session_scope()` call.
    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(
        bind=engine, autoflush=False, autocommit=False,
        expire_on_commit=False, future=True,
    )
    Base.metadata.create_all(
        engine,
        tables=[
            t for t in Base.metadata.tables.values()
            if t.name == "stack_embedding_cache"
        ],
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

    # Replace path resolution and pixel loading to keep the test hermetic.
    def fake_resolve_paths(items):
        return [
            server_mod._ResolvedItem(
                stack_id=it.stack_id, slice_index=it.slice_index,
                instance_id=10_000 + it.stack_id * 100 + it.slice_index,
                file_path=f"/tmp/fake/{it.stack_id}_{it.slice_index}.dcm",
                frame_index=0,
            )
            for it in items
        ]
    monkeypatch.setattr(server_mod, "_resolve_paths", fake_resolve_paths)

    def fake_load(item):
        # Distinct per-stack signal so the encoder sees variability.
        seed = (item.stack_id * 7 + item.slice_index) % 251
        rng = np.random.default_rng(seed)
        return rng.integers(0, 256, size=(224, 224, 3), dtype=np.uint8)
    monkeypatch.setattr(server_mod, "_load_and_preprocess", fake_load)

    c = TestClient(server_mod.app)
    yield c, _scope, engine
    EncoderRegistry.reset()


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------


def test_health_basic(client):
    c, _, _ = client
    r = c.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["device"] in {"cpu", "cuda", "mps"}
    assert body["preprocess_version"] == "v1"
    # Encoders are loaded eagerly via set_for_testing.
    assert set(body["encoders_loaded"]) == {"biomedclip", "siglip2"}


# ---------------------------------------------------------------------------
# /embed
# ---------------------------------------------------------------------------


def test_embed_concat_writes_per_encoder_rows(client):
    c, scope, _ = client
    payload = {
        "items": [
            {"stack_id": 1, "slice_index": 0},
            {"stack_id": 1, "slice_index": 1},
            {"stack_id": 2, "slice_index": 0},
        ],
        "mode": "concat",
    }
    r = c.post("/embed", json=payload)
    assert r.status_code == 200, r.text
    body = r.json()

    assert body["encoder_chain"] == ["biomedclip", "siglip2"]
    assert body["dim_total"] == 6  # 4 + 2
    assert body["n_items"] == 3
    # 2 encoders × 3 items, no cache hits initially.
    assert body["n_computed"] == 6
    assert body["n_cached"] == 0
    assert len(body["cached_keys"]) == 6

    # Verify both encoder rows landed for each item.
    with scope() as s:
        rows = s.query(StackEmbeddingCache).all()
        assert len(rows) == 6
        per_encoder = {r.encoder_name for r in rows}
        assert per_encoder == {"biomedclip", "siglip2"}


def test_embed_second_call_uses_cache(client):
    c, scope, _ = client
    payload = {
        "items": [{"stack_id": 5, "slice_index": 0}],
        "mode": "biomedclip",
    }
    r1 = c.post("/embed", json=payload)
    assert r1.status_code == 200
    assert r1.json()["n_computed"] == 1
    assert r1.json()["n_cached"] == 0

    r2 = c.post("/embed", json=payload)
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["n_computed"] == 0
    assert body2["n_cached"] == 1


def test_embed_single_encoder_mode(client):
    c, _, _ = client
    payload = {
        "items": [{"stack_id": 7, "slice_index": 0}],
        "mode": "siglip2",
    }
    r = c.post("/embed", json=payload)
    assert r.status_code == 200
    body = r.json()
    assert body["encoder_chain"] == ["siglip2"]
    assert body["dim_total"] == 2


def test_embed_empty_items(client):
    c, _, _ = client
    r = c.post("/embed", json={"items": [], "mode": "concat"})
    assert r.status_code == 200
    body = r.json()
    assert body["n_items"] == 0
    assert body["n_computed"] == 0
    assert body["dim_total"] == 0


def test_embed_unknown_mode_returns_400(client):
    c, _, _ = client
    r = c.post("/embed", json={"items": [{"stack_id": 1, "slice_index": 0}], "mode": "xx"})
    assert r.status_code == 400
    assert "encoder mode" in r.json()["detail"].lower()


def test_embed_no_resolvable_items_returns_404(client, monkeypatch):
    c, _, _ = client
    monkeypatch.setattr(server_mod, "_resolve_paths", lambda items: [])
    r = c.post("/embed", json={"items": [{"stack_id": 1, "slice_index": 0}], "mode": "concat"})
    assert r.status_code == 404
