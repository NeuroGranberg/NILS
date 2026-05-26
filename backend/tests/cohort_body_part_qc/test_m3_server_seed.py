"""M3 — worker /seed endpoint test."""

from __future__ import annotations

from contextlib import contextmanager

import numpy as np
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from cohorts.models import Base

from qc.body_part import embedding_cache as ec_mod
from qc.body_part import server as server_mod
from qc.body_part.encoders import BaseEncoder, EncoderInfo, EncoderRegistry


class FakeBC(BaseEncoder):
    """BiomedCLIP fake — encodes images as a deterministic 4-D vector
    keyed by stack_id, and encodes "brain"/"spine" prompts to the
    matching unit basis vector."""

    info = EncoderInfo(
        name="biomedclip", version="v1.0.0", dim_image=4, dim_text=4,
    )

    def encode_images(self, images):
        # Each "image" carries its stack_id encoded in pixel mean — see
        # the fixture's fake_load below. We map that to a basis vector.
        out = []
        for arr in images:
            mean = float(arr.mean())
            # Even stack_id → "brain" basis [1,0,0,0]; odd → "spine" [0,1,0,0]
            v = np.zeros(4, dtype=np.float32)
            if int(round(mean)) % 2 == 0:
                v[0] = 1.0
            else:
                v[1] = 1.0
            # Add tiny jitter so FPS produces distinct picks.
            v += np.random.default_rng(int(round(mean))).normal(0, 0.01, 4).astype(np.float32)
            v /= max(np.linalg.norm(v), 1e-12)
            out.append(v)
        return np.stack(out, axis=0).astype(np.float32)

    def encode_texts(self, prompts):
        out = []
        for p in prompts:
            v = np.zeros(4, dtype=np.float32)
            if "brain" in p.lower() or "head" in p.lower():
                v[0] = 1.0
            elif "spine" in p.lower() or "vertebra" in p.lower() or "cervical" in p.lower() \
                    or "thoracic" in p.lower() or "lumbar" in p.lower():
                v[1] = 1.0
            else:
                v[2] = 1.0  # generic "elsewhere" axis
            out.append(v)
        return np.stack(out, axis=0).astype(np.float32)


@pytest.fixture
def client(monkeypatch):
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

    EncoderRegistry.set_for_testing(biomedclip=FakeBC(), siglip2=None)

    def fake_resolve_paths(items):
        return [
            server_mod._ResolvedItem(
                stack_id=it.stack_id, slice_index=it.slice_index,
                instance_id=10_000 + it.stack_id,
                file_path=f"/tmp/{it.stack_id}.dcm", frame_index=0,
            )
            for it in items
        ]
    monkeypatch.setattr(server_mod, "_resolve_paths", fake_resolve_paths)

    def fake_load(item):
        # mean = stack_id so FakeBC picks the right basis.
        return np.full((224, 224, 3), item.stack_id % 256, dtype=np.uint8)
    monkeypatch.setattr(server_mod, "_load_and_preprocess", fake_load)

    yield TestClient(server_mod.app)
    EncoderRegistry.reset()


def test_seed_returns_brain_candidates_only(client):
    # 6 even stack_ids (Brain) + 6 odd (Spine) — the seeder should
    # pick only even ones for category="Brain".
    items = [{"stack_id": i, "slice_index": 0} for i in range(2, 14)]
    r = client.post(
        "/seed",
        json={"category": "Brain", "items": items, "n_target": 10},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["category"] == "Brain"
    assert body["encoder_name"] == "biomedclip"
    assert body["n_input"] == 12
    picked_ids = [c["stack_id"] for c in body["candidates"]]
    assert all(sid % 2 == 0 for sid in picked_ids), picked_ids
    assert len(picked_ids) > 0
    # zs_prob should be high for these.
    assert all(c["zs_prob"] >= 0.5 for c in body["candidates"])


def test_seed_empty_items(client):
    r = client.post(
        "/seed",
        json={"category": "Brain", "items": [], "n_target": 10},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["n_input"] == 0
    assert body["candidates"] == []


def test_seed_uses_cache_on_second_call(client):
    items = [{"stack_id": i, "slice_index": 0} for i in (2, 4, 6, 8)]
    r1 = client.post(
        "/seed",
        json={"category": "Brain", "items": items, "n_target": 5},
    )
    assert r1.status_code == 200

    # Second call: same items, the embeddings are cached, so the load
    # callback should be unused. We verify by replacing it with a
    # raise-on-call sentinel.
    import qc.body_part.server as srv_mod
    def boom(item):  # noqa: ARG001
        raise AssertionError("cache miss — should not load DICOM")
    srv_mod._load_and_preprocess = boom

    r2 = client.post(
        "/seed",
        json={"category": "Brain", "items": items, "n_target": 5},
    )
    assert r2.status_code == 200, r2.text


def test_seed_no_resolvable_returns_404(client, monkeypatch):
    monkeypatch.setattr(server_mod, "_resolve_paths", lambda items: [])
    r = client.post(
        "/seed",
        json={"category": "Brain",
              "items": [{"stack_id": 1, "slice_index": 0}], "n_target": 5},
    )
    assert r.status_code == 404
