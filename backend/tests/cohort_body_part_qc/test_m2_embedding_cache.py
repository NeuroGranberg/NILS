"""M2 — embedding cache round-trip + version pinning tests."""

from __future__ import annotations

from contextlib import contextmanager

import numpy as np
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from cohorts.models import Base, StackEmbeddingCache

# Import models that have relationships back to Cohort so the SQLAlchemy
# registry is fully populated when the in-memory engine initializes
# mappers. Without this import, ``Cohort`` fails to configure because of
# the ``NilsDatasetPipelineStep.sort_order`` reference.
import nils_dataset_pipeline.models  # noqa: F401

from qc.body_part import embedding_cache as ec_mod
from qc.body_part.embedding_cache import (
    CacheKey,
    batch_get_or_compute,
    get_or_compute,
    read_many,
    read_one,
    write_many,
)


@pytest.fixture
def db(monkeypatch):
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
    yield {"engine": engine, "scope": _scope, "SessionLocal": SessionLocal}


def _arr(*xs):
    return np.array(xs, dtype=np.float32)


# ---------------------------------------------------------------------------
# write/read round-trip
# ---------------------------------------------------------------------------


def test_write_read_round_trip(db):
    key = CacheKey(1, 0, "biomedclip", "v1.0.0")
    arr = _arr(0.1, 0.2, 0.3, 0.4)
    with db["scope"]() as s:
        n = write_many(s, [(key, arr)])
        assert n == 1
    with db["scope"]() as s:
        out = read_one(s, key, expected_dim=4)
        assert out is not None
        np.testing.assert_array_equal(out, arr)


def test_read_one_returns_none_on_miss(db):
    with db["scope"]() as s:
        assert read_one(s, CacheKey(99, 0, "biomedclip", "v1.0.0"), 4) is None


def test_dim_mismatch_raises(db):
    key = CacheKey(1, 0, "siglip2", "v1.0.0")
    arr = _arr(1.0, 2.0)  # dim=2
    with db["scope"]() as s:
        write_many(s, [(key, arr)])
    with db["scope"]() as s:
        with pytest.raises(ValueError):
            read_one(s, key, expected_dim=8)


def test_upsert_replaces_value(db):
    key = CacheKey(1, 0, "biomedclip", "v1.0.0")
    a = _arr(1.0, 0.0, 0.0, 0.0)
    b = _arr(0.0, 1.0, 0.0, 0.0)
    with db["scope"]() as s:
        write_many(s, [(key, a)])
    with db["scope"]() as s:
        write_many(s, [(key, b)])  # upsert
    with db["scope"]() as s:
        out = read_one(s, key, expected_dim=4)
        np.testing.assert_array_equal(out, b)


def test_version_pinning_keeps_separate_rows(db):
    k1 = CacheKey(1, 0, "biomedclip", "v1.0.0")
    k2 = CacheKey(1, 0, "biomedclip", "v2.0.0")
    a = _arr(1, 2, 3, 4)
    b = _arr(5, 6, 7, 8)
    with db["scope"]() as s:
        write_many(s, [(k1, a), (k2, b)])
    with db["scope"]() as s:
        np.testing.assert_array_equal(read_one(s, k1, 4), a)
        np.testing.assert_array_equal(read_one(s, k2, 4), b)
        rows = s.query(StackEmbeddingCache).all()
        assert len(rows) == 2


# ---------------------------------------------------------------------------
# get_or_compute
# ---------------------------------------------------------------------------


def test_get_or_compute_cache_hit(db):
    key = CacheKey(1, 0, "biomedclip", "v1.0.0")
    arr = _arr(0.5, 0.5, 0.5, 0.5)
    with db["scope"]() as s:
        write_many(s, [(key, arr)])

    calls = {"n": 0}
    def compute():
        calls["n"] += 1
        return _arr(9, 9, 9, 9)

    out = get_or_compute(key, expected_dim=4, compute_fn=compute)
    np.testing.assert_array_equal(out, arr)
    assert calls["n"] == 0


def test_get_or_compute_cache_miss_writes(db):
    key = CacheKey(2, 1, "biomedclip", "v1.0.0")
    calls = {"n": 0}
    def compute():
        calls["n"] += 1
        return _arr(7, 7, 7, 7)

    out = get_or_compute(key, expected_dim=4, compute_fn=compute)
    np.testing.assert_array_equal(out, _arr(7, 7, 7, 7))
    assert calls["n"] == 1

    # Re-fetching should now be a hit.
    out2 = get_or_compute(key, expected_dim=4, compute_fn=compute)
    np.testing.assert_array_equal(out2, out)
    assert calls["n"] == 1


def test_get_or_compute_dim_mismatch(db):
    key = CacheKey(3, 0, "biomedclip", "v1.0.0")
    def compute():
        return _arr(1, 2)  # wrong dim
    with pytest.raises(ValueError):
        get_or_compute(key, expected_dim=4, compute_fn=compute)


# ---------------------------------------------------------------------------
# batch_get_or_compute
# ---------------------------------------------------------------------------


def test_batch_get_or_compute_mixed(db):
    k_hit = CacheKey(1, 0, "biomedclip", "v1.0.0")
    k_miss_a = CacheKey(2, 0, "biomedclip", "v1.0.0")
    k_miss_b = CacheKey(3, 0, "biomedclip", "v1.0.0")

    pre = _arr(0.1, 0.2, 0.3, 0.4)
    with db["scope"]() as s:
        write_many(s, [(k_hit, pre)])

    calls = {"n": 0}
    def make_fn(value):
        def fn():
            calls["n"] += 1
            return _arr(*value)
        return fn

    out = batch_get_or_compute(
        [
            (k_hit, make_fn([0, 0, 0, 0])),
            (k_miss_a, make_fn([1, 0, 0, 0])),
            (k_miss_b, make_fn([0, 1, 0, 0])),
        ],
        expected_dim=4,
    )

    assert calls["n"] == 2  # only the misses
    np.testing.assert_array_equal(out[k_hit], pre)
    np.testing.assert_array_equal(out[k_miss_a], _arr(1, 0, 0, 0))
    np.testing.assert_array_equal(out[k_miss_b], _arr(0, 1, 0, 0))

    # Both misses are now persisted.
    with db["scope"]() as s:
        rows = s.query(StackEmbeddingCache).all()
        assert len(rows) == 3


# ---------------------------------------------------------------------------
# read_many — chunking (regression for Postgres StatementTooComplex on
# cohorts with thousands of stacks).
# ---------------------------------------------------------------------------


def test_read_many_empty_returns_empty_dict(db):
    with db["scope"]() as s:
        assert read_many(s, [], expected_dim=4) == {}


def test_read_many_below_chunk_returns_all_hits(db):
    keys = [CacheKey(i, 0, "biomedclip", "v1.0.0") for i in range(5)]
    arrs = [_arr(i, i, i, i) for i in range(5)]
    with db["scope"]() as s:
        write_many(s, list(zip(keys, arrs)))

    with db["scope"]() as s:
        out = read_many(s, keys, expected_dim=4)
    assert len(out) == 5
    for k, a in zip(keys, arrs):
        np.testing.assert_array_equal(out[k], a)


def test_read_many_chunked_across_boundary(db, monkeypatch):
    """With CHUNK=3 and 7 keys, read_many must run 3 sub-queries and
    merge results identically to a single-query read."""
    monkeypatch.setenv("BODY_PART_READ_MANY_CHUNK", "3")

    keys = [CacheKey(i, 0, "biomedclip", "v1.0.0") for i in range(7)]
    arrs = [_arr(i + 1, 0, 0, 0) for i in range(7)]
    with db["scope"]() as s:
        write_many(s, list(zip(keys, arrs)))

    with db["scope"]() as s:
        out = read_many(s, keys, expected_dim=4)
    assert len(out) == 7
    for k, a in zip(keys, arrs):
        np.testing.assert_array_equal(out[k], a)


def test_read_many_chunked_with_misses(db, monkeypatch):
    """Misses across chunk boundaries must simply be absent from the
    output dict; no exception, no wrong-key entries."""
    monkeypatch.setenv("BODY_PART_READ_MANY_CHUNK", "2")

    # 6 requested; only the even ones are persisted.
    requested = [CacheKey(i, 0, "biomedclip", "v1.0.0") for i in range(6)]
    persisted = [CacheKey(i, 0, "biomedclip", "v1.0.0") for i in (0, 2, 4)]
    arrs = [_arr(i, 0, 0, 0) for i in (0, 2, 4)]
    with db["scope"]() as s:
        write_many(s, list(zip(persisted, arrs)))

    with db["scope"]() as s:
        out = read_many(s, requested, expected_dim=4)

    assert set(out.keys()) == set(persisted)
    for k, a in zip(persisted, arrs):
        np.testing.assert_array_equal(out[k], a)


def test_read_many_chunk_invalid_env_falls_back_to_default(db, monkeypatch):
    """Bad chunk-size env vars must not break read_many."""
    monkeypatch.setenv("BODY_PART_READ_MANY_CHUNK", "not-a-number")

    key = CacheKey(1, 0, "biomedclip", "v1.0.0")
    arr = _arr(0.5, 0.5, 0.5, 0.5)
    with db["scope"]() as s:
        write_many(s, [(key, arr)])
    with db["scope"]() as s:
        out = read_many(s, [key], expected_dim=4)
    np.testing.assert_array_equal(out[key], arr)


def test_read_many_partitions_by_encoder_and_chunks_each(db, monkeypatch):
    """Each (encoder_name, encoder_version) partition is independently
    chunked. Mixing encoders should still return everything."""
    monkeypatch.setenv("BODY_PART_READ_MANY_CHUNK", "2")

    bc_keys = [CacheKey(i, 0, "biomedclip", "v1.0.0") for i in range(3)]
    sg_keys = [CacheKey(i, 0, "siglip2", "v1.0.0") for i in range(3)]
    bc_arrs = [_arr(i, 1, 0, 0) for i in range(3)]
    sg_arrs = [_arr(i, 2, 0, 0) for i in range(3)]
    with db["scope"]() as s:
        write_many(s, list(zip(bc_keys, bc_arrs)) + list(zip(sg_keys, sg_arrs)))

    with db["scope"]() as s:
        out = read_many(s, bc_keys + sg_keys, expected_dim=4)

    for k, a in zip(bc_keys, bc_arrs):
        np.testing.assert_array_equal(out[k], a)
    for k, a in zip(sg_keys, sg_arrs):
        np.testing.assert_array_equal(out[k], a)
