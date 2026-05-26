"""Unit tests for the in-process MASQC session-list cache."""

from __future__ import annotations

import time

import pytest

from qc import main_acquisition_cache as cache


@pytest.fixture(autouse=True)
def _clean_cache():
    cache.clear_all()
    yield
    cache.clear_all()


def test_set_get_roundtrip():
    key = cache.make_key(1, {"base": ["t1"]}, "code", False)
    cache.set(key, {"sessions": ["a", "b"]})
    assert cache.get(key) == {"sessions": ["a", "b"]}


def test_miss_returns_none():
    key = cache.make_key(1, {}, "code", False)
    assert cache.get(key) is None


def test_filters_hash_is_canonical():
    """Filter dicts that differ only by list order / key order hash equally."""
    k1 = cache.make_key(1, {"base": ["t1", "t2"], "tech": ["a"]}, "code", False)
    k2 = cache.make_key(1, {"tech": ["a"], "base": ["t2", "t1"]}, "code", False)
    assert k1 == k2


def test_key_distinguishes_display_id_type_and_multistack():
    base = {"base": ["t1"]}
    k_code = cache.make_key(1, base, "code", False)
    k_broms = cache.make_key(1, base, "BROMS", False)
    k_multi = cache.make_key(1, base, "code", True)
    assert k_code != k_broms
    assert k_code != k_multi
    assert k_broms != k_multi


def test_invalidate_cohort_drops_only_that_cohort():
    k1 = cache.make_key(1, {}, "code", False)
    k2 = cache.make_key(2, {}, "code", False)
    cache.set(k1, {"v": 1})
    cache.set(k2, {"v": 2})
    cache.invalidate_cohort(1)
    assert cache.get(k1) is None
    assert cache.get(k2) == {"v": 2}


def test_ttl_expiry(monkeypatch):
    """A cached entry past its TTL should not be returned."""
    base_now = [1000.0]
    monkeypatch.setattr(cache, "_TTL_SECONDS", 5.0)
    monkeypatch.setattr(cache.time, "monotonic", lambda: base_now[0])

    key = cache.make_key(1, {}, "code", False)
    cache.set(key, "fresh")
    assert cache.get(key) == "fresh"

    base_now[0] += 10  # well past TTL
    assert cache.get(key) is None
