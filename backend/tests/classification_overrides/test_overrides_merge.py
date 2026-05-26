"""Tests for classification.overrides merge logic.

These tests exercise the pure-functional merge layer: YAML in + delta in ->
merged dict out. No DB, no pipeline, just the contract.
"""

from __future__ import annotations

import pytest

from classification.overrides import (
    EDITABLE_BUCKETS,
    apply_bucket_override,
    build_catalog,
    expand_patterns,
    get_by_path,
    is_valid_bucket,
    merge_overrides,
    set_by_path,
)


# -----------------------------------------------------------------------------
# apply_bucket_override
# -----------------------------------------------------------------------------


def test_apply_bucket_override_empty_delta_is_identity():
    defaults = ["t1w", "t1 w", "t1-w"]
    assert apply_bucket_override(defaults, [], []) == defaults


def test_apply_bucket_override_adds_in_order():
    defaults = ["a", "b"]
    assert apply_bucket_override(defaults, ["c", "d"], []) == ["a", "b", "c", "d"]


def test_apply_bucket_override_removes_case_insensitive():
    defaults = ["Foo", "bar", "baz"]
    assert apply_bucket_override(defaults, [], ["FOO"]) == ["bar", "baz"]


def test_apply_bucket_override_add_and_remove():
    defaults = ["a", "b", "c"]
    out = apply_bucket_override(defaults, ["d"], ["b"])
    assert out == ["a", "c", "d"]


def test_apply_bucket_override_dedup_case_insensitive():
    # If user tries to add a keyword that already exists, no dup.
    defaults = ["Foo"]
    out = apply_bucket_override(defaults, ["foo", "bar"], [])
    assert out == ["Foo", "bar"]


def test_apply_bucket_override_drops_empty_preserves_whitespace():
    # Leading/trailing whitespace is preserved (some defaults like " -c"
    # rely on it for word-boundary matching). Empty/whitespace-only dropped.
    defaults = ["a"]
    out = apply_bucket_override(defaults, ["  b  ", "", "   "], [])
    assert out == ["a", "  b  "]


# -----------------------------------------------------------------------------
# get_by_path / set_by_path / expand_patterns
# -----------------------------------------------------------------------------


def test_get_by_path_returns_none_for_missing():
    doc = {"a": {"b": [1, 2]}}
    assert get_by_path(doc, "a.b") == [1, 2]
    assert get_by_path(doc, "a.c") is None
    assert get_by_path(doc, "nope") is None


def test_set_by_path_creates_nested_dicts():
    doc: dict = {}
    set_by_path(doc, "a.b.c", [1, 2, 3])
    assert doc == {"a": {"b": {"c": [1, 2, 3]}}}


def test_expand_patterns_wildcards():
    doc = {"bases": {"T1w": {"keywords": []}, "T2w": {"keywords": []}}}
    out = expand_patterns(doc, ["bases.*.keywords"])
    assert set(out) == {"bases.T1w.keywords", "bases.T2w.keywords"}


def test_expand_patterns_exact_path_passthrough():
    doc = {"negative_keywords": []}
    out = expand_patterns(doc, ["negative_keywords"])
    assert out == ["negative_keywords"]


def test_expand_patterns_missing_segment_returns_empty():
    doc = {"bases": {"T1w": {}}}  # no "keywords" child
    out = expand_patterns(doc, ["bases.*.keywords"])
    assert out == []


# -----------------------------------------------------------------------------
# merge_overrides end-to-end on real YAML
# -----------------------------------------------------------------------------


def test_merge_overrides_empty_delta_returns_identical_docs():
    merged = merge_overrides(None, {})
    # Every editable bucket still has a list and physics/flags remain.
    base_doc = merged["base-detection.yaml"]
    assert isinstance(base_doc["bases"]["T1w"]["keywords"], list)
    assert "physics" in base_doc["bases"]["T1w"]


def test_merge_overrides_adds_and_removes_in_real_file():
    delta = {
        ("base", "bases.T1w.keywords"): {
            "added": ["t1_custom"],
            "removed": ["t1w"],
        },
    }
    merged = merge_overrides(None, delta)
    kw = merged["base-detection.yaml"]["bases"]["T1w"]["keywords"]
    assert "t1_custom" in kw
    assert "t1w" not in [k.lower() for k in kw]


def test_merge_overrides_non_keyword_fields_untouched():
    # exclusive_flags, physics, rules, etc. must survive the merge.
    delta = {("contrast", "negative_keywords"): {"added": ["foo"], "removed": []}}
    merged = merge_overrides(None, delta)
    contrast_doc = merged["contrast-detection.yaml"]
    assert "detection" in contrast_doc  # rule block still present
    assert "structured" in contrast_doc


def test_merge_overrides_unknown_axis_is_ignored(caplog):
    with caplog.at_level("WARNING"):
        merge_overrides(None, {("nonsense", "foo"): {"added": ["x"], "removed": []}})
    assert "unknown axis" in caplog.text.lower()


def test_merge_overrides_unknown_bucket_path_is_ignored(caplog):
    with caplog.at_level("WARNING"):
        merged = merge_overrides(
            None,
            {("base", "bases.DoesNotExist.keywords"): {"added": ["x"], "removed": []}},
        )
    assert "path not found" in caplog.text.lower() or "unknown bucket" in caplog.text.lower()
    # Other buckets still merged correctly.
    assert merged["base-detection.yaml"]["bases"]["T1w"]["keywords"]


def test_merge_overrides_is_pure_does_not_mutate_defaults():
    # Run twice with different overrides; second run's defaults should be clean.
    delta1 = {("base", "bases.T1w.keywords"): {"added": ["aaa"], "removed": []}}
    merged1 = merge_overrides(None, delta1)
    assert "aaa" in merged1["base-detection.yaml"]["bases"]["T1w"]["keywords"]

    merged2 = merge_overrides(None, {})
    assert "aaa" not in merged2["base-detection.yaml"]["bases"]["T1w"]["keywords"]


# -----------------------------------------------------------------------------
# Catalog / validation
# -----------------------------------------------------------------------------


def test_build_catalog_includes_all_editable_axes():
    catalog = build_catalog()
    axes = {entry["axis"] for entry in catalog}
    assert axes == set(EDITABLE_BUCKETS.keys())


def test_build_catalog_has_expected_buckets():
    catalog = build_catalog()
    by_axis = {entry["axis"]: entry for entry in catalog}
    # Contrast has exactly 2 top-level buckets.
    assert {b["bucket_path"] for b in by_axis["contrast"]["buckets"]} == {
        "negative_keywords", "positive_keywords",
    }
    # Base should include T1w, T2w at minimum.
    base_paths = {b["bucket_path"] for b in by_axis["base"]["buckets"]}
    assert "bases.T1w.keywords" in base_paths
    assert "bases.T2w.keywords" in base_paths


def test_is_valid_bucket_true_for_existing_path():
    assert is_valid_bucket("base", "bases.T1w.keywords") is True
    assert is_valid_bucket("contrast", "negative_keywords") is True


def test_is_valid_bucket_false_for_unknown():
    assert is_valid_bucket("base", "bases.DoesNotExist.keywords") is False
    assert is_valid_bucket("bogus_axis", "whatever") is False
