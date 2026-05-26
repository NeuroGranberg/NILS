"""M3 — prompt pack tests."""

from __future__ import annotations

from qc.body_part.prompts import (
    DEFAULT_PROMPTS,
    all_categories,
    prompts_for_category,
)


def test_default_categories_present():
    cats = set(all_categories())
    assert {"Brain", "Brain-Neck", "Spine", "Chest"}.issubset(cats)


def test_default_packs_have_positives_and_negatives():
    for name, pack in DEFAULT_PROMPTS.items():
        assert pack.positive, f"{name} has no positives"
        assert pack.negative, f"{name} has no negatives"
        # Negatives should not include the category's own positives.
        for p in pack.positive:
            assert p not in pack.negative, (
                f"{name}: positive {p!r} found in negatives"
            )


def test_unknown_category_falls_back_to_generic():
    pack = prompts_for_category("Abdomen")
    assert any("abdomen" in p.lower() for p in pack.positive)
    assert pack.negative  # some negatives present


def test_unknown_category_strips_whitespace():
    pack = prompts_for_category("  Knee  ")
    assert any("knee" in p.lower() for p in pack.positive)


def test_known_category_returns_curated_pack():
    pack = prompts_for_category("Brain")
    assert any("brain" in p.lower() for p in pack.positive)
    assert any("spine" in n.lower() or "chest" in n.lower() for n in pack.negative)
