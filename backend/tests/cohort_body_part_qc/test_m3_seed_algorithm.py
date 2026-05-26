"""M3 — seeding algorithm tests (no torch / no FastAPI).

Synthesizes embeddings on the unit sphere, gives each cluster a textual
"prompt" by averaging the cluster centre, and verifies the seeder
selects the right cluster with diverse picks.
"""

from __future__ import annotations

import numpy as np

from qc.body_part.seed import (
    DEFAULT_SEED_CONFIG,
    SeedConfig,
    farthest_point_indices,
    score_zero_shot,
    select_seed_candidates,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unit(v):
    v = np.asarray(v, dtype=np.float32)
    return v / max(np.linalg.norm(v), 1e-12)


def _make_cluster(center, n, jitter=0.05, seed=0):
    rng = np.random.default_rng(seed)
    pts = np.tile(np.asarray(center, dtype=np.float32), (n, 1))
    pts += rng.normal(0.0, jitter, size=pts.shape).astype(np.float32)
    norms = np.linalg.norm(pts, axis=1, keepdims=True).clip(min=1e-12)
    return pts / norms


# ---------------------------------------------------------------------------
# score_zero_shot
# ---------------------------------------------------------------------------


def test_score_zs_higher_for_aligned_image():
    pos = _unit([1.0, 0.0, 0.0])
    neg = np.stack([_unit([0, 1, 0]), _unit([0, 0, 1])])

    aligned = _unit([1.0, 0.05, 0.05])[None, :]
    away = _unit([0.0, 1.0, 0.0])[None, :]
    img = np.vstack([aligned, away])
    zs, margin = score_zero_shot(img, pos, neg, temperature=20.0)
    assert zs[0] > zs[1]
    assert margin[0] > margin[1]


def test_score_zs_softmax_normalised():
    pos = _unit([1.0, 0.0, 0.0])
    neg = np.stack([_unit([0, 1, 0])])
    img = _unit([0.6, 0.4, 0.0])[None, :]
    zs, _ = score_zero_shot(img, pos, neg, temperature=10.0)
    assert 0.0 <= zs[0] <= 1.0


def test_score_zs_empty_input():
    pos = _unit([1.0, 0.0])
    neg = _unit([0.0, 1.0])[None, :]
    zs, m = score_zero_shot(np.zeros((0, 2), dtype=np.float32), pos, neg)
    assert zs.shape == (0,) and m.shape == (0,)


# ---------------------------------------------------------------------------
# farthest_point_indices
# ---------------------------------------------------------------------------


def test_fps_two_clusters_alternates():
    # Two disjoint clusters; FPS should pick from both before
    # returning to the first.
    a = _make_cluster([1, 0, 0], 5, jitter=0.01, seed=1)
    b = _make_cluster([-1, 0, 0], 5, jitter=0.01, seed=2)
    pts = np.vstack([a, b])
    picks = farthest_point_indices(pts, 4, seed=0)
    # Among the first two picks we should have one from each cluster.
    first = picks[0]
    second = picks[1]
    assert (first < 5) != (second < 5), (
        f"FPS first two picks both in same cluster: {picks}"
    )


def test_fps_caps_at_n():
    pts = _make_cluster([1, 0, 0], 3, seed=1)
    picks = farthest_point_indices(pts, 100, seed=0)
    assert len(picks) <= 3


def test_fps_empty():
    assert farthest_point_indices(np.zeros((0, 4), dtype=np.float32), 5) == []


def test_fps_zero_target():
    pts = _make_cluster([1, 0], 5, seed=1)
    assert farthest_point_indices(pts, 0) == []


def test_fps_handles_duplicates():
    # 10 identical rows + 1 different row → max 2 unique picks possible.
    base = _unit([1, 0, 0])
    other = _unit([0, 1, 0])
    pts = np.vstack([np.tile(base, (10, 1)), other[None, :]])
    picks = farthest_point_indices(pts, 5, seed=0)
    # At most one of the duplicates is used + the other distinct point.
    assert len(picks) <= 2


# ---------------------------------------------------------------------------
# select_seed_candidates
# ---------------------------------------------------------------------------


def test_select_seed_picks_positives_only():
    """Two clusters: one near the positive prompt, one near the negative.
    The seeder should pick from the positive cluster only."""
    pos_dir = _unit([1, 0, 0, 0])
    neg_dir = _unit([0, 1, 0, 0])

    pos_cluster = _make_cluster(pos_dir, 30, jitter=0.02, seed=1)
    neg_cluster = _make_cluster(neg_dir, 30, jitter=0.02, seed=2)
    img = np.vstack([pos_cluster, neg_cluster])

    pos_text = pos_dir
    neg_texts = np.stack([neg_dir])

    picks = select_seed_candidates(
        img, pos_text, neg_texts,
        config=SeedConfig(n_target=10, p_min=0.6, m_min=0.1,
                          softmax_temperature=20.0),
    )
    assert len(picks) > 0
    # All picked indices belong to the positive cluster (rows 0..29).
    assert all(p.index < 30 for p in picks), [p.index for p in picks]
    # zs_prob ordering sanity: all are above the threshold.
    assert all(p.zs_prob >= 0.6 for p in picks)


def test_select_seed_falls_back_when_strict_gate_empties():
    """If p_min/m_min is too strict, the seeder relaxes to fallbacks."""
    pos_dir = _unit([1, 0, 0])
    neg_dir = _unit([0, 1, 0])
    img = _make_cluster(_unit([0.7, 0.7, 0.0]), 50, jitter=0.05, seed=1)
    picks = select_seed_candidates(
        img, pos_dir, np.stack([neg_dir]),
        config=SeedConfig(
            n_target=5, p_min=0.99, m_min=0.99,
            fallback_p_min=0.0, fallback_m_min=-1.0,
            softmax_temperature=5.0,
        ),
    )
    assert len(picks) > 0


def test_select_seed_returns_empty_for_empty_input():
    pos = _unit([1, 0, 0])
    neg = _unit([0, 1, 0])[None, :]
    picks = select_seed_candidates(
        np.zeros((0, 3), dtype=np.float32), pos, neg,
    )
    assert picks == []


def test_select_seed_diversity_within_positive_cluster():
    """Within the positive cluster, picks should be diverse (≥ 5
    distinct indices for n_target=5)."""
    pos_dir = _unit([1, 0, 0, 0, 0])
    neg_dir = _unit([0, 1, 0, 0, 0])

    pos_cluster = _make_cluster(pos_dir, 200, jitter=0.05, seed=42)
    picks = select_seed_candidates(
        pos_cluster, pos_dir, np.stack([neg_dir]),
        config=SeedConfig(n_target=5, p_min=0.6, m_min=0.05,
                          softmax_temperature=20.0),
    )
    assert len({p.index for p in picks}) == len(picks)
    assert len(picks) == 5


def test_select_seed_respects_n_target_zero():
    pos = _unit([1, 0, 0])
    neg = _unit([0, 1, 0])[None, :]
    pts = _make_cluster(pos, 10, seed=1)
    picks = select_seed_candidates(
        pts, pos, neg, config=SeedConfig(n_target=0),
    )
    assert picks == []
