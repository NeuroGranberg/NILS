"""Tests for keyword-prior seeding and technique-weighted subsampling.

Tests the new two-stage seed pipeline:
  1. partition_by_category() splits candidates by keyword label
  2. select_prior_candidates() does FPS-only diversity selection
  3. _stratified_subsample() gives proportional technique representation
"""

from __future__ import annotations

import numpy as np
import pytest

from qc.body_part.candidates import (
    CandidateStack,
    DEFAULT_CATEGORY_KEYWORD_MAP,
    partition_by_category,
)
from qc.body_part.seed import PriorCandidate, select_prior_candidates


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_candidate(
    stack_id: int,
    body_part: str | None = None,
    technique: str = "MPRAGE",
    orientation: str = "axial",
) -> CandidateStack:
    return CandidateStack(
        stack_id=stack_id,
        slice_index=50,
        series_instance_uid=f"1.2.3.{stack_id}",
        study_id=stack_id + 1000,
        subject_id=stack_id + 2000,
        subject_code=f"sub-{stack_id:04d}",
        session_date="2024-01-01",
        technique=technique,
        orientation=orientation,
        body_part=body_part,
        num_slices=100,
    )


# ---------------------------------------------------------------------------
# partition_by_category
# ---------------------------------------------------------------------------


class TestPartitionByCategory:
    def test_brain_splits_correctly(self):
        candidates = [
            _make_candidate(1, body_part="brain"),
            _make_candidate(2, body_part="brain"),
            _make_candidate(3, body_part="spine"),
            _make_candidate(4, body_part=None),
            _make_candidate(5, body_part=None),
        ]
        prior, null = partition_by_category(candidates, "Brain")
        assert len(prior) == 2
        assert all(c.body_part == "brain" for c in prior)
        assert len(null) == 2
        assert all(c.body_part is None for c in null)

    def test_spine_excludes_brain_from_prior(self):
        candidates = [
            _make_candidate(1, body_part="brain"),
            _make_candidate(2, body_part="spine"),
            _make_candidate(3, body_part=None),
        ]
        prior, null = partition_by_category(candidates, "Spine")
        assert len(prior) == 1
        assert prior[0].body_part == "spine"
        assert len(null) == 1

    def test_brain_neck_matches_both_keywords(self):
        candidates = [
            _make_candidate(1, body_part="brain-neck"),
            _make_candidate(2, body_part="neck"),
            _make_candidate(3, body_part="brain"),
            _make_candidate(4, body_part=None),
        ]
        prior, null = partition_by_category(candidates, "Brain-Neck")
        assert len(prior) == 2
        assert {c.body_part for c in prior} == {"brain-neck", "neck"}
        assert len(null) == 1

    def test_chest_has_no_priors(self):
        """Chest has no keyword mappings → empty prior, all None go to null."""
        candidates = [
            _make_candidate(1, body_part="brain"),
            _make_candidate(2, body_part="spine"),
            _make_candidate(3, body_part=None),
            _make_candidate(4, body_part=None),
        ]
        prior, null = partition_by_category(candidates, "Chest")
        assert len(prior) == 0
        assert len(null) == 2

    def test_custom_mapping_overrides_default(self):
        candidates = [
            _make_candidate(1, body_part="brain"),
            _make_candidate(2, body_part="head"),
            _make_candidate(3, body_part=None),
        ]
        custom = {"Brain": ["brain", "head"]}
        prior, null = partition_by_category(candidates, "Brain", custom)
        assert len(prior) == 2
        assert {c.body_part for c in prior} == {"brain", "head"}

    def test_case_insensitive_matching(self):
        candidates = [
            _make_candidate(1, body_part="Brain"),
            _make_candidate(2, body_part="BRAIN"),
            _make_candidate(3, body_part=None),
        ]
        prior, null = partition_by_category(candidates, "Brain")
        assert len(prior) == 2

    def test_empty_candidates(self):
        prior, null = partition_by_category([], "Brain")
        assert prior == []
        assert null == []

    def test_all_null_body_part(self):
        candidates = [_make_candidate(i, body_part=None) for i in range(10)]
        prior, null = partition_by_category(candidates, "Brain")
        assert len(prior) == 0
        assert len(null) == 10


# ---------------------------------------------------------------------------
# select_prior_candidates (FPS diversity)
# ---------------------------------------------------------------------------


class TestSelectPriorCandidates:
    def test_returns_up_to_n_target(self):
        # 100 random embeddings.
        rng = np.random.default_rng(42)
        embeddings = rng.normal(size=(100, 512)).astype(np.float32)
        picks = select_prior_candidates(embeddings, n_target=20)
        assert len(picks) == 20
        assert all(isinstance(p, PriorCandidate) for p in picks)

    def test_fewer_than_n_target(self):
        rng = np.random.default_rng(42)
        embeddings = rng.normal(size=(5, 512)).astype(np.float32)
        picks = select_prior_candidates(embeddings, n_target=20)
        assert len(picks) == 5

    def test_empty_embeddings(self):
        embeddings = np.zeros((0, 512), dtype=np.float32)
        picks = select_prior_candidates(embeddings, n_target=20)
        assert picks == []

    def test_deterministic(self):
        rng = np.random.default_rng(42)
        embeddings = rng.normal(size=(50, 512)).astype(np.float32)
        picks1 = select_prior_candidates(embeddings, n_target=10, seed=999)
        picks2 = select_prior_candidates(embeddings, n_target=10, seed=999)
        assert [p.index for p in picks1] == [p.index for p in picks2]

    def test_diverse_picks(self):
        """Verify FPS picks spread out across 3 distinct clusters."""
        rng = np.random.default_rng(42)
        # Three well-separated clusters.
        c1 = rng.normal(loc=[5, 0, 0], scale=0.1, size=(50, 3)).astype(np.float32)
        c2 = rng.normal(loc=[0, 5, 0], scale=0.1, size=(50, 3)).astype(np.float32)
        c3 = rng.normal(loc=[0, 0, 5], scale=0.1, size=(50, 3)).astype(np.float32)
        embeddings = np.vstack([c1, c2, c3])
        picks = select_prior_candidates(embeddings, n_target=9)
        # Should pick roughly equal from each cluster.
        from_c1 = sum(1 for p in picks if p.index < 50)
        from_c2 = sum(1 for p in picks if 50 <= p.index < 100)
        from_c3 = sum(1 for p in picks if p.index >= 100)
        assert from_c1 >= 2 and from_c2 >= 2 and from_c3 >= 2
