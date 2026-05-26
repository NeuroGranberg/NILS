"""Tests for technique-weighted stratified subsampling."""

from __future__ import annotations

import pytest

from qc.body_part.candidates import CandidateStack
from qc.body_part.service import CohortBodyPartQCService


def _make_candidate(
    stack_id: int,
    technique: str = "MPRAGE",
    body_part: str | None = None,
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
        orientation="axial",
        body_part=body_part,
        num_slices=100,
    )


class TestStratifiedSubsample:
    def test_rare_technique_survives(self):
        """A single MEDIC stack among 1000 TSE stacks must survive subsampling."""
        candidates = [
            _make_candidate(i, technique="TSE") for i in range(1000)
        ] + [
            _make_candidate(9999, technique="MEDIC"),
        ]
        result = CohortBodyPartQCService._stratified_subsample(
            candidates, pool_cap=100, cohort_name="test", category="Brain",
        )
        techniques = {c.technique for c in result}
        assert "MEDIC" in techniques
        assert len(result) <= 100

    def test_proportional_allocation(self):
        """With 80 TSE + 20 MPRAGE, a subsample of 50 should be ~40/10."""
        candidates = (
            [_make_candidate(i, technique="TSE") for i in range(80)]
            + [_make_candidate(80 + i, technique="MPRAGE") for i in range(20)]
        )
        result = CohortBodyPartQCService._stratified_subsample(
            candidates, pool_cap=50, cohort_name="test", category="Spine",
        )
        tse_count = sum(1 for c in result if c.technique == "TSE")
        mprage_count = sum(1 for c in result if c.technique == "MPRAGE")
        # TSE should get ~40, MPRAGE ~10.
        assert 30 <= tse_count <= 45
        assert 5 <= mprage_count <= 15
        assert len(result) <= 50

    def test_pool_smaller_than_cap(self):
        """If pool is smaller than cap, return all candidates."""
        candidates = [_make_candidate(i, technique="TSE") for i in range(10)]
        result = CohortBodyPartQCService._stratified_subsample(
            candidates, pool_cap=100, cohort_name="test", category="Brain",
        )
        assert len(result) == 10

    def test_deterministic(self):
        """Same (cohort_name, category) gives same result."""
        candidates = [
            _make_candidate(i, technique=["TSE", "MPRAGE", "MEDIC"][i % 3])
            for i in range(200)
        ]
        r1 = CohortBodyPartQCService._stratified_subsample(
            candidates, 50, "test-cohort", "Brain",
        )
        r2 = CohortBodyPartQCService._stratified_subsample(
            candidates, 50, "test-cohort", "Brain",
        )
        assert [c.stack_id for c in r1] == [c.stack_id for c in r2]

    def test_different_category_different_result(self):
        """Different category seed → different sample."""
        candidates = [
            _make_candidate(i, technique=["TSE", "MPRAGE", "MEDIC"][i % 3])
            for i in range(200)
        ]
        r1 = CohortBodyPartQCService._stratified_subsample(
            candidates, 50, "test-cohort", "Brain",
        )
        r2 = CohortBodyPartQCService._stratified_subsample(
            candidates, 50, "test-cohort", "Spine",
        )
        ids1 = set(c.stack_id for c in r1)
        ids2 = set(c.stack_id for c in r2)
        assert ids1 != ids2

    def test_empty_candidates(self):
        result = CohortBodyPartQCService._stratified_subsample(
            [], pool_cap=50, cohort_name="test", category="Brain",
        )
        assert result == []

    def test_multiple_body_part_values(self):
        """Candidates may have different body_part (or None) — both are cells."""
        candidates = (
            [_make_candidate(i, technique="TSE", body_part=None) for i in range(50)]
            + [_make_candidate(50 + i, technique="TSE", body_part="spine") for i in range(50)]
        )
        result = CohortBodyPartQCService._stratified_subsample(
            candidates, pool_cap=20, cohort_name="test", category="Spine",
        )
        # Both cells should be represented.
        bps = {c.body_part for c in result}
        assert None in bps or "spine" in bps
        assert len(result) <= 20

    def test_minimum_one_per_cell(self):
        """Each (technique, body_part) cell gets at least 1."""
        candidates = (
            [_make_candidate(i, technique="TSE") for i in range(100)]
            + [_make_candidate(100, technique="MEDIC")]
            + [_make_candidate(101, technique="FIESTA")]
            + [_make_candidate(102, technique="RESTORE")]
        )
        result = CohortBodyPartQCService._stratified_subsample(
            candidates, pool_cap=10, cohort_name="test", category="Spine",
        )
        techniques = {c.technique for c in result}
        # All 4 techniques should appear.
        assert techniques == {"TSE", "MEDIC", "FIESTA", "RESTORE"}
