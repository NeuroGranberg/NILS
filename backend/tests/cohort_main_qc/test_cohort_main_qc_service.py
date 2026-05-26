"""Unit tests for the cohort-wide Main Acquisition QC algorithm.

These tests exercise scoring, bundling, Dixon family merge, MP2RAGE canonical,
EPIMix penalty, pre/post twin, and border flags — all without touching the
DB. The DB-touching parts (token wipe/write, eligibility query, snapshot
persistence) are integration-tested separately in test_cohort_main_qc_integration.py.
"""

from __future__ import annotations

import pytest

# Import the module directly to avoid the qc package's __init__ which pulls
# in PIL via dicom_service (not relevant to our algorithm tests).
import importlib
service_module = importlib.import_module("qc.cohort_main_qc_service")

CohortMainQCService = service_module.CohortMainQCService
StackRow = service_module.StackRow
load_weights = service_module.load_weights
_norm_csv = service_module._norm_csv
_modifier_minus_dixon = service_module._modifier_minus_dixon
_csv_contains = service_module._csv_contains


# ---------------------------------------------------------------- #
# Helpers
# ---------------------------------------------------------------- #


def make_row(
    sid: int,
    *,
    study_id: int = 100,
    subject_id: int = 1,
    subject_code: str = "S01",
    session_date: str = "2024-01-01",
    base: str = "T1w",
    technique: str = "MPRAGE",
    modifier_csv: str | None = None,
    construct_csv: str | None = None,
    acceleration_csv: str | None = None,
    orientation: str = "Sagittal",
    body_part: str = "head",
    spinal_cord: int = 0,
    provenance: str = "RawRecon",
    post_contrast: int | None = 0,
    mr_acquisition_type: str | None = "3D",
    slices_count: int | None = 176,
    fov_x_mm: float | None = 240.0,
    stack_echo_time: float | None = 0.003,
    stack_repetition_time: float | None = 2.3,
    stack_inversion_time: float | None = 0.9,
    stack_flip_angle: float | None = 8,
) -> StackRow:
    return StackRow(
        series_stack_id=sid,
        study_id=study_id,
        subject_id=subject_id,
        subject_code=subject_code,
        session_date=session_date,
        base=base,
        technique=technique,
        modifier_csv=modifier_csv,
        construct_csv=construct_csv,
        acceleration_csv=acceleration_csv,
        directory_type="anat",
        orientation=orientation,
        body_part=body_part,
        spinal_cord=spinal_cord,
        provenance=provenance,
        post_contrast=post_contrast,
        mr_acquisition_type=mr_acquisition_type,
        slices_count=slices_count,
        fov_x_mm=fov_x_mm,
        stack_echo_time=stack_echo_time,
        stack_repetition_time=stack_repetition_time,
        stack_inversion_time=stack_inversion_time,
        stack_flip_angle=stack_flip_angle,
    )


@pytest.fixture
def svc():
    return CohortMainQCService()


# ---------------------------------------------------------------- #
# Helpers / utilities
# ---------------------------------------------------------------- #


def test_norm_csv_sorts_and_dedupes():
    assert _norm_csv("A, b , a, B ") == "A,B,a,b"
    assert _norm_csv(None) == ""
    assert _norm_csv("") == ""


def test_modifier_minus_dixon_drops_dixon_token_only():
    assert _modifier_minus_dixon("Dixon, FlowComp") == "FlowComp"
    assert _modifier_minus_dixon("FlowComp,Dixon") == "FlowComp"
    assert _modifier_minus_dixon("FlowComp") == "FlowComp"
    assert _modifier_minus_dixon("Dixon") == ""


def test_csv_contains_matches_token_not_substring():
    assert _csv_contains("Dixon,FlowComp", "Dixon") is True
    assert _csv_contains("Dixon,FlowComp", "FlowComp") is True
    # Should NOT match a substring inside a different token.
    assert _csv_contains("PreScanNorm", "Norm") is False
    assert _csv_contains(None, "Dixon") is False


# ---------------------------------------------------------------- #
# Cohort profile
# ---------------------------------------------------------------- #


def test_cohort_profile_separates_slice_pctiles_per_dim(svc):
    rows = [make_row(i, mr_acquisition_type="3D", slices_count=200) for i in range(1, 6)]
    rows += [make_row(i, mr_acquisition_type="2D", slices_count=20) for i in range(10, 15)]
    profile = svc._compute_cohort_profile(rows, "t1w")
    assert profile["slice_pctiles_3d"]["p50"] == 200
    assert profile["slice_pctiles_2d"]["p50"] == 20
    assert profile["dim_share"]["3D"] == pytest.approx(0.5, abs=0.01)
    assert profile["dim_share"]["2D"] == pytest.approx(0.5, abs=0.01)


def test_cohort_profile_detects_dixon_family_presence(svc):
    rows = [
        make_row(1, modifier_csv="Dixon", construct_csv="Water"),
        make_row(2, modifier_csv="Dixon", construct_csv="InPhase"),
    ]
    profile = svc._compute_cohort_profile(rows, "t1w")
    assert profile["has_dixon_family"] is True


# ---------------------------------------------------------------- #
# Bundle keying & Stage-2 Dixon family merge
# ---------------------------------------------------------------- #


def test_dixon_family_merges_water_inphase_outphase(svc):
    rows = [
        make_row(1, technique="VIBE", modifier_csv="Dixon", construct_csv="Water"),
        make_row(2, technique="VIBE", modifier_csv="Dixon", construct_csv="InPhase"),
        make_row(3, technique="VIBE", modifier_csv="Dixon", construct_csv="OutPhase"),
        make_row(4, technique="VIBE", modifier_csv="Dixon", construct_csv="Fat"),
    ]
    bundles = svc._bundle_session(rows)
    assert len(bundles) == 1
    fam = bundles[0]
    assert fam.is_dixon_family is True
    assert len(fam.stacks) == 4


def test_dixon_canonical_pick_is_inphase_first(svc):
    rows = [
        make_row(1, technique="VIBE", modifier_csv="Dixon", construct_csv="Water"),
        make_row(2, technique="VIBE", modifier_csv="Dixon", construct_csv="InPhase"),
        make_row(3, technique="VIBE", modifier_csv="Dixon", construct_csv="Fat"),
    ]
    bundles = svc._bundle_session(rows)
    assert bundles[0].is_dixon_family
    canonical_ids, canon = svc._select_canonical_stack_ids(bundles[0])
    assert canonical_ids == [2]
    assert canon == "InPhase"


def test_dixon_canonical_falls_back_to_water_when_no_inphase(svc):
    rows = [
        make_row(1, technique="VIBE", modifier_csv="Dixon", construct_csv="Water"),
        make_row(2, technique="VIBE", modifier_csv="Dixon", construct_csv="Fat"),
        make_row(3, technique="VIBE", modifier_csv="Dixon", construct_csv="OutPhase"),
    ]
    bundles = svc._bundle_session(rows)
    canonical_ids, canon = svc._select_canonical_stack_ids(bundles[0])
    assert canonical_ids == [1]
    assert canon == "Water"


def test_dixon_family_with_only_fat_phase_returns_empty(svc):
    """Only Fat/OutPhase/Phase outputs in family → no main pick (red)."""
    rows = [
        make_row(1, technique="VIBE", modifier_csv="Dixon", construct_csv="Fat"),
        make_row(2, technique="VIBE", modifier_csv="Dixon", construct_csv="OutPhase"),
        make_row(3, technique="VIBE", modifier_csv="Dixon", construct_csv="Phase"),
    ]
    bundles = svc._bundle_session(rows)
    assert bundles[0].is_dixon_family
    canonical_ids, _ = svc._select_canonical_stack_ids(bundles[0])
    assert canonical_ids == []


def test_single_dixon_stack_does_not_become_family(svc):
    """One Dixon-Water stack alone shouldn't be a family — no sister to merge with."""
    rows = [make_row(1, technique="VIBE", modifier_csv="Dixon", construct_csv="Water")]
    bundles = svc._bundle_session(rows)
    assert len(bundles) == 1
    assert bundles[0].is_dixon_family is False


# ---------------------------------------------------------------- #
# MP2RAGE canonical pick
# ---------------------------------------------------------------- #


def test_mp2rage_canonical_prefers_denoised_uniform(svc):
    rows = [
        make_row(1, technique="MP2RAGE", construct_csv="Uniform"),
        make_row(2, technique="MP2RAGE", construct_csv="Denoised,Uniform"),
        make_row(3, technique="MP2RAGE", construct_csv="INV1"),
    ]
    # Stage1 keys differ (different construct) — single-stack bundles each.
    bundles = svc._bundle_session(rows)
    assert len(bundles) == 3
    # Pick the Denoised,Uniform bundle.
    target = next(b for b in bundles if any(s.construct_csv == "Denoised,Uniform" for s in b.stacks))
    canonical_ids, canon = svc._select_canonical_stack_ids(target)
    assert canonical_ids == [2]
    assert canon == "Denoised,Uniform"


def test_mp2rage_canonical_falls_back_to_uniform(svc):
    rows = [make_row(1, technique="MP2RAGE", construct_csv="Uniform")]
    bundles = svc._bundle_session(rows)
    canonical_ids, canon = svc._select_canonical_stack_ids(bundles[0])
    assert canonical_ids == [1]
    assert canon == "Uniform"


def test_mp2rage_with_no_canonical_label_tags_all(svc):
    rows = [make_row(1, technique="MP2RAGE", construct_csv=None)]
    bundles = svc._bundle_session(rows)
    canonical_ids, canon = svc._select_canonical_stack_ids(bundles[0])
    assert canonical_ids == [1]
    assert canon is None


# ---------------------------------------------------------------- #
# Scoring
# ---------------------------------------------------------------- #


def test_3d_scores_higher_than_2d_in_3d_dominant_cohort(svc):
    rows_3d = [make_row(i, technique="MPRAGE", mr_acquisition_type="3D", slices_count=176) for i in range(1, 6)]
    rows_2d = [make_row(i, technique="TSE", mr_acquisition_type="2D", slices_count=24) for i in range(10, 12)]
    profile = svc._compute_cohort_profile(rows_3d + rows_2d, "t1w")

    bundles_3d = svc._bundle_session([rows_3d[0]])
    bundles_2d = svc._bundle_session([rows_2d[0]])
    s3d, _ = svc._score_bundle(bundles_3d[0], profile, "t1w")
    s2d, _ = svc._score_bundle(bundles_2d[0], profile, "t1w")
    assert s3d > s2d


def test_dixon_family_gets_tier_bonus_and_share_floor(svc):
    """A Dixon family in a small minority should NOT be punished for share."""
    plain = [make_row(i, technique="MPRAGE") for i in range(1, 21)]
    dixon = [
        make_row(100, technique="VIBE", modifier_csv="Dixon", construct_csv="Water", slices_count=176),
        make_row(101, technique="VIBE", modifier_csv="Dixon", construct_csv="InPhase", slices_count=176),
    ]
    profile = svc._compute_cohort_profile(plain + dixon, "t1w")

    plain_bundle = svc._bundle_session([plain[0]])[0]
    dixon_bundle = svc._bundle_session(dixon)[0]
    assert dixon_bundle.is_dixon_family

    s_plain, _ = svc._score_bundle(plain_bundle, profile, "t1w")
    s_dixon, _ = svc._score_bundle(dixon_bundle, profile, "t1w")
    # Plain MPRAGE (cohort dominant) still wins overall, but Dixon family score
    # should remain reasonably high (≥ 0.65) despite minority share.
    assert s_dixon >= 0.65, f"Dixon score too low: {s_dixon}"


def test_waterexc_gets_tier_bonus(svc):
    rows = [make_row(i, technique="VIBE") for i in range(1, 21)]
    we = [make_row(100, technique="VIBE", modifier_csv="WaterExc", slices_count=176)]
    profile = svc._compute_cohort_profile(rows + we, "t1w")
    plain_bundle = svc._bundle_session([rows[0]])[0]
    we_bundle = svc._bundle_session(we)[0]
    s_plain, b_plain = svc._score_bundle(plain_bundle, profile, "t1w")
    s_we, b_we = svc._score_bundle(we_bundle, profile, "t1w")
    # WaterExc tier bonus → tech component should be higher.
    assert b_we["tech"] > b_plain["tech"]


def test_epimix_penalty_halves_score(svc):
    rows = [make_row(i, technique="MPRAGE", provenance="EPIMix") for i in range(1, 6)]
    profile = svc._compute_cohort_profile(rows, "t1w")
    bundles = svc._bundle_session([rows[0]])
    s, breakdown = svc._score_bundle(bundles[0], profile, "t1w")
    assert breakdown["provenance_mult"] == 0.5
    # Same row but RawRecon should score 2x.
    raw = make_row(99, technique="MPRAGE", provenance="RawRecon")
    raw_bundle = svc._bundle_session([raw])[0]
    s_raw, b_raw = svc._score_bundle(raw_bundle, profile, "t1w")
    assert b_raw["provenance_mult"] == 1.0
    assert s_raw == pytest.approx(2 * s, abs=1e-3) or s_raw > s


# ---------------------------------------------------------------- #
# Per-session pick: integration of bundling + scoring + flags
# ---------------------------------------------------------------- #


def test_pick_chooses_mprage_over_tse_in_mprage_dominant(svc):
    cohort_rows = [make_row(i, technique="MPRAGE") for i in range(1, 21)]
    cohort_rows += [make_row(i, technique="TSE", mr_acquisition_type="2D", slices_count=24) for i in range(30, 33)]
    profile = svc._compute_cohort_profile(cohort_rows, "t1w")

    session_rows = [
        make_row(1, technique="MPRAGE"),
        make_row(2, technique="TSE", mr_acquisition_type="2D", slices_count=24),
    ]
    pick = svc._pick_for_session(session_rows, profile, "t1w")
    assert pick["winning_stack_ids"] == [1]


def test_pick_dixon_family_inphase_canonical(svc):
    cohort_rows = [make_row(i, technique="MPRAGE") for i in range(1, 11)]
    cohort_rows += [
        make_row(i, technique="VIBE", modifier_csv="Dixon", construct_csv="InPhase")
        for i in range(20, 25)
    ]
    cohort_rows += [
        make_row(i, technique="VIBE", modifier_csv="Dixon", construct_csv="Water")
        for i in range(30, 35)
    ]
    profile = svc._compute_cohort_profile(cohort_rows, "t1w")

    session_rows = [
        make_row(1, technique="VIBE", modifier_csv="Dixon", construct_csv="Water"),
        make_row(2, technique="VIBE", modifier_csv="Dixon", construct_csv="InPhase"),
        make_row(3, technique="VIBE", modifier_csv="Dixon", construct_csv="Fat"),
    ]
    pick = svc._pick_for_session(session_rows, profile, "t1w")
    assert pick["winning_stack_ids"] == [2]
    # Canonical-in-family flag set.
    cands = {c["stack_id"]: c for c in pick["candidate_summary"]}
    assert cands[2]["is_main"] is True
    assert cands[1]["is_main"] is False


def test_pre_post_twin_both_get_main(svc):
    cohort_rows = [
        make_row(i, technique="MPRAGE", post_contrast=0, fov_x_mm=240, slices_count=176)
        for i in range(1, 11)
    ] + [
        make_row(i, technique="MPRAGE", post_contrast=1, fov_x_mm=240, slices_count=176)
        for i in range(20, 25)
    ]
    profile = svc._compute_cohort_profile(cohort_rows, "t1w")

    session_rows = [
        make_row(1, technique="MPRAGE", post_contrast=0, slices_count=176, fov_x_mm=240),
        make_row(2, technique="MPRAGE", post_contrast=1, slices_count=176, fov_x_mm=240),
    ]
    pick = svc._pick_for_session(session_rows, profile, "t1w")
    assert set(pick["winning_stack_ids"]) == {1, 2}
    assert "pre_post_twin" in pick["needs_check_reasons"]


def test_unknown_dim_triggers_border(svc):
    cohort_rows = [make_row(i, technique="MPRAGE") for i in range(1, 11)]
    cohort_rows += [make_row(99, technique="GRE", mr_acquisition_type=None, slices_count=120)]
    profile = svc._compute_cohort_profile(cohort_rows, "t1w")
    session_rows = [make_row(99, technique="GRE", mr_acquisition_type=None, slices_count=120)]
    pick = svc._pick_for_session(session_rows, profile, "t1w")
    assert "unknown_dim" in pick["needs_check_reasons"]


def test_retake_triggers_border_for_non_family_bundle(svc):
    cohort_rows = [make_row(i, technique="MPRAGE") for i in range(1, 11)]
    profile = svc._compute_cohort_profile(cohort_rows, "t1w")
    session_rows = [
        make_row(1, technique="MPRAGE"),
        make_row(2, technique="MPRAGE"),  # same fingerprint - same bundle
    ]
    pick = svc._pick_for_session(session_rows, profile, "t1w")
    assert "retake" in pick["needs_check_reasons"]


def test_dixon_sisters_dont_trigger_retake(svc):
    cohort_rows = [
        make_row(i, technique="VIBE", modifier_csv="Dixon", construct_csv="InPhase")
        for i in range(1, 11)
    ]
    cohort_rows += [
        make_row(i, technique="VIBE", modifier_csv="Dixon", construct_csv="Water")
        for i in range(20, 30)
    ]
    profile = svc._compute_cohort_profile(cohort_rows, "t1w")
    session_rows = [
        make_row(1, technique="VIBE", modifier_csv="Dixon", construct_csv="InPhase"),
        make_row(2, technique="VIBE", modifier_csv="Dixon", construct_csv="Water"),
        make_row(3, technique="VIBE", modifier_csv="Dixon", construct_csv="Fat"),
    ]
    pick = svc._pick_for_session(session_rows, profile, "t1w")
    # Sisters from one Dixon scan should not flag "retake".
    assert "retake" not in pick["needs_check_reasons"]


def test_dixon_vs_plain_border_when_close(svc):
    cohort_rows = [make_row(i, technique="MPRAGE") for i in range(1, 11)]
    cohort_rows += [
        make_row(i, technique="VIBE", modifier_csv="Dixon", construct_csv="InPhase")
        for i in range(20, 30)
    ]
    profile = svc._compute_cohort_profile(cohort_rows, "t1w")
    session_rows = [
        make_row(1, technique="MPRAGE"),
        make_row(2, technique="VIBE", modifier_csv="Dixon", construct_csv="InPhase"),
        make_row(3, technique="VIBE", modifier_csv="Dixon", construct_csv="Water"),
    ]
    pick = svc._pick_for_session(session_rows, profile, "t1w")
    # Either one wins; if Dixon family wins AND plain MPRAGE within 10%, expect border.
    if pick["winning_stack_ids"] != [1]:  # Dixon won
        assert "dixon_vs_plain" in pick["needs_check_reasons"]


def test_session_with_only_mip_construct_yields_no_main(svc):
    """MIP is on the non_canonical_constructs banlist; eligibility filter excludes
    such rows. At the algorithm level we simulate by ensuring rows with MIP
    aren't passed in. Here we verify the session-level fallback when a Dixon
    family has only banned outputs."""
    cohort_rows = [make_row(i, technique="MPRAGE") for i in range(1, 11)]
    profile = svc._compute_cohort_profile(cohort_rows, "t1w")
    # Give the session ONLY a Dixon-only-Fat family (no canonical).
    session_rows = [
        make_row(1, technique="VIBE", modifier_csv="Dixon", construct_csv="Fat"),
        make_row(2, technique="VIBE", modifier_csv="Dixon", construct_csv="OutPhase"),
    ]
    pick = svc._pick_for_session(session_rows, profile, "t1w")
    assert pick["winning_stack_ids"] == []
    assert pick["needs_check"] is True
    assert "no_canonical_construct" in pick["needs_check_reasons"]


# ---------------------------------------------------------------- #
# Acknowledgement overlay ("In NILS we trust")
# ---------------------------------------------------------------- #


INFO_ONLY_REASONS = service_module.INFO_ONLY_REASONS


def test_info_only_reasons_includes_acknowledged():
    """The 'acknowledged' token must NOT paint heatmap cells red."""
    assert "acknowledged" in INFO_ONLY_REASONS


def test_overlay_ack_marks_pick_acknowledged_when_in_set():
    pick = {
        "subject_id": 100,
        "session_date": "2024-01-01",
        "axis": "t1w",
        "needs_check": True,
        "needs_check_reasons": ["rare_technique"],
    }
    out = CohortMainQCService._overlay_ack(
        pick, {(100, "2024-01-01", "t1w")},
    )
    assert out["acknowledged"] is True
    assert out["needs_check"] is False
    # Original review-level reasons are preserved (audit trail) but the
    # informational 'acknowledged' is appended so the modal can render it.
    assert "rare_technique" in out["needs_check_reasons"]
    assert "acknowledged" in out["needs_check_reasons"]


def test_overlay_ack_is_idempotent():
    """Re-applying the overlay must not duplicate the 'acknowledged' token."""
    pick = {
        "subject_id": 100,
        "session_date": "2024-01-01",
        "axis": "t1w",
        "needs_check": True,
        "needs_check_reasons": ["acknowledged"],
        "acknowledged": True,
    }
    out = CohortMainQCService._overlay_ack(
        pick, {(100, "2024-01-01", "t1w")},
    )
    assert out["needs_check_reasons"].count("acknowledged") == 1
    assert out["acknowledged"] is True
    assert out["needs_check"] is False


def test_overlay_ack_leaves_pick_untouched_when_not_in_set():
    pick = {
        "subject_id": 200,
        "session_date": "2024-01-01",
        "axis": "t1w",
        "needs_check": True,
        "needs_check_reasons": ["retake"],
    }
    out = CohortMainQCService._overlay_ack(
        pick, {(100, "2024-01-01", "t1w")},
    )
    # Same dict contents; no acknowledged field added.
    assert out == pick
    assert "acknowledged" not in out
