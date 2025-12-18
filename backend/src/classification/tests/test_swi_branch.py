"""
Tests for SWI Branch Logic

Tests the SWI-specific classification branch that handles:
- Magnitude images (source, T2*-weighted underlying)
- Phase maps (iron/calcium differentiation)
- SWI processed (combined mag × phase^n)
- MinIP (minimum intensity projection - venogram)
- MIP (maximum intensity projection)
- QSM (quantitative susceptibility mapping)

All SWI outputs have base="SWI" to indicate the contrast type.
The technique axis (GRE or EPI) indicates the acquisition method.

Version: 2.0.0 - Updated for new construct names and base values
"""

import pytest
from src.classification.core.context import ClassificationContext
from src.classification.branches.swi import (
    apply_swi_logic,
    detect_swi_output_type,
    get_swi_output_info,
)
from src.classification.branches.common import BranchResult


# =============================================================================
# Fixture: Common contexts
# =============================================================================

@pytest.fixture
def swi_magnitude_ctx():
    """SWI magnitude source image.

    Note: Magnitude source has M token but NO SWI token in ImageType.
    The SWI token indicates processed SWI output.
    Real example: ORIGINAL\\PRIMARY\\M\\NORM\\DIS2D with "magnitude images" text
    """
    return ClassificationContext(
        image_type=r"ORIGINAL\PRIMARY\M\NORM\DIS2D",  # M but no SWI token
        scanning_sequence="GR",
        stack_sequence_name="*swi3d1r",
        text_search_blob="magnitude images swi",
    )


@pytest.fixture
def swi_phase_ctx():
    """SWI phase map.

    Note: Phase source has P token but no SWI token.
    Real example: ORIGINAL\\PRIMARY\\P\\DIS2D with "phase images" text
    """
    return ClassificationContext(
        image_type=r"ORIGINAL\PRIMARY\P\DIS2D",  # P but no SWI token
        scanning_sequence="GR",
        stack_sequence_name="*swi3d1r",
        text_search_blob="phase images swi",
    )


@pytest.fixture
def swi_processed_ctx():
    """SWI combined/processed image.

    Note: The SWI token in ImageType indicates processed SWI output,
    regardless of ORIGINAL/DERIVED. Both patterns are valid:
    - ORIGINAL\\PRIMARY\\M\\SWI\\... (older scanners)
    - DERIVED\\PRIMARY\\M\\SWI\\... (newer multi-echo SWI)

    Real example: ORIGINAL\\PRIMARY\\M\\SWI\\NORM\\DIS2D with swi images text
    """
    return ClassificationContext(
        image_type=r"ORIGINAL\PRIMARY\M\SWI\NORM\DIS2D",  # has SWI token = processed
        scanning_sequence="GR",
        stack_sequence_name="*swi3d1r",
        text_search_blob="swi images brain",
    )


@pytest.fixture
def swi_minip_ctx():
    """SWI MinIP projection."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\MNIP\SWI",
        scanning_sequence="GR",
        stack_sequence_name="*swi3d1r",
    )


@pytest.fixture
def swi_qsm_ctx():
    """QSM - Quantitative susceptibility map."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\QSM",
        scanning_sequence="GR",
        stack_sequence_name="*swi3d1r",
    )


# =============================================================================
# Test: apply_swi_logic
# =============================================================================

class TestSWIMagnitude:
    """Tests for SWI magnitude detection."""

    def test_magnitude_detected_from_m_token(self, swi_magnitude_ctx):
        """Magnitude image with M token should return SWI base, Magnitude construct."""
        result = apply_swi_logic(swi_magnitude_ctx)

        assert result.base == "SWI"
        assert result.construct == "Magnitude"
        assert result.skip_base_detection is True
        assert result.directory_type == "anat"

    def test_magnitude_has_high_confidence(self, swi_magnitude_ctx):
        """Magnitude detection should have high confidence."""
        result = apply_swi_logic(swi_magnitude_ctx)
        assert result.confidence >= 0.85

    def test_magnitude_original_image(self):
        """Original magnitude without explicit SWI token."""
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\MAGNITUDE",
            scanning_sequence="GR",
            stack_sequence_name="*swi3d1r",
        )
        result = apply_swi_logic(ctx)

        assert result.base == "SWI"
        assert result.construct == "Magnitude"


class TestSWIPhase:
    """Tests for SWI phase map detection."""

    def test_phase_detected_from_p_token(self, swi_phase_ctx):
        """Phase map with P token should return SWI base, Phase construct."""
        result = apply_swi_logic(swi_phase_ctx)

        assert result.base == "SWI"
        assert result.construct == "Phase"
        assert result.skip_base_detection is True
        assert result.directory_type == "anat"

    def test_phase_has_high_confidence(self, swi_phase_ctx):
        """Phase detection should have high confidence."""
        result = apply_swi_logic(swi_phase_ctx)
        assert result.confidence >= 0.90

    def test_phase_explicit_token(self):
        """Explicit PHASE token in ImageType."""
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\PHASE\SWI",
            scanning_sequence="GR",
            stack_sequence_name="*swi3d1r",
        )
        result = apply_swi_logic(ctx)

        assert result.base == "SWI"
        assert result.construct == "Phase"


class TestSWIProcessed:
    """Tests for SWI processed/combined image detection."""

    def test_processed_detected_from_derived_swi(self, swi_processed_ctx):
        """Derived SWI should return SWI base, SWI construct."""
        result = apply_swi_logic(swi_processed_ctx)

        assert result.base == "SWI"
        assert result.construct == "SWI"
        assert result.skip_base_detection is True
        assert result.directory_type == "anat"

    def test_processed_has_high_confidence(self, swi_processed_ctx):
        """SWI processed detection should have high confidence."""
        result = apply_swi_logic(swi_processed_ctx)
        assert result.confidence >= 0.90


class TestSWIMinIP:
    """Tests for SWI MinIP detection."""

    def test_minip_detected_from_mnip_token(self, swi_minip_ctx):
        """MinIP token should return SWI base, MinIP construct."""
        result = apply_swi_logic(swi_minip_ctx)

        assert result.base == "SWI"
        assert result.construct == "MinIP"
        assert result.skip_base_detection is True
        assert result.directory_type == "anat"

    def test_minip_has_highest_priority(self):
        """MinIP should be detected even with other tokens present."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\M\MNIP\SWI",
            scanning_sequence="GR",
            stack_sequence_name="*swi3d1r",
        )
        result = apply_swi_logic(ctx)

        assert result.construct == "MinIP"

    def test_minip_has_highest_confidence(self, swi_minip_ctx):
        """MinIP detection should have highest confidence."""
        result = apply_swi_logic(swi_minip_ctx)
        assert result.confidence >= 0.95

    def test_minip_alternate_token(self):
        """MINIP alternate spelling should work."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\MINIP",
            scanning_sequence="GR",
            stack_sequence_name="*swi3d1r",
        )
        result = apply_swi_logic(ctx)

        assert result.construct == "MinIP"


class TestSWIMIP:
    """Tests for SWI MIP (Maximum Intensity Projection) detection."""

    def test_mip_detected_from_mip_token(self):
        """MIP token should return SWI base, MIP construct."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\MIP\SWI",
            scanning_sequence="GR",
            stack_sequence_name="*swi3d1r",
        )
        result = apply_swi_logic(ctx)

        assert result.base == "SWI"
        assert result.construct == "MIP"
        assert result.skip_base_detection is True
        assert result.directory_type == "anat"

    def test_mip_from_text_keyword(self):
        """MIP keyword in text should detect MIP."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\SWI",
            scanning_sequence="GR",
            text_search_blob="SWI MIP projection",
        )
        result = apply_swi_logic(ctx)

        assert result.construct == "MIP"


class TestSWIQSM:
    """Tests for QSM detection."""

    def test_qsm_detected_from_token(self, swi_qsm_ctx):
        """QSM token should return SWI base, QSM construct."""
        result = apply_swi_logic(swi_qsm_ctx)

        assert result.base == "SWI"
        assert result.construct == "QSM"
        assert result.skip_base_detection is True
        assert result.directory_type == "anat"

    def test_qsm_has_highest_confidence(self, swi_qsm_ctx):
        """QSM detection should have highest confidence."""
        result = apply_swi_logic(swi_qsm_ctx)
        assert result.confidence >= 0.95

    def test_qsm_susceptibility_keyword(self):
        """SUSCEPTIBILITY keyword should detect QSM."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\SUSCEPTIBILITY",
            scanning_sequence="GR",
            stack_sequence_name="*swi3d1r",
        )
        result = apply_swi_logic(ctx)

        assert result.construct == "QSM"


# =============================================================================
# Test: Detection priority
# =============================================================================

class TestSWIDetectionPriority:
    """Tests for SWI output type detection priority."""

    def test_minip_beats_swi(self):
        """MinIP should take priority over SWI token."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\MNIP\SWI",
            scanning_sequence="GR",
            stack_sequence_name="*swi3d1r",
        )
        result = apply_swi_logic(ctx)
        assert result.construct == "MinIP"

    def test_qsm_beats_phase(self):
        """QSM should take priority over phase."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\QSM\P",
            scanning_sequence="GR",
            stack_sequence_name="*swi3d1r",
        )
        result = apply_swi_logic(ctx)
        assert result.construct == "QSM"

    def test_phase_beats_magnitude(self):
        """Phase-only should not be detected as magnitude."""
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\P\SWI",
            scanning_sequence="GR",
            stack_sequence_name="*swi3d1r",
        )
        result = apply_swi_logic(ctx)
        assert result.construct == "Phase"
        assert result.base == "SWI"


# =============================================================================
# Test: detect_swi_output_type
# =============================================================================

class TestDetectSWIOutputType:
    """Tests for detect_swi_output_type function."""

    def test_detects_minip(self, swi_minip_ctx):
        """Should detect MinIP output type."""
        output_type = detect_swi_output_type(swi_minip_ctx)
        assert output_type == "minip"

    def test_detects_qsm(self, swi_qsm_ctx):
        """Should detect QSM output type."""
        output_type = detect_swi_output_type(swi_qsm_ctx)
        assert output_type == "qsm"

    def test_detects_phase(self, swi_phase_ctx):
        """Should detect phase output type."""
        output_type = detect_swi_output_type(swi_phase_ctx)
        assert output_type == "phase"

    def test_detects_swi_processed(self, swi_processed_ctx):
        """Should detect SWI processed output type."""
        output_type = detect_swi_output_type(swi_processed_ctx)
        assert output_type == "swi"

    def test_detects_magnitude(self, swi_magnitude_ctx):
        """Should detect magnitude output type."""
        output_type = detect_swi_output_type(swi_magnitude_ctx)
        assert output_type == "magnitude"


# =============================================================================
# Test: get_swi_output_info
# =============================================================================

class TestGetSWIOutputInfo:
    """Tests for get_swi_output_info function."""

    def test_minip_info(self):
        """Should return correct info for MinIP."""
        info = get_swi_output_info("minip")
        assert info["base"] == "SWI"
        assert info["construct"] == "MinIP"

    def test_magnitude_info(self):
        """Should return correct info for magnitude."""
        info = get_swi_output_info("magnitude")
        assert info["base"] == "SWI"
        assert info["construct"] == "Magnitude"

    def test_phase_info(self):
        """Should return correct info for phase."""
        info = get_swi_output_info("phase")
        assert info["base"] == "SWI"
        assert info["construct"] == "Phase"

    def test_qsm_info(self):
        """Should return correct info for QSM."""
        info = get_swi_output_info("qsm")
        assert info["base"] == "SWI"
        assert info["construct"] == "QSM"

    def test_swi_processed_info(self):
        """Should return correct info for SWI processed."""
        info = get_swi_output_info("swi")
        assert info["base"] == "SWI"
        assert info["construct"] == "SWI"

    def test_mip_info(self):
        """Should return correct info for MIP."""
        info = get_swi_output_info("mip")
        assert info["base"] == "SWI"
        assert info["construct"] == "MIP"

    def test_unknown_returns_default(self):
        """Unknown output type should return default info."""
        info = get_swi_output_info("unknown_type")
        assert info["base"] == "SWI"
        assert info["construct"] == "SWI"  # Default is SWI, not Magnitude


# =============================================================================
# Test: Evidence tracking
# =============================================================================

class TestSWIEvidence:
    """Tests for evidence tracking in SWI branch."""

    def test_minip_has_evidence(self, swi_minip_ctx):
        """MinIP result should include evidence."""
        result = apply_swi_logic(swi_minip_ctx)

        assert len(result.evidence) > 0
        assert result.evidence[0].target == "MinIP"

    def test_qsm_has_evidence(self, swi_qsm_ctx):
        """QSM result should include evidence."""
        result = apply_swi_logic(swi_qsm_ctx)

        assert len(result.evidence) > 0
        assert result.evidence[0].target == "QSM"

    def test_evidence_has_description(self, swi_magnitude_ctx):
        """Evidence should include human-readable description."""
        result = apply_swi_logic(swi_magnitude_ctx)

        assert len(result.evidence) > 0
        assert result.evidence[0].description != ""


# =============================================================================
# Test: Edge cases
# =============================================================================

class TestSWIEdgeCases:
    """Tests for edge cases in SWI branch."""

    def test_empty_context_fallback(self):
        """Empty context should return fallback result (SWI)."""
        ctx = ClassificationContext()
        result = apply_swi_logic(ctx)

        # Should return fallback SWI with SWI base (not Magnitude)
        # Magnitude requires explicit indicator (M token or "magnitude" keyword)
        assert result.base == "SWI"
        assert result.construct == "SWI"
        assert result.confidence < 0.85

    def test_multi_echo_swi(self):
        """Multi-echo SWI should be handled.

        Note: M\\SWI has SWI token → processed SWI, not magnitude.
        Magnitude source would have M without SWI token.
        """
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\M\SWI",
            scanning_sequence="GR",
            stack_sequence_name="*swi3d4r",
            stack_key="multi_echo",
        )
        result = apply_swi_logic(ctx)

        # Has SWI token → processed SWI output (not magnitude source)
        assert result.base == "SWI"
        assert result.construct == "SWI"

    def test_epi_swi(self):
        """EPI-based SWI should be detected."""
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\SWI",
            scanning_sequence="EP",
            stack_sequence_name="*epfid3d1_15",
        )
        result = apply_swi_logic(ctx)

        # EPI-SWI should still be classified with SWI base
        assert result.skip_base_detection is True
        assert result.base == "SWI"

    def test_gre_swi_technique(self):
        """Standard GRE-SWI should return GRE technique."""
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\M\SWI",
            scanning_sequence="GR",
            stack_sequence_name="*swi3d1r",
        )
        result = apply_swi_logic(ctx)

        assert result.technique == "GRE"
        assert result.skip_technique_detection is True

    def test_epi_swi_technique(self):
        """EPI-SWI should return EPI technique."""
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\M\SWI",
            scanning_sequence="EP",
            stack_sequence_name="*epfid3d1_15",
        )
        result = apply_swi_logic(ctx)

        assert result.technique == "EPI"
        assert result.skip_technique_detection is True

    def test_ge_research_mode_epi_text_fallback(self):
        """GE Research Mode (RM) should use text fallback for technique.

        Real case: b37f117ee00f7bd0 2023-04-12
        - scanning_sequence="RM" (GE Research Mode, not EP or GR)
        - text_search_blob="swi ax 3depiks isat hr" (contains "3depiks")
        - Should detect EPI technique from text, not default to GRE
        """
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\OTHER",
            scanning_sequence="RM",  # GE Research Mode - neither EP nor GR
            text_search_blob="swi ax 3depiks isat hr m1 ms utredning",
        )
        result = apply_swi_logic(ctx)

        # Should detect EPI from "3depiks" in text_search_blob
        assert result.technique == "EPI"
        assert result.skip_technique_detection is True
        assert result.base == "SWI"

    def test_ge_research_mode_gre_default(self):
        """GE Research Mode without EPI keyword should default to GRE.

        When scanning_sequence is RM and no EPI keyword in text,
        technique should default to GRE (standard SWI).
        """
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\M\SWI",
            scanning_sequence="RM",  # GE Research Mode
            text_search_blob="swi ax hr isat",  # No EPI keyword
        )
        result = apply_swi_logic(ctx)

        # Should default to GRE when no EPI indicator
        assert result.technique == "GRE"
        assert result.skip_technique_detection is True


# =============================================================================
# Test: Text blob detection (semantic normalizer integration)
# =============================================================================

class TestSWITextBlobDetection:
    """Tests for text_search_blob keyword detection.

    Note: The semantic normalizer expands abbreviations (pha→phase, mag→magnitude)
    during fingerprint construction BEFORE swi.py receives the context.
    These tests use normalized text that reflects what swi.py actually sees.
    """

    def test_phase_from_text_keyword(self):
        """'phase' keyword in text should detect Phase.

        In real usage, 'pha' in SeriesDescription is normalized to 'phase'
        by the semantic normalizer before reaching swi.py.
        """
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\SWI",
            scanning_sequence="GR",
            text_search_blob="swi phase images",  # Normalized from "swi_pha_images"
        )
        result = apply_swi_logic(ctx)

        assert result.construct == "Phase"

    def test_magnitude_from_text_keyword(self):
        """'magnitude' keyword in text should detect Magnitude.

        In real usage, 'mag' in SeriesDescription is normalized to 'magnitude'
        by the semantic normalizer before reaching swi.py.

        Note: ImageType takes priority over text. If ImageType has SWI token,
        it's processed SWI. For magnitude source, ImageType has M without SWI.
        """
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\M",  # M without SWI token = magnitude source
            scanning_sequence="GR",
            text_search_blob="swi magnitude source",  # Normalized from "swi_mag_source"
        )
        result = apply_swi_logic(ctx)

        assert result.construct == "Magnitude"

    def test_minip_from_text_keyword(self):
        """'minip' keyword in text should detect MinIP."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\SWI",
            scanning_sequence="GR",
            text_search_blob="SWI minip projection",
        )
        result = apply_swi_logic(ctx)

        assert result.construct == "MinIP"

    def test_qsm_from_text_keyword(self):
        """'qsm' keyword in text should detect QSM."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY",
            scanning_sequence="GR",
            text_search_blob="QSM susceptibility map",
        )
        result = apply_swi_logic(ctx)

        assert result.construct == "QSM"
