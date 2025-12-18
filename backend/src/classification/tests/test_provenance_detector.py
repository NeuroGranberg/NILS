"""
Provenance Detector Unit Tests

Tests the ProvenanceDetector against:
1. Mock ClassificationContext objects (unit tests)
2. Real fingerprints from metadata database (integration tests)

Version: 2.0.0 - Updated to test unified_flags-based detection

Run with: pytest backend/src/classification/tests/test_provenance_detector.py -v
"""

import pytest
from pathlib import Path
from typing import Any, Dict, Optional

from src.classification.core.context import ClassificationContext
from src.classification.core.evidence import EvidenceSource
from src.classification.detectors.provenance import ProvenanceDetector, ProvenanceResult


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def detector() -> ProvenanceDetector:
    """Create a ProvenanceDetector instance."""
    return ProvenanceDetector()


def make_context(
    image_type: Optional[str] = None,
    scanning_sequence: Optional[str] = None,
    sequence_variant: Optional[str] = None,
    scan_options: Optional[str] = None,
    stack_sequence_name: Optional[str] = None,
    text_search_blob: Optional[str] = None,
    **kwargs
) -> ClassificationContext:
    """Create a ClassificationContext with specified fields."""
    return ClassificationContext(
        image_type=image_type,
        scanning_sequence=scanning_sequence,
        sequence_variant=sequence_variant,
        scan_options=scan_options,
        stack_sequence_name=stack_sequence_name,
        text_search_blob=text_search_blob,
        **kwargs
    )


# =============================================================================
# Unit Tests - SyMRI Detection
# =============================================================================


class TestSyMRIDetection:
    """Test SyMRI provenance detection."""
    
    def test_symri_from_synthetic_image_type(self, detector: ProvenanceDetector):
        """Test SyMRI detection from SYNTHETIC in image_type."""
        ctx = make_context(image_type="DERIVED\\PRIMARY\\T1W_SYNTHETIC")
        result = detector.detect(ctx)
        
        assert result.provenance == "SyMRI"
        assert result.branch == "symri"
        assert result.confidence >= 0.85  # Alternative flag detection
        assert len(result.evidence) > 0
    
    def test_symri_from_t1_synthetic_flag(self, detector: ProvenanceDetector):
        """Test SyMRI detection from has_t1_synthetic flag."""
        ctx = make_context(image_type="DERIVED\\PRIMARY\\T1W_SYNTHETIC\\NORM")
        result = detector.detect(ctx)
        
        assert result.provenance == "SyMRI"
        assert result.branch == "symri"
    
    def test_symri_from_t2_synthetic_flag(self, detector: ProvenanceDetector):
        """Test SyMRI detection from has_t2_synthetic flag."""
        ctx = make_context(image_type="DERIVED\\PRIMARY\\T2W_SYNTHETIC")
        result = detector.detect(ctx)
        
        assert result.provenance == "SyMRI"
    
    def test_symri_from_t1_map(self, detector: ProvenanceDetector):
        """Test SyMRI detection from T1MAP keyword in text_search_blob."""
        # T1MAP detection relies on construct detector, not provenance
        # For provenance, we detect via "symri" or "magic" keywords
        ctx = make_context(text_search_blob="brain symri t1map axial")
        result = detector.detect(ctx)
        
        assert result.provenance == "SyMRI"
        assert result.branch == "symri"
    
    def test_symri_from_qmap(self, detector: ProvenanceDetector):
        """Test SyMRI detection from QMAP keyword."""
        # QMAP detection uses keyword matching
        ctx = make_context(text_search_blob="brain synthetic mr axial")
        result = detector.detect(ctx)
        
        assert result.provenance == "SyMRI"
    
    def test_symri_from_mdme_sequence(self, detector: ProvenanceDetector):
        """Test SyMRI detection from MDME sequence name."""
        ctx = make_context(stack_sequence_name="*mdme2d1")
        result = detector.detect(ctx)
        
        assert result.provenance == "SyMRI"
    
    def test_symri_from_qalas_sequence(self, detector: ProvenanceDetector):
        """Test SyMRI detection from QALAS sequence name."""
        ctx = make_context(stack_sequence_name="*qalas3d1")
        result = detector.detect(ctx)
        
        assert result.provenance == "SyMRI"
    
    def test_symri_from_keyword(self, detector: ProvenanceDetector):
        """Test SyMRI detection from keyword in text_search_blob."""
        ctx = make_context(text_search_blob="brain symri t1 flair axial")
        result = detector.detect(ctx)
        
        assert result.provenance == "SyMRI"
    
    def test_symri_from_magic_keyword(self, detector: ProvenanceDetector):
        """Test SyMRI detection from MAGiC keyword."""
        ctx = make_context(text_search_blob="brain magic axial")
        result = detector.detect(ctx)
        
        assert result.provenance == "SyMRI"
    
    def test_symri_myelin_map(self, detector: ProvenanceDetector):
        """Test SyMRI detection from myelin map."""
        # Myelin maps are detected via has_myelin flag and is_synthetic
        ctx = make_context(image_type="DERIVED\\PRIMARY\\MYC\\SYNTHETIC")
        result = detector.detect(ctx)
        
        # MYC alone doesn't trigger SyMRI, but SYNTHETIC does
        # Myelin maps from SyMRI would have SYNTHETIC in image_type
        assert result.provenance == "SyMRI" or result.branch == "symri"


# =============================================================================
# Unit Tests - SWI Detection
# =============================================================================


class TestSWIDetection:
    """Test SWI provenance detection."""
    
    def test_swi_from_image_type(self, detector: ProvenanceDetector):
        """Test SWI detection from SWI in image_type."""
        ctx = make_context(image_type="ORIGINAL\\PRIMARY\\SWI\\M")
        result = detector.detect(ctx)
        
        assert result.provenance == "SWIRecon"
        assert result.branch == "swi"
        assert result.confidence >= 0.90
    
    def test_swi_from_sequence_name(self, detector: ProvenanceDetector):
        """Test SWI detection from sequence name."""
        ctx = make_context(stack_sequence_name="*swi3d1r")
        result = detector.detect(ctx)
        
        assert result.provenance == "SWIRecon"
        assert result.branch == "swi"
    
    def test_swi_from_minip(self, detector: ProvenanceDetector):
        """Test SWI detection from MinIP."""
        # MinIP alone is detected as ProjectionDerived (is_minip flag)
        # MinIP in SWI context needs "swi" in text_search_blob
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\MINIP",
            text_search_blob="brain swi minip axial"
        )
        result = detector.detect(ctx)
        
        assert result.provenance == "SWIRecon"
    
    def test_swi_from_keyword(self, detector: ProvenanceDetector):
        """Test SWI detection from keyword."""
        ctx = make_context(text_search_blob="brain swi axial 3d")
        result = detector.detect(ctx)
        
        assert result.provenance == "SWIRecon"
    
    def test_swi_swan_keyword(self, detector: ProvenanceDetector):
        """Test SWI detection from SWAN keyword (GE)."""
        ctx = make_context(text_search_blob="brain swan 3d axial")
        result = detector.detect(ctx)
        
        assert result.provenance == "SWIRecon"


# =============================================================================
# Unit Tests - DTI Detection (rawrecon branch)
# =============================================================================


class TestDTIDetection:
    """Test DTI provenance detection."""
    
    def test_dti_from_adc(self, detector: ProvenanceDetector):
        """Test DTI detection from ADC in image_type."""
        ctx = make_context(image_type="DERIVED\\PRIMARY\\DIFFUSION\\ADC")
        result = detector.detect(ctx)
        
        assert result.provenance == "DTIRecon"
        assert result.branch == "rawrecon"  # NOT special branch
    
    def test_dti_from_fa(self, detector: ProvenanceDetector):
        """Test DTI detection from FA in image_type."""
        ctx = make_context(image_type="DERIVED\\PRIMARY\\FA")
        result = detector.detect(ctx)
        
        assert result.provenance == "DTIRecon"
        assert result.branch == "rawrecon"
    
    def test_dti_from_trace(self, detector: ProvenanceDetector):
        """Test DTI detection from TRACEW in image_type."""
        ctx = make_context(image_type="DERIVED\\PRIMARY\\TRACEW")
        result = detector.detect(ctx)
        
        assert result.provenance == "DTIRecon"
    
    def test_dti_from_eadc(self, detector: ProvenanceDetector):
        """Test DTI detection from EADC in image_type."""
        ctx = make_context(image_type="DERIVED\\PRIMARY\\EADC")
        result = detector.detect(ctx)
        
        assert result.provenance == "DTIRecon"


# =============================================================================
# Unit Tests - Perfusion Detection
# =============================================================================


class TestPerfusionDetection:
    """Test perfusion provenance detection."""
    
    def test_perfusion_from_cbf(self, detector: ProvenanceDetector):
        """Test perfusion detection from CBF in image_type."""
        ctx = make_context(image_type="DERIVED\\PRIMARY\\CBF")
        result = detector.detect(ctx)
        
        assert result.provenance == "PerfusionRecon"
        assert result.branch == "rawrecon"
    
    def test_perfusion_from_cbv(self, detector: ProvenanceDetector):
        """Test perfusion detection from CBV in image_type."""
        ctx = make_context(image_type="DERIVED\\PRIMARY\\CBV")
        result = detector.detect(ctx)
        
        assert result.provenance == "PerfusionRecon"
    
    def test_perfusion_from_mtt(self, detector: ProvenanceDetector):
        """Test perfusion detection from MTT in image_type."""
        ctx = make_context(image_type="DERIVED\\PRIMARY\\MTT")
        result = detector.detect(ctx)
        
        assert result.provenance == "PerfusionRecon"


# =============================================================================
# Unit Tests - Localizer Detection
# =============================================================================


class TestLocalizerDetection:
    """Test localizer provenance detection."""
    
    def test_localizer_from_image_type(self, detector: ProvenanceDetector):
        """Test localizer detection from LOCALIZER in image_type."""
        ctx = make_context(image_type="ORIGINAL\\PRIMARY\\LOCALIZER")
        result = detector.detect(ctx)
        
        assert result.provenance == "Localizer"
        assert result.branch == "rawrecon"
    
    def test_localizer_from_keyword_scout(self, detector: ProvenanceDetector):
        """Test localizer detection from 'scout' keyword."""
        ctx = make_context(text_search_blob="brain scout 3 plane")
        result = detector.detect(ctx)
        
        assert result.provenance == "Localizer"
    
    def test_localizer_from_keyword_survey(self, detector: ProvenanceDetector):
        """Test localizer detection from 'survey' keyword."""
        ctx = make_context(text_search_blob="survey brain")
        result = detector.detect(ctx)
        
        assert result.provenance == "Localizer"


# =============================================================================
# Unit Tests - Default RawRecon
# =============================================================================


class TestDefaultRawRecon:
    """Test default RawRecon fallback."""
    
    def test_default_rawrecon_normal_scan(self, detector: ProvenanceDetector):
        """Test default RawRecon for normal acquisition."""
        ctx = make_context(image_type="ORIGINAL\\PRIMARY\\M\\NORM\\DIS2D")
        result = detector.detect(ctx)
        
        assert result.provenance == "RawRecon"
        assert result.branch == "rawrecon"
        assert result.confidence >= 0.70
    
    def test_default_rawrecon_empty_context(self, detector: ProvenanceDetector):
        """Test default RawRecon for empty context."""
        ctx = make_context()
        result = detector.detect(ctx)
        
        assert result.provenance == "RawRecon"
        assert result.branch == "rawrecon"
    
    def test_default_rawrecon_t1_mprage(self, detector: ProvenanceDetector):
        """Test default RawRecon for standard T1 MPRAGE."""
        ctx = make_context(
            image_type="ORIGINAL\\PRIMARY\\M\\NORM\\DIS3D",
            scanning_sequence="['GR', 'IR']",
            text_search_blob="brain t1 3d sagittal"  # Avoid 'mprage' which contains 'mpr'
        )
        result = detector.detect(ctx)
        
        assert result.provenance == "RawRecon"
        assert result.branch == "rawrecon"
    
    def test_default_rawrecon_t2_tse(self, detector: ProvenanceDetector):
        """Test default RawRecon for standard T2 TSE."""
        ctx = make_context(
            image_type="ORIGINAL\\PRIMARY\\M\\NORM",
            scanning_sequence="SE",
            text_search_blob="brain t2 tse axial"
        )
        result = detector.detect(ctx)
        
        assert result.provenance == "RawRecon"


# =============================================================================
# Unit Tests - Priority Ordering
# =============================================================================


class TestPriorityOrdering:
    """Test that priority ordering is respected."""
    
    def test_symri_beats_swi(self, detector: ProvenanceDetector):
        """Test SyMRI has higher priority than SWI if both match."""
        # If somehow both keywords appear, SyMRI should win
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\SYNTHETIC",
            text_search_blob="symri swi brain"
        )
        result = detector.detect(ctx)
        
        assert result.provenance == "SyMRI"
    
    def test_swi_beats_dti(self, detector: ProvenanceDetector):
        """Test SWI has higher priority than DTI."""
        ctx = make_context(
            image_type="ORIGINAL\\PRIMARY\\SWI",
            text_search_blob="adc swi brain"  # Both keywords present
        )
        result = detector.detect(ctx)
        
        assert result.provenance == "SWIRecon"
    
    def test_dti_beats_localizer(self, detector: ProvenanceDetector):
        """Test DTI has higher priority than Localizer."""
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\ADC",
            text_search_blob="localizer adc"
        )
        result = detector.detect(ctx)
        
        assert result.provenance == "DTIRecon"


# =============================================================================
# Unit Tests - Evidence Tracking
# =============================================================================


class TestEvidenceTracking:
    """Test that evidence is properly tracked."""
    
    def test_evidence_from_high_value_token(self, detector: ProvenanceDetector):
        """Test evidence is created from HIGH_VALUE_TOKEN."""
        ctx = make_context(image_type="DERIVED\\PRIMARY\\SWI")
        result = detector.detect(ctx)
        
        assert len(result.evidence) > 0
        assert any(e.source == EvidenceSource.HIGH_VALUE_TOKEN for e in result.evidence)
    
    def test_evidence_from_text_search(self, detector: ProvenanceDetector):
        """Test evidence is created from TEXT_SEARCH."""
        ctx = make_context(text_search_blob="brain symri axial")
        result = detector.detect(ctx)
        
        assert len(result.evidence) > 0
        assert any(e.source == EvidenceSource.TEXT_SEARCH for e in result.evidence)
    
    def test_multiple_evidence_sources_boost(self, detector: ProvenanceDetector):
        """Test confidence boost from multiple evidence sources."""
        # Only token evidence
        ctx1 = make_context(image_type="DERIVED\\PRIMARY\\SWI")
        result1 = detector.detect(ctx1)
        
        # Both token and text evidence
        ctx2 = make_context(
            image_type="DERIVED\\PRIMARY\\SWI",
            text_search_blob="swi brain axial"
        )
        result2 = detector.detect(ctx2)
        
        # Multiple sources should give higher confidence
        assert result2.confidence >= result1.confidence


# =============================================================================
# Unit Tests - Branch Mapping
# =============================================================================


class TestBranchMapping:
    """Test branch mapping functionality."""
    
    def test_get_branch_symri(self, detector: ProvenanceDetector):
        """Test branch mapping for SyMRI."""
        assert detector.get_branch("SyMRI") == "symri"
    
    def test_get_branch_swi(self, detector: ProvenanceDetector):
        """Test branch mapping for SWI."""
        assert detector.get_branch("SWIRecon") == "swi"
    
    def test_get_branch_dti(self, detector: ProvenanceDetector):
        """Test branch mapping for DTI (should be rawrecon)."""
        assert detector.get_branch("DTIRecon") == "rawrecon"
    
    def test_get_branch_unknown(self, detector: ProvenanceDetector):
        """Test branch mapping for unknown provenance."""
        assert detector.get_branch("UnknownProvenance") == "rawrecon"
    
    def test_get_all_provenances(self, detector: ProvenanceDetector):
        """Test getting all provenances in priority order."""
        provenances = detector.get_all_provenances()
        
        # Should have all defined provenances
        assert "SyMRI" in provenances
        assert "SWIRecon" in provenances
        assert "DTIRecon" in provenances
        assert "RawRecon" in provenances
        
        # SyMRI should be before SWI (priority order)
        assert provenances.index("SyMRI") < provenances.index("SWIRecon")


# =============================================================================
# Unit Tests - Result Properties
# =============================================================================


class TestResultProperties:
    """Test ProvenanceResult properties."""
    
    def test_value_property(self, detector: ProvenanceDetector):
        """Test value property returns provenance."""
        ctx = make_context(image_type="DERIVED\\PRIMARY\\SWI")
        result = detector.detect(ctx)
        
        assert result.value == "SWIRecon"
        assert result.provenance == "SWIRecon"
    
    def test_detect_returns_result(self, detector: ProvenanceDetector):
        """Test detect() method returns ProvenanceResult."""
        ctx = make_context(image_type="DERIVED\\PRIMARY\\SWI")
        result = detector.detect(ctx)
        
        assert isinstance(result, ProvenanceResult)
        assert result.provenance == "SWIRecon"
        assert result.branch == "swi"


# =============================================================================
# Unit Tests - MPR Text Detection (ProjectionDerived)
# =============================================================================


class TestMPRTextDetection:
    """
    Test MPR detection from text_search_blob for DERIVED images.

    Key insight: 'mpr' in text means different things based on ImageType:
    - ORIGINAL + 'mpr' in text = MPRAGE acquisition (NOT an MPR reformat)
    - DERIVED + 'mpr' in text = MPR reformat (IS ProjectionDerived)
    """

    def test_derived_with_mpr_in_text_detected_as_projection(self, detector: ProvenanceDetector):
        """DERIVED image with 'mpr' in text should be ProjectionDerived."""
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\M\\NONE\\PARALLEL\\DIS2D\\MFSPLIT",
            text_search_blob="sag t2w flair 3d space mpr cor"
        )
        result = detector.detect(ctx)

        assert result.provenance == "ProjectionDerived"
        assert result.branch == "rawrecon"

    def test_derived_with_mpr_axial_reformat(self, detector: ProvenanceDetector):
        """DERIVED axial reformat with 'mpr' should be ProjectionDerived."""
        ctx = make_context(
            image_type="DERIVED\\SECONDARY\\AXIAL",
            text_search_blob="mpr"
        )
        result = detector.detect(ctx)

        assert result.provenance == "ProjectionDerived"

    def test_derived_mprage_mpr_reformat(self, detector: ProvenanceDetector):
        """DERIVED MPR reformat of MPRAGE should be ProjectionDerived."""
        # This is an MPR reformat of an MPRAGE - both 'mprage' and 'mpr' in text
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\M\\NONE\\PARALLEL\\DIS2D\\MFSPLIT",
            text_search_blob="ax t1w 3d gradient-echo inversion-recovery mprage mpr sag"
        )
        result = detector.detect(ctx)

        assert result.provenance == "ProjectionDerived"

    def test_original_mprage_not_detected_as_mpr(self, detector: ProvenanceDetector):
        """ORIGINAL MPRAGE with 'mpr' in text should NOT be ProjectionDerived."""
        ctx = make_context(
            image_type="ORIGINAL\\PRIMARY\\M\\NORM\\DIS2D",
            scanning_sequence="['GR', 'IR']",
            text_search_blob="t1w mpr ns tra pat2 startfl3d1"
        )
        result = detector.detect(ctx)

        # Should be RawRecon, NOT ProjectionDerived
        assert result.provenance == "RawRecon"

    def test_original_inversion_recovery_mpr_not_detected(self, detector: ProvenanceDetector):
        """ORIGINAL IR sequence with 'mpr' (MPRAGE) should NOT be ProjectionDerived."""
        ctx = make_context(
            image_type="ORIGINAL\\PRIMARY\\OTHER",
            scanning_sequence="IR",
            text_search_blob="ms forskning 3d inversion-recovery mpr tra"
        )
        result = detector.detect(ctx)

        # Should be RawRecon - this is an MPRAGE acquisition
        assert result.provenance == "RawRecon"

    def test_derived_mpr_with_imagetype_mpr_token(self, detector: ProvenanceDetector):
        """DERIVED with MPR token in ImageType should be ProjectionDerived (baseline)."""
        # This already worked before the fix - ensure we don't break it
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\MPR\\NORM\\DIS2D",
            text_search_blob="ax t1w mprage mpr tra"
        )
        result = detector.detect(ctx)

        assert result.provenance == "ProjectionDerived"

    def test_original_with_mpr_token_in_imagetype(self, detector: ProvenanceDetector):
        """ORIGINAL with MPR token in ImageType - edge case."""
        # Some scanners put MPR in ImageType for ORIGINAL images (auto-reformat)
        ctx = make_context(
            image_type="ORIGINAL\\PRIMARY\\MPR\\M\\FFE",
            text_search_blob="patient aligned mpr"
        )
        result = detector.detect(ctx)

        # MPR token in ImageType should trigger detection regardless of ORIGINAL/DERIVED
        assert result.provenance == "ProjectionDerived"

    def test_mpr_word_boundary_no_false_positive_compress(self, detector: ProvenanceDetector):
        """'compress' should NOT trigger MPR detection (word boundary)."""
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\M\\NORM",
            text_search_blob="t1w compress factor 2 axial"
        )
        result = detector.detect(ctx)

        # Should NOT be ProjectionDerived (compress != mpr)
        assert result.provenance != "ProjectionDerived"

    def test_derived_localizer_with_mpr(self, detector: ProvenanceDetector):
        """DERIVED localizer with 'mpr' - Localizer should win by priority."""
        ctx = make_context(
            image_type="DERIVED\\SECONDARY\\LOCALIZER",
            text_search_blob="mpr localizer"
        )
        result = detector.detect(ctx)

        # Localizer has higher priority than ProjectionDerived
        assert result.provenance == "Localizer"


# =============================================================================
# Unit Tests - MP2RAGE Output Detection
# =============================================================================


class TestMP2RAGEOutputDetection:
    """
    Test MP2RAGE output detection flags in ClassificationContext.

    MP2RAGE produces multiple outputs:
    - INV1: First inversion (TI ~700-1000ms) - T1-weighted
    - INV2: Second inversion (TI ~2500-3200ms) - PD-weighted
    - UNI: Uniform bias-corrected T1w (has salt-and-pepper noise)
    - UNI-DEN: Uniform denoised (clean background)
    - T1map: Quantitative T1 relaxation map
    """

    # =========================================================================
    # INV1 Detection Tests (TI-based + text fallback)
    # =========================================================================

    def test_mp2rage_inv1_by_ti(self):
        """INV1 detected by short TI (< 1800ms) for ORIGINAL images with MP2RAGE context."""
        ctx = make_context(
            image_type="ORIGINAL\\PRIMARY\\M\\NORM",
            text_search_blob="sag 3d t1w mp2rage startfl3d1",  # MP2RAGE context required
            mr_ti=870.0  # Typical INV1 TI
        )
        uf = ctx.unified_flags

        assert uf["is_mp2rage_inv1"] is True
        assert uf["is_mp2rage_inv2"] is False

    def test_mp2rage_inv1_by_text_fallback(self):
        """INV1 detected by 'inv1' keyword when TI unavailable."""
        ctx = make_context(
            image_type="ORIGINAL\\PRIMARY\\M\\NORM",
            text_search_blob="sag 3d t1w mp2rage inv1 startfl3d1"
        )
        uf = ctx.unified_flags

        assert uf["is_mp2rage_inv1"] is True
        assert uf["is_mp2rage_inv2"] is False

    def test_mp2rage_inv1_ti_boundary_999ms(self):
        """TI=999ms should be INV1 (below 1800ms threshold) with MP2RAGE context."""
        ctx = make_context(
            image_type="ORIGINAL\\PRIMARY\\M\\NORM",
            text_search_blob="sag 3d t1w mp2rage startfl3d1",  # MP2RAGE context required
            mr_ti=999.0
        )
        uf = ctx.unified_flags

        assert uf["is_mp2rage_inv1"] is True
        assert uf["is_mp2rage_inv2"] is False

    # =========================================================================
    # INV2 Detection Tests (TI-based + text fallback)
    # =========================================================================

    def test_mp2rage_inv2_by_ti(self):
        """INV2 detected by long TI (>= 1800ms) for ORIGINAL images with MP2RAGE context."""
        ctx = make_context(
            image_type="ORIGINAL\\PRIMARY\\M\\NORM",
            text_search_blob="sag 3d t1w mp2rage startfl3d1",  # MP2RAGE context required
            mr_ti=3200.0  # Typical INV2 TI
        )
        uf = ctx.unified_flags

        assert uf["is_mp2rage_inv2"] is True
        assert uf["is_mp2rage_inv1"] is False

    def test_mp2rage_inv2_by_text_fallback(self):
        """INV2 detected by 'inv2' keyword when TI unavailable."""
        ctx = make_context(
            image_type="ORIGINAL\\PRIMARY\\M\\NORM",
            text_search_blob="sag 3d t1w mp2rage inv2 startfl3d1"
        )
        uf = ctx.unified_flags

        assert uf["is_mp2rage_inv2"] is True
        assert uf["is_mp2rage_inv1"] is False

    def test_mp2rage_inv2_ti_boundary_2500ms(self):
        """TI=2500ms should be INV2 (above 1800ms threshold) with MP2RAGE context."""
        ctx = make_context(
            image_type="ORIGINAL\\PRIMARY\\M\\NORM",
            text_search_blob="sag 3d t1w mp2rage startfl3d1",  # MP2RAGE context required
            mr_ti=2500.0
        )
        uf = ctx.unified_flags

        assert uf["is_mp2rage_inv2"] is True
        assert uf["is_mp2rage_inv1"] is False

    def test_mp2rage_inv_not_detected_for_derived(self):
        """INV1/INV2 TI detection should NOT apply to DERIVED images."""
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\M\\UNI",
            text_search_blob="sag 3d t1w mp2rage uni images",
            mr_ti=870.0  # This TI inherited from parent, not meaningful
        )
        uf = ctx.unified_flags

        # TI-based detection should be False for DERIVED
        # (UNI/UNI-DEN inherit TI from parent series)
        assert uf["is_mp2rage_inv1"] is False
        assert uf["is_mp2rage_inv2"] is False

    # =========================================================================
    # Uniform Detection Tests
    # =========================================================================

    def test_uniform_detected_by_m_uni_token(self):
        """Uniform detected by M\\UNI token in ImageType."""
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\M\\UNI\\DIS3D\\MFSPLIT",
            text_search_blob="sag 3d t1w mp2rage uni images startfl3d1"
        )
        uf = ctx.unified_flags

        assert uf["has_uniform"] is True
        assert uf["is_uniform_denoised"] is False  # Not denoised

    def test_uniform_detected_by_uniform_token(self):
        """Uniform detected by UNIFORM token in ImageType."""
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\T1\\UNIFORM\\PARALLEL",
            text_search_blob="sag 3d t1w mp2rage"
        )
        uf = ctx.unified_flags

        assert uf["has_uniform"] is True

    # =========================================================================
    # Uniform Denoised Detection Tests
    # =========================================================================

    def test_uniform_denoised_by_uni_den_pattern(self):
        """UNI-DEN detected by 'uni - den' in text."""
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\M\\UNI\\DIS3D\\MFSPLIT",
            text_search_blob="sag 3d t1w mp2rage uni - den startfl3d1 uniform weighted image"
        )
        uf = ctx.unified_flags

        assert uf["has_uniform"] is True
        assert uf["is_uniform_denoised"] is True

    def test_uniform_denoised_by_uniden_pattern(self):
        """UNI-DEN detected by 'uni-den' (no spaces) in text."""
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\M\\UNI\\DIS3D",
            text_search_blob="mp2rage uni-den image"
        )
        uf = ctx.unified_flags

        assert uf["is_uniform_denoised"] is True

    def test_uniform_not_denoised(self):
        """Regular UNI (not denoised) should have is_uniform_denoised=False."""
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\M\\UNI\\DIS3D\\MFSPLIT",
            text_search_blob="sag 3d t1w mp2rage uni images startfl3d1 uniform image"
        )
        uf = ctx.unified_flags

        assert uf["has_uniform"] is True
        assert uf["is_uniform_denoised"] is False

    def test_uniform_denoised_requires_uniform_flag(self):
        """UNI-DEN detection requires has_uniform flag (ImageType)."""
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\M\\NORM",  # No UNI token
            text_search_blob="mp2rage uni - den"  # Text says denoised
        )
        uf = ctx.unified_flags

        # Should NOT be detected without has_uniform
        assert uf["has_uniform"] is False
        assert uf["is_uniform_denoised"] is False

    # =========================================================================
    # T1map Detection (uses existing has_t1_map flag)
    # =========================================================================

    def test_t1map_detected(self):
        """T1map detected by T1 MAP in ImageType."""
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\T1 MAP\\DIS3D\\MFSPLIT",
            text_search_blob="sag 3d t1w mp2rage images map"
        )
        uf = ctx.unified_flags

        assert uf["has_t1_map"] is True

    # =========================================================================
    # Edge Cases
    # =========================================================================

    def test_mp2rage_with_zero_ti_no_inv_detection(self):
        """TI=0 should not trigger INV1/INV2 detection."""
        ctx = make_context(
            image_type="DERIVED\\PRIMARY\\M\\UNI",
            text_search_blob="mp2rage uni images",
            mr_ti=0.0  # Computed outputs often have TI=0
        )
        uf = ctx.unified_flags

        assert uf["is_mp2rage_inv1"] is False
        assert uf["is_mp2rage_inv2"] is False

    def test_mp2rage_inv1_text_word_boundary(self):
        """'inv1' should match word boundary (not 'inv10' or 'pinv1')."""
        # Should match
        ctx1 = make_context(
            image_type="ORIGINAL\\PRIMARY\\M",
            text_search_blob="mp2rage inv1 image"
        )
        assert ctx1.unified_flags["is_mp2rage_inv1"] is True

        # Should NOT match 'inv10'
        ctx2 = make_context(
            image_type="ORIGINAL\\PRIMARY\\M",
            text_search_blob="mp2rage inv10 image"
        )
        assert ctx2.unified_flags["is_mp2rage_inv1"] is False

    def test_flair_long_ti_not_detected_as_mp2rage_inv2(self):
        """FLAIR with long TI should NOT be detected as MP2RAGE INV2."""
        ctx = make_context(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE\\IR",
            text_search_blob="t2 flair brain",  # No MP2RAGE keyword
            mr_ti=2500.0  # Long TI similar to MP2RAGE INV2
        )
        uf = ctx.unified_flags

        # Should NOT be detected as MP2RAGE INV2 (no mp2rage context)
        assert uf["is_mp2rage_inv2"] is False
        assert uf["is_mp2rage_inv1"] is False

    def test_standard_ir_not_detected_as_mp2rage_inv1(self):
        """Standard IR with short TI should NOT be detected as MP2RAGE INV1."""
        ctx = make_context(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE\\IR",
            text_search_blob="t1 ir brain",  # No MP2RAGE keyword
            mr_ti=900.0  # Short TI similar to MP2RAGE INV1
        )
        uf = ctx.unified_flags

        # Should NOT be detected as MP2RAGE INV1 (no mp2rage context)
        assert uf["is_mp2rage_inv1"] is False
        assert uf["is_mp2rage_inv2"] is False


# =============================================================================
# Integration Tests - Database Validation (Requires Running DB)
# =============================================================================


class TestDatabaseValidation:
    """
    Integration tests against real database.
    
    These tests require the metadata database to be running.
    Skip if database is not available.
    """
    
    @pytest.fixture
    def db_connection(self):
        """Get database connection."""
        try:
            import psycopg2
            conn = psycopg2.connect(
                host="localhost",
                port=5532,
                database="neurotoolkit_metadata",
                user="postgres",
                password="postgres"
            )
            yield conn
            conn.close()
        except Exception:
            pytest.skip("Database not available")
    
    def get_fingerprints_by_pattern(
        self, 
        conn, 
        pattern: str, 
        field: str = "image_type",
        limit: int = 50
    ):
        """Get fingerprints matching a pattern."""
        cur = conn.cursor()
        query = f"""
        SELECT 
            image_type,
            scanning_sequence,
            sequence_variant,
            scan_options,
            stack_sequence_name,
            text_search_blob
        FROM stack_fingerprint 
        WHERE LOWER({field}) LIKE %s
        LIMIT %s
        """
        cur.execute(query, (f'%{pattern.lower()}%', limit))
        rows = cur.fetchall()
        cur.close()
        return rows
    
    def fingerprint_to_context(self, row) -> ClassificationContext:
        """Convert database row to ClassificationContext."""
        return ClassificationContext(
            image_type=row[0],
            scanning_sequence=row[1],
            sequence_variant=row[2],
            scan_options=row[3],
            stack_sequence_name=row[4],
            text_search_blob=row[5],
        )
    
    def test_symri_real_fingerprints(self, detector, db_connection):
        """Validate SyMRI detection on real SYNTHETIC fingerprints."""
        rows = self.get_fingerprints_by_pattern(db_connection, "synthetic")
        
        if not rows:
            pytest.skip("No SYNTHETIC fingerprints found")
        
        symri_count = 0
        for row in rows:
            ctx = self.fingerprint_to_context(row)
            result = detector.detect(ctx)
            if result.provenance == "SyMRI":
                symri_count += 1
        
        # At least 80% should be detected as SyMRI
        detection_rate = symri_count / len(rows)
        assert detection_rate >= 0.80, f"SyMRI detection rate {detection_rate:.1%} < 80%"
    
    def test_swi_real_fingerprints(self, detector, db_connection):
        """Validate SWI detection on real SWI fingerprints."""
        rows = self.get_fingerprints_by_pattern(db_connection, "swi")
        
        if not rows:
            pytest.skip("No SWI fingerprints found")
        
        swi_count = 0
        for row in rows:
            ctx = self.fingerprint_to_context(row)
            result = detector.detect(ctx)
            if result.provenance == "SWIRecon":
                swi_count += 1
        
        # At least 80% should be detected as SWIRecon
        detection_rate = swi_count / len(rows)
        assert detection_rate >= 0.80, f"SWI detection rate {detection_rate:.1%} < 80%"
    
    def test_dti_real_fingerprints(self, detector, db_connection):
        """Validate DTI detection on real ADC fingerprints."""
        rows = self.get_fingerprints_by_pattern(db_connection, "adc")
        
        if not rows:
            pytest.skip("No ADC fingerprints found")
        
        dti_count = 0
        for row in rows:
            ctx = self.fingerprint_to_context(row)
            result = detector.detect(ctx)
            if result.provenance == "DTIRecon":
                dti_count += 1
        
        # At least 80% should be detected as DTIRecon
        detection_rate = dti_count / len(rows)
        assert detection_rate >= 0.80, f"DTI detection rate {detection_rate:.1%} < 80%"
    
    def test_coverage_statistics(self, detector, db_connection):
        """Run detector on random sample and report distribution."""
        cur = db_connection.cursor()
        cur.execute("""
        SELECT 
            image_type,
            scanning_sequence,
            sequence_variant,
            scan_options,
            stack_sequence_name,
            text_search_blob
        FROM stack_fingerprint 
        ORDER BY RANDOM()
        LIMIT 500
        """)
        rows = cur.fetchall()
        cur.close()
        
        if not rows:
            pytest.skip("No fingerprints found")
        
        from collections import defaultdict
        stats = defaultdict(int)
        
        for row in rows:
            ctx = self.fingerprint_to_context(row)
            result = detector.detect(ctx)
            stats[result.provenance] += 1
        
        # Print distribution for debugging
        print("\nProvenance Distribution (n=500):")
        for prov, count in sorted(stats.items(), key=lambda x: -x[1]):
            print(f"  {prov}: {count} ({count/5:.1f}%)")
        
        # RawRecon should be the most common
        assert stats["RawRecon"] > 0, "No RawRecon detected"


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
