"""
Tests for BaseContrastDetector

Tests the four-tier detection strategy:
1. Technique inference
2. Exclusive flags
3. Keywords
4. Physics ranges

Test data derived from real database fingerprints (400K+ stacks).
"""

import pytest
from pathlib import Path

from ..core.context import ClassificationContext
from ..detectors.base_contrast import BaseContrastDetector, BaseContrastResult


@pytest.fixture
def detector():
    """Create a BaseContrastDetector instance."""
    return BaseContrastDetector()


# =============================================================================
# TIER 1: TECHNIQUE INFERENCE TESTS
# =============================================================================

class TestTechniqueInference:
    """Test technique-to-base inference."""
    
    def test_mprage_implies_t1w(self, detector):
        """MPRAGE always implies T1w base."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="mprage brain",
        )
        result = detector.detect(ctx, technique="MPRAGE")
        assert result.value == "T1w"
        assert result.confidence >= 0.90
    
    def test_mp2rage_implies_t1w(self, detector):
        """MP2RAGE implies T1w base."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="mp2rage",
        )
        result = detector.detect(ctx, technique="MP2RAGE")
        assert result.value == "T1w"
        assert result.confidence >= 0.90
    
    def test_tof_implies_t1w(self, detector):
        """TOF-MRA implies T1w base (inflow enhancement)."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="tof mra",
        )
        result = detector.detect(ctx, technique="TOF-MRA")
        assert result.value == "T1w"
        assert result.confidence >= 0.85
    
    def test_swi_implies_swi(self, detector):
        """SWI technique implies SWI base."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\SWI",
            text_search_blob="swi brain",
        )
        result = detector.detect(ctx, technique="SWI")
        assert result.value == "SWI"
        assert result.confidence >= 0.90
    
    def test_dwi_epi_implies_dwi(self, detector):
        """DWI-EPI implies DWI base."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\DIFFUSION",
            text_search_blob="dwi brain",
        )
        result = detector.detect(ctx, technique="DWI-EPI")
        assert result.value == "DWI"
        assert result.confidence >= 0.90


# =============================================================================
# TIER 2: EXCLUSIVE FLAGS TESTS
# =============================================================================

class TestExclusiveFlags:
    """Test exclusive unified_flag detection."""
    
    def test_dwi_flag_implies_dwi(self, detector):
        """is_dwi flag implies DWI base."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\DIFFUSION",
            scanning_sequence="EP\\SE",
            text_search_blob="brain",  # No explicit DWI keyword
            mr_diffusion_b_value="1000",
        )
        result = detector.detect(ctx)
        assert result.value == "DWI"
        assert result.confidence >= 0.85
    
    def test_adc_flag_implies_dwi(self, detector):
        """has_adc flag implies DWI base."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\DIFFUSION\\ADC",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.value == "DWI"
        assert result.confidence >= 0.85
    
    def test_fa_flag_implies_dwi(self, detector):
        """has_fa flag implies DWI base."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\DIFFUSION\\FA",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.value == "DWI"
        assert result.confidence >= 0.85
    
    def test_cbf_flag_implies_pwi(self, detector):
        """has_cbf flag implies PWI base."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\PERFUSION\\CBF",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.value == "PWI"
        assert result.confidence >= 0.85
    
    def test_swi_flag_implies_swi(self, detector):
        """has_swi flag implies SWI base."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\SWI\\NORM",
            scanning_sequence="GR",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.value == "SWI"
        assert result.confidence >= 0.85


# =============================================================================
# TIER 3: FLAIR SPECIAL HANDLING
# =============================================================================

class TestFlairHandling:
    """Test FLAIR T1 vs T2 differentiation."""
    
    def test_explicit_t1_flair(self, detector):
        """Explicit 'T1 FLAIR' in text implies T1w."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE\\IR",
            text_search_blob="t1 flair spine",
            mr_te=9.0,
            mr_ti=800.0,
        )
        result = detector.detect(ctx)
        assert result.value == "T1w"
        assert result.confidence >= 0.80
    
    def test_explicit_t2_flair(self, detector):
        """Explicit 'T2 FLAIR' in text implies T2w."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE\\IR",
            text_search_blob="t2 flair brain",
            mr_te=120.0,
            mr_ti=2500.0,
        )
        result = detector.detect(ctx)
        assert result.value == "T2w"
        assert result.confidence >= 0.80
    
    def test_generic_flair_short_te_is_t1(self, detector):
        """Generic FLAIR with short TE (< 40ms) is T1-FLAIR."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE\\IR",
            text_search_blob="flair spine",  # No T1/T2 prefix
            mr_te=23.0,  # Short TE
            mr_ti=785.0,
        )
        result = detector.detect(ctx)
        assert result.value == "T1w"
        assert result.confidence >= 0.80
    
    def test_generic_flair_long_te_is_t2(self, detector):
        """Generic FLAIR with long TE (>= 40ms) is T2-FLAIR."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE\\IR",
            text_search_blob="flair brain",  # No T1/T2 prefix
            mr_te=120.0,  # Long TE
            mr_ti=2500.0,
        )
        result = detector.detect(ctx)
        assert result.value == "T2w"
        assert result.confidence >= 0.80
    
    def test_flair_no_te_defaults_to_t2(self, detector):
        """Generic FLAIR without TE defaults to T2-FLAIR."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE\\IR",
            text_search_blob="flair brain",
            mr_te=None,
            mr_ti=2500.0,
        )
        result = detector.detect(ctx)
        assert result.value == "T2w"
        assert result.confidence >= 0.65


# =============================================================================
# TIER 4: DUAL-ECHO HANDLING
# =============================================================================

class TestDualEchoHandling:
    """Test PD+T2 dual-echo stack splitting."""
    
    def test_dual_echo_short_te_is_pd(self, detector):
        """Dual-echo series with short TE is PD."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE",
            text_search_blob="pd+t2_tse_tra brain",
            mr_te=16.0,  # Short TE → PD echo
            mr_tr=3200.0,
        )
        result = detector.detect(ctx)
        assert result.value == "PDw"
        assert result.confidence >= 0.80
    
    def test_dual_echo_long_te_is_t2(self, detector):
        """Dual-echo series with long TE is T2."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE",
            text_search_blob="pd+t2_tse_tra brain",
            mr_te=95.0,  # Long TE → T2 echo
            mr_tr=3200.0,
        )
        result = detector.detect(ctx)
        assert result.value == "T2w"
        assert result.confidence >= 0.80
    
    def test_dual_echo_variants(self, detector):
        """Test various dual-echo naming conventions."""
        for keyword in ["pd t2", "t2 pd", "pd/t2", "proton density t2"]:
            ctx = ClassificationContext(
                image_type="ORIGINAL\\PRIMARY\\M\\ND",
                scanning_sequence="SE",
                text_search_blob=f"{keyword} brain",
                mr_te=90.0,
                mr_tr=4000.0,
            )
            result = detector.detect(ctx)
            assert result.value == "T2w", f"Failed for keyword: {keyword}"


# =============================================================================
# TIER 5: KEYWORD MATCHING
# =============================================================================

class TestKeywordMatching:
    """Test keyword-based detection."""
    
    def test_t1w_keyword(self, detector):
        """T1w keyword detection."""
        for keyword in ["t1w", "t1 w", "t1-w", "t1 weighted"]:
            ctx = ClassificationContext(
                image_type="ORIGINAL\\PRIMARY\\M\\ND",
                text_search_blob=f"{keyword} brain",
            )
            result = detector.detect(ctx)
            assert result.value == "T1w", f"Failed for: {keyword}"
            assert result.confidence >= 0.80
    
    def test_t2w_keyword(self, detector):
        """T2w keyword detection."""
        for keyword in ["t2w", "t2 w", "t2-w", "t2 weighted"]:
            ctx = ClassificationContext(
                image_type="ORIGINAL\\PRIMARY\\M\\ND",
                text_search_blob=f"{keyword} brain",
            )
            result = detector.detect(ctx)
            assert result.value == "T2w", f"Failed for: {keyword}"
            assert result.confidence >= 0.80
    
    def test_pdw_keyword(self, detector):
        """PDw keyword detection."""
        for keyword in ["pdw", "pd w", "proton density"]:
            ctx = ClassificationContext(
                image_type="ORIGINAL\\PRIMARY\\M\\ND",
                text_search_blob=f"{keyword} brain",
            )
            result = detector.detect(ctx)
            assert result.value == "PDw", f"Failed for: {keyword}"
            assert result.confidence >= 0.80
    
    def test_dwi_keyword(self, detector):
        """DWI keyword detection."""
        for keyword in ["dwi", "diffusion", "dti"]:
            ctx = ClassificationContext(
                image_type="ORIGINAL\\PRIMARY\\M\\ND",
                text_search_blob=f"{keyword} brain",
            )
            result = detector.detect(ctx)
            assert result.value == "DWI", f"Failed for: {keyword}"
            assert result.confidence >= 0.80
    
    def test_swi_keyword(self, detector):
        """SWI keyword detection."""
        for keyword in ["swi", "susceptibility weighted"]:
            ctx = ClassificationContext(
                image_type="ORIGINAL\\PRIMARY\\M\\ND",
                text_search_blob=f"{keyword} brain",
            )
            result = detector.detect(ctx)
            assert result.value == "SWI", f"Failed for: {keyword}"
            assert result.confidence >= 0.80


# =============================================================================
# TIER 6: PHYSICS-BASED INFERENCE
# =============================================================================

class TestPhysicsInference:
    """Test TR/TE/TI physics-based detection."""
    
    def test_se_t1w_physics(self, detector):
        """SE with short TR/TE implies T1w."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE",
            text_search_blob="brain",  # No explicit weighting
            mr_tr=500.0,
            mr_te=10.0,
        )
        result = detector.detect(ctx)
        assert result.value == "T1w"
        assert result.confidence >= 0.65
    
    def test_se_t2w_physics(self, detector):
        """SE with long TR/TE implies T2w."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE",
            text_search_blob="brain",
            mr_tr=4000.0,
            mr_te=90.0,
        )
        result = detector.detect(ctx)
        assert result.value == "T2w"
        assert result.confidence >= 0.65
    
    def test_se_pdw_physics(self, detector):
        """SE with long TR, short TE implies PDw."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE",
            text_search_blob="brain",
            mr_tr=3000.0,
            mr_te=15.0,
        )
        result = detector.detect(ctx)
        assert result.value == "PDw"
        assert result.confidence >= 0.60
    
    def test_gre_t1w_physics(self, detector):
        """GRE with short TE implies T1w."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="GR",
            text_search_blob="brain",
            mr_te=3.0,
        )
        result = detector.detect(ctx)
        assert result.value == "T1w"
        assert result.confidence >= 0.65
    
    def test_gre_t2star_physics(self, detector):
        """GRE with long TE implies T2*w."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="GR",
            text_search_blob="brain",
            mr_te=25.0,
        )
        result = detector.detect(ctx)
        assert result.value == "T2*w"
        assert result.confidence >= 0.65
    
    def test_ir_stir_physics(self, detector):
        """IR with short TI (< 300ms) implies STIR-like (T2w base)."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE\\IR",
            text_search_blob="spine",
            mr_ti=150.0,  # Short TI for fat nulling
            mr_te=50.0,
        )
        result = detector.detect(ctx)
        assert result.value == "T2w"
        assert result.confidence >= 0.70
    
    def test_ir_standard_physics(self, detector):
        """IR with medium TI (300-1500ms) implies T1w."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE\\IR",
            text_search_blob="brain",
            mr_ti=800.0,  # Medium TI
            mr_te=10.0,
        )
        result = detector.detect(ctx)
        assert result.value == "T1w"
        assert result.confidence >= 0.65


# =============================================================================
# FALLBACK TESTS
# =============================================================================

class TestFallback:
    """Test fallback to Unknown."""
    
    def test_empty_context_returns_unknown(self, detector):
        """Empty context returns Unknown."""
        ctx = ClassificationContext()
        result = detector.detect(ctx)
        assert result.value == "Unknown"
        assert result.confidence <= 0.55
    
    def test_ambiguous_physics_returns_unknown(self, detector):
        """Ambiguous physics without keywords returns Unknown."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE",
            text_search_blob="brain",  # No weighting keywords
            mr_tr=1500.0,  # Ambiguous TR
            mr_te=35.0,    # Ambiguous TE
        )
        result = detector.detect(ctx)
        # Should fall through to Unknown
        assert result.value == "Unknown"


# =============================================================================
# REAL DATABASE EXAMPLES
# =============================================================================

class TestRealDatabaseExamples:
    """Test with real fingerprints from database."""
    
    def test_real_t1_mprage(self, detector):
        """Real MPRAGE fingerprint."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND\\NORM",
            scanning_sequence="['GR', 'IR']",
            sequence_variant="['SK', 'SP', 'MP', 'OSP']",
            scan_options="IR",
            stack_sequence_name="*tfl3d1_16ns",
            text_search_blob="t1_mprage_sag_p2_iso t1_mprage_sag_p2_iso brain head",
            mr_tr=2300.0,
            mr_te=2.98,
            mr_ti=900.0,
        )
        result = detector.detect(ctx, technique="MPRAGE")
        assert result.value == "T1w"
        assert result.confidence >= 0.90
    
    def test_real_t2_flair(self, detector):
        """Real T2-FLAIR fingerprint."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="['SE', 'IR']",
            sequence_variant="['SK', 'SP', 'MP', 'OSP']",
            scan_options="['IR', 'PFP', 'FS']",
            stack_sequence_name="*tir2d1_21",
            text_search_blob="t2_flair_tra brain head",
            mr_tr=9000.0,
            mr_te=109.0,
            mr_ti=2500.0,
        )
        result = detector.detect(ctx)
        assert result.value == "T2w"
        assert result.confidence >= 0.80
    
    def test_real_t1_flair(self, detector):
        """Real T1-FLAIR fingerprint."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="['SE', 'IR']",
            stack_sequence_name="tir2d1_7",
            text_search_blob="sag t1 flair spine",
            mr_tr=2000.0,
            mr_te=9.8,
            mr_ti=900.0,
        )
        result = detector.detect(ctx)
        assert result.value == "T1w"
        assert result.confidence >= 0.80
    
    def test_real_dwi(self, detector):
        """Real DWI fingerprint."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\DIFFUSION",
            scanning_sequence="['SE', 'EP']",
            sequence_variant="['SK', 'SP']",
            scan_options="FS",
            stack_sequence_name="*ep_b1000t",
            text_search_blob="dwi brain head",
            mr_diffusion_b_value="1000",
            mr_tr=4500.0,
            mr_te=90.0,
        )
        result = detector.detect(ctx)
        assert result.value == "DWI"
        assert result.confidence >= 0.85
    
    def test_real_adc(self, detector):
        """Real ADC map fingerprint."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\DIFFUSION\\ADC\\DIS2D",
            scanning_sequence="['SE', 'EP']",
            stack_sequence_name="*ep_b0_1000",
            text_search_blob="ep2d_diff_3scan_trace_p2_adc brain head",
        )
        result = detector.detect(ctx)
        assert result.value == "DWI"
        assert result.confidence >= 0.85
    
    def test_real_swi(self, detector):
        """Real SWI fingerprint."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\SWI\\NORM\\DIS2D",
            scanning_sequence="GR",
            sequence_variant="['SP', 'OSP']",
            stack_sequence_name="*swi3d1r",
            text_search_blob="swi brain head",
            mr_acquisition_type="3D",
            mr_tr=28.0,
            mr_te=20.0,
        )
        result = detector.detect(ctx)
        assert result.value == "SWI"
        assert result.confidence >= 0.85
    
    def test_real_pd_t2_dual_echo_pd(self, detector):
        """Real PD+T2 dual-echo - PD stack."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE",
            stack_sequence_name="*tse2d2rs5",
            text_search_blob="pd+t2_tse_tra brain head",
            mr_tr=3200.0,
            mr_te=17.0,  # Short TE → PD
        )
        result = detector.detect(ctx)
        assert result.value == "PDw"
        assert result.confidence >= 0.80
    
    def test_real_pd_t2_dual_echo_t2(self, detector):
        """Real PD+T2 dual-echo - T2 stack."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            scanning_sequence="SE",
            stack_sequence_name="*tse2d2rs5",
            text_search_blob="pd+t2_tse_tra brain head",
            mr_tr=3200.0,
            mr_te=100.0,  # Long TE → T2
        )
        result = detector.detect(ctx)
        assert result.value == "T2w"
        assert result.confidence >= 0.80


# =============================================================================
# SYNTHETIC MRI OUTPUTS
# =============================================================================

class TestSyntheticMRI:
    """Test synthetic MRI base contrast detection."""
    
    def test_synthetic_t1w(self, detector):
        """Synthetic T1w has T1w base."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\T1\\SYNTHETIC",
            scanning_sequence="SE",
            sequence_variant="SYNTHETIC",
            text_search_blob="synthetic t1 brain",
        )
        result = detector.detect(ctx)
        assert result.value == "T1w"
        assert result.confidence >= 0.85
    
    def test_synthetic_t2w(self, detector):
        """Synthetic T2w has T2w base."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\T2\\SYNTHETIC",
            scanning_sequence="SE",
            sequence_variant="SYNTHETIC",
            text_search_blob="synthetic t2 brain",
        )
        result = detector.detect(ctx)
        assert result.value == "T2w"
        assert result.confidence >= 0.85
    
    def test_synthetic_flair(self, detector):
        """Synthetic FLAIR has T2w base."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\T2FLAIR\\SYNTHETIC",
            scanning_sequence="['SE', 'IR']",
            sequence_variant="SYNTHETIC",
            text_search_blob="synthetic flair brain",
        )
        result = detector.detect(ctx)
        assert result.value == "T2w"
        assert result.confidence >= 0.85
    
    def test_synthetic_pdw(self, detector):
        """Synthetic PDw has PDw base."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\PD\\SYNTHETIC",
            scanning_sequence="SE",
            sequence_variant="SYNTHETIC",
            text_search_blob="synthetic pd brain",
        )
        result = detector.detect(ctx)
        assert result.value == "PDw"
        assert result.confidence >= 0.85


# =============================================================================
# QUANTITATIVE MAPS
# =============================================================================

class TestQuantitativeMaps:
    """Test quantitative map base contrast detection."""
    
    def test_t1_map(self, detector):
        """T1 map has T1w base."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\T1 MAP",
            text_search_blob="t1 map brain",
        )
        result = detector.detect(ctx)
        assert result.value == "T1w"
        assert result.confidence >= 0.80
    
    def test_t2_map(self, detector):
        """T2 map has T2w base."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\T2 MAP",
            text_search_blob="t2 map brain",
        )
        result = detector.detect(ctx)
        assert result.value == "T2w"
        assert result.confidence >= 0.80
    
    def test_r1_map(self, detector):
        """R1 map (1/T1) has T1w base."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\R1",
            scanning_sequence="GENERATED",
            text_search_blob="r1 map brain",
        )
        result = detector.detect(ctx)
        assert result.value == "T1w"
        assert result.confidence >= 0.80
    
    def test_r2_map(self, detector):
        """R2 map (1/T2) has T2w base."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\R2",
            scanning_sequence="GENERATED",
            text_search_blob="r2 map brain",
        )
        result = detector.detect(ctx)
        assert result.value == "T2w"
        assert result.confidence >= 0.80


# =============================================================================
# EXPLAIN DETECTION
# =============================================================================

class TestExplainDetection:
    """Test explain_detection method."""
    
    def test_explain_technique_inference(self, detector):
        """Explain shows technique inference."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="mprage brain",
        )
        explanation = detector.explain_detection(ctx, technique="MPRAGE")
        assert "T1w" in explanation
        assert "MPRAGE" in explanation
        assert "technique" in explanation.lower()
    
    def test_explain_keyword_match(self, detector):
        """Explain shows keyword match."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="t2w brain",
        )
        explanation = detector.explain_detection(ctx)
        assert "T2w" in explanation
        assert "keyword" in explanation.lower() or "Keyword" in explanation
