"""
Tests for ConstructDetector.

Tests detection of computed/derived constructs:
- Diffusion: ADC, eADC, FA, Trace, MD
- Perfusion: CBF, CBV, MTT, Tmax, TTP
- Dixon: Water, Fat, InPhase, OutPhase
- Quantitative: T1map, T2map, R1map, R2map
- Synthetic: SyntheticT1w, SyntheticT2w, SyntheticFLAIR, SyntheticPDw
- SWI: PhaseMap, QSM
- Projection: MIP, MinIP, MPR
"""

import pytest
from src.classification.core.context import ClassificationContext
from src.classification.detectors.construct import (
    ConstructDetector,
    ConstructMatch,
    ConstructDetectorOutput,
)


@pytest.fixture
def detector():
    """Create detector instance for tests."""
    return ConstructDetector()


# =============================================================================
# OUTPUT STRUCTURE TESTS
# =============================================================================

class TestOutputStructure:
    """Test the output data structures."""
    
    def test_empty_output(self, detector):
        """Original acquisition produces empty construct list."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="t1 mprage brain",
        )
        result = detector.detect(ctx)
        assert isinstance(result, ConstructDetectorOutput)
        assert result.constructs == []
        assert result.values == []
        assert result.construct_csv == ""
        assert result.has_constructs is False
    
    def test_single_construct(self, detector):
        """Single construct detected."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\DIFFUSION\\ADC",
            text_search_blob="adc map brain",
        )
        result = detector.detect(ctx)
        assert result.has_constructs is True
        assert len(result.constructs) == 1
        assert result.construct_csv == "ADC"
    
    def test_multiple_constructs(self, detector):
        """Multiple constructs detected."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\DIFFUSION\\ADC\\FA",
            text_search_blob="adc fa dti brain",
        )
        result = detector.detect(ctx)
        assert result.has_constructs is True
        assert len(result.constructs) >= 2
        assert "ADC" in result.values
        assert "FA" in result.values
    
    def test_construct_csv_sorted(self, detector):
        """Construct CSV is alphabetically sorted."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\DIFFUSION\\FA\\ADC\\TRACEW",
            text_search_blob="fa adc trace dti",
        )
        result = detector.detect(ctx)
        # Should be sorted alphabetically
        assert result.construct_csv == ",".join(sorted(result.values))


# =============================================================================
# DIFFUSION CONSTRUCTS
# =============================================================================

class TestDiffusionConstructs:
    """Test diffusion-derived construct detection."""
    
    def test_adc_by_flag(self, detector):
        """ADC detected by exclusive flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\DIFFUSION\\ADC",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("ADC")
        assert result.get("ADC").category == "diffusion"
        assert result.get("ADC").confidence >= 0.90
    
    def test_adc_by_keyword(self, detector):
        """ADC detected by keyword."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\M",
            text_search_blob="apparent diffusion coefficient map brain",
        )
        result = detector.detect(ctx)
        assert result.has("ADC")
    
    def test_eadc_by_flag(self, detector):
        """eADC detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\DIFFUSION\\EADC",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("eADC")
    
    def test_fa_by_flag(self, detector):
        """FA detected by exclusive flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\DIFFUSION\\FA",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("FA")
        assert result.get("FA").category == "diffusion"
    
    def test_fa_by_keyword(self, detector):
        """FA detected by keyword."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\M",
            text_search_blob="fractional anisotropy map brain",
        )
        result = detector.detect(ctx)
        assert result.has("FA")
    
    def test_trace_by_flag(self, detector):
        """Trace detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\DIFFUSION\\TRACEW",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("Trace")
    
    def test_trace_by_isodwi(self, detector):
        """Trace detected by ISODWI keyword."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\DIFFUSION\\ISODWI",
            text_search_blob="isotropic dwi brain",
        )
        result = detector.detect(ctx)
        assert result.has("Trace")
    
    def test_md_by_keyword(self, detector):
        """MD detected by keyword only (no flag)."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\M",
            text_search_blob="mean diffusivity map brain",
        )
        result = detector.detect(ctx)
        assert result.has("MD")
    
    def test_multiple_diffusion_maps(self, detector):
        """Multiple diffusion maps from same DTI."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\DIFFUSION\\ADC\\FA",
            text_search_blob="adc fa dti brain",
        )
        result = detector.detect(ctx)
        assert result.has("ADC")
        assert result.has("FA")
        diffusion = result.by_category("diffusion")
        assert len(diffusion) >= 2


# =============================================================================
# PERFUSION CONSTRUCTS
# =============================================================================

class TestPerfusionConstructs:
    """Test perfusion-derived construct detection."""
    
    def test_cbf_by_flag(self, detector):
        """CBF detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\PERFUSION\\CBF",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("CBF")
        assert result.get("CBF").category == "perfusion"
    
    def test_cbv_by_flag(self, detector):
        """CBV detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\PERFUSION\\CBV",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("CBV")
    
    def test_mtt_by_flag(self, detector):
        """MTT detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\PERFUSION\\MTT",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("MTT")
    
    def test_tmax_by_flag(self, detector):
        """Tmax detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\PERFUSION\\TMAX",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("Tmax")
    
    def test_ttp_by_flag(self, detector):
        """TTP detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\PERFUSION\\TTP",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("TTP")
    
    def test_all_perfusion_maps(self, detector):
        """All perfusion maps detected."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\PERFUSION",
            text_search_blob="cbf cbv mtt tmax ttp perfusion brain",
        )
        result = detector.detect(ctx)
        perfusion = result.by_category("perfusion")
        assert len(perfusion) >= 5


# =============================================================================
# DIXON CONSTRUCTS
# =============================================================================

class TestDixonConstructs:
    """Test Dixon fat-water separation constructs."""
    
    def test_water_by_flag(self, detector):
        """Water image detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\WATER",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("Water")
        assert result.get("Water").category == "dixon"
    
    def test_fat_by_flag(self, detector):
        """Fat image detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\FAT",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("Fat")
    
    def test_inphase_by_flag(self, detector):
        """In-phase image detected by flag."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\IN_PHASE",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("InPhase")
    
    def test_outphase_by_flag(self, detector):
        """Out-of-phase image detected by flag."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\OUT_PHASE",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("OutPhase")
    
    def test_water_by_keyword(self, detector):
        """Water image detected by keyword."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\M",
            text_search_blob="dixon water only liver",
        )
        result = detector.detect(ctx)
        assert result.has("Water")


# =============================================================================
# QUANTITATIVE MAP CONSTRUCTS
# =============================================================================

class TestQuantitativeConstructs:
    """Test quantitative map construct detection."""
    
    def test_t1map_by_flag(self, detector):
        """T1map detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\T1 MAP",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("T1map")
        assert result.get("T1map").category == "quantitative"
    
    def test_t2map_by_flag(self, detector):
        """T2map detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\T2 MAP",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("T2map")
    
    def test_r1map_by_flag(self, detector):
        """R1map detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\R1",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("R1map")
    
    def test_r2map_by_flag(self, detector):
        """R2map detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\R2",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("R2map")
    
    def test_pdmap_by_keyword(self, detector):
        """PDmap detected by keyword."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\QMAP\\PD",
            text_search_blob="proton density map brain",
        )
        result = detector.detect(ctx)
        assert result.has("PDmap")
    
    def test_t1map_by_keyword(self, detector):
        """T1map detected by keyword."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\M",
            text_search_blob="t1 mapping brain",
        )
        result = detector.detect(ctx)
        assert result.has("T1map")


# =============================================================================
# SYNTHETIC MRI CONSTRUCTS
# =============================================================================

class TestSyntheticConstructs:
    """Test synthetic MRI construct detection."""
    
    def test_synthetic_t1w_by_flag(self, detector):
        """SyntheticT1w detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\T1\\SYNTHETIC",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("SyntheticT1w")
        assert result.get("SyntheticT1w").category == "synthetic"
    
    def test_synthetic_t2w_by_flag(self, detector):
        """SyntheticT2w detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\T2\\SYNTHETIC",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("SyntheticT2w")
    
    def test_synthetic_flair_by_flag(self, detector):
        """SyntheticFLAIR detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\T2FLAIR\\SYNTHETIC",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("SyntheticFLAIR")
    
    def test_synthetic_pdw_by_flag(self, detector):
        """SyntheticPDw detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\PD\\SYNTHETIC",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("SyntheticPDw")
    
    def test_myelin_by_keyword(self, detector):
        """MyelinMap detected by keyword."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\MYELIN",
            text_search_blob="myelin water fraction brain",
        )
        result = detector.detect(ctx)
        assert result.has("MyelinMap")


# =============================================================================
# SWI / SUSCEPTIBILITY CONSTRUCTS
# =============================================================================

class TestSWIConstructs:
    """Test SWI-related construct detection."""

    def test_phase_by_flag(self, detector):
        """Phase detected by flag (renamed from PhaseMap)."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\P",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("Phase")
    
    def test_qsm_by_keyword(self, detector):
        """QSM detected by keyword."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\M",
            text_search_blob="qsm susceptibility map brain",
        )
        result = detector.detect(ctx)
        assert result.has("QSM")


# =============================================================================
# PROJECTION CONSTRUCTS
# =============================================================================

class TestProjectionConstructs:
    """Test projection/reformat construct detection."""
    
    def test_mip_by_flag(self, detector):
        """MIP detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\SECONDARY\\MIP",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("MIP")
        assert result.get("MIP").category == "projection"
    
    def test_minip_by_flag(self, detector):
        """MinIP detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\SECONDARY\\MINIP",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("MinIP")
    
    def test_mpr_by_flag(self, detector):
        """MPR detected by flag."""
        ctx = ClassificationContext(
            image_type="DERIVED\\SECONDARY\\MPR",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("MPR")
    
    def test_mip_by_keyword(self, detector):
        """MIP detected by keyword."""
        ctx = ClassificationContext(
            image_type="DERIVED\\SECONDARY\\M",
            text_search_blob="maximum intensity projection brain",
        )
        result = detector.detect(ctx)
        assert result.has("MIP")


# =============================================================================
# COMPLEX COMPONENT CONSTRUCTS
# =============================================================================

class TestComponentConstructs:
    """Test complex image component construct detection."""
    
    def test_magnitude_by_keyword(self, detector):
        """Magnitude detected by explicit keyword."""
        # Note: "M" in ImageType is too common - only explicit keywords trigger
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M",
            text_search_blob="magnitude image brain",
        )
        result = detector.detect(ctx)
        assert result.has("Magnitude")
    
    def test_real_by_flag(self, detector):
        """Real detected by flag."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\R",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("Real")
    
    def test_imaginary_by_flag(self, detector):
        """Imaginary detected by flag."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\I",
            text_search_blob="brain",
        )
        result = detector.detect(ctx)
        assert result.has("Imaginary")
    
    def test_standard_m_no_construct(self, detector):
        """Standard M in ImageType should NOT trigger Magnitude construct."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="t1 brain",
        )
        result = detector.detect(ctx)
        assert not result.has("Magnitude")


# =============================================================================
# FIELD MAP CONSTRUCTS
# =============================================================================

class TestFieldMapConstructs:
    """Test field map construct detection."""
    
    def test_b0map_by_keyword(self, detector):
        """B0map detected by keyword."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\M",
            text_search_blob="b0 field map brain",
        )
        result = detector.detect(ctx)
        assert result.has("B0map")
    
    def test_b1map_by_keyword(self, detector):
        """B1map detected by keyword."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\M",
            text_search_blob="b1+ map brain",
        )
        result = detector.detect(ctx)
        assert result.has("B1map")


# =============================================================================
# EXPLAIN DETECTION
# =============================================================================

class TestExplainDetection:
    """Test explain_detection method."""
    
    def test_explain_no_constructs(self, detector):
        """Explain with no constructs."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\M\\ND",
            text_search_blob="brain imaging",
        )
        explanation = detector.explain_detection(ctx)
        assert "No constructs" in explanation
    
    def test_explain_single_construct(self, detector):
        """Explain with single construct."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\DIFFUSION\\ADC",
            text_search_blob="brain",
        )
        explanation = detector.explain_detection(ctx)
        assert "ADC" in explanation
        assert "construct_csv" in explanation
    
    def test_explain_grouped_by_category(self, detector):
        """Explain groups constructs by category."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\DIFFUSION\\ADC\\FA",
            text_search_blob="adc fa brain",
        )
        explanation = detector.explain_detection(ctx)
        assert "DIFFUSION" in explanation


# =============================================================================
# UTILITY METHODS
# =============================================================================

class TestUtilityMethods:
    """Test utility methods."""

    def test_get_all_constructs(self, detector):
        """Get all construct names."""
        constructs = detector.get_all_constructs()
        assert "ADC" in constructs
        assert "FA" in constructs
        assert "T1map" in constructs
        assert "MIP" in constructs
        assert len(constructs) >= 30

    def test_get_categories(self, detector):
        """Get all categories."""
        categories = detector.get_categories()
        assert "diffusion" in categories
        assert "perfusion" in categories
        assert "quantitative" in categories
        assert "synthetic" in categories
        assert "projection" in categories

    def test_get_constructs_by_category(self, detector):
        """Get constructs in a category."""
        diffusion = detector.get_constructs_by_category("diffusion")
        assert "ADC" in diffusion
        assert "FA" in diffusion
        assert "Trace" in diffusion


# =============================================================================
# REAL DATABASE EXAMPLES
# =============================================================================

class TestRealDatabaseExamples:
    """Test with real database patterns."""
    
    def test_siemens_adc(self, detector):
        """Siemens ADC map."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\DIFFUSION\\ADC\\NORM\\DIS2D",
            text_search_blob="adc brain",
        )
        result = detector.detect(ctx)
        assert result.has("ADC")
    
    def test_ge_perfusion(self, detector):
        """GE perfusion maps."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\PERFUSION\\RELCBV\\DIS2D\\MOCO",
            text_search_blob="rcbv perfusion brain",
        )
        result = detector.detect(ctx)
        assert result.has("CBV")
    
    def test_philips_t1map(self, detector):
        """Philips T1 map."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\T1 MAP\\DIS3D\\MFSPLIT",
            text_search_blob="t1 mapping brain",
        )
        result = detector.detect(ctx)
        assert result.has("T1map")
    
    def test_symri_synthetic(self, detector):
        """SyMRI synthetic images."""
        ctx = ClassificationContext(
            image_type="DERIVED\\PRIMARY\\T1W_SYNTHETIC",
            sequence_variant="SYNTHETIC",
            text_search_blob="synthetic t1 brain",
        )
        result = detector.detect(ctx)
        assert result.has("SyntheticT1w")
