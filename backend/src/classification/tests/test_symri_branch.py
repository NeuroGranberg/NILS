"""
Tests for SyMRI Branch Logic

Tests the SyMRI-specific classification branch that handles:
- Raw source data (MDME, QALAS)
- Quantitative maps (T1map, T2map, PDmap, R1, R2, B1)
- Synthetic weighted images (T1w, T2w, PDw)
- Synthetic IR contrasts (FLAIR, DIR, PSIR, STIR)
- Myelin maps

Version: 1.0.0
"""

import pytest
from src.classification.core.context import ClassificationContext
from src.classification.branches.symri import (
    apply_symri_logic,
    detect_symri_output_type,
    get_symri_output_info,
    classify_symri_complex_data,
)
from src.classification.branches.common import BranchResult


# =============================================================================
# Fixtures: Quantitative Maps
# =============================================================================

@pytest.fixture
def symri_t1map_ctx():
    """SyMRI T1 map."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\T1MAP",
        scanning_sequence="SE (SYNTHETIC)",
        stack_sequence_name="*mdme3d",
    )


@pytest.fixture
def symri_t2map_ctx():
    """SyMRI T2 map."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\T2MAP",
        scanning_sequence="SE (SYNTHETIC)",
        stack_sequence_name="*mdme3d",
    )


@pytest.fixture
def symri_pdmap_ctx():
    """SyMRI PD map."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\PDMAP",
        scanning_sequence="SE (SYNTHETIC)",
        stack_sequence_name="*mdme3d",
    )


@pytest.fixture
def symri_r1map_ctx():
    """SyMRI R1 map."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\R1",
        scanning_sequence="SE (SYNTHETIC)",
        stack_sequence_name="*mdme3d",
    )


@pytest.fixture
def symri_r2map_ctx():
    """SyMRI R2 map."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\R2",
        scanning_sequence="SE (SYNTHETIC)",
        stack_sequence_name="*mdme3d",
    )


@pytest.fixture
def symri_b1map_ctx():
    """SyMRI B1 map."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\B1MAP",
        scanning_sequence="SE (SYNTHETIC)",
        stack_sequence_name="*mdme3d",
    )


# =============================================================================
# Fixtures: Synthetic Weighted
# =============================================================================

@pytest.fixture
def symri_synthetic_t1w_ctx():
    """SyMRI Synthetic T1-weighted."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\T1\SYNTHETIC",
        scanning_sequence="SE (SYNTHETIC)",
        stack_sequence_name="*mdme3d",
    )


@pytest.fixture
def symri_synthetic_t2w_ctx():
    """SyMRI Synthetic T2-weighted."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\T2\SYNTHETIC",
        scanning_sequence="SE (SYNTHETIC)",
        stack_sequence_name="*mdme3d",
    )


@pytest.fixture
def symri_synthetic_pdw_ctx():
    """SyMRI Synthetic PD-weighted."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\PD\SYNTHETIC",
        scanning_sequence="SE (SYNTHETIC)",
        stack_sequence_name="*mdme3d",
    )


# =============================================================================
# Fixtures: Synthetic IR Contrasts
# =============================================================================

@pytest.fixture
def symri_synthetic_flair_ctx():
    """SyMRI Synthetic FLAIR."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\T2FLAIR\SYNTHETIC",
        scanning_sequence="SE (SYNTHETIC)",
        stack_sequence_name="*mdme3d",
    )


@pytest.fixture
def symri_synthetic_dir_ctx():
    """SyMRI Synthetic DIR."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\DIR SYNTHETIC",
        scanning_sequence="SE (SYNTHETIC)",
        stack_sequence_name="*mdme3d",
    )


@pytest.fixture
def symri_synthetic_psir_ctx():
    """SyMRI Synthetic PSIR."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\PSIR SYNTHETIC",
        scanning_sequence="SE (SYNTHETIC)",
        stack_sequence_name="*mdme3d",
    )


@pytest.fixture
def symri_synthetic_stir_ctx():
    """SyMRI Synthetic STIR."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\T2STIR\SYNTHETIC",
        scanning_sequence="SE (SYNTHETIC)",
        stack_sequence_name="*mdme3d",
    )


# =============================================================================
# Fixtures: Other
# =============================================================================

@pytest.fixture
def symri_myelin_ctx():
    """SyMRI Myelin map."""
    return ClassificationContext(
        image_type=r"DERIVED\PRIMARY\MYC",
        scanning_sequence="SE (SYNTHETIC)",
        stack_sequence_name="*mdme3d",
    )


@pytest.fixture
def symri_source_mdme_ctx():
    """SyMRI MDME raw source."""
    return ClassificationContext(
        image_type=r"ORIGINAL\PRIMARY\M",
        scanning_sequence="SE",
        stack_sequence_name="*mdme3d",
    )


@pytest.fixture
def symri_source_qalas_ctx():
    """SyMRI QALAS raw source."""
    return ClassificationContext(
        image_type=r"ORIGINAL\PRIMARY\M",
        scanning_sequence="SE",
        stack_sequence_name="*qalas3d",
    )


# =============================================================================
# Test: Quantitative Maps
# =============================================================================

class TestSyMRIQuantitativeMaps:
    """Tests for SyMRI quantitative map detection."""

    def test_multi_qmap_detected_by_imagetype(self):
        """GE MAGiC bundled T1/T2/PD maps (MULTI_QMAP) should return MultiQmap construct.

        Real case: 3d3eca819b1f55cb 2023-06-19
        - ImageType: DERIVED\\PRIMARY\\MULTI_QMAP\\ENCRYPTED\\T1\\T2\\PD
        - Series description: "QMaps (T1T2PD)"
        """
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\MULTI_QMAP\ENCRYPTED\T1\T2\PD",
            text_search_blob="qmaps t1t2pd mrc436 overloard",
        )
        result = apply_symri_logic(ctx)

        assert result.base is None  # Quantitative maps have no tissue contrast
        assert result.construct == "MultiQmap"
        assert result.technique == "MDME"
        assert result.skip_base_detection is True
        assert result.directory_type == "anat"
        assert result.confidence >= 0.90

    def test_multi_qmap_detected_by_qmaps_keyword(self):
        """'qmaps' keyword in text should detect MultiQmap."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\OTHER",
            text_search_blob="qmaps t1 t2 pd combined maps",
        )
        result = apply_symri_logic(ctx)

        assert result.base is None
        assert result.construct == "MultiQmap"

    def test_multi_qmap_priority_over_individual_maps(self):
        """MultiQmap should be detected before individual T1/T2/PD maps.

        MULTI_QMAP ImageType contains T1, T2, PD tokens that could trigger
        individual map detection. MultiQmap must be checked first.
        """
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\MULTI_QMAP\ENCRYPTED\T1\T2\PD",
            text_search_blob="qmaps combined",
        )
        result = apply_symri_logic(ctx)

        # Should be MultiQmap, NOT T1map/T2map/PDmap
        assert result.construct == "MultiQmap"
        assert result.construct != "T1map"
        assert result.construct != "T2map"
        assert result.construct != "PDmap"

    def test_t1map_detected(self, symri_t1map_ctx):
        """T1 map should return no base, T1map construct."""
        result = apply_symri_logic(symri_t1map_ctx)
        
        assert result.base is None
        assert result.construct == "T1map"
        assert result.skip_base_detection is True
        assert result.directory_type == "anat"
    
    def test_t2map_detected(self, symri_t2map_ctx):
        """T2 map should return no base, T2map construct."""
        result = apply_symri_logic(symri_t2map_ctx)
        
        assert result.base is None
        assert result.construct == "T2map"
        assert result.skip_base_detection is True
    
    def test_pdmap_detected(self, symri_pdmap_ctx):
        """PD map should return no base, PDmap construct."""
        result = apply_symri_logic(symri_pdmap_ctx)
        
        assert result.base is None
        assert result.construct == "PDmap"
        assert result.skip_base_detection is True
    
    def test_r1map_detected(self, symri_r1map_ctx):
        """R1 map should return no base, R1map construct."""
        result = apply_symri_logic(symri_r1map_ctx)
        
        assert result.base is None
        assert result.construct == "R1map"
        assert result.skip_base_detection is True
    
    def test_r2map_detected(self, symri_r2map_ctx):
        """R2 map should return no base, R2map construct."""
        result = apply_symri_logic(symri_r2map_ctx)
        
        assert result.base is None
        assert result.construct == "R2map"
        assert result.skip_base_detection is True
    
    def test_b1map_detected(self, symri_b1map_ctx):
        """B1 map should return no base, B1map construct, fmap directory."""
        result = apply_symri_logic(symri_b1map_ctx)
        
        assert result.base is None
        assert result.construct == "B1map"
        assert result.directory_type == "fmap"
    
    def test_maps_have_high_confidence(self, symri_t1map_ctx):
        """Quantitative maps should have high confidence."""
        result = apply_symri_logic(symri_t1map_ctx)
        assert result.confidence >= 0.90


# =============================================================================
# Test: Synthetic Weighted Images
# =============================================================================

class TestSyMRISyntheticWeighted:
    """Tests for SyMRI synthetic weighted image detection."""
    
    def test_synthetic_t1w_detected(self, symri_synthetic_t1w_ctx):
        """Synthetic T1w should return T1w base, SyntheticT1w construct."""
        result = apply_symri_logic(symri_synthetic_t1w_ctx)
        
        assert result.base == "T1w"
        assert result.construct == "SyntheticT1w"
        assert result.skip_base_detection is True
        assert result.directory_type == "anat"
    
    def test_synthetic_t2w_detected(self, symri_synthetic_t2w_ctx):
        """Synthetic T2w should return T2w base, SyntheticT2w construct."""
        result = apply_symri_logic(symri_synthetic_t2w_ctx)
        
        assert result.base == "T2w"
        assert result.construct == "SyntheticT2w"
        assert result.skip_base_detection is True
    
    def test_synthetic_pdw_detected(self, symri_synthetic_pdw_ctx):
        """Synthetic PDw should return PDw base, SyntheticPDw construct."""
        result = apply_symri_logic(symri_synthetic_pdw_ctx)
        
        assert result.base == "PDw"
        assert result.construct == "SyntheticPDw"
        assert result.skip_base_detection is True
    
    def test_synthetic_has_high_confidence(self, symri_synthetic_t1w_ctx):
        """Synthetic images should have high confidence."""
        result = apply_symri_logic(symri_synthetic_t1w_ctx)
        assert result.confidence >= 0.95


# =============================================================================
# Test: Synthetic IR Contrasts
# =============================================================================

class TestSyMRISyntheticIR:
    """Tests for SyMRI synthetic IR contrast detection."""
    
    def test_synthetic_flair_detected(self, symri_synthetic_flair_ctx):
        """Synthetic FLAIR should return T2w base, SyntheticFLAIR construct, FLAIR modifier."""
        result = apply_symri_logic(symri_synthetic_flair_ctx)
        
        assert result.base == "T2w"
        assert result.construct == "SyntheticFLAIR"
        assert "FLAIR" in (result.modifiers_add or [])
        assert result.skip_base_detection is True
    
    def test_synthetic_dir_detected(self, symri_synthetic_dir_ctx):
        """Synthetic DIR should return T2w base, SyntheticDIR construct, DIR modifier."""
        result = apply_symri_logic(symri_synthetic_dir_ctx)
        
        assert result.base == "T2w"
        assert result.construct == "SyntheticDIR"
        assert "DIR" in (result.modifiers_add or [])
    
    def test_synthetic_psir_detected(self, symri_synthetic_psir_ctx):
        """Synthetic PSIR should return T1w base, SyntheticPSIR construct, PSIR modifier."""
        result = apply_symri_logic(symri_synthetic_psir_ctx)
        
        assert result.base == "T1w"
        assert result.construct == "SyntheticPSIR"
        assert "PSIR" in (result.modifiers_add or [])
    
    def test_synthetic_stir_detected(self, symri_synthetic_stir_ctx):
        """Synthetic STIR should return T2w base, SyntheticSTIR construct, STIR modifier."""
        result = apply_symri_logic(symri_synthetic_stir_ctx)
        
        assert result.base == "T2w"
        assert result.construct == "SyntheticSTIR"
        assert "STIR" in (result.modifiers_add or [])
    
    def test_synthetic_ir_has_high_confidence(self, symri_synthetic_flair_ctx):
        """Synthetic IR images should have high confidence."""
        result = apply_symri_logic(symri_synthetic_flair_ctx)
        assert result.confidence >= 0.95


# =============================================================================
# Test: Myelin Maps
# =============================================================================

class TestSyMRIMyelinMap:
    """Tests for SyMRI myelin map detection."""
    
    def test_myelin_detected(self, symri_myelin_ctx):
        """Myelin map should return no base, MyelinMap construct."""
        result = apply_symri_logic(symri_myelin_ctx)
        
        assert result.base is None
        assert result.construct == "MyelinMap"
        assert result.skip_base_detection is True
        assert result.directory_type == "anat"
    
    def test_myelin_has_highest_priority(self):
        """Myelin should be detected even with other tokens."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\MYC\T1",
            scanning_sequence="SE (SYNTHETIC)",
            stack_sequence_name="*mdme3d",
        )
        result = apply_symri_logic(ctx)
        
        assert result.construct == "MyelinMap"
    
    def test_myelin_has_high_confidence(self, symri_myelin_ctx):
        """Myelin detection should have high confidence."""
        result = apply_symri_logic(symri_myelin_ctx)
        assert result.confidence >= 0.95


# =============================================================================
# Test: Raw Source Data
# =============================================================================

class TestSyMRIRawSource:
    """Tests for SyMRI raw source data detection."""

    def test_mdme_source_detected(self, symri_source_mdme_ctx):
        """MDME source should return no base, Magnitude construct."""
        result = apply_symri_logic(symri_source_mdme_ctx)

        assert result.base is None
        assert result.construct == "Magnitude"
        assert result.skip_base_detection is True
        assert result.directory_type == "anat"

    def test_qalas_source_detected(self, symri_source_qalas_ctx):
        """QALAS source should return no base, Magnitude construct."""
        result = apply_symri_logic(symri_source_qalas_ctx)

        assert result.base is None
        assert result.construct == "Magnitude"
        assert result.skip_base_detection is True


# =============================================================================
# Test: detect_symri_output_type
# =============================================================================

class TestDetectSyMRIOutputType:
    """Tests for detect_symri_output_type function."""
    
    def test_detects_t1_map(self, symri_t1map_ctx):
        """Should detect T1 map output type."""
        output_type = detect_symri_output_type(symri_t1map_ctx)
        assert output_type == "t1_map"
    
    def test_detects_t2_map(self, symri_t2map_ctx):
        """Should detect T2 map output type."""
        output_type = detect_symri_output_type(symri_t2map_ctx)
        assert output_type == "t2_map"
    
    def test_detects_myelin(self, symri_myelin_ctx):
        """Should detect myelin map output type."""
        output_type = detect_symri_output_type(symri_myelin_ctx)
        assert output_type == "myelin_map"
    
    def test_detects_synthetic_flair(self, symri_synthetic_flair_ctx):
        """Should detect synthetic FLAIR output type."""
        output_type = detect_symri_output_type(symri_synthetic_flair_ctx)
        assert output_type == "synthetic_flair"
    
    def test_detects_synthetic_t1w(self, symri_synthetic_t1w_ctx):
        """Should detect synthetic T1w output type."""
        output_type = detect_symri_output_type(symri_synthetic_t1w_ctx)
        assert output_type == "synthetic_t1w"
    
    def test_detects_source_mdme(self, symri_source_mdme_ctx):
        """Should detect MDME source as magnitude output type."""
        output_type = detect_symri_output_type(symri_source_mdme_ctx)
        assert output_type == "magnitude"

    def test_detects_source_qalas(self, symri_source_qalas_ctx):
        """Should detect QALAS source as magnitude output type."""
        output_type = detect_symri_output_type(symri_source_qalas_ctx)
        assert output_type == "magnitude"

    def test_detects_multi_qmap(self):
        """Should detect MultiQmap output type for GE MAGiC bundled maps."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\MULTI_QMAP\ENCRYPTED\T1\T2\PD",
            text_search_blob="qmaps t1t2pd",
        )
        output_type = detect_symri_output_type(ctx)
        assert output_type == "multi_qmap"


# =============================================================================
# Test: get_symri_output_info
# =============================================================================

class TestGetSyMRIOutputInfo:
    """Tests for get_symri_output_info function."""
    
    def test_t1_map_info(self):
        """Should return correct info for T1 map."""
        info = get_symri_output_info("t1_map")
        assert info["base"] is None
        assert info["construct"] == "T1map"
    
    def test_synthetic_flair_info(self):
        """Should return correct info for synthetic FLAIR."""
        info = get_symri_output_info("synthetic_flair")
        assert info["base"] == "T2w"
        assert info["construct"] == "SyntheticFLAIR"
        assert "FLAIR" in info.get("modifiers", [])
    
    def test_myelin_map_info(self):
        """Should return correct info for myelin map."""
        info = get_symri_output_info("myelin_map")
        assert info["base"] is None
        assert info["construct"] == "MyelinMap"
    
    def test_unknown_returns_default(self):
        """Unknown output type should return default info (Magnitude)."""
        info = get_symri_output_info("unknown_type")
        assert info["base"] is None
        assert info["construct"] == "Magnitude"


# =============================================================================
# Test: classify_symri_complex_data
# =============================================================================

class TestClassifySyMRIComplexData:
    """Tests for classify_symri_complex_data function."""
    
    def test_detects_magnitude_component(self):
        """Should detect magnitude component."""
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\MAGNITUDE",
            scanning_sequence="SE",
            stack_sequence_name="*mdme3d",
        )
        component, echo_info = classify_symri_complex_data(ctx)
        assert component == "magnitude"
    
    def test_detects_phase_component(self):
        """Should detect phase component."""
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\PHASE",
            scanning_sequence="SE",
            stack_sequence_name="*mdme3d",
        )
        component, echo_info = classify_symri_complex_data(ctx)
        assert component == "phase"
    
    def test_returns_echo_info_from_stack_key(self):
        """Should return echo info from stack_key."""
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\M",
            scanning_sequence="SE",
            stack_sequence_name="*mdme3d",
            stack_key="echo_1",
        )
        component, echo_info = classify_symri_complex_data(ctx)
        assert echo_info == "echo_1"


# =============================================================================
# Test: Detection Priority
# =============================================================================

class TestSyMRIDetectionPriority:
    """Tests for SyMRI output type detection priority."""
    
    def test_myelin_beats_t1map(self):
        """Myelin should take priority over T1 map."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\MYC\T1MAP",
            scanning_sequence="SE (SYNTHETIC)",
            stack_sequence_name="*mdme3d",
        )
        result = apply_symri_logic(ctx)
        assert result.construct == "MyelinMap"
    
    def test_map_beats_synthetic(self):
        """Quantitative map should take priority over synthetic."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\T1MAP\T1\SYNTHETIC",
            scanning_sequence="SE (SYNTHETIC)",
            stack_sequence_name="*mdme3d",
        )
        result = apply_symri_logic(ctx)
        assert result.construct == "T1map"
    
    def test_flair_beats_t2_synthetic(self):
        """FLAIR should take priority over generic T2 synthetic."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\T2FLAIR\SYNTHETIC",
            scanning_sequence="SE (SYNTHETIC)",
            stack_sequence_name="*mdme3d",
        )
        result = apply_symri_logic(ctx)
        assert result.construct == "SyntheticFLAIR"


# =============================================================================
# Test: Evidence Tracking
# =============================================================================

class TestSyMRIEvidence:
    """Tests for evidence tracking in SyMRI branch."""
    
    def test_t1map_has_evidence(self, symri_t1map_ctx):
        """T1 map result should include evidence."""
        result = apply_symri_logic(symri_t1map_ctx)
        
        assert len(result.evidence) > 0
        assert result.evidence[0].target == "T1map"
    
    def test_synthetic_flair_has_evidence(self, symri_synthetic_flair_ctx):
        """Synthetic FLAIR result should include evidence."""
        result = apply_symri_logic(symri_synthetic_flair_ctx)
        
        assert len(result.evidence) > 0
        assert result.evidence[0].target == "SyntheticFLAIR"
    
    def test_evidence_has_description(self, symri_t1map_ctx):
        """Evidence should include human-readable description."""
        result = apply_symri_logic(symri_t1map_ctx)
        
        assert len(result.evidence) > 0
        assert result.evidence[0].description != ""


# =============================================================================
# Test: Edge Cases
# =============================================================================

class TestSyMRIEdgeCases:
    """Tests for edge cases in SyMRI branch."""

    def test_empty_context_fallback(self):
        """Empty context should return fallback Magnitude result."""
        ctx = ClassificationContext()
        result = apply_symri_logic(ctx)

        # Should return Magnitude fallback with lower confidence
        assert result.base is None
        assert result.construct == "Magnitude"
        assert result.confidence <= 0.70
        # Even empty context should skip detection (we're in SyMRI branch)
        assert result.skip_base_detection is True

    def test_generic_synthetic_marker(self):
        """Generic SYNTHETIC marker without specific type falls back to Magnitude."""
        ctx = ClassificationContext(
            image_type=r"DERIVED\PRIMARY\SYNTHETIC",
            scanning_sequence="SE (SYNTHETIC)",
            stack_sequence_name="*mdme3d",
        )
        result = apply_symri_logic(ctx)

        # Without specific type, falls back to Magnitude (raw source)
        assert result.construct == "Magnitude"
        assert result.skip_base_detection is True

    def test_multi_echo_source(self):
        """Multi-echo MDME source should be detected as Magnitude."""
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\M",
            scanning_sequence="SE",
            stack_sequence_name="*mdme3d",
            stack_key="multi_echo",
        )
        result = apply_symri_logic(ctx)

        # Should detect as Magnitude (raw source)
        assert result.base is None
        assert result.construct == "Magnitude"
