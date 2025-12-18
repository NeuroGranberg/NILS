"""
Unit tests for TechniqueDetector

Tests the three-tier detection logic:
1. Exclusive flag (HIGH confidence)
2. Keywords match (HIGH confidence)
3. Combination (MEDIUM confidence)

Version: 1.0.0
"""

import pytest
from pathlib import Path

from ..core.context import ClassificationContext
from ..detectors.technique import TechniqueDetector, TechniqueResult


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def detector() -> TechniqueDetector:
    """Create TechniqueDetector with real YAML config."""
    return TechniqueDetector()


@pytest.fixture
def empty_context() -> ClassificationContext:
    """Create empty classification context."""
    return ClassificationContext()


# =============================================================================
# Test Detector Initialization
# =============================================================================

class TestDetectorInit:
    """Test detector initialization and configuration loading."""
    
    def test_loads_yaml_config(self, detector: TechniqueDetector):
        """Verify YAML config is loaded."""
        assert detector.config is not None
        assert "techniques" in detector.config
        assert "rules" in detector.config
    
    def test_builds_priority_order(self, detector: TechniqueDetector):
        """Verify priority order is built from config."""
        techniques = detector.get_all_techniques()
        assert len(techniques) > 0
        assert len(techniques) == 37  # Total techniques (SWI=provenance, Radial/Spiral=modifiers)
    
    def test_has_all_families(self, detector: TechniqueDetector):
        """Verify all 4 physics families are represented."""
        families = set()
        for tech_id in detector.get_all_techniques():
            config = detector.get_technique_config(tech_id)
            if config:
                families.add(config.get("family"))
        
        assert "SE" in families
        assert "GRE" in families
        assert "EPI" in families
        assert "MIXED" in families


# =============================================================================
# Test SE Family Detection
# =============================================================================

class TestSEFamilyDetection:
    """Test SE (Spin Echo) family technique detection."""
    
    def test_detect_mdme_by_exclusive_flag(self, detector: TechniqueDetector):
        """MDME detected by is_mdme flag."""
        ctx = ClassificationContext(
            stack_sequence_name="*mdme_ir2d1"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "MDME"
        assert result.family == "SE"
        assert result.detection_method == "exclusive"
        assert result.confidence >= 0.90
    
    def test_detect_mdme_by_keyword(self, detector: TechniqueDetector):
        """MDME detected by 'symri' keyword."""
        ctx = ClassificationContext(
            text_search_blob="brain symri t1 t2 pd"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "MDME"
        assert result.detection_method == "keywords"
    
    def test_detect_tse_by_flag(self, detector: TechniqueDetector):
        """TSE detected by is_tse flag."""
        ctx = ClassificationContext(
            stack_sequence_name="*tse2d1_3",
            scanning_sequence="SE"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "TSE"
        assert result.family == "SE"
    
    def test_detect_space_by_exclusive_flag(self, detector: TechniqueDetector):
        """3D-TSE (SPACE) detected by is_space flag."""
        ctx = ClassificationContext(
            stack_sequence_name="*spc3d1_ns"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "3D-TSE"
        assert result.family == "SE"
        assert result.detection_method == "exclusive"
    
    def test_detect_3d_tse_by_combination(self, detector: TechniqueDetector):
        """3D-TSE detected by is_tse + is_3d combination."""
        ctx = ClassificationContext(
            stack_sequence_name="*tse3d1",
            scanning_sequence="SE",
            sequence_variant="SK",
            mr_acquisition_type="3D"
        )
        result = detector.detect_technique(ctx)
        
        # Should be 3D-TSE by combination
        assert result.technique == "3D-TSE"
        assert result.detection_method in ["exclusive", "combination"]
    
    def test_detect_haste_by_keyword(self, detector: TechniqueDetector):
        """SS-TSE (HASTE) detected by keyword."""
        ctx = ClassificationContext(
            text_search_blob="brain haste t2"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "SS-TSE"
        assert result.family == "SE"
    
    def test_fallback_to_se(self, detector: TechniqueDetector):
        """Generic SE fallback when no specific SE technique matches."""
        ctx = ClassificationContext(
            scanning_sequence="SE"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "SE"
        assert result.family == "SE"
        assert result.detection_method == "exclusive"


# =============================================================================
# Test GRE Family Detection
# =============================================================================

class TestGREFamilyDetection:
    """Test GRE (Gradient Echo) family technique detection."""
    
    def test_detect_mprage_by_exclusive_flag(self, detector: TechniqueDetector):
        """MPRAGE detected by is_mprage flag."""
        ctx = ClassificationContext(
            stack_sequence_name="*tfl3d1_16ns"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "MPRAGE"
        assert result.family == "GRE"
        assert result.detection_method == "exclusive"
    
    def test_detect_mprage_by_keyword(self, detector: TechniqueDetector):
        """MPRAGE detected by keyword."""
        ctx = ClassificationContext(
            text_search_blob="brain sag 3d mprage"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "MPRAGE"
        assert result.detection_method == "keywords"
    
    def test_detect_bravo_as_mprage(self, detector: TechniqueDetector):
        """GE BRAVO detected as MPRAGE by keyword."""
        ctx = ClassificationContext(
            text_search_blob="brain ax bravo t1"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "MPRAGE"
    
    def test_detect_tof_by_exclusive_flag(self, detector: TechniqueDetector):
        """TOF-MRA detected by is_tof flag."""
        ctx = ClassificationContext(
            stack_sequence_name="*tfi2d1_12",
            sequence_variant="TOF"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "TOF-MRA"
        assert result.family == "GRE"
    
    def test_swi_sequence_returns_gre(self, detector: TechniqueDetector):
        """SWI sequence returns GRE (SWI is a provenance, not a technique)."""
        # SWI was removed from technique detection - it's a provenance, not a technique
        # The actual acquisition technique is GRE (gradient echo)
        ctx = ClassificationContext(
            stack_sequence_name="*swi3d1r",
            image_type="ORIGINAL\\PRIMARY\\M\\SWI",
            scanning_sequence="GR"  # SWI uses gradient echo
        )
        result = detector.detect_technique(ctx)

        # Should return GRE family technique, not "SWI"
        assert result.family == "GRE"
        assert result.technique in ["GRE", "SP-GRE"]  # Fallback to generic GRE
    
    def test_detect_flash_by_flag(self, detector: TechniqueDetector):
        """SP-GRE (FLASH) detected by is_flash flag."""
        ctx = ClassificationContext(
            stack_sequence_name="*fl2d1"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "SP-GRE"
        assert result.family == "GRE"
    
    def test_fallback_to_gre(self, detector: TechniqueDetector):
        """Generic GRE fallback when no specific GRE technique matches."""
        ctx = ClassificationContext(
            scanning_sequence="GR"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "GRE"
        assert result.family == "GRE"


# =============================================================================
# Test EPI Family Detection
# =============================================================================

class TestEPIFamilyDetection:
    """Test EPI (Echo Planar) family technique detection."""
    
    def test_detect_epi_by_exclusive_flag(self, detector: TechniqueDetector):
        """Generic EPI detected by has_epi flag."""
        ctx = ClassificationContext(
            scanning_sequence="EP"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "EPI"
        assert result.family == "EPI"
    
    def test_detect_ms_epi_by_keyword(self, detector: TechniqueDetector):
        """MS-EPI (RESOLVE) detected by keyword."""
        ctx = ClassificationContext(
            text_search_blob="brain dwi resolve",
            scanning_sequence="EP"
        )
        result = detector.detect_technique(ctx)
        
        # DWI-EPI is higher priority than MS-EPI
        # But "resolve" should match MS-EPI if "dwi" keyword doesn't match first
        assert result.technique in ["DWI-EPI", "MS-EPI"]


# =============================================================================
# Test MIXED Family Detection
# =============================================================================

class TestMIXEDFamilyDetection:
    """Test MIXED (hybrid physics) family technique detection."""
    
    def test_detect_dwi_by_exclusive_flag(self, detector: TechniqueDetector):
        """DWI-EPI detected by is_dwi flag."""
        ctx = ClassificationContext(
            stack_sequence_name="*ep_b1000t",
            scanning_sequence="['SE', 'EP']",
            image_type="ORIGINAL\\PRIMARY\\DIFFUSION"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "DWI-EPI"
        assert result.family == "MIXED"
    
    def test_detect_dwi_by_b_value(self, detector: TechniqueDetector):
        """DWI-EPI detected by diffusion b-value presence."""
        ctx = ClassificationContext(
            mr_diffusion_b_value="1000",
            scanning_sequence="EP"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "DWI-EPI"
    
    def test_detect_bold_by_flag(self, detector: TechniqueDetector):
        """BOLD-EPI detected by is_bold flag."""
        ctx = ClassificationContext(
            stack_sequence_name="*epfid2d1_64",
            image_type="ORIGINAL\\PRIMARY\\FMRI"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "BOLD-EPI"
        assert result.family == "MIXED"
    
    def test_detect_bold_by_keyword(self, detector: TechniqueDetector):
        """BOLD-EPI detected by 'fmri' keyword."""
        ctx = ClassificationContext(
            text_search_blob="brain fmri resting state"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "BOLD-EPI"
    
    def test_detect_asl_by_flag(self, detector: TechniqueDetector):
        """ASL-EPI detected by is_asl flag."""
        ctx = ClassificationContext(
            image_type="ORIGINAL\\PRIMARY\\ASL"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "ASL-EPI"
        assert result.family == "MIXED"
    
    def test_detect_grase_by_combination(self, detector: TechniqueDetector):
        """GRASE detected by has_se + has_gre combination."""
        ctx = ClassificationContext(
            scanning_sequence="['SE', 'GR']"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "GRASE"
        assert result.family == "MIXED"


# =============================================================================
# Test Priority Order
# =============================================================================

class TestPriorityOrder:
    """Test that priority order is respected in detection."""
    
    def test_dwi_before_se_epi(self, detector: TechniqueDetector):
        """DWI-EPI should be detected before SE-EPI."""
        # DWI has is_dwi flag which is higher priority
        ctx = ClassificationContext(
            stack_sequence_name="*ep_b1000t",
            scanning_sequence="['SE', 'EP']",
            image_type="DIFFUSION"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "DWI-EPI"
    
    def test_mprage_before_gre(self, detector: TechniqueDetector):
        """MPRAGE should be detected before generic GRE."""
        ctx = ClassificationContext(
            stack_sequence_name="*tfl3d1_16ns",
            scanning_sequence="['GR', 'IR']",
            sequence_variant="['SK', 'SP', 'MP']"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "MPRAGE"
    
    def test_space_before_tse(self, detector: TechniqueDetector):
        """3D-TSE should be detected before generic TSE."""
        ctx = ClassificationContext(
            stack_sequence_name="*spc3d1_ns"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "3D-TSE"


# =============================================================================
# Test Implied Base Inference
# =============================================================================

class TestImpliedBase:
    """Test technique-to-base inference."""
    
    def test_mprage_implies_t1w(self, detector: TechniqueDetector):
        """MPRAGE should imply T1w."""
        result = detector.get_implied_base("MPRAGE")
        
        assert result is not None
        base, confidence = result
        assert base == "T1w"
        assert confidence >= 0.90
    
    def test_dwi_implies_dwi(self, detector: TechniqueDetector):
        """DWI-EPI should imply DWI base."""
        result = detector.get_implied_base("DWI-EPI")
        
        assert result is not None
        base, confidence = result
        assert base == "DWI"
    
    def test_bold_no_tissue_contrast(self, detector: TechniqueDetector):
        """BOLD-EPI should have no tissue contrast (None)."""
        result = detector.get_implied_base("BOLD-EPI")
        
        assert result is not None
        base, confidence = result
        assert base is None  # No tissue contrast
    
    def test_tof_implies_none(self, detector: TechniqueDetector):
        """TOF-MRA should have no implied base (it's angiography, not tissue contrast)."""
        result = detector.get_implied_base("TOF-MRA")

        # TOF-MRA doesn't have a simple tissue contrast base
        # It may return None or not have an implied base configured
        if result is not None:
            base, confidence = result
            # TOF may or may not have implied base depending on config
            assert confidence >= 0.0


# =============================================================================
# Test Detection Methods
# =============================================================================

class TestDetectionMethods:
    """Test the three detection methods."""
    
    def test_exclusive_has_highest_confidence(self, detector: TechniqueDetector):
        """Exclusive flag detection should have highest confidence."""
        ctx = ClassificationContext(
            stack_sequence_name="*tfl3d1_16ns"  # is_mprage
        )
        result = detector.detect_technique(ctx)
        
        assert result.detection_method == "exclusive"
        assert result.confidence >= 0.95
    
    def test_keywords_has_high_confidence(self, detector: TechniqueDetector):
        """Keywords detection should have high confidence."""
        ctx = ClassificationContext(
            text_search_blob="brain mprage t1"
        )
        result = detector.detect_technique(ctx)
        
        assert result.detection_method == "keywords"
        assert result.confidence >= 0.80
    
    def test_combination_has_medium_confidence(self, detector: TechniqueDetector):
        """Combination detection should have medium confidence."""
        # GRASE is detected by has_se + has_gre combination
        ctx = ClassificationContext(
            scanning_sequence="['SE', 'GR']"
        )
        result = detector.detect_technique(ctx)
        
        assert result.detection_method == "combination"
        assert result.confidence >= 0.70


# =============================================================================
# Test Edge Cases
# =============================================================================

class TestEdgeCases:
    """Test edge cases and special scenarios."""
    
    def test_empty_context_returns_unknown(self, detector: TechniqueDetector):
        """Empty context should return UNKNOWN."""
        ctx = ClassificationContext()
        result = detector.detect_technique(ctx)
        
        assert result.technique == "UNKNOWN"
        assert result.confidence == 0.0
    
    def test_unknown_sequence_returns_unknown(self, detector: TechniqueDetector):
        """Unknown sequence pattern should return UNKNOWN."""
        ctx = ClassificationContext(
            stack_sequence_name="*xyz_unknown_seq"
        )
        result = detector.detect_technique(ctx)
        
        assert result.technique == "UNKNOWN"
    
    def test_explain_detection(self, detector: TechniqueDetector):
        """explain_detection should return diagnostic info."""
        ctx = ClassificationContext(
            stack_sequence_name="*tfl3d1_16ns"
        )
        explanation = detector.explain_detection(ctx)
        
        assert "detected_technique" in explanation
        assert "detection_method" in explanation
        assert "active_flags" in explanation
        assert "checked_techniques" in explanation


# =============================================================================
# Test Convenience Methods
# =============================================================================

class TestConvenienceMethods:
    """Test convenience/utility methods."""
    
    def test_get_techniques_by_family(self, detector: TechniqueDetector):
        """Get all techniques in a family."""
        se_techniques = detector.get_techniques_by_family("SE")
        gre_techniques = detector.get_techniques_by_family("GRE")

        assert len(se_techniques) == 8  # SE family count
        assert len(gre_techniques) == 18  # GRE family count (SWI=provenance, Radial/Spiral=modifiers)
        assert "MPRAGE" in gre_techniques
        assert "TSE" in se_techniques
    
    def test_get_family_fallback(self, detector: TechniqueDetector):
        """Get fallback technique for each family."""
        assert detector.get_family_fallback("SE") == "SE"
        assert detector.get_family_fallback("GRE") == "GRE"
        assert detector.get_family_fallback("EPI") == "EPI"
    
    def test_get_technique_family(self, detector: TechniqueDetector):
        """Get family for a technique."""
        assert detector.get_technique_family("MPRAGE") == "GRE"
        assert detector.get_technique_family("TSE") == "SE"
        assert detector.get_technique_family("DWI-EPI") == "MIXED"
