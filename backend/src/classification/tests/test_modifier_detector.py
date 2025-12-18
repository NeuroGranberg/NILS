"""
Unit tests for ModifierDetector.

Tests modifier detection logic including:
- Three-tier detection (exclusive, keywords, combination)
- Mutual exclusion groups (IR_CONTRAST, TRAJECTORY)
- Additive modifiers (independent can combine)
- Evidence tracking and confidence scoring
"""

import pytest
from ..core.context import ClassificationContext
from ..detectors.modifier import ModifierDetector, ModifierResult


class TestDetectorInit:
    """Test detector initialization."""
    
    def test_loads_yaml_config(self):
        """Detector loads YAML configuration."""
        detector = ModifierDetector()
        assert detector.config is not None
        assert "modifiers" in detector.config
    
    def test_has_modifiers(self):
        """Detector has modifiers configured."""
        detector = ModifierDetector()
        modifiers = detector.get_all_modifiers()
        assert len(modifiers) >= 10
        assert "FLAIR" in modifiers
        assert "STIR" in modifiers
        assert "FatSat" in modifiers
    
    def test_has_exclusion_groups(self):
        """Detector has exclusion groups configured."""
        detector = ModifierDetector()
        assert "IR_CONTRAST" in detector._exclusion_groups
        assert "TRAJECTORY" in detector._exclusion_groups


class TestIRContrastModifiers:
    """Test IR_CONTRAST group modifiers."""
    
    def test_detect_flair_by_exclusive_flag(self):
        """FLAIR detected via is_flair flag."""
        detector = ModifierDetector()
        # Use sequence name that sets is_flair (contains 'flair')
        ctx = ClassificationContext(
            text_search_blob="ax t2 brain",
            stack_sequence_name="*tir2d1_16_flair",  # Sets is_flair
        )
        result = detector.detect_modifiers(ctx)
        assert "FLAIR" in result.modifiers
        assert result.matches[0].detection_method == "exclusive"
    
    def test_detect_flair_by_keyword(self):
        """FLAIR detected via keyword."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t2 flair brain",
        )
        result = detector.detect_modifiers(ctx)
        assert "FLAIR" in result.modifiers
        assert result.matches[0].detection_method == "keywords"

    def test_detect_flair_by_siemens_dark_fluid_normalized(self):
        """FLAIR detected via Siemens 'da-fl' (Dark Fluid) after hyphen normalization.

        Real case: e9a7a469b01e3673 2019-03-28
        - Original series: "t2_spc_da-fl_sag_fs_REK 3mm tra"
        - After normalization: "t2w spc da - fl sag fs rek 3mm tra"
        - The "da-fl" becomes "da - fl" with spaces around the hyphen
        """
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="t2w spc da - fl sag fs rek 3mm tra starspcir 242ns",
        )
        result = detector.detect_modifiers(ctx)
        assert "FLAIR" in result.modifiers
        assert result.matches[0].detection_method == "keywords"

    def test_detect_stir_by_keyword(self):
        """STIR detected via keyword."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="stir cor spine",
        )
        result = detector.detect_modifiers(ctx)
        assert "STIR" in result.modifiers
    
    def test_detect_tirm_as_stir(self):
        """TIRM keyword maps to STIR."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax tirm fs brain",
        )
        result = detector.detect_modifiers(ctx)
        assert "STIR" in result.modifiers
    
    def test_detect_dir_by_keyword(self):
        """DIR detected via keyword."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax dir brain",
        )
        result = detector.detect_modifiers(ctx)
        assert "DIR" in result.modifiers
    
    def test_detect_psir_by_keyword(self):
        """PSIR detected via keyword."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="sag psir spine",
        )
        result = detector.detect_modifiers(ctx)
        assert "PSIR" in result.modifiers
    
    def test_flair_beats_stir_in_priority(self):
        """FLAIR has higher priority than STIR in IR_CONTRAST group."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="flair stir",  # Both keywords
        )
        result = detector.detect_modifiers(ctx)
        # Only FLAIR should be in result (higher priority)
        assert result.modifiers == ["FLAIR"]
    
    def test_flair_beats_generic_ir(self):
        """FLAIR beats generic IR when both could match."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="flair brain",
            scanning_sequence="SE\\IR",  # has_ir=True
        )
        result = detector.detect_modifiers(ctx)
        assert result.modifiers == ["FLAIR"]


class TestFatSuppressionModifiers:
    """Test fat suppression modifiers."""
    
    def test_detect_fatsat_by_flag(self):
        """FatSat detected via has_fat_sat flag."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t2 tse",
            scan_options="FS\\PFP",  # FS sets has_fat_sat
        )
        result = detector.detect_modifiers(ctx)
        assert "FatSat" in result.modifiers
        assert result.matches[0].detection_method == "exclusive"
    
    def test_detect_fatsat_by_keyword(self):
        """FatSat detected via keyword."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t2 tse fatsat",
        )
        result = detector.detect_modifiers(ctx)
        assert "FatSat" in result.modifiers
    
    def test_detect_spair_as_fatsat(self):
        """SPAIR keyword maps to FatSat."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t2 spair",
        )
        result = detector.detect_modifiers(ctx)
        assert "FatSat" in result.modifiers
    
    def test_water_excitation_by_flag(self):
        """WaterExcitation detected via has_water_excitation flag."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t2 3d",
            scan_options="WE",
        )
        result = detector.detect_modifiers(ctx)
        assert "WaterExc" in result.modifiers  # Uses name field from YAML


class TestTrajectoryModifiers:
    """Test TRAJECTORY group modifiers."""
    
    def test_detect_radial_by_flag(self):
        """Radial detected via is_radial flag."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t2 brain",
            scan_options="PROP_GEMS",  # Sets is_radial via has_propeller_gems
        )
        result = detector.detect_modifiers(ctx)
        assert "Radial" in result.modifiers
    
    def test_detect_radial_by_keyword_propeller(self):
        """Radial detected via propeller keyword."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t2 propeller brain",
        )
        result = detector.detect_modifiers(ctx)
        assert "Radial" in result.modifiers
    
    def test_detect_radial_by_keyword_blade(self):
        """Radial detected via blade keyword."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t2 blade brain",
        )
        result = detector.detect_modifiers(ctx)
        assert "Radial" in result.modifiers
    
    def test_detect_spiral_by_keyword(self):
        """Spiral detected via keyword."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="spiral asl",
        )
        result = detector.detect_modifiers(ctx)
        assert "Spiral" in result.modifiers
    
    def test_radial_beats_spiral(self):
        """Radial has higher priority than Spiral in TRAJECTORY group."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="propeller spiral",  # Both keywords
        )
        result = detector.detect_modifiers(ctx)
        assert result.modifiers == ["Radial"]


class TestIndependentModifiers:
    """Test independent (non-exclusive) modifiers."""
    
    def test_detect_dixon_by_keyword(self):
        """Dixon detected via keyword."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t1 vibe dixon",
        )
        result = detector.detect_modifiers(ctx)
        assert "Dixon" in result.modifiers
    
    def test_detect_mt_by_keyword(self):
        """MT detected via keyword (magnetization transfer)."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="magnetization transfer t1 3d brain",
        )
        result = detector.detect_modifiers(ctx)
        assert "MT" in result.modifiers
    
    def test_detect_mt_by_flag(self):
        """MT detected via has_mtc flag."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="t1 3d brain",
            sequence_variant="SP\\SS\\MTC",
        )
        result = detector.detect_modifiers(ctx)
        assert "MT" in result.modifiers
    
    def test_detect_flowcomp_by_keyword(self):
        """FlowComp detected via keyword."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t2 tse flow comp",
        )
        result = detector.detect_modifiers(ctx)
        assert "FlowComp" in result.modifiers
    
    def test_detect_blackblood_by_keyword(self):
        """BlackBlood detected via keyword."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t1 black blood cardiac",
        )
        result = detector.detect_modifiers(ctx)
        assert "BlackBlood" in result.modifiers


class TestAdditiveModifiers:
    """Test that multiple independent modifiers can apply."""
    
    def test_flair_plus_fatsat(self):
        """FLAIR and FatSat can combine."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax flair spair brain",
            scan_options="FS",
        )
        result = detector.detect_modifiers(ctx)
        assert set(result.modifiers) == {"FLAIR", "FatSat"}
    
    def test_multiple_independent_modifiers(self):
        """Multiple independent modifiers can all apply."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="fatsat with flow comp",
        )
        result = detector.detect_modifiers(ctx)
        assert set(result.modifiers) == {"FatSat", "FlowComp"}
    
    def test_ir_plus_trajectory_plus_independent(self):
        """IR group + trajectory group + independent can combine."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="flair fatsat propeller",
        )
        result = detector.detect_modifiers(ctx)
        assert set(result.modifiers) == {"FLAIR", "FatSat", "Radial"}


class TestNoModifiers:
    """Test cases where no modifiers should be detected."""
    
    def test_empty_context(self):
        """Empty context returns no modifiers."""
        detector = ModifierDetector()
        ctx = ClassificationContext()
        result = detector.detect_modifiers(ctx)
        assert result.modifiers == []
    
    def test_plain_t1_tse(self):
        """Plain T1 TSE without modifiers."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t1 tse brain",
            scanning_sequence="SE",
        )
        result = detector.detect_modifiers(ctx)
        assert result.modifiers == []
    
    def test_plain_t2_gre(self):
        """Plain T2* GRE without modifiers."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t2 gre brain",
            scanning_sequence="GR",
        )
        result = detector.detect_modifiers(ctx)
        assert result.modifiers == []


class TestDetectionConfidence:
    """Test confidence scoring for different detection methods."""
    
    def test_exclusive_has_highest_confidence(self):
        """Exclusive flag detection has 0.95 confidence."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t2",
            scan_options="FS",  # has_fat_sat flag
        )
        result = detector.detect_modifiers(ctx)
        fatsat_match = [m for m in result.matches if m.modifier == "FatSat"][0]
        assert fatsat_match.confidence == 0.95
        assert fatsat_match.detection_method == "exclusive"
    
    def test_keywords_has_high_confidence(self):
        """Keyword detection has 0.85 confidence."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t2 flair brain",
        )
        result = detector.detect_modifiers(ctx)
        flair_match = [m for m in result.matches if m.modifier == "FLAIR"][0]
        assert flair_match.confidence == 0.85
        assert flair_match.detection_method == "keywords"
    
    def test_average_confidence_for_multiple(self):
        """Overall confidence is average of individual confidences."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="flair fatsat",
            scan_options="FS",  # FatSat via exclusive
        )
        result = detector.detect_modifiers(ctx)
        # FLAIR: 0.85 (keywords), FatSat: 0.95 (exclusive)
        expected_avg = (0.85 + 0.95) / 2
        assert abs(result.confidence - expected_avg) < 0.01


class TestEvidenceTracking:
    """Test evidence is properly tracked."""
    
    def test_exclusive_evidence(self):
        """Exclusive detection has proper evidence."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t2",
            scan_options="FS",
        )
        result = detector.detect_modifiers(ctx)
        fatsat_match = [m for m in result.matches if m.modifier == "FatSat"][0]
        assert len(fatsat_match.evidence) == 1
        assert fatsat_match.evidence[0].field == "unified_flags"
    
    def test_keyword_evidence(self):
        """Keyword detection has proper evidence."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t2 flair brain",
        )
        result = detector.detect_modifiers(ctx)
        flair_match = [m for m in result.matches if m.modifier == "FLAIR"][0]
        assert len(flair_match.evidence) == 1
        assert flair_match.evidence[0].field == "text_search_blob"


class TestConvenienceMethods:
    """Test convenience methods."""
    
    def test_get_all_modifiers(self):
        """get_all_modifiers returns all modifier IDs."""
        detector = ModifierDetector()
        modifiers = detector.get_all_modifiers()
        assert "FLAIR" in modifiers
        assert "STIR" in modifiers
        assert "FatSat" in modifiers
        assert "Radial" in modifiers
    
    def test_get_group_members(self):
        """get_group_members returns group members."""
        detector = ModifierDetector()
        ir_members = detector.get_group_members("IR_CONTRAST")
        assert "FLAIR" in ir_members
        assert "STIR" in ir_members
    
    def test_get_independent_modifiers(self):
        """get_independent_modifiers returns non-grouped modifiers."""
        detector = ModifierDetector()
        independent = detector.get_independent_modifiers()
        assert "FatSat" in independent
        assert "MT" in independent
        assert "FLAIR" not in independent  # In IR_CONTRAST group
    
    def test_has_ir_modifier(self):
        """has_ir_modifier correctly identifies IR modifiers."""
        detector = ModifierDetector()
        result = detector.detect_modifiers(ClassificationContext(
            text_search_blob="ax t2 flair",
        ))
        assert detector.has_ir_modifier(result) is True
        
        result = detector.detect_modifiers(ClassificationContext(
            text_search_blob="ax t2 tse",
        ))
        assert detector.has_ir_modifier(result) is False
    
    def test_has_fat_suppression(self):
        """has_fat_suppression correctly identifies fat suppression."""
        detector = ModifierDetector()
        result = detector.detect_modifiers(ClassificationContext(
            text_search_blob="ax t2 fatsat",
        ))
        assert detector.has_fat_suppression(result) is True
        
        # STIR also counts as fat suppression
        result = detector.detect_modifiers(ClassificationContext(
            text_search_blob="stir cor",
        ))
        assert detector.has_fat_suppression(result) is True
    
    def test_has_trajectory_modifier(self):
        """has_trajectory_modifier correctly identifies trajectory modifiers."""
        detector = ModifierDetector()
        result = detector.detect_modifiers(ClassificationContext(
            text_search_blob="ax t2 propeller",
        ))
        assert detector.has_trajectory_modifier(result) is True


class TestAxisResultConversion:
    """Test conversion to AxisResult."""
    
    def test_to_axis_result(self):
        """ModifierResult converts to AxisResult correctly."""
        detector = ModifierDetector()
        result = detector.detect_modifiers(ClassificationContext(
            text_search_blob="ax t2 flair fatsat",
        ))
        axis_result = result.to_axis_result()
        assert axis_result.value == "FLAIR,FatSat"
        assert axis_result.confidence > 0
        assert len(axis_result.evidence) >= 2
    
    def test_detect_returns_axis_result(self):
        """detect() method returns AxisResult."""
        detector = ModifierDetector()
        axis_result = detector.detect(ClassificationContext(
            text_search_blob="ax t2 flair",
        ))
        assert axis_result.value == "FLAIR"


class TestExplainDetection:
    """Test the explain_detection method."""
    
    def test_explain_provides_details(self):
        """explain_detection provides useful debug info."""
        detector = ModifierDetector()
        ctx = ClassificationContext(
            text_search_blob="ax t2 flair fatsat",
        )
        explanation = detector.explain_detection(ctx)
        
        assert "detected_modifiers" in explanation
        assert "modifier_csv" in explanation
        assert "match_count" in explanation
        assert "match_details" in explanation
        assert "exclusion_groups" in explanation
