"""
Tests for Body Part Detector

Tests the spinal cord detection system that:
1. Detects spine-related keywords in text_search_blob
2. Triggers manual review when spine is detected
3. Returns unknown when no spine keywords found (NOT brain-only)

Version: 1.0.0
"""

import pytest
from src.classification.core.context import ClassificationContext
from src.classification.detectors.body_part import (
    BodyPartDetector,
    BodyPartResult,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def detector():
    """Create a BodyPartDetector instance."""
    return BodyPartDetector()


@pytest.fixture
def ctx_spine_english():
    """Context with English spine keyword."""
    return ClassificationContext(
        text_search_blob="ax t1 cervical spine brain mprage",
    )


@pytest.fixture
def ctx_spine_swedish():
    """Context with Swedish spine keyword."""
    return ClassificationContext(
        text_search_blob="ax t1 halsrygg hjärna mprage",
    )


@pytest.fixture
def ctx_brain_only():
    """Context with brain only (no spine keywords)."""
    return ClassificationContext(
        text_search_blob="ax t1 brain flair 3d mprage",
    )


@pytest.fixture
def ctx_empty():
    """Empty context."""
    return ClassificationContext()


# =============================================================================
# Test: English Spine Keywords
# =============================================================================

class TestEnglishSpineKeywords:
    """Tests for English spine keyword detection."""
    
    def test_spine_keyword(self, detector, ctx_spine_english):
        """'spine' keyword → spinal_cord = 1."""
        result = detector.detect_body_part(ctx_spine_english)
        
        assert result.spinal_cord == 1
        assert result.detection_method == "text_positive"
        assert "spine" in result.matched_keyword
    
    def test_cervical_keyword(self, detector):
        """'cervical' keyword → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 cervical brain",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
        assert result.matched_keyword == "cervical"
    
    def test_thoracic_keyword(self, detector):
        """'thoracic' keyword → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 thoracic cord",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
        assert result.matched_keyword == "thoracic"
    
    def test_lumbar_keyword(self, detector):
        """'lumbar' keyword → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 lumbar spine",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
    
    def test_spinal_cord_keyword(self, detector):
        """'spinal cord' keyword → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 spinal cord lesion",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
    
    def test_vertebral_keyword(self, detector):
        """'vertebral' keyword → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 vertebral artery",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
    
    def test_c_spine_notation(self, detector):
        """'c-spine' notation → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 c-spine brain",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
    
    def test_t_spine_notation(self, detector):
        """'t-spine' notation → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 t-spine",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1


# =============================================================================
# Test: Swedish Spine Keywords
# =============================================================================

class TestSwedishSpineKeywords:
    """Tests for Swedish spine keyword detection."""
    
    def test_halsrygg_keyword(self, detector, ctx_spine_swedish):
        """Swedish 'halsrygg' (cervical spine) → spinal_cord = 1."""
        result = detector.detect_body_part(ctx_spine_swedish)
        
        assert result.spinal_cord == 1
        # 'rygg' should match (part of halsrygg)
        assert "rygg" in result.matched_keyword
    
    def test_rygg_keyword(self, detector):
        """Swedish 'rygg' (spine) → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 rygg hjärna",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
        assert result.matched_keyword == "rygg"
    
    def test_nacke_keyword(self, detector):
        """Swedish 'nacke' (neck) → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 nacke hjärna",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
        assert result.matched_keyword == "nacke"


# =============================================================================
# Test: German Spine Keywords
# =============================================================================

class TestGermanSpineKeywords:
    """Tests for German spine keyword detection."""
    
    def test_hws_keyword(self, detector):
        """German 'hws' (cervical) → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 hws kopf",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
        assert result.matched_keyword == "hws"
    
    def test_bws_keyword(self, detector):
        """German 'bws' (thoracic) → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 bws",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
        assert result.matched_keyword == "bws"
    
    def test_wirbel_keyword(self, detector):
        """German 'wirbel' (vertebra) → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 wirbel kopf",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
        assert result.matched_keyword == "wirbel"


# =============================================================================
# Test: French Spine Keywords
# =============================================================================

class TestFrenchSpineKeywords:
    """Tests for French spine keyword detection."""
    
    def test_rachis_keyword(self, detector):
        """French 'rachis' (spine) → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 rachis cerveau",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
        assert result.matched_keyword == "rachis"
    
    def test_moelle_keyword(self, detector):
        """French 'moelle' (spinal cord) → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 moelle cerveau",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
        assert result.matched_keyword == "moelle"


# =============================================================================
# Test: Pathology Keywords
# =============================================================================

class TestPathologyKeywords:
    """Tests for pathology-related spine keyword detection."""
    
    def test_myelitis_keyword(self, detector):
        """'myelit' (myelitis) → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 myelitis brain",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
        assert "myelit" in result.matched_keyword
    
    def test_myelopathy_keyword(self, detector):
        """'myelopathy' → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 myelopathy brain",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
    
    def test_syringomyelia_keyword(self, detector):
        """'syringomyelia' → spinal_cord = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 syringomyelia",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1


# =============================================================================
# Test: No Spine Keywords (Unknown)
# =============================================================================

class TestNoSpineKeywords:
    """Tests for cases with no spine keywords."""
    
    def test_brain_only_returns_unknown(self, detector, ctx_brain_only):
        """No spine keywords → spinal_cord = None (not brain-only)."""
        result = detector.detect_body_part(ctx_brain_only)
        
        assert result.spinal_cord is None
        assert result.detection_method == "unknown"
        assert result.matched_keyword is None
    
    def test_empty_context_returns_unknown(self, detector, ctx_empty):
        """Empty context → spinal_cord = None."""
        result = detector.detect_body_part(ctx_empty)
        
        assert result.spinal_cord is None
        assert result.detection_method == "unknown"
    
    def test_no_text_blob_returns_unknown(self, detector):
        """No text_search_blob → spinal_cord = None."""
        ctx = ClassificationContext(
            image_type=r"ORIGINAL\PRIMARY\M\NORM",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord is None


# =============================================================================
# Test: Review Triggering
# =============================================================================

class TestReviewTriggering:
    """Tests for manual review triggering."""
    
    def test_spine_triggers_review(self, detector, ctx_spine_english):
        """Spine detection should trigger review."""
        result = detector.detect_body_part(ctx_spine_english)
        
        assert result.triggers_review is True
    
    def test_unknown_no_review(self, detector, ctx_brain_only):
        """Unknown result should not trigger review."""
        result = detector.detect_body_part(ctx_brain_only)
        
        assert result.triggers_review is False
    
    def test_get_review_reason(self, detector):
        """get_review_reason should return correct code."""
        reason = detector.get_review_reason()
        
        assert reason == "bodypart:spine_detected"


# =============================================================================
# Test: Confidence Levels
# =============================================================================

class TestConfidenceLevels:
    """Tests for confidence levels (always low for text-based)."""
    
    def test_spine_has_low_confidence(self, detector, ctx_spine_english):
        """Spine detection should have low confidence."""
        result = detector.detect_body_part(ctx_spine_english)
        
        assert result.confidence == 0.65
        assert result.confidence < 0.70  # Low confidence threshold
    
    def test_unknown_has_zero_confidence(self, detector, ctx_brain_only):
        """Unknown result should have zero confidence."""
        result = detector.detect_body_part(ctx_brain_only)
        
        assert result.confidence == 0.0


# =============================================================================
# Test: BodyPartResult Properties
# =============================================================================

class TestBodyPartResultProperties:
    """Tests for BodyPartResult helper properties."""
    
    def test_is_spine(self, detector, ctx_spine_english):
        """is_spine should be True for spinal_cord=1."""
        result = detector.detect_body_part(ctx_spine_english)
        
        assert result.is_spine is True
        assert result.is_brain_only is False
        assert result.is_unknown is False
    
    def test_is_unknown(self, detector, ctx_brain_only):
        """is_unknown should be True for spinal_cord=None."""
        result = detector.detect_body_part(ctx_brain_only)
        
        assert result.is_spine is False
        assert result.is_brain_only is False
        assert result.is_unknown is True
    
    def test_value_alias(self, detector, ctx_spine_english):
        """value property should alias spinal_cord."""
        result = detector.detect_body_part(ctx_spine_english)
        
        assert result.value == result.spinal_cord
    
    def test_to_axis_result_spine(self, detector, ctx_spine_english):
        """to_axis_result should return correct value for spine."""
        result = detector.detect_body_part(ctx_spine_english)
        axis_result = result.to_axis_result()
        
        assert axis_result.value == "spinal_cord"
        assert axis_result.confidence == 0.65
    
    def test_to_axis_result_unknown(self, detector, ctx_brain_only):
        """to_axis_result should return correct value for unknown."""
        result = detector.detect_body_part(ctx_brain_only)
        axis_result = result.to_axis_result()
        
        assert axis_result.value == "unknown"


# =============================================================================
# Test: Convenience Methods
# =============================================================================

class TestConvenienceMethods:
    """Tests for detector convenience methods."""
    
    def test_get_spine_keywords(self, detector):
        """get_spine_keywords should return list."""
        keywords = detector.get_spine_keywords()
        
        assert isinstance(keywords, list)
        assert len(keywords) > 0
        assert "spine" in keywords
        assert "cervical" in keywords
        assert "rygg" in keywords
    
    def test_explain_detection(self, detector, ctx_spine_english):
        """explain_detection should return detailed dict."""
        explanation = detector.explain_detection(ctx_spine_english)
        
        assert "spinal_cord" in explanation
        assert "confidence" in explanation
        assert "detection_method" in explanation
        assert "matched_keyword" in explanation
        assert "triggers_review" in explanation
        assert explanation["spinal_cord"] == 1
    
    def test_debug_text_matching(self, detector):
        """debug_text_matching should show matched keywords."""
        debug = detector.debug_text_matching("ax t1 cervical spine brain")
        
        assert "spine" in debug["matched_spine_keywords"]
        assert "cervical" in debug["matched_spine_keywords"]
        assert debug["would_trigger_review"] is True


# =============================================================================
# Test: Edge Cases
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and special scenarios."""
    
    def test_case_insensitive(self, detector):
        """Detection should be case-insensitive."""
        ctx = ClassificationContext(
            text_search_blob="AX T1 CERVICAL SPINE BRAIN",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
    
    def test_multiple_spine_keywords(self, detector):
        """Multiple spine keywords should still return first match."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 cervical spine thoracic vertebral",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
        # Should match one of the keywords
        assert result.matched_keyword is not None
    
    def test_long_text_blob(self, detector):
        """Detection should work with long text."""
        ctx = ClassificationContext(
            text_search_blob="this is a very long description with lots of words " * 10 + "spine",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1
    
    def test_spine_in_middle_of_word(self, detector):
        """'spine' should match even in middle of text."""
        ctx = ClassificationContext(
            text_search_blob="brain_spine_combined",
        )
        result = detector.detect_body_part(ctx)
        
        assert result.spinal_cord == 1


# =============================================================================
# Test: Evidence Tracking
# =============================================================================

class TestEvidenceTracking:
    """Tests for evidence tracking."""
    
    def test_spine_has_evidence(self, detector, ctx_spine_english):
        """Spine result should include evidence."""
        result = detector.detect_body_part(ctx_spine_english)
        
        assert len(result.evidence) == 1
        assert result.evidence[0].target == "spinal_cord"
    
    def test_unknown_no_evidence(self, detector, ctx_brain_only):
        """Unknown result should have no evidence."""
        result = detector.detect_body_part(ctx_brain_only)
        
        assert len(result.evidence) == 0
    
    def test_evidence_has_description(self, detector, ctx_spine_english):
        """Evidence should include human-readable description."""
        result = detector.detect_body_part(ctx_spine_english)
        
        assert len(result.evidence) > 0
        assert result.evidence[0].description != ""
        assert "spine" in result.evidence[0].description.lower()
