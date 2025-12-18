"""
Tests for Contrast Agent Detector

Tests the two-tier contrast detection system:
1. Structured source (contrast_search_blob from DICOM tags)
2. Text source (text_search_blob pattern matching)

Version: 1.0.0
"""

import pytest
from src.classification.core.context import ClassificationContext
from src.classification.detectors.contrast import (
    ContrastDetector,
    ContrastResult,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def detector():
    """Create a ContrastDetector instance."""
    return ContrastDetector()


@pytest.fixture
def ctx_structured_contrast():
    """Context with DICOM contrast tags (contrast_search_blob)."""
    return ClassificationContext(
        contrast_search_blob="agent:dotarem route:iv dose:15.0",
    )


@pytest.fixture
def ctx_positive_text():
    """Context with positive contrast text."""
    return ClassificationContext(
        text_search_blob="ax t1 post gd brain 3d mprage",
    )


@pytest.fixture
def ctx_negative_text():
    """Context with negative contrast text."""
    return ClassificationContext(
        text_search_blob="ax t1 utan gd brain 3d mprage",
    )


@pytest.fixture
def ctx_no_contrast():
    """Context with no contrast information."""
    return ClassificationContext(
        text_search_blob="ax t1 brain 3d mprage",
    )


@pytest.fixture
def ctx_empty():
    """Empty context."""
    return ClassificationContext()


# =============================================================================
# Test: Structured Source (contrast_search_blob)
# =============================================================================

class TestStructuredSource:
    """Tests for DICOM structured contrast detection."""
    
    def test_structured_source_positive(self, detector, ctx_structured_contrast):
        """contrast_search_blob present → post_contrast = 1."""
        result = detector.detect_contrast(ctx_structured_contrast)
        
        assert result.post_contrast == 1
        assert result.detection_method == "structured"
        assert result.confidence == 0.95
    
    def test_structured_has_evidence(self, detector, ctx_structured_contrast):
        """Structured detection should have evidence."""
        result = detector.detect_contrast(ctx_structured_contrast)
        
        assert len(result.evidence) == 1
        assert result.evidence[0].field == "contrast_search_blob"
        assert "dotarem" in result.evidence[0].value
    
    def test_structured_various_agents(self, detector):
        """Various contrast agent names should be detected."""
        agents = ["magnevist", "omniscan", "gadovist", "prohance"]
        
        for agent in agents:
            ctx = ClassificationContext(
                contrast_search_blob=f"agent:{agent} route:iv",
            )
            result = detector.detect_contrast(ctx)
            
            assert result.post_contrast == 1, f"Failed for agent: {agent}"
            assert result.detection_method == "structured"
    
    def test_structured_overrides_negative_text(self, detector):
        """Structured source should override negative text."""
        ctx = ClassificationContext(
            contrast_search_blob="agent:dotarem route:iv",
            text_search_blob="ax t1 utan gd brain",  # Negative keyword
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 1
        assert result.detection_method == "structured"


# =============================================================================
# Test: Text Source - Positive Keywords
# =============================================================================

class TestPositiveKeywords:
    """Tests for positive contrast keyword detection."""
    
    def test_positive_post_gd(self, detector, ctx_positive_text):
        """'post gd' → post_contrast = 1."""
        result = detector.detect_contrast(ctx_positive_text)
        
        assert result.post_contrast == 1
        assert result.detection_method == "text_positive"
        assert result.matched_keyword == "post gd"
    
    def test_positive_brand_names(self, detector):
        """Brand names should trigger positive detection."""
        brands = ["dotarem", "magnevist", "omniscan", "gadovist", "clariscan"]
        
        for brand in brands:
            ctx = ClassificationContext(
                text_search_blob=f"ax t1 {brand} brain",
            )
            result = detector.detect_contrast(ctx)
            
            assert result.post_contrast == 1, f"Failed for brand: {brand}"
            assert result.matched_keyword == brand
    
    def test_positive_swedish_plus_k(self, detector):
        """Swedish '+k' notation → post_contrast = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 +k brain flair",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 1
        assert result.matched_keyword == "+k"
    
    def test_positive_swedish_plus_gd(self, detector):
        """Swedish '+gd' notation → post_contrast = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 +gd brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 1
        assert result.matched_keyword == "+gd"
    
    def test_positive_swedish_med_gd(self, detector):
        """Swedish 'med gd' → post_contrast = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 med gd brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 1
        assert result.matched_keyword == "med gd"
    
    def test_positive_english_post_contrast(self, detector):
        """English 'post-contrast' → post_contrast = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 post-contrast brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 1
        assert result.matched_keyword == "post-contrast"
    
    def test_positive_english_with_contrast(self, detector):
        """English 'with contrast' → post_contrast = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 with contrast brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 1
        assert result.matched_keyword == "with contrast"
    
    def test_positive_german_mit_gd(self, detector):
        """German 'mit gd' → post_contrast = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 mit gd brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 1
        assert result.matched_keyword == "mit gd"
    
    def test_positive_gadolinium_generic(self, detector):
        """Generic 'gadolinium' → post_contrast = 1."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 gadolinium brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 1
        assert result.matched_keyword == "gadolinium"
    
    def test_positive_confidence_lower_than_structured(self, detector, ctx_positive_text):
        """Text positive confidence should be lower than structured."""
        result = detector.detect_contrast(ctx_positive_text)
        
        assert result.confidence == 0.80
        assert result.confidence < 0.95  # Structured confidence


# =============================================================================
# Test: Text Source - Negative Keywords
# =============================================================================

class TestNegativeKeywords:
    """Tests for negative contrast keyword detection."""
    
    def test_negative_utan_gd(self, detector, ctx_negative_text):
        """Swedish 'utan gd' → post_contrast = 0."""
        result = detector.detect_contrast(ctx_negative_text)
        
        assert result.post_contrast == 0
        assert result.detection_method == "text_negative"
        assert result.matched_keyword == "utan gd"
    
    def test_negative_pre_contrast(self, detector):
        """English 'pre-contrast' → post_contrast = 0."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 pre-contrast brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 0
        assert result.matched_keyword == "pre-contrast"
    
    def test_negative_without_contrast(self, detector):
        """English 'without contrast' → post_contrast = 0."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 without contrast brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 0
        assert result.matched_keyword == "without contrast"
    
    def test_negative_non_contrast(self, detector):
        """English 'non-contrast' → post_contrast = 0."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 non-contrast brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 0
        assert result.matched_keyword == "non-contrast"
    
    def test_negative_german_ohne_gd(self, detector):
        """German 'ohne gd' → post_contrast = 0."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 ohne gd brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 0
        assert result.matched_keyword == "ohne gd"
    
    def test_negative_german_nativ(self, detector):
        """German 'nativ' → post_contrast = 0."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 nativ brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 0
        assert result.matched_keyword == "nativ"
    
    def test_negative_swedish_ej_gd(self, detector):
        """Swedish 'ej gd' → post_contrast = 0."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 ej gd brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 0
        assert result.matched_keyword == "ej gd"
    
    def test_negative_french_sans_gd(self, detector):
        """French 'sans gd' → post_contrast = 0."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 sans gd brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 0
        assert result.matched_keyword == "sans gd"
    
    def test_negative_confidence(self, detector, ctx_negative_text):
        """Negative text confidence should be 0.85."""
        result = detector.detect_contrast(ctx_negative_text)
        
        assert result.confidence == 0.85


# =============================================================================
# Test: Priority - Negative Overrides Positive
# =============================================================================

class TestNegativeOverridesPositive:
    """Tests for negative keyword priority over positive."""
    
    def test_negative_overrides_positive_same_text(self, detector):
        """Negative keyword should override positive in same text."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 +gd pre-contrast brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 0
        assert result.detection_method == "text_negative"
        assert result.matched_keyword == "pre-contrast"
    
    def test_negative_checked_first(self, detector):
        """Negative keywords should be checked before positive."""
        # This text has both "utan gd" (negative) and "gd" (in positive list via gadolinium)
        ctx = ClassificationContext(
            text_search_blob="ax t1 utan gd brain gadolinium",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 0
        assert result.matched_keyword == "utan gd"
    
    def test_pre_gd_vs_post_gd(self, detector):
        """'pre gd' should win over implied positive."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 pre gd brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 0
        assert result.matched_keyword == "pre gd"


# =============================================================================
# Test: Unknown / No Match
# =============================================================================

class TestUnknown:
    """Tests for unknown contrast status."""
    
    def test_no_keywords_unknown(self, detector, ctx_no_contrast):
        """No keywords → post_contrast = None."""
        result = detector.detect_contrast(ctx_no_contrast)
        
        assert result.post_contrast is None
        assert result.detection_method == "unknown"
        assert result.matched_keyword is None
    
    def test_empty_context_unknown(self, detector, ctx_empty):
        """Empty context → post_contrast = None."""
        result = detector.detect_contrast(ctx_empty)
        
        assert result.post_contrast is None
        assert result.detection_method == "unknown"
    
    def test_no_evidence_for_unknown(self, detector, ctx_no_contrast):
        """Unknown result should have no evidence."""
        result = detector.detect_contrast(ctx_no_contrast)
        
        assert len(result.evidence) == 0
    
    def test_unknown_confidence_zero(self, detector, ctx_no_contrast):
        """Unknown result should have confidence 0."""
        result = detector.detect_contrast(ctx_no_contrast)
        
        assert result.confidence == 0.0


# =============================================================================
# Test: ContrastResult Properties
# =============================================================================

class TestContrastResultProperties:
    """Tests for ContrastResult helper properties."""
    
    def test_is_positive(self, detector, ctx_positive_text):
        """is_positive should be True for post_contrast=1."""
        result = detector.detect_contrast(ctx_positive_text)
        
        assert result.is_positive is True
        assert result.is_negative is False
        assert result.is_unknown is False
    
    def test_is_negative(self, detector, ctx_negative_text):
        """is_negative should be True for post_contrast=0."""
        result = detector.detect_contrast(ctx_negative_text)
        
        assert result.is_positive is False
        assert result.is_negative is True
        assert result.is_unknown is False
    
    def test_is_unknown(self, detector, ctx_no_contrast):
        """is_unknown should be True for post_contrast=None."""
        result = detector.detect_contrast(ctx_no_contrast)
        
        assert result.is_positive is False
        assert result.is_negative is False
        assert result.is_unknown is True
    
    def test_value_alias(self, detector, ctx_positive_text):
        """value property should alias post_contrast."""
        result = detector.detect_contrast(ctx_positive_text)
        
        assert result.value == result.post_contrast
    
    def test_to_axis_result_positive(self, detector, ctx_positive_text):
        """to_axis_result should return correct value for positive."""
        result = detector.detect_contrast(ctx_positive_text)
        axis_result = result.to_axis_result()
        
        assert axis_result.value == "post_contrast"
        assert axis_result.confidence == 0.80
    
    def test_to_axis_result_negative(self, detector, ctx_negative_text):
        """to_axis_result should return correct value for negative."""
        result = detector.detect_contrast(ctx_negative_text)
        axis_result = result.to_axis_result()
        
        assert axis_result.value == "pre_contrast"
    
    def test_to_axis_result_unknown(self, detector, ctx_no_contrast):
        """to_axis_result should return correct value for unknown."""
        result = detector.detect_contrast(ctx_no_contrast)
        axis_result = result.to_axis_result()
        
        assert axis_result.value == "unknown"


# =============================================================================
# Test: Convenience Methods
# =============================================================================

class TestConvenienceMethods:
    """Tests for detector convenience methods."""
    
    def test_get_negative_keywords(self, detector):
        """get_negative_keywords should return list."""
        keywords = detector.get_negative_keywords()
        
        assert isinstance(keywords, list)
        assert len(keywords) > 0
        assert "utan gd" in keywords
        assert "pre-contrast" in keywords
    
    def test_get_positive_keywords(self, detector):
        """get_positive_keywords should return list."""
        keywords = detector.get_positive_keywords()
        
        assert isinstance(keywords, list)
        assert len(keywords) > 0
        assert "dotarem" in keywords
        assert "+gd" in keywords
    
    def test_explain_detection(self, detector, ctx_positive_text):
        """explain_detection should return detailed dict."""
        explanation = detector.explain_detection(ctx_positive_text)
        
        assert "post_contrast" in explanation
        assert "confidence" in explanation
        assert "detection_method" in explanation
        assert "matched_keyword" in explanation
        assert explanation["post_contrast"] == 1
    
    def test_debug_text_matching(self, detector):
        """debug_text_matching should show matched keywords."""
        debug = detector.debug_text_matching("ax t1 utan gd +k brain")
        
        assert "utan gd" in debug["matched_negative_keywords"]
        assert "+k" in debug["matched_positive_keywords"]
        assert debug["detection_result"].startswith("pre_contrast")


# =============================================================================
# Test: Edge Cases
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and special scenarios."""
    
    def test_case_insensitive(self, detector):
        """Detection should be case-insensitive."""
        ctx = ClassificationContext(
            text_search_blob="AX T1 POST GD BRAIN",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 1
    
    def test_minus_k_with_space(self, detector):
        """' -k' (with space) should trigger negative."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 -k brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 0
        assert " -k" in result.matched_keyword
    
    def test_med_kontrast_hyphenated(self, detector):
        """'med-kontrast' (hyphenated Swedish) should trigger positive."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 med-kontrast brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 1
    
    def test_underscore_variant_utan_gd(self, detector):
        """'utan_gd' (underscore) should trigger negative."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 utan_gd brain",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 0
    
    def test_long_text_blob(self, detector):
        """Detection should work with long text."""
        ctx = ClassificationContext(
            text_search_blob="this is a very long description with lots of words " * 10 + "dotarem",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 1
    
    def test_special_chars_in_text(self, detector):
        """Detection should handle special characters."""
        ctx = ClassificationContext(
            text_search_blob="ax_t1_+gd_brain_3d",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 1


# =============================================================================
# Test: Real-world Examples
# =============================================================================

class TestRealWorldExamples:
    """Tests based on real-world series descriptions."""
    
    def test_swedish_clinical_positive(self, detector):
        """Real Swedish clinical positive example."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 mprage +gd hjärna 3d",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 1
    
    def test_swedish_clinical_negative(self, detector):
        """Real Swedish clinical negative example."""
        ctx = ClassificationContext(
            text_search_blob="ax t1 mprage utan gd hjärna 3d",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 0
    
    def test_english_clinical_post(self, detector):
        """Real English clinical post-contrast example."""
        ctx = ClassificationContext(
            text_search_blob="brain ax t1 post contrast 3d mprage",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 1
    
    def test_english_clinical_pre(self, detector):
        """Real English clinical pre-contrast example."""
        ctx = ClassificationContext(
            text_search_blob="brain ax t1 pre gd 3d mprage",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 0
    
    def test_german_clinical_mit_km(self, detector):
        """Real German clinical example with KM."""
        # Note: mit km not in keywords by default - testing mit gd
        ctx = ClassificationContext(
            text_search_blob="kopf ax t1 mit gd 3d mprage",
        )
        result = detector.detect_contrast(ctx)
        
        assert result.post_contrast == 1
