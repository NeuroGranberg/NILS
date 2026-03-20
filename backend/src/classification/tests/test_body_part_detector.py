"""
Tests for Body Part Detector (Multi-Category, YAML-Driven)

Tests the multi-category body part detection system:
  - Detects body part categories (brain, spine, neck, brain-neck) from text_search_blob
  - Resolution: spine > brain-neck > neck > brain > unknown
  - brain-neck is assigned when both brain AND neck keywords appear
  - spine always wins over everything
  - Heuristic removed (keyword-only detection)

Version: 3.1.0
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
    return BodyPartDetector()


@pytest.fixture
def ctx_spine_english():
    return ClassificationContext(
        text_search_blob="ax t1 thoracic spine mprage",
    )


@pytest.fixture
def ctx_neck_english():
    return ClassificationContext(
        text_search_blob="ax t1 neck scan protocol",
    )


@pytest.fixture
def ctx_brain_only():
    return ClassificationContext(
        text_search_blob="ax t1 brain flair 3d mprage",
    )


@pytest.fixture
def ctx_empty():
    return ClassificationContext()


# =============================================================================
# Test: Spine Detection
# =============================================================================

class TestSpineDetection:

    def test_spine_keyword(self, detector, ctx_spine_english):
        result = detector.detect_body_part(ctx_spine_english)
        assert result.body_part == "spine"
        assert result.spinal_cord == 1

    def test_thoracic_keyword(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 thoracic cord")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_lumbar_keyword(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 lumbar spine")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_spinal_cord_keyword(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 spinal cord lesion")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_vertebral_keyword(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 vertebral artery")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_t_spine_notation(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 t-spine")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_l_spine_notation(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 l-spine")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"


# =============================================================================
# Test: Neck Detection
# =============================================================================

class TestNeckDetection:

    def test_neck_keyword(self, detector, ctx_neck_english):
        result = detector.detect_body_part(ctx_neck_english)
        assert result.body_part == "neck"
        assert result.spinal_cord == 1
        assert result.is_neck is True

    def test_nacke_keyword(self, detector):
        """Swedish 'nacke' -> neck."""
        ctx = ClassificationContext(text_search_blob="ax t1 nacke protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "neck"

    def test_hals_keyword(self, detector):
        """Swedish/German/Dutch 'hals' -> neck."""
        ctx = ClassificationContext(text_search_blob="ax t1 hals protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "neck"

    def test_nek_keyword(self, detector):
        """Dutch 'nek' -> neck."""
        ctx = ClassificationContext(text_search_blob="ax t1 nek protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "neck"


# =============================================================================
# Test: Brain-Neck Combined Detection
# =============================================================================

class TestBrainNeckDetection:

    def test_neck_plus_brain_yields_brain_neck(self, detector):
        """Both neck + brain keywords -> brain-neck (combined scan)."""
        ctx = ClassificationContext(text_search_blob="ax t1 neck brain protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "brain-neck"
        assert result.is_brain_neck is True
        assert result.has_conflict is False

    def test_hals_plus_kopf_yields_brain_neck(self, detector):
        """Swedish/German: hals (neck) + kopf (brain) -> brain-neck."""
        ctx = ClassificationContext(text_search_blob="ax t1 hals kopf protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "brain-neck"

    def test_nacke_plus_hjarna_yields_brain_neck(self, detector):
        """Swedish: nacke (neck) + hjarna (brain) -> brain-neck."""
        ctx = ClassificationContext(text_search_blob="ax t1 nacke hjarna protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "brain-neck"

    def test_brain_neck_spinal_cord_is_0(self, detector):
        """brain-neck backward compat: spinal_cord=0 (it is a brain scan)."""
        ctx = ClassificationContext(text_search_blob="ax t1 neck brain protocol")
        result = detector.detect_body_part(ctx)
        assert result.spinal_cord == 0

    def test_brain_neck_triggers_review(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 neck brain protocol")
        result = detector.detect_body_part(ctx)
        assert result.triggers_review is True

    def test_brain_neck_has_two_evidence(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 neck brain protocol")
        result = detector.detect_body_part(ctx)
        assert len(result.evidence) == 2
        targets = {e.target for e in result.evidence}
        assert "neck" in targets
        assert "brain" in targets

    def test_cou_plus_cerveau_yields_brain_neck(self, detector):
        """French: cou (neck) + cerveau (brain) -> brain-neck."""
        ctx = ClassificationContext(text_search_blob="ax t1 cou cerveau protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "brain-neck"


# =============================================================================
# Test: Resolution Priority
# =============================================================================

class TestResolutionPriority:

    def test_spine_beats_brain(self, detector):
        """spine + brain -> spine (spine always wins)."""
        ctx = ClassificationContext(text_search_blob="ax t1 spine brain protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"
        assert result.has_conflict is True

    def test_spine_beats_neck(self, detector):
        """spine + neck -> spine (spine always wins)."""
        ctx = ClassificationContext(text_search_blob="ax t1 spine neck protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_spine_beats_neck_and_brain(self, detector):
        """spine + neck + brain -> spine."""
        ctx = ClassificationContext(text_search_blob="ax t1 spine neck brain protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_brain_neck_beats_neck_alone(self, detector):
        """neck + brain -> brain-neck beats plain neck."""
        ctx = ClassificationContext(text_search_blob="ax t1 neck brain protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "brain-neck"

    def test_brain_neck_beats_brain_alone(self, detector):
        """neck + brain -> brain-neck beats plain brain."""
        ctx = ClassificationContext(text_search_blob="ax t1 neck brain protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part != "brain"
        assert result.body_part == "brain-neck"

    def test_neck_only_stays_neck(self, detector):
        """neck without brain -> neck (not brain-neck)."""
        ctx = ClassificationContext(text_search_blob="ax t1 neck protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "neck"

    def test_brain_only_stays_brain(self, detector):
        """brain without neck -> brain (not brain-neck)."""
        ctx = ClassificationContext(text_search_blob="ax t1 brain protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "brain"


# =============================================================================
# Test: Neck vs Spine Priority
# =============================================================================

class TestNeckSpinePriority:

    def test_cervical_matches_spine_not_neck(self, detector):
        """'cervical' is a spine keyword."""
        ctx = ClassificationContext(text_search_blob="ax t1 cervical")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_c_spine_is_spine(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 c-spine")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_hws_is_spine(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 hws")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"


# =============================================================================
# Test: Swedish Keywords
# =============================================================================

class TestSwedishKeywords:

    def test_halsrygg_matches_spine(self, detector):
        """'halsrygg' contains 'rygg' -> spine match and 'hals' -> neck match.
        Spine always wins when both match."""
        ctx = ClassificationContext(text_search_blob="ax t1 halsrygg")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_rygg_keyword(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 rygg protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_nacke_keyword(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 nacke protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "neck"


# =============================================================================
# Test: German Keywords
# =============================================================================

class TestGermanKeywords:

    def test_hws_keyword(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 hws kopf")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_bws_keyword(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 bws")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_wirbel_keyword(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 wirbel kopf")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"


# =============================================================================
# Test: French Keywords
# =============================================================================

class TestFrenchKeywords:

    def test_rachis_keyword(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 rachis cerveau")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_moelle_keyword(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 moelle cerveau")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_cou_keyword(self, detector):
        """French 'cou' -> neck (no brain keyword)."""
        ctx = ClassificationContext(text_search_blob="ax t1 cou protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "neck"


# =============================================================================
# Test: Pathology Keywords
# =============================================================================

class TestPathologyKeywords:

    def test_myelitis(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 myelitis")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_myelopathy(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 myelopathy")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_syringomyelia(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 syringomyelia")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"


# =============================================================================
# Test: Brain Detection
# =============================================================================

class TestBrainDetection:

    def test_brain_keyword(self, detector, ctx_brain_only):
        result = detector.detect_body_part(ctx_brain_only)
        assert result.body_part == "brain"
        assert result.spinal_cord == 0
        assert result.is_brain is True

    def test_fmri_keyword(self, detector):
        ctx = ClassificationContext(text_search_blob="ax bold fmri task protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "brain"

    def test_hippocampus_keyword(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 hippocampus high-res")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "brain"

    def test_brain_does_not_trigger_review(self, detector, ctx_brain_only):
        result = detector.detect_body_part(ctx_brain_only)
        assert result.triggers_review is False


# =============================================================================
# Test: Unknown / Empty
# =============================================================================

class TestUnknownEmpty:

    def test_empty_context(self, detector, ctx_empty):
        result = detector.detect_body_part(ctx_empty)
        assert result.body_part is None
        assert result.spinal_cord is None
        assert result.is_unknown is True

    def test_no_text_blob(self, detector):
        ctx = ClassificationContext(image_type=r"ORIGINAL\PRIMARY\M\NORM")
        result = detector.detect_body_part(ctx)
        assert result.body_part is None

    def test_no_matching_keywords(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 random sequence protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part is None
        assert result.detection_method in ("default", "unknown")


# =============================================================================
# Test: Conflict Detection (spine + brain)
# =============================================================================

class TestConflictDetection:

    def test_spine_and_brain_conflict(self, detector):
        """Spine keyword + brain keyword -> spine wins, conflict flagged."""
        ctx = ClassificationContext(text_search_blob="ax t1 spine brain protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"
        assert result.has_conflict is True
        assert result.triggers_review is True

    def test_neck_and_brain_is_brain_neck_not_conflict(self, detector):
        """Neck keyword + brain keyword -> brain-neck (NOT a conflict)."""
        ctx = ClassificationContext(text_search_blob="ax t1 neck brain protocol")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "brain-neck"
        assert result.has_conflict is False


# =============================================================================
# Test: Review Triggering
# =============================================================================

class TestReviewTriggering:

    def test_spine_triggers_review(self, detector, ctx_spine_english):
        result = detector.detect_body_part(ctx_spine_english)
        assert result.triggers_review is True

    def test_neck_triggers_review(self, detector, ctx_neck_english):
        result = detector.detect_body_part(ctx_neck_english)
        assert result.triggers_review is True

    def test_brain_neck_triggers_review(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 neck brain protocol")
        result = detector.detect_body_part(ctx)
        assert result.triggers_review is True

    def test_brain_no_review(self, detector, ctx_brain_only):
        result = detector.detect_body_part(ctx_brain_only)
        assert result.triggers_review is False

    def test_unknown_no_review(self, detector):
        ctx = ClassificationContext(text_search_blob="random protocol")
        result = detector.detect_body_part(ctx)
        assert result.triggers_review is False

    def test_get_review_reason_spine(self, detector):
        assert detector.get_review_reason("spine") == "bodypart:spine_detected"

    def test_get_review_reason_neck(self, detector):
        assert detector.get_review_reason("neck") == "bodypart:neck_detected"

    def test_get_review_reason_brain_neck(self, detector):
        assert detector.get_review_reason("brain-neck") == "bodypart:brain_neck_detected"


# =============================================================================
# Test: Backward Compatibility (spinal_cord property)
# =============================================================================

class TestBackwardCompat:

    def test_spine_spinal_cord_is_1(self, detector, ctx_spine_english):
        result = detector.detect_body_part(ctx_spine_english)
        assert result.spinal_cord == 1

    def test_neck_spinal_cord_is_1(self, detector, ctx_neck_english):
        result = detector.detect_body_part(ctx_neck_english)
        assert result.spinal_cord == 1

    def test_brain_neck_spinal_cord_is_0(self, detector):
        """brain-neck is a brain scan, spinal_cord=0."""
        ctx = ClassificationContext(text_search_blob="ax t1 neck brain protocol")
        result = detector.detect_body_part(ctx)
        assert result.spinal_cord == 0

    def test_brain_spinal_cord_is_0(self, detector, ctx_brain_only):
        result = detector.detect_body_part(ctx_brain_only)
        assert result.spinal_cord == 0

    def test_unknown_spinal_cord_is_none(self, detector, ctx_empty):
        result = detector.detect_body_part(ctx_empty)
        assert result.spinal_cord is None


# =============================================================================
# Test: BodyPartResult Properties
# =============================================================================

class TestBodyPartResultProperties:

    def test_is_spine(self, detector, ctx_spine_english):
        result = detector.detect_body_part(ctx_spine_english)
        assert result.is_spine is True
        assert result.is_neck is False
        assert result.is_brain is False
        assert result.is_brain_neck is False
        assert result.is_unknown is False

    def test_is_neck(self, detector, ctx_neck_english):
        result = detector.detect_body_part(ctx_neck_english)
        assert result.is_spine is False
        assert result.is_neck is True
        assert result.is_brain is False
        assert result.is_brain_neck is False

    def test_is_brain(self, detector, ctx_brain_only):
        result = detector.detect_body_part(ctx_brain_only)
        assert result.is_spine is False
        assert result.is_neck is False
        assert result.is_brain is True
        assert result.is_brain_neck is False

    def test_is_brain_neck(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 neck brain protocol")
        result = detector.detect_body_part(ctx)
        assert result.is_brain_neck is True
        assert result.is_spine is False
        assert result.is_neck is False
        assert result.is_brain is False

    def test_value_alias(self, detector, ctx_spine_english):
        result = detector.detect_body_part(ctx_spine_english)
        assert result.value == result.body_part

    def test_to_axis_result_spine(self, detector, ctx_spine_english):
        result = detector.detect_body_part(ctx_spine_english)
        axis_result = result.to_axis_result()
        assert axis_result.value == "spine"

    def test_to_axis_result_neck(self, detector, ctx_neck_english):
        result = detector.detect_body_part(ctx_neck_english)
        axis_result = result.to_axis_result()
        assert axis_result.value == "neck"

    def test_to_axis_result_brain(self, detector, ctx_brain_only):
        result = detector.detect_body_part(ctx_brain_only)
        axis_result = result.to_axis_result()
        assert axis_result.value == "brain"

    def test_to_axis_result_brain_neck(self, detector):
        ctx = ClassificationContext(text_search_blob="ax t1 neck brain protocol")
        result = detector.detect_body_part(ctx)
        axis_result = result.to_axis_result()
        assert axis_result.value == "brain-neck"

    def test_to_axis_result_unknown(self, detector, ctx_empty):
        result = detector.detect_body_part(ctx_empty)
        axis_result = result.to_axis_result()
        assert axis_result.value == "unknown"


# =============================================================================
# Test: Confidence
# =============================================================================

class TestConfidence:

    def test_spine_confidence(self, detector, ctx_spine_english):
        result = detector.detect_body_part(ctx_spine_english)
        assert result.confidence == 0.65

    def test_unknown_zero_confidence(self, detector, ctx_empty):
        result = detector.detect_body_part(ctx_empty)
        assert result.confidence == 0.0


# =============================================================================
# Test: Convenience Methods
# =============================================================================

class TestConvenienceMethods:

    def test_categories_list(self, detector):
        cats = detector.categories
        assert "neck" in cats
        assert "spine" in cats
        assert "brain" in cats
        assert "brain-neck" in cats

    def test_get_category_keywords(self, detector):
        spine_kws = detector.get_category_keywords("spine")
        assert "spine" in spine_kws
        assert "thoracic" in spine_kws

        neck_kws = detector.get_category_keywords("neck")
        assert "neck" in neck_kws or "nacke" in neck_kws

    def test_brain_neck_has_no_keywords(self, detector):
        """brain-neck is synthetic: no keywords of its own."""
        kws = detector.get_category_keywords("brain-neck")
        assert kws == []

    def test_explain_detection(self, detector, ctx_spine_english):
        explanation = detector.explain_detection(ctx_spine_english)
        assert explanation["body_part"] == "spine"
        assert explanation["matched_category"] == "spine"
        assert "evidence" in explanation

    def test_debug_text_matching(self, detector):
        debug = detector.debug_text_matching("ax t1 cervical spine brain")
        assert "spine" in debug["category_matches"]
        assert "brain" in debug["category_matches"]
        assert debug["would_trigger_review"] is True

    def test_debug_brain_neck(self, detector):
        debug = detector.debug_text_matching("ax t1 neck brain protocol")
        assert "neck" in debug["category_matches"]
        assert "brain" in debug["category_matches"]
        assert "brain-neck" in debug["detection_result"]
        assert debug["would_trigger_review"] is True


# =============================================================================
# Test: Edge Cases
# =============================================================================

class TestEdgeCases:

    def test_case_insensitive(self, detector):
        ctx = ClassificationContext(text_search_blob="AX T1 CERVICAL SPINE")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_multiple_keywords(self, detector):
        ctx = ClassificationContext(
            text_search_blob="ax t1 cervical spine thoracic vertebral"
        )
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_long_text_blob(self, detector):
        ctx = ClassificationContext(
            text_search_blob="lots of words " * 50 + "spine"
        )
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"

    def test_spine_in_compound_word(self, detector):
        ctx = ClassificationContext(text_search_blob="brain_spine_combined")
        result = detector.detect_body_part(ctx)
        assert result.body_part == "spine"


# =============================================================================
# Test: Evidence
# =============================================================================

class TestEvidence:

    def test_spine_has_evidence(self, detector, ctx_spine_english):
        result = detector.detect_body_part(ctx_spine_english)
        assert len(result.evidence) >= 1
        assert result.evidence[0].target == "spine"

    def test_brain_has_evidence(self, detector, ctx_brain_only):
        result = detector.detect_body_part(ctx_brain_only)
        assert len(result.evidence) >= 1
        assert result.evidence[0].target == "brain"

    def test_unknown_no_evidence(self, detector, ctx_empty):
        result = detector.detect_body_part(ctx_empty)
        assert len(result.evidence) == 0

    def test_conflict_has_multiple_evidence(self, detector):
        """Spine + brain conflict produces 2 evidence entries."""
        ctx = ClassificationContext(text_search_blob="ax t1 spine brain")
        result = detector.detect_body_part(ctx)
        assert len(result.evidence) == 2

    def test_brain_neck_has_two_evidence(self, detector):
        """Brain-neck produces evidence for both neck and brain."""
        ctx = ClassificationContext(text_search_blob="ax t1 neck brain")
        result = detector.detect_body_part(ctx)
        assert len(result.evidence) == 2
        targets = {e.target for e in result.evidence}
        assert targets == {"neck", "brain"}
