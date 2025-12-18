"""
Body Part Detector

Detects if imaging includes spinal cord for classification branching.

This detector focuses specifically on spinal cord detection because:
1. Spinal cord imaging requires different classification considerations
2. Spine series mixed with brain series need to be flagged for review
3. Some protocols scan both brain and spine in the same study

Detection is TEXT-BASED only (lower confidence) since there's no structured
DICOM tag that reliably indicates spinal cord coverage.

Output: Three-state value (1=spine detected, 0=brain only, None=unknown)

Note: This detector always flags spine detection for manual review since
text-based detection is prone to errors.

Version: 1.0.0
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..core.context import ClassificationContext
from ..core.evidence import (
    AxisResult,
    Evidence,
    EvidenceSource,
)
from .base_detector import BaseDetector


@dataclass
class BodyPartResult:
    """
    Result of body part detection.

    Attributes:
        spinal_cord: Three-state value (1=spine detected, 0=brain only, None=unknown)
        confidence: Detection confidence (always low for text-based detection)
        detection_method: How it was detected
            - "text_positive": Spine keyword found in text_search_blob
            - "text_brain_only": Brain-only indicators, no spine hints
            - "unknown": No detection possible
            - "heuristic": Geometry suggests possible spine scan
        matched_keyword: The keyword that matched (for text detection)
        triggers_review: Whether this detection should trigger manual review
        evidence: List of evidence that contributed to detection
        has_conflict: Whether heuristic detected possible spine scan
        conflict_reason: Reason for heuristic flag (e.g., elongated FOV + simple technique)
    """
    spinal_cord: Optional[int]  # 1=spine, 0=brain only, None=unknown
    confidence: float
    detection_method: str
    matched_keyword: Optional[str] = None
    triggers_review: bool = False
    evidence: List[Evidence] = field(default_factory=list)
    has_conflict: bool = False
    conflict_reason: Optional[str] = None

    @property
    def value(self) -> Optional[int]:
        """Alias for spinal_cord (consistency with other detectors)."""
        return self.spinal_cord

    @property
    def is_spine(self) -> bool:
        """Check if spine was detected."""
        return self.spinal_cord == 1

    @property
    def is_brain_only(self) -> bool:
        """Check if brain-only was detected."""
        return self.spinal_cord == 0

    @property
    def is_unknown(self) -> bool:
        """Check if body part is unknown."""
        return self.spinal_cord is None

    def to_axis_result(self) -> AxisResult:
        """Convert to AxisResult for integration with ClassificationResult."""
        if self.spinal_cord == 1:
            value_str = "spinal_cord"
        elif self.spinal_cord == 0:
            value_str = "brain_only"
        else:
            value_str = "unknown"

        return AxisResult(
            value=value_str,
            confidence=self.confidence,
            evidence=self.evidence,
            alternatives=[],
            has_conflict=self.has_conflict,
            conflict_target=self.conflict_reason,
        )


class BodyPartDetector(BaseDetector):
    """
    Detect if imaging includes spinal cord.

    This is a focused detector for one specific task: identifying series
    that include spinal cord imaging. This is important because:

    1. Spine sequences need different classification handling
    2. Mixed brain+spine studies need flagging
    3. Spine-only studies may be misclassified without this check

    Detection is TEXT-BASED only:
    - Searches text_search_blob for spine-related keywords
    - Multilingual support (English, Swedish, German, French)
    - LOW confidence because text-based detection is error-prone
    - Always triggers review when spine is detected

    Detection Logic:
    1. IF spine keyword matches → spinal_cord = 1, trigger review
    2. ELSE → spinal_cord = None (unknown, NOT "brain only")

    Heuristic Check:
    After text detection, checks geometry + technique for possible spine scan:
    - Elongated FOV (aspect ratio < 0.8 or > 1.25)
    - Simple technique (SE, TSE, GRE, FLASH, etc.)
    - Low slice count (< 80)
    If all conditions met → flag for review (possible spine scan).

    Note: We do NOT try to detect "brain only" since absence of spine
    keywords doesn't reliably mean the image is brain-only.
    """

    YAML_FILENAME = "body_part-detection.yaml"

    # Confidence thresholds - LOW because text-based detection is unreliable
    CONFIDENCE_THRESHOLDS = {
        "text_positive": 0.65,   # Spine keyword found (still low confidence)
        "unknown": 0.0,          # No detection possible
    }

    # Simple techniques that could indicate spine imaging
    # Complex techniques (DWI-EPI, ASL, BOLD, etc.) are unlikely to be spine
    SIMPLE_TECHNIQUES = {
        "SE", "TSE", "FSE", "GRE", "FLASH", "MEDIC",
        "VIBE", "FIESTA", "bSSFP", "TRUFI", "MPRAGE",
        "HASTE", "BLADE",
    }
    
    def __init__(self, yaml_dir: Optional[Path] = None):
        """
        Initialize body part detector.

        Args:
            yaml_dir: Directory containing detection YAML files.
                     If None, uses default location.
        """
        yaml_path = None
        if yaml_dir:
            yaml_path = Path(yaml_dir) / self.YAML_FILENAME
        else:
            yaml_path = Path(__file__).parent.parent / "detection_yaml" / self.YAML_FILENAME

        super().__init__(yaml_path)

        # Load keywords from config
        self._positive_keywords: List[str] = self.config.get("positive_keywords", [])
        self._brain_only_keywords: List[str] = self.config.get("brain_only_keywords", [])

        # Detection rules
        detection_config = self.config.get("detection", {})
        self._spine_triggers_review = detection_config.get("spine_triggers_review", True)
        self._review_reason = detection_config.get("review_reason", "bodypart:spine_detected")
        self._conflict_triggers_review = detection_config.get("conflict_triggers_review", True)
        self._conflict_review_reason = detection_config.get("conflict_review_reason", "bodypart:spine_brain_conflict")
    
    @property
    def axis_name(self) -> str:
        return "body_part"
    
    def detect(self, ctx: ClassificationContext) -> AxisResult:
        """
        Detect body part (spinal cord focus).
        
        Args:
            ctx: Classification context with fingerprint data
        
        Returns:
            AxisResult with body part status, confidence, and evidence
        """
        result = self.detect_body_part(ctx)
        return result.to_axis_result()
    
    def detect_body_part(
        self,
        ctx: ClassificationContext,
        technique: Optional[str] = None,
        aspect_ratio: Optional[float] = None,
        slices_count: Optional[int] = None
    ) -> BodyPartResult:
        """
        Detect if imaging includes spinal cord.

        Detection priority:
        1. If spine AND brain keywords both match → conflict (flag for review)
        2. If only spine keyword matches → spinal_cord = 1
        3. If only brain keyword matches → spinal_cord = 0 (brain only)
        4. No match → spinal_cord = None (unknown)

        After detection, applies heuristic check for possible spine scan
        based on geometry + technique (elongated FOV + simple technique + low slices).

        Args:
            ctx: Classification context
            technique: Detected technique name (for heuristic check)
            aspect_ratio: FOV aspect ratio (for heuristic check)
            slices_count: Number of slices (for heuristic check)

        Returns:
            BodyPartResult with detection details
        """
        text_blob = ctx.text_search_blob

        if not text_blob:
            # No text to search - unknown, but still check heuristic
            result = BodyPartResult(
                spinal_cord=None,
                confidence=self.CONFIDENCE_THRESHOLDS["unknown"],
                detection_method="unknown",
                matched_keyword=None,
                triggers_review=False,
                evidence=[],
            )
            # Check heuristic even without text
            return self._apply_spine_heuristic(result, technique, aspect_ratio, slices_count)

        # Normalize for matching
        text_lower = text_blob.lower()

        # Check for both spine and brain keywords
        spine_match = self._match_keywords(text_lower, self._positive_keywords)
        brain_match = self._match_keywords(text_lower, self._brain_only_keywords)

        # Case 1: Both spine AND brain keywords match → conflict
        # This often happens when body_part_examined is incorrectly set to SPINE
        # but the series_description indicates brain imaging (e.g., fMRI, BOLD)
        if spine_match and brain_match:
            return BodyPartResult(
                spinal_cord=None,  # Unknown due to conflict
                confidence=self.CONFIDENCE_THRESHOLDS["text_positive"],
                detection_method="conflict",
                matched_keyword=spine_match,
                triggers_review=self._conflict_triggers_review,
                evidence=[
                    Evidence(
                        source=EvidenceSource.TEXT_SEARCH,
                        field="text_search_blob",
                        value=spine_match,
                        target="spinal_cord",
                        weight=0.5,  # Lower weight due to conflict
                        description=f"Spine keyword '{spine_match}' found, but brain keyword '{brain_match}' also found",
                    ),
                    Evidence(
                        source=EvidenceSource.TEXT_SEARCH,
                        field="text_search_blob",
                        value=brain_match,
                        target="brain_only",
                        weight=0.5,
                        description=f"Brain keyword '{brain_match}' suggests brain imaging",
                    ),
                ],
                has_conflict=True,
                conflict_reason=f"spine:'{spine_match}' vs brain:'{brain_match}'",
            )

        # Case 2: Only spine keyword matches → spinal_cord = 1
        if spine_match:
            return BodyPartResult(
                spinal_cord=1,
                confidence=self.CONFIDENCE_THRESHOLDS["text_positive"],
                detection_method="text_positive",
                matched_keyword=spine_match,
                triggers_review=self._spine_triggers_review,
                evidence=[Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=spine_match,
                    target="spinal_cord",
                    weight=self.CONFIDENCE_THRESHOLDS["text_positive"],
                    description=f"Spine keyword '{spine_match}' found → spinal cord imaging",
                )],
            )

        # Case 3: Only brain keyword matches → brain only (spinal_cord = 0)
        if brain_match:
            return BodyPartResult(
                spinal_cord=0,
                confidence=self.CONFIDENCE_THRESHOLDS["text_positive"],
                detection_method="text_brain_only",
                matched_keyword=brain_match,
                triggers_review=False,  # Brain-only doesn't need review
                evidence=[Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=brain_match,
                    target="brain_only",
                    weight=self.CONFIDENCE_THRESHOLDS["text_positive"],
                    description=f"Brain keyword '{brain_match}' found → brain-only imaging",
                )],
            )

        # Case 4: No keywords matched - unknown
        # Apply heuristic check for possible spine scan
        result = BodyPartResult(
            spinal_cord=None,
            confidence=self.CONFIDENCE_THRESHOLDS["unknown"],
            detection_method="unknown",
            matched_keyword=None,
            triggers_review=False,
            evidence=[],
        )
        return self._apply_spine_heuristic(result, technique, aspect_ratio, slices_count)

    def _apply_spine_heuristic(
        self,
        result: BodyPartResult,
        technique: Optional[str],
        aspect_ratio: Optional[float],
        slices_count: Optional[int]
    ) -> BodyPartResult:
        """
        Apply spine heuristic check and update result if suspicious.

        If heuristic detects possible spine scan, sets has_conflict=True
        and triggers_review=True.

        Args:
            result: The detection result to update
            technique: Detected technique name
            aspect_ratio: FOV aspect ratio
            slices_count: Number of slices

        Returns:
            Updated BodyPartResult with heuristic info if triggered
        """
        heuristic_reason = self._check_spine_heuristic(
            technique, aspect_ratio, slices_count
        )

        if heuristic_reason:
            result.has_conflict = True
            result.conflict_reason = heuristic_reason
            result.triggers_review = True

        return result

    def _check_spine_heuristic(
        self,
        technique: Optional[str],
        aspect_ratio: Optional[float],
        slices_count: Optional[int]
    ) -> Optional[str]:
        """
        Check if geometry + technique suggests possible spine scan.

        Heuristic conditions (all must be met):
        1. Aspect ratio < 0.8 OR > 1.25 (elongated/non-square FOV)
        2. Technique is in SIMPLE_TECHNIQUES set
        3. Slice count < 80

        Args:
            technique: Detected technique name
            aspect_ratio: FOV aspect ratio (fov_x / fov_y)
            slices_count: Number of slices in stack

        Returns:
            Reason string if suspicious, None otherwise
        """
        if technique is None or aspect_ratio is None or slices_count is None:
            return None

        # Check aspect ratio (non-square FOV)
        is_elongated = aspect_ratio < 0.8 or aspect_ratio > 1.25

        # Check technique is simple (not complex like DWI-EPI, BOLD-EPI, etc.)
        is_simple_technique = technique in self.SIMPLE_TECHNIQUES

        # Check slice count (spine scans typically have fewer slices)
        is_low_slices = slices_count < 80

        if is_elongated and is_simple_technique and is_low_slices:
            return f"heuristic:elongated_fov({aspect_ratio:.2f})+simple_technique({technique})+low_slices({slices_count})"

        return None

    def _match_keywords(
        self,
        text: str,
        keywords: List[str]
    ) -> Optional[str]:
        """
        Match keywords against text.
        
        Args:
            text: Lowercase text to search
            keywords: List of keywords to match
        
        Returns:
            First matched keyword, or None if no match
        """
        for keyword in keywords:
            # Keywords in YAML are already lowercase
            if keyword in text:
                return keyword
        return None
    
    # =========================================================================
    # Convenience Methods
    # =========================================================================
    
    def get_spine_keywords(self) -> List[str]:
        """Get list of spine-related keywords."""
        return self._positive_keywords.copy()

    def get_brain_keywords(self) -> List[str]:
        """Get list of brain-specific keywords."""
        return self._brain_only_keywords.copy()

    def get_review_reason(self) -> str:
        """Get the review reason code for spine detection."""
        return self._review_reason

    def get_conflict_review_reason(self) -> str:
        """Get the review reason code for spine/brain conflict."""
        return self._conflict_review_reason
    
    def explain_detection(self, ctx: ClassificationContext) -> Dict[str, Any]:
        """
        Generate detailed explanation of body part detection.
        
        Args:
            ctx: Classification context
        
        Returns:
            Dict with detection explanation
        """
        result = self.detect_body_part(ctx)
        
        explanation = {
            "spinal_cord": result.spinal_cord,
            "confidence": result.confidence,
            "detection_method": result.detection_method,
            "matched_keyword": result.matched_keyword,
            "triggers_review": result.triggers_review,
            "text_blob_preview": ctx.text_search_blob[:200] if ctx.text_search_blob else None,
            "evidence": [
                {
                    "source": e.source.value,
                    "field": e.field,
                    "value": e.value,
                    "weight": e.weight,
                    "description": e.description,
                }
                for e in result.evidence
            ],
        }
        
        return explanation
    
    def debug_text_matching(self, text: str) -> Dict[str, Any]:
        """
        Debug which keywords would match in given text.

        Useful for testing and understanding detection.

        Args:
            text: Text to check

        Returns:
            Dict with matching results
        """
        text_lower = text.lower()

        matched_spine = [kw for kw in self._positive_keywords if kw in text_lower]
        matched_brain = [kw for kw in self._brain_only_keywords if kw in text_lower]

        # Determine what would be detected
        if matched_spine and matched_brain:
            detection = f"conflict (spine:'{matched_spine[0]}' vs brain:'{matched_brain[0]}')"
            would_trigger_review = True
        elif matched_spine:
            detection = f"spinal_cord (matched: {matched_spine[0]})"
            would_trigger_review = True
        elif matched_brain:
            detection = f"brain_only (matched: {matched_brain[0]})"
            would_trigger_review = False
        else:
            detection = "unknown (no keywords matched)"
            would_trigger_review = False

        return {
            "input_text": text,
            "normalized_text": text_lower,
            "matched_spine_keywords": matched_spine,
            "matched_brain_keywords": matched_brain,
            "detection_result": detection,
            "would_trigger_review": would_trigger_review,
        }
