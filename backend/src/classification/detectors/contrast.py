"""
Contrast Agent Detector

Detects contrast agent administration status (pre/post contrast).

Detection uses a two-tier approach:
1. Structured source (HIGH confidence): contrast_search_blob from DICOM tags
   - If NOT NULL → post_contrast = 1 (contrast was administered)
   
2. Text source (MEDIUM confidence): text_search_blob pattern matching
   - NEGATIVE keywords checked FIRST → post_contrast = 0
   - POSITIVE keywords checked second → post_contrast = 1
   - No match → post_contrast = NULL (unknown)

Output: Three-state value (1=yes, 0=no, None=unknown)

Multilingual support: Swedish, English, German, French, Italian, Norwegian, Danish, Dutch

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
class ContrastResult:
    """
    Result of contrast agent detection.
    
    Attributes:
        post_contrast: Three-state value (1=yes, 0=no, None=unknown)
        confidence: Detection confidence (0.0 - 1.0)
        detection_method: How it was detected
            - "structured": From contrast_search_blob DICOM tags
            - "text_negative": From negative keyword in text_search_blob
            - "text_positive": From positive keyword in text_search_blob
            - "unknown": No detection possible
        matched_keyword: The keyword that matched (for text detection)
        evidence: List of evidence that contributed to detection
    """
    post_contrast: Optional[int]  # 1=yes, 0=no, None=unknown
    confidence: float
    detection_method: str
    matched_keyword: Optional[str] = None
    evidence: List[Evidence] = field(default_factory=list)
    
    @property
    def value(self) -> Optional[int]:
        """Alias for post_contrast (consistency with other detectors)."""
        return self.post_contrast
    
    @property
    def is_positive(self) -> bool:
        """Check if contrast was detected as positive."""
        return self.post_contrast == 1
    
    @property
    def is_negative(self) -> bool:
        """Check if contrast was detected as negative."""
        return self.post_contrast == 0
    
    @property
    def is_unknown(self) -> bool:
        """Check if contrast status is unknown."""
        return self.post_contrast is None
    
    def to_axis_result(self) -> AxisResult:
        """Convert to AxisResult for integration with ClassificationResult."""
        # Convert three-state to string for AxisResult value
        if self.post_contrast == 1:
            value_str = "post_contrast"
        elif self.post_contrast == 0:
            value_str = "pre_contrast"
        else:
            value_str = "unknown"
        
        return AxisResult(
            value=value_str,
            confidence=self.confidence,
            evidence=self.evidence,
            alternatives=[],
        )


class ContrastDetector(BaseDetector):
    """
    Detect contrast agent administration from DICOM metadata.
    
    Detection uses a two-tier approach:
    
    TIER 1: Structured source (contrast_search_blob)
    - Built from DICOM contrast tags (ContrastBolusAgent, ContrastBolusRoute, etc.)
    - If NOT NULL → post_contrast = 1 (HIGH confidence: 0.95)
    - This is the most reliable method
    
    TIER 2: Text source (text_search_blob)
    - Pattern matching in series description, protocol name, etc.
    - NEGATIVE keywords checked FIRST (override positive)
    - Examples: "utan gd", "pre-contrast", "nativ", " -k"
    - Then POSITIVE keywords checked
    - Examples: "dotarem", "+gd", "med kontrast", "post contrast"
    
    Priority: Structured > Negative text > Positive text
    """
    
    YAML_FILENAME = "contrast-detection.yaml"
    
    # Confidence thresholds
    CONFIDENCE_THRESHOLDS = {
        "structured": 0.95,      # DICOM contrast tags present
        "text_negative": 0.85,   # Negative keyword matched
        "text_positive": 0.80,   # Positive keyword matched
        "unknown": 0.0,          # No detection possible
    }
    
    def __init__(self, yaml_dir: Optional[Path] = None):
        """
        Initialize contrast detector.
        
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
        self._negative_keywords: List[str] = self.config.get("negative_keywords", [])
        self._positive_keywords: List[str] = self.config.get("positive_keywords", [])
        
        # Detection rules
        detection_config = self.config.get("detection", {})
        self._negative_overrides_positive = detection_config.get("negative_overrides_positive", True)
        self._structured_overrides_text = detection_config.get("structured_overrides_text", True)
    
    @property
    def axis_name(self) -> str:
        return "contrast"
    
    def detect(self, ctx: ClassificationContext) -> AxisResult:
        """
        Detect contrast agent status.
        
        Args:
            ctx: Classification context with fingerprint data
        
        Returns:
            AxisResult with contrast status, confidence, and evidence
        """
        result = self.detect_contrast(ctx)
        return result.to_axis_result()
    
    def detect_contrast(self, ctx: ClassificationContext) -> ContrastResult:
        """
        Detect contrast agent status with full result details.
        
        Detection priority:
        1. Structured source (contrast_search_blob NOT NULL) → post_contrast = 1
        2. Text negative keywords → post_contrast = 0
        3. Text positive keywords → post_contrast = 1
        4. No match → post_contrast = None (unknown)
        
        Args:
            ctx: Classification context
        
        Returns:
            ContrastResult with detection details
        """
        # =====================================================================
        # TIER 1: Structured source (contrast_search_blob)
        # =====================================================================
        # If contrast_search_blob is NOT NULL, it means DICOM contrast tags
        # were populated → contrast was administered
        if ctx.contrast_search_blob:
            return ContrastResult(
                post_contrast=1,
                confidence=self.CONFIDENCE_THRESHOLDS["structured"],
                detection_method="structured",
                matched_keyword=None,
                evidence=[Evidence(
                    source=EvidenceSource.DICOM_STRUCTURED,
                    field="contrast_search_blob",
                    value=ctx.contrast_search_blob[:100],  # Truncate for display
                    target="post_contrast",
                    weight=self.CONFIDENCE_THRESHOLDS["structured"],
                    description=f"DICOM contrast tags present: {ctx.contrast_search_blob[:50]}...",
                )],
            )
        
        # =====================================================================
        # TIER 2: Text source (text_search_blob)
        # =====================================================================
        text_blob = ctx.text_search_blob
        if not text_blob:
            # No text to search - unknown
            return ContrastResult(
                post_contrast=None,
                confidence=self.CONFIDENCE_THRESHOLDS["unknown"],
                detection_method="unknown",
                matched_keyword=None,
                evidence=[],
            )
        
        # Normalize for matching (already lowercase from fingerprint builder)
        text_lower = text_blob.lower()
        
        # Check NEGATIVE keywords FIRST (they override positive)
        negative_match = self._match_keywords(text_lower, self._negative_keywords)
        if negative_match:
            return ContrastResult(
                post_contrast=0,
                confidence=self.CONFIDENCE_THRESHOLDS["text_negative"],
                detection_method="text_negative",
                matched_keyword=negative_match,
                evidence=[Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=negative_match,
                    target="pre_contrast",
                    weight=self.CONFIDENCE_THRESHOLDS["text_negative"],
                    description=f"Negative keyword '{negative_match}' found → pre-contrast",
                )],
            )
        
        # Check POSITIVE keywords
        positive_match = self._match_keywords(text_lower, self._positive_keywords)
        if positive_match:
            return ContrastResult(
                post_contrast=1,
                confidence=self.CONFIDENCE_THRESHOLDS["text_positive"],
                detection_method="text_positive",
                matched_keyword=positive_match,
                evidence=[Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=positive_match,
                    target="post_contrast",
                    weight=self.CONFIDENCE_THRESHOLDS["text_positive"],
                    description=f"Positive keyword '{positive_match}' found → post-contrast",
                )],
            )
        
        # No match - unknown
        return ContrastResult(
            post_contrast=None,
            confidence=self.CONFIDENCE_THRESHOLDS["unknown"],
            detection_method="unknown",
            matched_keyword=None,
            evidence=[],
        )
    
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
    
    def get_negative_keywords(self) -> List[str]:
        """Get list of negative keywords."""
        return self._negative_keywords.copy()
    
    def get_positive_keywords(self) -> List[str]:
        """Get list of positive keywords."""
        return self._positive_keywords.copy()
    
    def explain_detection(self, ctx: ClassificationContext) -> Dict[str, Any]:
        """
        Generate detailed explanation of contrast detection.
        
        Args:
            ctx: Classification context
        
        Returns:
            Dict with detection explanation
        """
        result = self.detect_contrast(ctx)
        
        explanation = {
            "post_contrast": result.post_contrast,
            "confidence": result.confidence,
            "detection_method": result.detection_method,
            "matched_keyword": result.matched_keyword,
            "has_structured_source": ctx.contrast_search_blob is not None,
            "structured_content": ctx.contrast_search_blob[:100] if ctx.contrast_search_blob else None,
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
        
        matched_negative = [kw for kw in self._negative_keywords if kw in text_lower]
        matched_positive = [kw for kw in self._positive_keywords if kw in text_lower]
        
        # Determine what would be detected
        if matched_negative:
            detection = "pre_contrast (negative keyword matched)"
        elif matched_positive:
            detection = "post_contrast (positive keyword matched)"
        else:
            detection = "unknown (no keywords matched)"
        
        return {
            "input_text": text,
            "normalized_text": text_lower,
            "matched_negative_keywords": matched_negative,
            "matched_positive_keywords": matched_positive,
            "detection_result": detection,
            "note": "Negative keywords override positive (checked first)",
        }
