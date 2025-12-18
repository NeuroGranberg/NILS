"""
Classification Output Contract

Defines the ClassificationResult dataclass that matches the
SeriesClassificationCache database schema.

Version: 3.2.0
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .evidence import AxisResult, Evidence


@dataclass
class ClassificationResult:
    """
    Final classification output matching SeriesClassificationCache schema.
    
    This is the output contract for the classification pipeline.
    All axis values, flags, and review information are captured here.
    
    Attributes:
        base: Base contrast weighting (T1w, T2w, DWI, etc.)
        technique: Pulse sequence family (MPRAGE, TSE, etc.)
        modifier_csv: Comma-separated modifiers (FLAIR,FatSat)
        construct_csv: Comma-separated constructs (ADC,FA)
        provenance: Single provenance value (SyMRI, SWIRecon, DTIRecon, RawRecon, etc.)
        acceleration_csv: Comma-separated acceleration (PI,SMS)
        directory_type: BIDS-like intent (anat, dwi, func, etc.)
        post_contrast: 1=yes, 0=no, None=unknown
        localizer: 1 if localizer/scout, 0 otherwise
        spinal_cord: 1=detected, 0=not, None=uncertain
        manual_review_required: 1 if review needed, 0 otherwise
        manual_review_reasons_csv: Comma-separated reason codes
    """
    
    # === Axis Values ===
    base: Optional[str] = None
    technique: Optional[str] = None
    modifier_csv: str = ""
    construct_csv: str = ""
    provenance: Optional[str] = None  # Single value: SyMRI, SWIRecon, DTIRecon, RawRecon, etc.
    acceleration_csv: str = ""
    
    # === Intent ===
    directory_type: str = "misc"
    
    # === Three-State Flags ===
    post_contrast: Optional[int] = None  # 1=yes, 0=no, None=unknown
    localizer: int = 0  # 0 or 1
    spinal_cord: Optional[int] = None  # 1=yes, 0=no, None=uncertain
    
    # === Review ===
    manual_review_required: int = 0  # 0 or 1
    manual_review_reasons_csv: str = ""
    
    # === Debug (not persisted to database) ===
    _confidences: Dict[str, float] = field(default_factory=dict, repr=False)
    _evidence: Dict[str, List[Evidence]] = field(default_factory=dict, repr=False)
    _axis_results: Dict[str, AxisResult] = field(default_factory=dict, repr=False)
    
    # =========================================================================
    # Serialization
    # =========================================================================
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize for database insertion.
        
        Returns:
            Dict matching SeriesClassificationCache columns
        """
        return {
            "base": self.base,
            "technique": self.technique,
            "modifier_csv": self.modifier_csv,
            "construct_csv": self.construct_csv,
            "provenance": self.provenance,  # Single value, not CSV
            "acceleration_csv": self.acceleration_csv,
            "directory_type": self.directory_type,
            "post_contrast": self.post_contrast,
            "localizer": self.localizer,
            "spinal_cord": self.spinal_cord,
            "manual_review_required": self.manual_review_required,
            "manual_review_reasons_csv": self.manual_review_reasons_csv,
        }
    
    def to_debug_dict(self) -> Dict[str, Any]:
        """
        Serialize with debug information included.
        
        Returns:
            Dict with all fields plus confidence and evidence
        """
        result = self.to_dict()
        result["_confidences"] = self._confidences
        result["_evidence"] = {
            axis: [
                {
                    "source": e.source.value,
                    "field": e.field,
                    "value": e.value,
                    "target": e.target,
                    "weight": e.weight,
                    "description": e.description,
                }
                for e in evidences
            ]
            for axis, evidences in self._evidence.items()
        }
        return result
    
    # =========================================================================
    # Setters with CSV Handling
    # =========================================================================
    
    def set_modifiers(self, modifiers: List[str]) -> None:
        """Set modifiers from list, sorted alphabetically."""
        self.modifier_csv = ",".join(sorted(set(modifiers))) if modifiers else ""
    
    def set_constructs(self, constructs: List[str]) -> None:
        """Set constructs from list, sorted alphabetically."""
        self.construct_csv = ",".join(sorted(set(constructs))) if constructs else ""
    
    def set_provenance(self, provenance: str) -> None:
        """Set provenance (single value)."""
        self.provenance = provenance if provenance else None
    
    def set_acceleration(self, acceleration: List[str]) -> None:
        """Set acceleration from list, sorted alphabetically."""
        self.acceleration_csv = ",".join(sorted(set(acceleration))) if acceleration else ""
    
    def add_review_reason(self, reason: str) -> None:
        """
        Add a manual review reason code.
        
        Args:
            reason: Reason code in format "target:mode"
                   e.g., "base:ambiguous", "technique:low_confidence"
        """
        existing = set(self.manual_review_reasons_csv.split(",")) if self.manual_review_reasons_csv else set()
        existing.discard("")
        existing.add(reason)
        self.manual_review_reasons_csv = ",".join(sorted(existing))
        self.manual_review_required = 1
    
    # =========================================================================
    # Getters with CSV Parsing
    # =========================================================================
    
    def get_modifiers(self) -> List[str]:
        """Get modifiers as list."""
        return [m for m in self.modifier_csv.split(",") if m]
    
    def get_constructs(self) -> List[str]:
        """Get constructs as list."""
        return [c for c in self.construct_csv.split(",") if c]
    
    def get_provenance(self) -> Optional[str]:
        """Get provenance (single value)."""
        return self.provenance
    
    def get_acceleration(self) -> List[str]:
        """Get acceleration as list."""
        return [a for a in self.acceleration_csv.split(",") if a]
    
    def get_review_reasons(self) -> List[str]:
        """Get review reasons as list."""
        return [r for r in self.manual_review_reasons_csv.split(",") if r]
    
    # =========================================================================
    # Confidence Tracking
    # =========================================================================
    
    def set_axis_result(self, axis: str, result: AxisResult) -> None:
        """
        Store axis result with confidence tracking.
        
        Args:
            axis: Axis name (base, technique, modifier, etc.)
            result: The AxisResult from detection
        """
        self._axis_results[axis] = result
        self._confidences[axis] = result.confidence
        self._evidence[axis] = result.evidence
        
        # Check for failure modes and add review reasons
        failure_mode = result.get_failure_mode()
        if failure_mode:
            self.add_review_reason(f"{axis}:{failure_mode}")
    
    def get_confidence(self, axis: str) -> float:
        """Get confidence for an axis."""
        return self._confidences.get(axis, 0.0)
    
    def get_overall_confidence(self) -> float:
        """
        Get minimum confidence across required axes.
        
        Required axes are: base, technique
        """
        required = ["base", "technique"]
        confidences = [self._confidences.get(axis, 0.0) for axis in required]
        return min(confidences) if confidences else 0.0
    
    # =========================================================================
    # Validation
    # =========================================================================
    
    def validate(self) -> List[str]:
        """
        Validate the classification result.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Check required fields for non-localizer
        if self.localizer == 0 and self.directory_type != "excluded":
            if self.base is None and self.construct_csv == "":
                errors.append("Non-localizer must have base or construct")
        
        # Check directory_type is valid
        valid_types = {"anat", "dwi", "func", "fmap", "perf", "localizer", "misc", "excluded"}
        if self.directory_type not in valid_types:
            errors.append(f"Invalid directory_type: {self.directory_type}")
        
        # Check three-state values
        if self.post_contrast is not None and self.post_contrast not in (0, 1):
            errors.append(f"Invalid post_contrast value: {self.post_contrast}")
        
        if self.spinal_cord is not None and self.spinal_cord not in (0, 1):
            errors.append(f"Invalid spinal_cord value: {self.spinal_cord}")
        
        # Check review reason format
        for reason in self.get_review_reasons():
            if ":" not in reason:
                errors.append(f"Invalid review reason format: {reason}")
        
        return errors


# =============================================================================
# Factory Functions
# =============================================================================


def create_excluded_result(reason: str = "secondary_reformat") -> ClassificationResult:
    """
    Create a result for excluded series.

    Excluded series do NOT require manual review - they are intentionally
    excluded (screenshots, secondary reformats, etc.).

    Args:
        reason: Why the series was excluded

    Returns:
        ClassificationResult with directory_type="excluded"
    """
    result = ClassificationResult(
        directory_type="excluded",
        manual_review_required=0,
        # Store reason without triggering review (don't use add_review_reason)
        manual_review_reasons_csv=f"excluded:{reason}",
    )
    return result


def create_localizer_result(
    technique: Optional[str] = None,
    base: Optional[str] = None
) -> ClassificationResult:
    """
    Create a result for localizer/scout series.
    
    Localizers still get full classification where possible.
    
    Args:
        technique: Detected technique if any
        base: Detected base contrast if any
    
    Returns:
        ClassificationResult with localizer=1
    """
    return ClassificationResult(
        base=base,
        technique=technique,
        directory_type="localizer",
        localizer=1,
        manual_review_required=0,
    )


# =============================================================================
# Manual Review Reason Code Reference
# =============================================================================

REVIEW_REASON_CODES = {
    # Target fields
    "base": ["ambiguous", "low_confidence", "conflict", "missing"],
    "technique": ["ambiguous", "low_confidence", "missing"],
    "modifier": ["ambiguous", "conflict"],
    "contrast": ["ambiguous", "conflict"],
    "intent": ["ambiguous"],
    "bodypart": ["spine_detected", "uncertain"],
    "excluded": ["secondary_reformat", "screenshot", "error_map"],
}
"""
Valid manual review reason codes.

Format: target:mode

Target Fields:
- base: Base contrast axis
- technique: Technique axis
- modifier: Modifier axis
- contrast: Post-contrast detection
- intent: Directory type / intent
- bodypart: Body part detection
- excluded: Exclusion reasons

Failure Modes:
- ambiguous: Multiple candidates with similar high confidence
- low_confidence: Top candidate has low confidence (<0.6)
- conflict: Strong evidence contradicts the top candidate
- missing: No valid candidate found
- spine_detected: Spine/spinal cord detected (always triggers review)
- uncertain: Detection uncertain
- secondary_reformat: Excluded as secondary reformat
- screenshot: Excluded as screenshot/pasted
- error_map: Excluded as error map
"""
