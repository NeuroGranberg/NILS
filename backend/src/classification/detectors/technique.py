"""
Technique Detector

Detects MRI acquisition technique using three-tier detection:
1. Exclusive flag (HIGH confidence) - Single definitive unified_flag
2. Keywords match (HIGH confidence) - Text search in series description
3. Combination (MEDIUM confidence) - Multiple unified_flags (AND logic)

Detection is priority-ordered: first match wins.
More specific techniques are checked before generic ones.

Techniques are grouped into 4 families based on physics:
- SE: Spin Echo (uses 180° refocusing pulse)
- GRE: Gradient Echo (gradient refocusing only)
- EPI: Echo Planar Imaging (pure EPI readout)
- MIXED: Hybrid physics (SE+GRE, SE+EPI, functional/diffusion)

Version: 1.0.0
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..core.context import ClassificationContext
from ..core.evidence import (
    AxisResult,
    Evidence,
    EvidenceSource,
    EVIDENCE_WEIGHTS,
)
from ..utils import match_any_keyword
from .base_detector import BaseDetector


@dataclass
class TechniqueResult:
    """
    Result of technique detection.

    Attributes:
        technique: Detected technique ID (e.g., "MPRAGE", "TSE", "DWI-EPI")
        name: Display name (e.g., "MPRAGE", "RESOLVE")
        family: Physics family (SE, GRE, EPI, MIXED)
        confidence: Detection confidence (0.0 - 1.0)
        detection_method: How it was detected ("exclusive", "keywords", "combination")
        evidence: List of evidence that contributed to detection
        has_conflict: Whether conflicting physics family evidence was found
        conflicting_family: The family that conflicts with detected technique
    """
    technique: str
    name: str
    family: str
    confidence: float
    detection_method: str
    evidence: List[Evidence] = field(default_factory=list)
    has_conflict: bool = False
    conflicting_family: Optional[str] = None

    def to_axis_result(self) -> AxisResult:
        """Convert to AxisResult for integration with ClassificationResult."""
        return AxisResult(
            value=self.name,  # Use display name, not dict key
            confidence=self.confidence,
            evidence=self.evidence,
            alternatives=[],
            has_conflict=self.has_conflict,
            conflict_target=self.conflicting_family,
        )


class TechniqueDetector(BaseDetector):
    """
    Detect MRI acquisition technique using unified_flags and keywords.

    Detection uses a three-tier approach (first match wins):
    1. Exclusive flag: Single unified_flag that definitively identifies technique
    2. Keywords match: Pattern match in text_search_blob
    3. Combination: All required flags must be True (AND logic)

    Priority order ensures specific techniques are checked before generic ones.
    Example: MPRAGE (specific) is checked before GRE (generic).

    Conflict Detection:
    After technique detection, checks if flags/text contradict the detected
    physics family. A GRE technique with SE flags/keywords is a conflict.
    """

    YAML_FILENAME = "technique-detection.yaml"

    # Confidence thresholds for each detection method
    CONFIDENCE_THRESHOLDS = {
        "exclusive": 0.95,   # Single definitive flag
        "keywords": 0.85,    # Keyword match in text
        "combination": 0.75, # Multiple flags AND
        "fallback": 0.60,    # Family-level fallback
    }

    # Unified flags that indicate specific physics families
    FAMILY_FLAGS: Dict[str, List[str]] = {
        "SE": ["has_se", "is_tse"],
        "GRE": ["has_gre"],
        "EPI": ["has_epi"],
    }

    # Text keywords that indicate specific physics families
    FAMILY_KEYWORDS: Dict[str, List[str]] = {
        "SE": ["spin echo", "tse", "turbo spin", "fast spin", "fse"],
        "GRE": ["gradient echo", "gradient recalled"],
        "EPI": ["echo planar"],
    }
    
    def __init__(self, yaml_dir: Optional[Path] = None):
        """
        Initialize technique detector.
        
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
        
        # Build priority-ordered list of techniques
        self._techniques: List[Tuple[str, Dict[str, Any]]] = []
        self._priority_order: List[str] = []
        self._build_technique_list()
    
    @property
    def axis_name(self) -> str:
        return "technique"
    
    def _build_technique_list(self) -> None:
        """
        Build priority-ordered list of techniques from config.
        
        Uses rules.priority_order from YAML to determine detection order.
        """
        techniques = self.config.get("techniques", {})
        rules = self.config.get("rules", {})
        self._priority_order = rules.get("priority_order", [])
        
        # Build ordered list based on priority_order
        for tech_id in self._priority_order:
            if tech_id in techniques:
                self._techniques.append((tech_id, techniques[tech_id]))
    
    def detect(self, ctx: ClassificationContext) -> AxisResult:
        """
        Detect technique using priority-based matching.
        
        Args:
            ctx: Classification context with fingerprint data
        
        Returns:
            AxisResult with technique value, confidence, and evidence
        """
        result = self.detect_technique(ctx)
        return result.to_axis_result()
    
    def detect_technique(self, ctx: ClassificationContext) -> TechniqueResult:
        """
        Detect technique and return full result.

        Uses three-tier detection in priority order:
        1. Check exclusive flag
        2. Check keywords
        3. Check combination

        First technique to match wins.

        After detection, checks for family conflicts (e.g., GRE technique
        detected but SE flags/keywords present).

        Args:
            ctx: Classification context

        Returns:
            TechniqueResult with technique, family, confidence, and evidence
        """
        # Get unified flags once
        unified_flags = ctx.unified_flags
        text_blob = ctx.text_search_blob or ""

        # Check each technique in priority order
        for tech_id, tech_config in self._techniques:
            result = self._check_technique(
                ctx, tech_id, tech_config, unified_flags, text_blob
            )
            if result:
                # Check for family conflicts (skip for MIXED family)
                if result.family not in ("MIXED", "UNKNOWN"):
                    result = self._add_conflict_check(result, unified_flags, text_blob)
                return result

        # No match - return unknown
        return TechniqueResult(
            technique="UNKNOWN",
            name="Unknown",
            family="UNKNOWN",
            confidence=0.0,
            detection_method="none",
            evidence=[Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value="no_match",
                target="UNKNOWN",
                weight=0.0,
                description="No technique pattern matched",
            )],
        )
    
    def _check_technique(
        self,
        ctx: ClassificationContext,
        tech_id: str,
        config: Dict[str, Any],
        unified_flags: Dict[str, bool],
        text_blob: str
    ) -> Optional[TechniqueResult]:
        """
        Check if context matches a specific technique using three-tier detection.
        
        Tier order:
        1. exclusive flag → HIGH confidence
        2. keywords match → HIGH confidence
        3. combination (AND) → MEDIUM confidence
        
        Args:
            ctx: Classification context
            tech_id: Technique ID (e.g., "MPRAGE")
            config: Technique configuration from YAML
            unified_flags: Pre-computed unified flags
            text_blob: text_search_blob for keyword matching
        
        Returns:
            TechniqueResult if matched, None otherwise
        """
        detection = config.get("detection", {}) or {}
        keywords = config.get("keywords", [])
        family = config.get("family", "UNKNOWN")
        name = config.get("name", tech_id)
        
        # --- TIER 1: Exclusive Flag ---
        exclusive_flag = detection.get("exclusive")
        if exclusive_flag:
            if unified_flags.get(exclusive_flag, False):
                return TechniqueResult(
                    technique=tech_id,
                    name=name,
                    family=family,
                    confidence=self.CONFIDENCE_THRESHOLDS["exclusive"],
                    detection_method="exclusive",
                    evidence=[Evidence(
                        source=EvidenceSource.HIGH_VALUE_TOKEN,
                        field="unified_flags",
                        value=exclusive_flag,
                        target=tech_id,
                        weight=self.CONFIDENCE_THRESHOLDS["exclusive"],
                        description=f"Exclusive flag {exclusive_flag}=True → {tech_id}",
                    )],
                )
        
        # --- TIER 2: Keywords Match ---
        if keywords and text_blob:
            matched_kw = match_any_keyword(text_blob, keywords)
            if matched_kw:
                return TechniqueResult(
                    technique=tech_id,
                    name=name,
                    family=family,
                    confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                    detection_method="keywords",
                    evidence=[Evidence(
                        source=EvidenceSource.TEXT_SEARCH,
                        field="text_search_blob",
                        value=matched_kw,
                        target=tech_id,
                        weight=self.CONFIDENCE_THRESHOLDS["keywords"],
                        description=f"Keyword '{matched_kw}' matched → {tech_id}",
                    )],
                )
        
        # --- TIER 3: Combination (AND logic) ---
        combination = detection.get("combination", [])
        if combination:
            all_match = all(unified_flags.get(flag, False) for flag in combination)
            if all_match:
                evidence = [
                    Evidence(
                        source=EvidenceSource.HIGH_VALUE_TOKEN,
                        field="unified_flags",
                        value=flag,
                        target=tech_id,
                        weight=self.CONFIDENCE_THRESHOLDS["combination"],
                        description=f"Combination flag {flag}=True",
                    )
                    for flag in combination
                ]
                return TechniqueResult(
                    technique=tech_id,
                    name=name,
                    family=family,
                    confidence=self.CONFIDENCE_THRESHOLDS["combination"],
                    detection_method="combination",
                    evidence=evidence,
                )
        
        # No match for this technique
        return None

    # =========================================================================
    # Conflict Detection
    # =========================================================================

    def _add_conflict_check(
        self,
        result: TechniqueResult,
        unified_flags: Dict[str, bool],
        text_blob: str
    ) -> TechniqueResult:
        """
        Check for family conflicts and update result if found.

        A conflict exists when the detected technique's physics family
        contradicts evidence from flags or text keywords.

        Example: MPRAGE (GRE family) detected but has_se flag is True.

        Args:
            result: The detection result to check
            unified_flags: Current unified flags
            text_blob: text_search_blob for keyword checking

        Returns:
            Updated TechniqueResult with conflict info if found
        """
        conflicting_family = self._check_family_conflict(
            result.family, unified_flags, text_blob.lower()
        )

        if conflicting_family:
            result.has_conflict = True
            result.conflicting_family = conflicting_family

        return result

    def _check_family_conflict(
        self,
        detected_family: str,
        unified_flags: Dict[str, bool],
        text_lower: str
    ) -> Optional[str]:
        """
        Check if flags/text contradict detected technique family.

        Conflict conditions:
        - Detected GRE but has SE flags/keywords (and NOT GRE keywords)
        - Detected SE but has GRE flags/keywords (and NOT SE keywords)
        - Detected GRE but has EPI flags/keywords (and technique not EPI-hybrid)

        Args:
            detected_family: The physics family of detected technique
            unified_flags: Current unified flags
            text_lower: Lowercase text_search_blob

        Returns:
            Conflicting family name, or None if no conflict
        """
        # Check each other family for conflicting evidence
        for other_family, flags in self.FAMILY_FLAGS.items():
            if other_family == detected_family:
                continue

            # Check if conflicting family flags are present
            if any(unified_flags.get(flag, False) for flag in flags):
                # Also check if detected family has its own flags set
                detected_flags = self.FAMILY_FLAGS.get(detected_family, [])
                if not any(unified_flags.get(f, False) for f in detected_flags):
                    # Conflicting flags present, detected flags absent → conflict
                    return other_family

        # Check text keywords for conflict
        for other_family, keywords in self.FAMILY_KEYWORDS.items():
            if other_family == detected_family:
                continue

            # Check if conflicting family keywords present
            if any(kw in text_lower for kw in keywords):
                # But also check if detected family keywords present
                detected_keywords = self.FAMILY_KEYWORDS.get(detected_family, [])
                if not any(kw in text_lower for kw in detected_keywords):
                    # Conflicting keywords present, detected keywords absent → conflict
                    return other_family

        return None

    # =========================================================================
    # Convenience Methods
    # =========================================================================

    def get_all_techniques(self) -> List[str]:
        """Get list of all technique IDs in priority order."""
        return [tech_id for tech_id, _ in self._techniques]
    
    def get_technique_metadata_map(self) -> Dict[str, Dict[str, str]]:
        """
        Get metadata for all techniques (name, family).
        
        Returns:
            Dict mapping technique ID to metadata dict:
            {
                "MPRAGE": {"name": "MPRAGE", "family": "GRE"},
                "MDME": {"name": "MDME", "family": "SE"},
                ...
            }
        """
        metadata = {}
        for tech_id, config in self._techniques:
            metadata[tech_id] = {
                "name": config.get("name", tech_id),
                "family": config.get("family", "UNKNOWN"),
            }
        return metadata

    def get_technique_config(self, tech_id: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific technique."""
        for tid, config in self._techniques:
            if tid == tech_id:
                return config
        return None
    
    def get_techniques_by_family(self, family: str) -> List[str]:
        """Get all technique IDs for a specific family."""
        return [
            tech_id for tech_id, config in self._techniques
            if config.get("family") == family
        ]
    
    def get_family_fallback(self, family: str) -> Optional[str]:
        """
        Get the fallback technique for a family.
        
        Fallback is the last technique in the family's priority order
        (e.g., "SE" is fallback for SE family, "GRE" for GRE family).
        """
        family_techniques = self.get_techniques_by_family(family)
        return family_techniques[-1] if family_techniques else None
    
    def explain_detection(self, ctx: ClassificationContext) -> Dict[str, Any]:
        """
        Explain why a technique was detected (for debugging).
        
        Returns detailed information about what was checked and matched.
        """
        result = self.detect_technique(ctx)
        unified_flags = ctx.unified_flags
        
        explanation = {
            "detected_technique": result.technique,
            "name": result.name,
            "family": result.family,
            "confidence": result.confidence,
            "detection_method": result.detection_method,
            "evidence_count": len(result.evidence),
            "evidence_details": [
                {
                    "source": e.source.value,
                    "field": e.field,
                    "value": e.value,
                    "weight": e.weight,
                    "description": e.description,
                }
                for e in result.evidence
            ],
            "active_flags": {
                k: v for k, v in unified_flags.items() 
                if v is True and not k.startswith("_")
            },
            "checked_techniques": [],
        }
        
        # Show what was checked for each technique (first 10)
        for tech_id, tech_config in self._techniques[:10]:
            detection = tech_config.get("detection", {}) or {}
            check_info = {
                "technique": tech_id,
                "family": tech_config.get("family"),
                "matched": tech_id == result.technique,
                "exclusive": detection.get("exclusive"),
                "keywords": tech_config.get("keywords", [])[:3],  # First 3
                "combination": detection.get("combination", []),
            }
            explanation["checked_techniques"].append(check_info)
        
        return explanation
    
    def get_implied_base(self, technique: str) -> Optional[Tuple[str, float]]:
        """
        Get the implied base contrast for a technique.
        
        Some techniques strongly imply a base contrast:
        - MPRAGE → T1w (0.95)
        - DWI-EPI → DWI (0.95)
        - BOLD-EPI → None (functional, no tissue contrast)
        
        Args:
            technique: The detected technique ID
        
        Returns:
            Tuple of (implied_base, confidence) or None if no inference
        """
        # Import from evidence module to avoid circular imports
        from ..core.evidence import TECHNIQUE_IMPLIES_BASE
        return TECHNIQUE_IMPLIES_BASE.get(technique)
    
    def get_technique_family(self, technique: str) -> Optional[str]:
        """Get the physics family for a technique."""
        config = self.get_technique_config(technique)
        return config.get("family") if config else None
