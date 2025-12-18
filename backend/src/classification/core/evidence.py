"""
Evidence & Confidence Model

Tracks decision logic for classification with weighted evidence sources.

Version: 3.1.1
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class EvidenceSource(Enum):
    """
    Categorical evidence sources with implicit confidence weights.
    
    Higher weights indicate more reliable evidence.
    
    Simplified model:
    - HIGH_VALUE_TOKEN: All parsed DICOM tag flags (replaces TEXT_EXACT)
    - TEXT_SEARCH: Pattern matching in text_search_blob (replaces TEXT_FUZZY)
    - DICOM_STRUCTURED: Structured DICOM fields (contrast_search_blob, etc.)
    """
    
    # High confidence sources (0.90-0.95)
    HIGH_VALUE_TOKEN = "high_value_token"
    """Parsed flags from DICOM tags (ImageType, ScanningSequence, SequenceName, etc.)"""
    
    TECHNIQUE_INFERENCE = "technique_inference"
    """Base inferred from technique (e.g., MPRAGE → T1w)"""
    
    DICOM_STRUCTURED = "dicom_structured"
    """Structured DICOM fields like contrast_search_blob, body_part_examined"""
    
    # Medium confidence sources (0.70-0.80)
    TEXT_SEARCH = "text_search"
    """Pattern match in text_search_blob"""
    
    MODIFIER_INFERENCE = "modifier_inference"
    """Base inferred from modifier + physics (e.g., FLAIR + TE>60 → T2w)"""
    
    PHYSICS_DISTINCT = "physics_distinct"
    """Physics parameters in non-overlapping diagnostic range"""
    
    # Low confidence sources (0.40-0.50)
    PHYSICS_OVERLAP = "physics_overlap"
    """Physics parameters in overlapping range (ambiguous)"""
    
    GEOMETRY_HINT = "geometry_hint"
    """FOV, aspect ratio, or other geometry-based heuristics"""


# Evidence weight mapping
EVIDENCE_WEIGHTS: Dict[EvidenceSource, float] = {
    EvidenceSource.HIGH_VALUE_TOKEN: 0.95,
    EvidenceSource.TECHNIQUE_INFERENCE: 0.90,
    EvidenceSource.DICOM_STRUCTURED: 0.95,
    EvidenceSource.TEXT_SEARCH: 0.75,
    EvidenceSource.MODIFIER_INFERENCE: 0.80,
    EvidenceSource.PHYSICS_DISTINCT: 0.70,
    EvidenceSource.PHYSICS_OVERLAP: 0.50,
    EvidenceSource.GEOMETRY_HINT: 0.40,
}


@dataclass
class Evidence:
    """
    Tracks a single decision point in classification.
    
    Each piece of evidence supports a target classification value
    with a confidence weight based on its source.
    
    Attributes:
        source: The type of evidence (determines base weight)
        field: The fingerprint field that provided this evidence
        value: The actual matched value or pattern
        target: The classification value this evidence supports
        weight: Confidence weight (0.0 - 1.0)
        description: Human-readable explanation
    
    Example:
        Evidence(
            source=EvidenceSource.HIGH_VALUE_TOKEN,
            field="image_type",
            value="DIFFUSION",
            target="DWI",
            weight=0.95,
            description="DIFFUSION token in ImageType"
        )
    """
    
    source: EvidenceSource
    field: str
    value: str
    target: str
    weight: float
    description: str
    
    @classmethod
    def from_token(
        cls,
        field: str,
        value: str,
        target: str,
        description: Optional[str] = None
    ) -> "Evidence":
        """
        Create evidence from a parsed DICOM token.
        
        Use this for any flag from parsed_image_type, parsed_scanning_sequence,
        parsed_sequence_variant, parsed_scan_options, or parsed_sequence_name.
        
        Automatically uses HIGH_VALUE_TOKEN source with 0.95 weight.
        """
        return cls(
            source=EvidenceSource.HIGH_VALUE_TOKEN,
            field=field,
            value=value,
            target=target,
            weight=EVIDENCE_WEIGHTS[EvidenceSource.HIGH_VALUE_TOKEN],
            description=description or f"{value} token in {field}",
        )
    
    @classmethod
    def from_text_search(
        cls,
        pattern: str,
        target: str,
        description: Optional[str] = None
    ) -> "Evidence":
        """
        Create evidence from text_search_blob pattern match.
        
        Args:
            pattern: The pattern that was matched
            target: The classification value this supports
            description: Optional human-readable explanation
        """
        return cls(
            source=EvidenceSource.TEXT_SEARCH,
            field="text_search_blob",
            value=pattern,
            target=target,
            weight=EVIDENCE_WEIGHTS[EvidenceSource.TEXT_SEARCH],
            description=description or f"'{pattern}' found in text_search_blob",
        )
    
    @classmethod
    def from_technique(
        cls,
        technique: str,
        implied_base: str,
        confidence: Optional[float] = None
    ) -> "Evidence":
        """
        Create evidence from technique inference.
        
        Args:
            technique: The detected technique (e.g., "MPRAGE")
            implied_base: The implied base contrast (e.g., "T1w")
            confidence: Optional custom confidence (defaults to 0.90)
        """
        return cls(
            source=EvidenceSource.TECHNIQUE_INFERENCE,
            field="technique",
            value=technique,
            target=implied_base,
            weight=confidence or EVIDENCE_WEIGHTS[EvidenceSource.TECHNIQUE_INFERENCE],
            description=f"{technique} implies {implied_base}",
        )
    
    @classmethod
    def from_modifier(
        cls,
        modifier: str,
        physics_hint: str,
        implied_base: str,
        description: Optional[str] = None
    ) -> "Evidence":
        """
        Create evidence from modifier + physics inference.
        
        Args:
            modifier: The detected modifier (e.g., "FLAIR")
            physics_hint: The physics parameter that helped (e.g., "TE>60")
            implied_base: The implied base contrast (e.g., "T2w")
        """
        return cls(
            source=EvidenceSource.MODIFIER_INFERENCE,
            field="modifier+physics",
            value=f"{modifier}+{physics_hint}",
            target=implied_base,
            weight=EVIDENCE_WEIGHTS[EvidenceSource.MODIFIER_INFERENCE],
            description=description or f"{modifier} with {physics_hint} implies {implied_base}",
        )
    
    @classmethod
    def from_physics(
        cls,
        parameters: Dict[str, float],
        target: str,
        is_distinct: bool = True,
        description: Optional[str] = None
    ) -> "Evidence":
        """
        Create evidence from physics parameters.
        
        Args:
            parameters: Dict of physics values (e.g., {"TR": 2300, "TE": 2.9})
            target: The classification value this supports
            is_distinct: True if in non-overlapping range (higher confidence)
        """
        source = EvidenceSource.PHYSICS_DISTINCT if is_distinct else EvidenceSource.PHYSICS_OVERLAP
        param_str = ", ".join(f"{k}={v}" for k, v in parameters.items())
        return cls(
            source=source,
            field="physics",
            value=param_str,
            target=target,
            weight=EVIDENCE_WEIGHTS[source],
            description=description or f"Physics ({param_str}) suggests {target}",
        )
    
    @classmethod
    def from_geometry(
        cls,
        hint: str,
        target: str,
        description: Optional[str] = None
    ) -> "Evidence":
        """
        Create evidence from geometry hints.
        
        Args:
            hint: The geometry hint (e.g., "FOV<300", "aspect_ratio>2")
            target: The classification value this supports
        """
        return cls(
            source=EvidenceSource.GEOMETRY_HINT,
            field="geometry",
            value=hint,
            target=target,
            weight=EVIDENCE_WEIGHTS[EvidenceSource.GEOMETRY_HINT],
            description=description or f"Geometry ({hint}) suggests {target}",
        )


@dataclass
class AxisResult:
    """
    Result for a single classification axis.

    Contains the final value, confidence, all contributing evidence,
    and any alternative candidates that were considered.

    Attributes:
        value: The classification result (None if undetermined)
        confidence: Overall confidence (0.0 - 1.0)
        evidence: All evidence that contributed to this result
        alternatives: Other candidates with their confidences
        has_conflict: Whether text evidence contradicts the prediction
        conflict_target: The conflicting value found in text (if any)
    """

    value: Optional[str] = None
    confidence: float = 0.0
    evidence: List[Evidence] = field(default_factory=list)
    alternatives: List[Tuple[str, float]] = field(default_factory=list)
    has_conflict: bool = False
    conflict_target: Optional[str] = None

    def add_evidence(self, evidence: Evidence) -> None:
        """Add a piece of evidence to this result."""
        self.evidence.append(evidence)

    def is_confident(self, threshold: float = 0.6) -> bool:
        """Check if confidence meets threshold."""
        return self.confidence >= threshold

    def is_ambiguous(self, threshold: float = 0.1) -> bool:
        """
        Check if result is ambiguous.

        Ambiguous means there's an alternative within threshold
        of the top result's confidence.
        """
        if not self.alternatives:
            return False
        top_alt_confidence = max(conf for _, conf in self.alternatives)
        return (self.confidence - top_alt_confidence) < threshold

    def get_failure_mode(self) -> Optional[str]:
        """
        Determine the failure mode if any.

        Returns:
            None if no failure, otherwise one of:
            - "missing": No value determined
            - "conflict": Text evidence contradicts the prediction
            - "low_confidence": Value determined but low confidence
            - "ambiguous": Multiple candidates with similar confidence
        """
        if self.value is None:
            return "missing"
        if self.has_conflict:
            return "conflict"
        if not self.is_confident():
            return "low_confidence"
        if self.is_ambiguous():
            return "ambiguous"
        return None


def calculate_confidence(evidences: List[Evidence], target: str) -> float:
    """
    Calculate confidence for a target value from evidence list.
    
    Uses the maximum evidence weight, with a small boost for
    multiple independent sources agreeing.
    
    Args:
        evidences: List of all evidence
        target: The target classification value
    
    Returns:
        Confidence score (0.0 - 1.0)
    """
    relevant = [e for e in evidences if e.target == target]
    
    if not relevant:
        return 0.0
    
    # Use max evidence weight
    max_weight = max(e.weight for e in relevant)
    
    # Boost if multiple independent sources agree
    # +5% per additional unique source type
    source_types = set(e.source for e in relevant)
    if len(source_types) >= 2:
        boost = 0.05 * (len(source_types) - 1)
        max_weight = min(max_weight + boost, 0.99)
    
    return max_weight


def select_best_candidate(
    evidences: List[Evidence],
    candidates: List[str]
) -> AxisResult:
    """
    Select the best classification candidate from evidence.
    
    Args:
        evidences: All collected evidence
        candidates: Possible classification values
    
    Returns:
        AxisResult with best candidate and alternatives
    """
    if not evidences:
        return AxisResult()
    
    # Calculate confidence for each candidate
    scored: List[Tuple[str, float]] = []
    for candidate in candidates:
        conf = calculate_confidence(evidences, candidate)
        if conf > 0:
            scored.append((candidate, conf))
    
    if not scored:
        return AxisResult(evidence=evidences)
    
    # Sort by confidence descending
    scored.sort(key=lambda x: x[1], reverse=True)
    
    best_value, best_conf = scored[0]
    alternatives = scored[1:] if len(scored) > 1 else []
    
    # Get evidence for best candidate
    best_evidence = [e for e in evidences if e.target == best_value]
    
    return AxisResult(
        value=best_value,
        confidence=best_conf,
        evidence=best_evidence,
        alternatives=alternatives,
    )


# =============================================================================
# Technique-to-Base Inference Rules
# =============================================================================
# 
# Maps technique ID → (implied_base, confidence)
# None for base means "no tissue contrast" (e.g., phase/velocity)
#
# These are used by TechniqueDetector.get_implied_base() and BaseDetector
# to infer base contrast from technique detection.
# =============================================================================

TECHNIQUE_IMPLIES_BASE: Dict[str, Tuple[Optional[str], float]] = {
    # === T1-weighted techniques ===
    # IR-prepared 3D GRE
    "MPRAGE": ("T1w", 0.95),
    "MEMPRAGE": ("T1w", 0.95),
    "MP2RAGE": ("T1w", 0.95),
    
    # Spoiled GRE (usually T1-weighted)
    "SP-GRE": ("T1w", 0.85),
    "FSP-GRE": ("T1w", 0.85),
    "VI-GRE": ("T1w", 0.85),
    
    # TOF MRA (T1 by inflow enhancement)
    "TOF-MRA": ("T1w", 0.90),
    
    # === T2-weighted techniques ===
    # Standard TSE/FSE
    "TSE": ("T2w", 0.70),  # Could be T1 or PD depending on TR/TE
    "3D-TSE": ("T2w", 0.70),
    "SS-TSE": ("T2w", 0.80),  # HASTE usually T2
    
    # === T2*-weighted techniques ===
    # Multi-echo GRE
    "ME-GRE": ("T2*w", 0.85),
    "comb-ME-GRE": ("T2*w", 0.90),
    
    # SWI
    "SWI": ("SWI", 0.95),
    
    # === Diffusion techniques ===
    "DWI-EPI": ("DWI", 0.95),
    "DWI-STEAM": ("DWI", 0.95),
    
    # === Functional/Perfusion (no tissue contrast implied) ===
    "BOLD-EPI": (None, 0.90),  # Functional, contrast from activation
    "ASL-EPI": (None, 0.90),   # Perfusion from labeling
    "Perfusion-EPI": (None, 0.90),  # DSC perfusion
    
    # === Velocity/Flow (no tissue contrast) ===
    "PC-MRA": (None, 0.95),  # Velocity encoded, no T1/T2 contrast
    
    # === Quantitative (no tissue contrast, quantitative maps) ===
    "QALAS": (None, 0.95),  # Quantitative mapping
    "VFA-GRE": (None, 0.90),  # T1 mapping
    "VFA-TSE": (None, 0.90),  # Variable flip angle
    
    # === SSFP variants ===
    # bSSFP has mixed T2/T1 contrast (T2/T1 ratio)
    "bSSFP": (None, 0.70),  # Mixed contrast
    "CISS": (None, 0.70),   # High-SNR for cisterns
    "SSFP": (None, 0.60),   # Various
    "DESS": (None, 0.60),   # Dual echo
    
    # === Quantitative/Synthetic ===
    "MDME": (None, 0.95),  # SyMRI acquisition, maps generated
    "MRF": (None, 0.95),   # MR Fingerprinting, quantitative
}


def get_implied_base(technique: str) -> Optional[Tuple[Optional[str], float]]:
    """
    Get the implied base contrast for a technique.
    
    Args:
        technique: The detected technique name
    
    Returns:
        Tuple of (implied_base, confidence) or None if no inference rule.
        implied_base can be None for techniques with no tissue contrast
        (e.g., phase contrast, functional).
    """
    return TECHNIQUE_IMPLIES_BASE.get(technique)
