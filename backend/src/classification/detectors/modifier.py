"""
Modifier Detector

Detects MRI acquisition modifiers using three-tier detection:
1. Exclusive flag (HIGH confidence) - Single definitive unified_flag
2. Keywords match (HIGH confidence) - Text search in series description
3. Combination (MEDIUM confidence) - Multiple unified_flags (AND logic)

KEY DIFFERENCES FROM TECHNIQUE DETECTION:
- Multiple modifiers can apply (additive, not first-match-wins)
- Some modifiers are mutually exclusive within groups (IR_CONTRAST, TRAJECTORY)
- Output is CSV: comma-separated, alphabetically sorted

MUTUAL EXCLUSION GROUPS:
- IR_CONTRAST: FLAIR, STIR, DIR, PSIR (highest priority wins, fallback to IR)
- TRAJECTORY: Radial, Spiral (highest priority wins)

Version: 1.0.0
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from ..core.context import ClassificationContext
from ..core.evidence import (
    AxisResult,
    Evidence,
    EvidenceSource,
)
from ..utils import match_any_keyword, list_to_csv
from .base_detector import BaseDetector


@dataclass
class ModifierMatch:
    """
    A single detected modifier.
    
    Attributes:
        modifier: Modifier ID (e.g., "FLAIR", "FatSat")
        name: Display name
        group: Exclusion group or None for independent
        priority: Priority within group (lower = higher priority)
        confidence: Detection confidence
        detection_method: How detected ("exclusive", "keywords", "combination")
        evidence: Evidence that contributed to detection
    """
    modifier: str
    name: str
    group: Optional[str]
    priority: int
    confidence: float
    detection_method: str
    evidence: List[Evidence] = field(default_factory=list)


@dataclass
class ModifierResult:
    """
    Result of modifier detection.
    
    Attributes:
        modifiers: List of detected modifier IDs (sorted alphabetically)
        modifier_csv: Comma-separated string of modifiers
        matches: Full ModifierMatch objects for each detection
        confidence: Overall confidence (average of individual confidences)
    """
    modifiers: List[str]
    modifier_csv: str
    matches: List[ModifierMatch]
    confidence: float
    
    def to_axis_result(self) -> AxisResult:
        """Convert to AxisResult for integration with ClassificationResult."""
        # Combine all evidence from all matches
        all_evidence = []
        for match in self.matches:
            all_evidence.extend(match.evidence)
        
        return AxisResult(
            value=self.modifier_csv,
            confidence=self.confidence,
            evidence=all_evidence,
            alternatives=[],
        )


class ModifierDetector(BaseDetector):
    """
    Detect MRI acquisition modifiers using unified_flags and keywords.
    
    Detection uses a three-tier approach:
    1. Exclusive flag: Single unified_flag that definitively identifies modifier
    2. Keywords match: Pattern match in text_search_blob
    3. Combination: All required flags must be True (AND logic)
    
    ADDITIVE LOGIC:
    - Multiple modifiers can match (e.g., FLAIR + FatSat)
    - Independent modifiers (group=null) are always additive
    
    MUTUAL EXCLUSION:
    - Within a group, only ONE modifier wins (highest priority)
    - IR_CONTRAST: FLAIR > STIR > DIR > PSIR > IR
    - TRAJECTORY: Radial > Spiral
    """
    
    YAML_FILENAME = "modifier-detection.yaml"
    
    # Confidence thresholds for each detection method
    CONFIDENCE_THRESHOLDS = {
        "exclusive": 0.95,   # Single definitive flag
        "keywords": 0.85,    # Keyword match in text
        "combination": 0.75, # Multiple flags AND
    }
    
    def __init__(self, yaml_dir: Optional[Path] = None):
        """
        Initialize modifier detector.
        
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
        
        # Parse configuration
        self._modifiers: Dict[str, Dict[str, Any]] = self.config.get("modifiers", {})
        self._exclusion_groups: Dict[str, Dict[str, Any]] = self.config.get("exclusion_groups", {})
        self._priority_order: List[str] = self.config.get("rules", {}).get("priority_order", [])
        
        # Build group membership map
        self._group_members: Dict[str, List[str]] = {}
        for group_name, group_info in self._exclusion_groups.items():
            self._group_members[group_name] = group_info.get("members", [])
    
    @property
    def axis_name(self) -> str:
        return "modifier"
    
    def detect(self, ctx: ClassificationContext) -> AxisResult:
        """
        Detect modifiers using additive matching with exclusion groups.
        
        Args:
            ctx: Classification context with fingerprint data
        
        Returns:
            AxisResult with modifier_csv value, confidence, and evidence
        """
        result = self.detect_modifiers(ctx)
        return result.to_axis_result()
    
    def detect_modifiers(self, ctx: ClassificationContext) -> ModifierResult:
        """
        Detect all applicable modifiers.
        
        Uses three-tier detection for each modifier.
        Multiple modifiers can match (additive).
        Within exclusion groups, only highest-priority wins.
        
        Args:
            ctx: Classification context
        
        Returns:
            ModifierResult with list of modifiers and CSV output
        """
        # Get unified flags once
        unified_flags = ctx.unified_flags
        text_blob = ctx.text_search_blob or ""
        
        # Collect all matches
        all_matches: List[ModifierMatch] = []
        
        # Check each modifier in priority order
        for mod_id in self._priority_order:
            if mod_id not in self._modifiers:
                continue
            
            mod_config = self._modifiers[mod_id]
            match = self._check_modifier(ctx, mod_id, mod_config, unified_flags, text_blob)
            if match:
                all_matches.append(match)
        
        # Also check any modifiers not in priority order
        for mod_id, mod_config in self._modifiers.items():
            if mod_id in self._priority_order:
                continue  # Already checked
            match = self._check_modifier(ctx, mod_id, mod_config, unified_flags, text_blob)
            if match:
                all_matches.append(match)
        
        # Apply exclusion group logic
        final_matches = self._apply_exclusion_groups(all_matches)
        
        # Sort modifiers alphabetically for output (use name field, not dict key)
        modifier_names = sorted([m.name for m in final_matches])
        modifier_csv = list_to_csv(modifier_names, sort=True)
        
        # Calculate overall confidence (average)
        # Note: No modifiers detected is a valid, confident result
        if final_matches:
            avg_confidence = sum(m.confidence for m in final_matches) / len(final_matches)
        else:
            avg_confidence = 0.8  # Confident that no modifiers apply
        
        return ModifierResult(
            modifiers=modifier_names,
            modifier_csv=modifier_csv,
            matches=final_matches,
            confidence=avg_confidence,
        )
    
    def _check_modifier(
        self,
        ctx: ClassificationContext,
        mod_id: str,
        config: Dict[str, Any],
        unified_flags: Dict[str, bool],
        text_blob: str
    ) -> Optional[ModifierMatch]:
        """
        Check if context matches a specific modifier using three-tier detection.
        
        Tier order:
        1. exclusive flag → HIGH confidence
        2. keywords match → HIGH confidence
        3. combination (AND) → MEDIUM confidence
        
        Args:
            ctx: Classification context
            mod_id: Modifier ID (e.g., "FLAIR")
            config: Modifier configuration from YAML
            unified_flags: Pre-computed unified flags
            text_blob: text_search_blob for keyword matching
        
        Returns:
            ModifierMatch if matched, None otherwise
        """
        detection = config.get("detection") or {}
        keywords = config.get("keywords", [])
        name = config.get("name", mod_id)
        group = config.get("group")
        priority = config.get("priority", 99)
        
        # --- TIER 1: Exclusive Flag ---
        exclusive_flag = detection.get("exclusive") if detection else None
        if exclusive_flag:
            if unified_flags.get(exclusive_flag, False):
                return ModifierMatch(
                    modifier=mod_id,
                    name=name,
                    group=group,
                    priority=priority,
                    confidence=self.CONFIDENCE_THRESHOLDS["exclusive"],
                    detection_method="exclusive",
                    evidence=[Evidence(
                        source=EvidenceSource.HIGH_VALUE_TOKEN,
                        field="unified_flags",
                        value=exclusive_flag,
                        target=mod_id,
                        weight=self.CONFIDENCE_THRESHOLDS["exclusive"],
                        description=f"Exclusive flag {exclusive_flag}=True → {mod_id}",
                    )],
                )
        
        # --- TIER 2: Keywords Match ---
        if keywords and text_blob:
            matched_kw = match_any_keyword(text_blob, keywords)
            if matched_kw:
                return ModifierMatch(
                    modifier=mod_id,
                    name=name,
                    group=group,
                    priority=priority,
                    confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                    detection_method="keywords",
                    evidence=[Evidence(
                        source=EvidenceSource.TEXT_SEARCH,
                        field="text_search_blob",
                        value=matched_kw,
                        target=mod_id,
                        weight=self.CONFIDENCE_THRESHOLDS["keywords"],
                        description=f"Keyword '{matched_kw}' matched → {mod_id}",
                    )],
                )
        
        # --- TIER 3: Combination (AND logic) ---
        combination = detection.get("combination", []) if detection else []
        if combination:
            all_match = all(unified_flags.get(flag, False) for flag in combination)
            if all_match:
                evidence = [
                    Evidence(
                        source=EvidenceSource.HIGH_VALUE_TOKEN,
                        field="unified_flags",
                        value=flag,
                        target=mod_id,
                        weight=self.CONFIDENCE_THRESHOLDS["combination"],
                        description=f"Combination flag {flag}=True",
                    )
                    for flag in combination
                ]
                return ModifierMatch(
                    modifier=mod_id,
                    name=name,
                    group=group,
                    priority=priority,
                    confidence=self.CONFIDENCE_THRESHOLDS["combination"],
                    detection_method="combination",
                    evidence=evidence,
                )
        
        # No match for this modifier
        return None
    
    def _apply_exclusion_groups(
        self,
        matches: List[ModifierMatch]
    ) -> List[ModifierMatch]:
        """
        Apply exclusion group logic to matches.
        
        Within each exclusion group, only the highest-priority match wins.
        Independent modifiers (group=None) are always kept.
        
        Args:
            matches: All raw matches before exclusion
        
        Returns:
            Filtered matches with exclusion applied
        """
        # Track winners by group
        group_winners: Dict[str, ModifierMatch] = {}
        independent: List[ModifierMatch] = []
        
        for match in matches:
            if match.group is None:
                # Independent modifier - always keep
                independent.append(match)
            else:
                # Check against current group winner
                current = group_winners.get(match.group)
                if current is None:
                    # First match for this group
                    group_winners[match.group] = match
                elif match.priority < current.priority:
                    # Lower priority number = higher priority
                    group_winners[match.group] = match
                # else: current winner has higher priority, keep it
        
        # Combine winners and independent
        final = list(group_winners.values()) + independent
        return final
    
    # =========================================================================
    # Convenience Methods
    # =========================================================================
    
    def get_all_modifiers(self) -> List[str]:
        """Get list of all modifier IDs."""
        return list(self._modifiers.keys())
    
    def get_modifier_config(self, mod_id: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific modifier."""
        return self._modifiers.get(mod_id)
    
    def get_group_members(self, group: str) -> List[str]:
        """Get all modifier IDs in an exclusion group."""
        return self._group_members.get(group, [])
    
    def get_independent_modifiers(self) -> List[str]:
        """Get all modifier IDs that are not in any exclusion group."""
        grouped = set()
        for members in self._group_members.values():
            grouped.update(members)
        return [m for m in self._modifiers if m not in grouped]
    
    def explain_detection(self, ctx: ClassificationContext) -> Dict[str, Any]:
        """
        Explain modifier detection (for debugging).
        
        Returns detailed information about what was checked and matched.
        """
        result = self.detect_modifiers(ctx)
        unified_flags = ctx.unified_flags
        
        explanation = {
            "detected_modifiers": result.modifiers,
            "modifier_csv": result.modifier_csv,
            "confidence": result.confidence,
            "match_count": len(result.matches),
            "match_details": [
                {
                    "modifier": m.modifier,
                    "name": m.name,
                    "group": m.group,
                    "priority": m.priority,
                    "confidence": m.confidence,
                    "method": m.detection_method,
                    "evidence": [
                        {
                            "source": e.source.value,
                            "field": e.field,
                            "value": e.value,
                            "description": e.description,
                        }
                        for e in m.evidence
                    ],
                }
                for m in result.matches
            ],
            "exclusion_groups": self._exclusion_groups,
            "active_modifier_flags": {
                k: v for k, v in unified_flags.items() 
                if v is True and any(
                    x in k.lower() for x in 
                    ['ir', 'flair', 'stir', 'dir', 'psir', 'fat', 'water', 
                     'dixon', 'mt', 'flow', 'radial', 'spiral']
                )
            },
        }
        
        return explanation
    
    def has_ir_modifier(self, result: ModifierResult) -> bool:
        """Check if result contains any IR-based modifier."""
        ir_group = self._group_members.get("IR_CONTRAST", [])
        return any(m in ir_group for m in result.modifiers)
    
    def has_fat_suppression(self, result: ModifierResult) -> bool:
        """Check if result contains any fat suppression modifier."""
        fat_sup = {"FatSat", "WaterExcitation", "STIR", "Dixon"}
        return any(m in fat_sup for m in result.modifiers)
    
    def has_trajectory_modifier(self, result: ModifierResult) -> bool:
        """Check if result contains a trajectory modifier."""
        traj = self._group_members.get("TRAJECTORY", [])
        return any(m in traj for m in result.modifiers)
