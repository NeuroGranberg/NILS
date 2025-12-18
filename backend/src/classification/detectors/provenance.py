"""
Provenance Detector

Detects image provenance (processing pipeline) for classification branching.
Provenance is detected FIRST, then determines which classification branch to use.

Detection uses three-tier approach (like TechniqueDetector):
1. Exclusive flag (HIGH confidence) - Single unified_flag
2. Keywords match (HIGH confidence) - Text search in series description
3. Combination (MEDIUM confidence) - Multiple unified_flags (AND logic)
4. Alternative flags (HIGH confidence) - Any of these flags (OR logic)

Provenances detected (10 total):
- SyMRI: Synthetic MRI (MAGiC, MDME, QALAS) → branch: symri
- SWIRecon: Susceptibility-weighted imaging → branch: swi
- DTIRecon: Diffusion tensor reconstruction → branch: rawrecon
- PerfusionRecon: Perfusion parameter maps → branch: rawrecon
- ASLRecon: Arterial spin labeling → branch: rawrecon
- BOLDRecon: BOLD fMRI → branch: rawrecon
- ProjectionDerived: MIP/MPR reformats → branch: rawrecon
- SubtractionDerived: Pre-post subtraction → branch: rawrecon
- Localizer: Scout/localizer images → branch: rawrecon
- RawRecon: Default (no match) → branch: rawrecon

Version: 2.1.0 - Now loads configuration from YAML
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..core.context import ClassificationContext
from ..core.evidence import Evidence, EvidenceSource, AxisResult
from ..utils import match_any_keyword
from .base_detector import BaseDetector


@dataclass
class ProvenanceResult:
    """
    Result of provenance detection.

    Attributes:
        provenance: Detected provenance name (e.g., "SyMRI", "RawRecon")
        branch: Classification branch to use ("symri", "swi", "rawrecon")
        confidence: Detection confidence (0.0 - 1.0)
        detection_method: How it was detected ("exclusive", "keywords", "combination", "alternative", "default")
        evidence: List of evidence that contributed to detection
    """

    provenance: str
    branch: str
    confidence: float
    detection_method: str
    evidence: List[Evidence] = field(default_factory=list)

    @property
    def value(self) -> str:
        """Alias for provenance (consistency with other detectors)."""
        return self.provenance

    def to_axis_result(self) -> AxisResult:
        """Convert to AxisResult for integration with ClassificationResult."""
        return AxisResult(
            value=self.provenance,
            confidence=self.confidence,
            evidence=self.evidence,
            alternatives=[],
        )


class ProvenanceDetector(BaseDetector):
    """
    Detect image provenance using unified_flags and priority-based matching.

    Detection uses a four-tier approach (first match wins):
    1. Exclusive flag: Single unified_flag that definitively identifies provenance
    2. Alternative flags: Any of these flags triggers detection (OR logic)
    3. Keywords match: Pattern match in text_search_blob
    4. Combination: All required flags must be True (AND logic)

    Priority order ensures specific provenances are checked before generic ones.
    Only SyMRI and SWIRecon get special classification branches.

    Configuration is loaded from provenance-detection.yaml.
    """

    YAML_FILENAME = "provenance-detection.yaml"

    # Default confidence thresholds (can be overridden by YAML)
    DEFAULT_CONFIDENCE = {
        "exclusive": 0.95,
        "keywords": 0.85,
        "combination": 0.75,
        "alternative": 0.85,
        "default": 0.80,
    }

    def __init__(self, yaml_dir: Optional[Path] = None):
        """
        Initialize ProvenanceDetector.

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

        # Load provenances and rules from config
        self._provenances: Dict[str, Dict[str, Any]] = self.config.get("provenances", {})
        self._rules: Dict[str, Any] = self.config.get("rules", {})
        self._priority_order: List[str] = self._rules.get("priority_order", [])
        self._default_provenance: str = self._rules.get("default_provenance", "RawRecon")

        # Load confidence thresholds from YAML or use defaults
        yaml_confidence = self._rules.get("confidence_thresholds", {})
        self._confidence = {
            "exclusive": yaml_confidence.get("exclusive", self.DEFAULT_CONFIDENCE["exclusive"]),
            "keywords": yaml_confidence.get("keywords", self.DEFAULT_CONFIDENCE["keywords"]),
            "combination": yaml_confidence.get("combination", self.DEFAULT_CONFIDENCE["combination"]),
            "alternative": yaml_confidence.get("alternative", self.DEFAULT_CONFIDENCE["alternative"]),
            "default": yaml_confidence.get("default", self.DEFAULT_CONFIDENCE["default"]),
        }

        # Build branch mapping from config
        self._branch_map: Dict[str, str] = {}
        branches_config = self.config.get("branches", {})
        for branch_name, branch_info in branches_config.items():
            for prov_name in branch_info.get("provenances", []):
                self._branch_map[prov_name] = branch_name

    @property
    def axis_name(self) -> str:
        return "provenance"

    def detect(self, ctx: ClassificationContext) -> ProvenanceResult:
        """
        Detect provenance using priority-based matching.

        Args:
            ctx: Classification context with DICOM metadata

        Returns:
            ProvenanceResult with provenance, branch, confidence, and evidence
        """
        uf = ctx.unified_flags
        text_blob = (ctx.text_search_blob or "").lower()

        # Check each provenance in priority order
        for prov_name in self._priority_order:
            if prov_name not in self._provenances:
                continue

            config = self._provenances[prov_name]

            # Skip default
            if config.get("is_default"):
                continue

            result = self._detect_provenance(prov_name, config, uf, text_blob)
            if result:
                return result

        # No match - return default RawRecon
        default_config = self._provenances.get(self._default_provenance, {})
        default_branch = default_config.get("branch", "rawrecon")

        return ProvenanceResult(
            provenance=self._default_provenance,
            branch=default_branch,
            confidence=self._confidence["default"],
            detection_method="default",
            evidence=[
                Evidence(
                    source=EvidenceSource.HIGH_VALUE_TOKEN,
                    field="default",
                    value="no_specific_match",
                    target=self._default_provenance,
                    weight=self._confidence["default"],
                    description=f"No specific provenance detected, using default {self._default_provenance}",
                )
            ],
        )

    def _detect_provenance(
        self,
        name: str,
        config: Dict[str, Any],
        uf: Dict[str, bool],
        text_blob: str,
    ) -> Optional[ProvenanceResult]:
        """
        Detect a single provenance using four-tier detection.

        Args:
            name: Provenance name
            config: Provenance configuration from YAML
            uf: Unified flags
            text_blob: Lowercase text search blob

        Returns:
            ProvenanceResult if detected, None otherwise
        """
        branch = config.get("branch", "rawrecon")

        # Detection config can be a dict or have fields at top level
        detection = config.get("detection") or {}
        exclusive = detection.get("exclusive") if isinstance(detection, dict) else None
        keywords = detection.get("keywords", []) if isinstance(detection, dict) else []
        combination = detection.get("combination") if isinstance(detection, dict) else None

        # Alternative flags are at top level in YAML
        alternative_flags = config.get("alternative_flags")

        # =================================================================
        # TIER 1: EXCLUSIVE FLAG
        # =================================================================
        if exclusive and uf.get(exclusive):
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value=exclusive,
                target=name,
                weight=self._confidence["exclusive"],
                description=f"Exclusive flag: {exclusive}",
            )
            return ProvenanceResult(
                provenance=name,
                branch=branch,
                confidence=self._confidence["exclusive"],
                detection_method="exclusive",
                evidence=[evidence],
            )

        # =================================================================
        # TIER 2: ALTERNATIVE FLAGS (OR logic)
        # =================================================================
        if alternative_flags:
            matched_flags = [f for f in alternative_flags if uf.get(f)]
            if matched_flags:
                evidence = Evidence(
                    source=EvidenceSource.HIGH_VALUE_TOKEN,
                    field="unified_flags",
                    value=", ".join(matched_flags),
                    target=name,
                    weight=self._confidence["alternative"],
                    description=f"Alternative flags: {', '.join(matched_flags)}",
                )
                return ProvenanceResult(
                    provenance=name,
                    branch=branch,
                    confidence=self._confidence["alternative"],
                    detection_method="alternative",
                    evidence=[evidence],
                )

        # =================================================================
        # TIER 3: KEYWORDS
        # =================================================================
        if keywords:
            matched_kw = match_any_keyword(text_blob, keywords)
            if matched_kw:
                evidence = Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=matched_kw,
                    target=name,
                    weight=self._confidence["keywords"],
                    description=f"Keyword match: {matched_kw}",
                )
                return ProvenanceResult(
                    provenance=name,
                    branch=branch,
                    confidence=self._confidence["keywords"],
                    detection_method="keyword",
                    evidence=[evidence],
                )

        # =================================================================
        # TIER 4: COMBINATION (AND logic)
        # =================================================================
        if combination:
            all_present = all(uf.get(f) for f in combination)
            if all_present:
                evidence = Evidence(
                    source=EvidenceSource.HIGH_VALUE_TOKEN,
                    field="unified_flags",
                    value=", ".join(combination),
                    target=name,
                    weight=self._confidence["combination"],
                    description=f"Combination flags: {', '.join(combination)}",
                )
                return ProvenanceResult(
                    provenance=name,
                    branch=branch,
                    confidence=self._confidence["combination"],
                    detection_method="combination",
                    evidence=[evidence],
                )

        return None

    def get_branch(self, provenance: str) -> str:
        """
        Get the classification branch for a provenance value.

        Args:
            provenance: Provenance name

        Returns:
            Branch name ("symri", "swi", or "rawrecon")
        """
        return self._branch_map.get(provenance, "rawrecon")

    def explain_detection(self, ctx: ClassificationContext) -> str:
        """
        Generate human-readable explanation of detection.

        Args:
            ctx: Classification context

        Returns:
            Formatted string explaining detection
        """
        result = self.detect(ctx)

        lines = [
            f"Provenance: {result.provenance}",
            f"Branch: {result.branch}",
            f"Confidence: {result.confidence:.0%}",
            f"Method: {result.detection_method}",
        ]

        if result.evidence:
            lines.append("\nEvidence:")
            for ev in result.evidence:
                lines.append(f"  - {ev.description}")

        return "\n".join(lines)

    def get_all_provenances(self) -> List[str]:
        """Get list of all provenance names in priority order."""
        return self._priority_order.copy()

    def get_branches(self) -> Dict[str, List[str]]:
        """Get branch mapping."""
        branches: Dict[str, List[str]] = {}
        branches_config = self.config.get("branches", {})
        for branch_name, branch_info in branches_config.items():
            branches[branch_name] = branch_info.get("provenances", [])
        return branches

    def get_provenance_config(self, name: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific provenance."""
        return self._provenances.get(name)
