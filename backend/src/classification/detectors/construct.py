"""
Construct Detector for MRI classification.

Detects computed/derived constructs from MRI acquisitions:
- Diffusion: ADC, eADC, FA, Trace, MD
- Perfusion: CBF, CBV, MTT, Tmax, TTP
- Dixon: Water, Fat, InPhase, OutPhase
- Quantitative: T1map, T2map, R1map, R2map, PDmap
- Synthetic: SyntheticT1w, SyntheticT2w, SyntheticFLAIR, SyntheticPDw, MyelinMap
- SWI: SWIProcessed, PhaseMap, QSM
- Projection: MIP, MinIP, MPR
- FieldMap: B0map, B1map
- Component: Magnitude, Real, Imaginary

Constructs are ADDITIVE - multiple can be detected from the same acquisition.
Output is construct_csv: comma-separated, alphabetically sorted.

Version: 2.0.0 - Now loads configuration from YAML
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any

from ..core.context import ClassificationContext
from ..core.evidence import Evidence, EvidenceSource
from .base_detector import BaseDetector


@dataclass
class ConstructMatch:
    """Result for a single detected construct."""

    name: str
    category: str
    confidence: float
    detection_method: str
    evidence: List[Evidence] = field(default_factory=list)

    @property
    def value(self) -> str:
        """Alias for name."""
        return self.name


@dataclass
class ConstructDetectorOutput:
    """Complete output from ConstructDetector."""

    constructs: List[ConstructMatch] = field(default_factory=list)

    @property
    def values(self) -> List[str]:
        """Get sorted list of construct names."""
        return sorted([c.name for c in self.constructs])

    @property
    def construct_csv(self) -> str:
        """Get comma-separated, alphabetically sorted construct string."""
        return ",".join(self.values)

    @property
    def has_constructs(self) -> bool:
        """Whether any construct was detected."""
        return len(self.constructs) > 0

    def has(self, name: str) -> bool:
        """Check if a specific construct was detected."""
        return name in [c.name for c in self.constructs]

    def get(self, name: str) -> Optional[ConstructMatch]:
        """Get a specific construct by name."""
        for c in self.constructs:
            if c.name == name:
                return c
        return None

    def by_category(self, category: str) -> List[ConstructMatch]:
        """Get all constructs in a category."""
        return [c for c in self.constructs if c.category == category]


class ConstructDetector(BaseDetector):
    """
    Detects computed/derived constructs from DICOM metadata.

    Constructs are outputs COMPUTED from raw acquisitions:
    - ADC/FA from diffusion-weighted imaging
    - CBF/CBV from perfusion imaging
    - T1map/T2map from quantitative sequences
    - Synthetic contrasts from SyMRI
    - MIP/MPR reformats

    Detection is ADDITIVE - multiple constructs can be detected simultaneously.
    Output is construct_csv: comma-separated, alphabetically sorted.

    Configuration is loaded from construct-detection.yaml.
    """

    YAML_FILENAME = "construct-detection.yaml"

    # Default confidence thresholds (can be overridden by YAML)
    DEFAULT_CONFIDENCE = {
        "exclusive": 0.95,
        "keywords": 0.85,
        "combination": 0.75,
    }

    def __init__(self, yaml_dir: Optional[Path] = None):
        """
        Initialize ConstructDetector.

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

        # Load constructs and rules from config
        self._constructs: Dict[str, Dict[str, Any]] = self.config.get("constructs", {})
        self._rules: Dict[str, Any] = self.config.get("rules", {})
        self._priority_order: List[str] = self._rules.get("priority_order", [])

        # Load confidence thresholds from YAML or use defaults
        yaml_confidence = self._rules.get("confidence_thresholds", {})
        self._confidence = {
            "exclusive": yaml_confidence.get("exclusive", self.DEFAULT_CONFIDENCE["exclusive"]),
            "keywords": yaml_confidence.get("keywords", self.DEFAULT_CONFIDENCE["keywords"]),
            "combination": yaml_confidence.get("combination", self.DEFAULT_CONFIDENCE["combination"]),
        }

    @property
    def axis_name(self) -> str:
        return "construct"

    def detect(self, ctx: ClassificationContext) -> ConstructDetectorOutput:
        """
        Detect all constructs from context.

        Args:
            ctx: Classification context with DICOM metadata

        Returns:
            ConstructDetectorOutput with list of detected constructs
        """
        results: List[ConstructMatch] = []

        uf = ctx.unified_flags
        text_blob = (ctx.text_search_blob or "").lower()
        is_derived = uf.get("is_derived", False)

        # Check constructs in priority order first
        checked = set()
        for name in self._priority_order:
            if name in self._constructs:
                checked.add(name)
                config = self._constructs[name]
                match = self._detect_construct(name, config, uf, text_blob, is_derived)
                if match:
                    results.append(match)

        # Then check any remaining constructs not in priority order
        for name, config in self._constructs.items():
            if name not in checked:
                match = self._detect_construct(name, config, uf, text_blob, is_derived)
                if match:
                    results.append(match)

        return ConstructDetectorOutput(constructs=results)

    def _detect_construct(
        self,
        name: str,
        config: Dict[str, Any],
        uf: Dict[str, bool],
        text_blob: str,
        is_derived: bool,
    ) -> Optional[ConstructMatch]:
        """
        Detect a single construct.

        Args:
            name: Construct name
            config: Construct configuration from YAML
            uf: Unified flags
            text_blob: Lowercase text search blob
            is_derived: Whether image is derived

        Returns:
            ConstructMatch if detected, None otherwise
        """
        # Get configuration values
        # Use 'name' field for display, fall back to YAML key if not specified
        display_name = config.get("name", name)
        category = config.get("category", "unknown")
        keywords = config.get("keywords", [])
        requires_derived = config.get("requires_derived", True)

        # Detection config can be a dict or null
        detection = config.get("detection") or {}
        exclusive = detection.get("exclusive") if isinstance(detection, dict) else None
        combination = detection.get("combination") if isinstance(detection, dict) else None

        # Check if derived is required but not present
        # (skip this check for components that can be original)
        if requires_derived and not is_derived:
            # Still allow detection if we have strong evidence
            pass

        # Priority 1: Exclusive flag
        if exclusive and uf.get(exclusive):
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value=exclusive,
                target=display_name,
                weight=self._confidence["exclusive"],
                description=f"Exclusive flag: {exclusive}",
            )
            return ConstructMatch(
                name=display_name,
                category=category,
                confidence=self._confidence["exclusive"],
                detection_method="exclusive_flag",
                evidence=[evidence],
            )

        # Priority 2: Keyword matching
        for kw in keywords:
            if kw in text_blob:
                evidence = Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=kw,
                    target=display_name,
                    weight=self._confidence["keywords"],
                    description=f"Keyword match: {kw}",
                )
                return ConstructMatch(
                    name=display_name,
                    category=category,
                    confidence=self._confidence["keywords"],
                    detection_method="keyword",
                    evidence=[evidence],
                )

        # Priority 3: Combination of flags
        if combination:
            flags_present = [f for f in combination if uf.get(f)]
            if len(flags_present) == len(combination):
                evidence = Evidence(
                    source=EvidenceSource.HIGH_VALUE_TOKEN,
                    field="unified_flags",
                    value=", ".join(flags_present),
                    target=display_name,
                    weight=self._confidence["combination"],
                    description=f"Combination flags: {', '.join(flags_present)}",
                )
                return ConstructMatch(
                    name=display_name,
                    category=category,
                    confidence=self._confidence["combination"],
                    detection_method="combination",
                    evidence=[evidence],
                )

        return None

    def explain_detection(self, ctx: ClassificationContext) -> str:
        """
        Generate human-readable explanation of detection results.

        Args:
            ctx: Classification context

        Returns:
            Formatted string explaining detection
        """
        output = self.detect(ctx)

        if not output.has_constructs:
            return "No constructs detected (original acquisition)."

        lines = [f"Detected {len(output.constructs)} construct(s):"]
        lines.append(f"construct_csv: \"{output.construct_csv}\"")

        # Group by category
        categories: Dict[str, List[ConstructMatch]] = {}
        for c in output.constructs:
            if c.category not in categories:
                categories[c.category] = []
            categories[c.category].append(c)

        for cat, constructs in sorted(categories.items()):
            lines.append(f"\n  [{cat.upper()}]")
            for c in constructs:
                lines.append(f"    {c.name}:")
                lines.append(f"      Confidence: {c.confidence:.0%}")
                lines.append(f"      Method: {c.detection_method}")
                for ev in c.evidence:
                    lines.append(f"      Evidence: {ev.description}")

        return "\n".join(lines)

    def get_all_constructs(self) -> List[str]:
        """Get list of all possible construct display names.
        
        Returns the 'name' field (display name) from each construct config,
        falling back to the YAML key if 'name' is not specified.
        This ensures consistency with what gets stored in construct_csv.
        """
        return sorted([
            config.get("name", key) for key, config in self._constructs.items()
        ])

    def get_constructs_by_category(self, category: str) -> List[str]:
        """Get all construct display names in a category."""
        return sorted([
            config.get("name", key) for key, config in self._constructs.items()
            if config.get("category") == category
        ])

    def get_categories(self) -> List[str]:
        """Get all construct categories."""
        categories = set()
        for config in self._constructs.values():
            cat = config.get("category")
            if cat:
                categories.add(cat)
        return sorted(categories)

    def get_construct_config(self, name: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific construct."""
        return self._constructs.get(name)
