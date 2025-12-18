"""QC Rules Engine - extensible rule evaluation system."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class RuleSeverity(str, Enum):
    """Severity levels for rule violations."""
    ERROR = "error"      # Definite mistake that must be fixed
    WARNING = "warning"  # Likely issue that should be reviewed
    INFO = "info"        # Informational, may not need action


class RuleCategory(str, Enum):
    """QC categories for rules."""
    BASE = "base"
    PROVENANCE = "provenance"
    TECHNIQUE = "technique"
    BODY_PART = "body_part"
    CONTRAST = "contrast"


@dataclass
class RuleViolation:
    """Represents a rule violation."""
    rule_id: str
    category: str
    severity: str
    message: str
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "rule_id": self.rule_id,
            "category": self.category,
            "severity": self.severity,
            "message": self.message,
            "details": self.details,
        }


@dataclass
class RuleContext:
    """Context data for rule evaluation."""
    # Classification fields
    base: Optional[str] = None
    technique: Optional[str] = None
    provenance: Optional[str] = None
    modifier_csv: Optional[str] = None
    construct_csv: Optional[str] = None
    directory_type: Optional[str] = None
    post_contrast: Optional[int] = None
    localizer: Optional[int] = None
    spinal_cord: Optional[int] = None

    # Geometry fields
    aspect_ratio: Optional[float] = None
    fov_x_mm: Optional[float] = None
    fov_y_mm: Optional[float] = None
    slices_count: Optional[int] = None
    rows: Optional[int] = None
    columns: Optional[int] = None

    # Review flags
    manual_review_required: Optional[int] = None
    manual_review_reasons_csv: Optional[str] = None

    # Series info
    series_description: Optional[str] = None
    modality: Optional[str] = None

    @classmethod
    def from_classification(cls, classification: dict) -> "RuleContext":
        """Create context from classification dictionary."""
        return cls(
            base=classification.get("base"),
            technique=classification.get("technique"),
            provenance=classification.get("provenance"),
            modifier_csv=classification.get("modifier_csv"),
            construct_csv=classification.get("construct_csv"),
            directory_type=classification.get("directory_type"),
            post_contrast=classification.get("post_contrast"),
            localizer=classification.get("localizer"),
            spinal_cord=classification.get("spinal_cord"),
            aspect_ratio=classification.get("aspect_ratio"),
            fov_x_mm=classification.get("fov_x_mm"),
            fov_y_mm=classification.get("fov_y_mm"),
            slices_count=classification.get("slices_count"),
            rows=classification.get("rows"),
            columns=classification.get("columns"),
            manual_review_required=classification.get("manual_review_required"),
            manual_review_reasons_csv=classification.get("manual_review_reasons_csv"),
            series_description=classification.get("series_description"),
            modality=classification.get("modality"),
        )


class QCRule(ABC):
    """Base class for QC rules."""

    rule_id: str
    category: RuleCategory
    name: str
    description: str
    severity: RuleSeverity = RuleSeverity.WARNING
    enabled: bool = True

    @abstractmethod
    def evaluate(self, ctx: RuleContext) -> Optional[RuleViolation]:
        """
        Evaluate the rule against context.

        Returns:
            RuleViolation if rule is violated, None if rule passes.
        """
        pass

    def _create_violation(self, message: str, details: dict = None) -> RuleViolation:
        """Helper to create a violation with this rule's metadata."""
        return RuleViolation(
            rule_id=self.rule_id,
            category=self.category.value,
            severity=self.severity.value,
            message=message,
            details=details or {},
        )


# =============================================================================
# Technique Rules
# =============================================================================


class TechniqueFamilyMismatchRule(QCRule):
    """
    Rule: Technique must match expected echo family.

    For example:
    - SPACE (3D-TSE) must have SE family, not GRE or EP
    - MPRAGE must have GRE family
    - DWI-EPI must have EP family
    """

    rule_id = "technique_family_mismatch"
    category = RuleCategory.TECHNIQUE
    name = "Technique-Family Consistency"
    description = "Validates that technique matches expected echo family"
    severity = RuleSeverity.ERROR

    # Map technique to expected echo family
    # Based on classification/detection_yaml/technique-detection.yaml
    TECHNIQUE_TO_FAMILY = {
        # SE family (Spin Echo)
        "TSE": "SE",
        "SPACE": "SE",
        "BLADE": "SE",
        "PROPELLER": "SE",
        "HASTE": "SE",
        "Multi-echo SE": "SE",
        "IR-TSE": "SE",
        "STIR": "SE",
        "FLAIR": "SE",
        "DIR": "SE",

        # GRE family (Gradient Echo)
        "GRE": "GRE",
        "FLASH": "GRE",
        "MPRAGE": "GRE",
        "MP2RAGE": "GRE",
        "ME-GRE": "GRE",
        "SWI": "GRE",
        "TOF-MRA": "GRE",
        "PC-MRA": "GRE",
        "CISS": "GRE",
        "FISP": "GRE",
        "TrueFISP": "GRE",
        "MEDIC": "GRE",

        # EPI family (Echo Planar)
        "EPI": "EP",
        "DWI-EPI": "EP",
        "BOLD-EPI": "EP",
        "Perfusion-EPI": "EP",
        "RESOLVE": "EP",
        "ASL": "EP",

        # Mixed/Special
        "MDME": "MIXED",  # SyMRI source - multi-echo
    }

    # Techniques that can legitimately have multiple families
    MULTI_FAMILY_TECHNIQUES = {"RESOLVE", "ASL", "MDME"}

    def evaluate(self, ctx: RuleContext) -> Optional[RuleViolation]:
        if not ctx.technique:
            return None

        expected_family = self.TECHNIQUE_TO_FAMILY.get(ctx.technique)
        if not expected_family:
            return None  # Unknown technique, can't validate

        if ctx.technique in self.MULTI_FAMILY_TECHNIQUES:
            return None  # Skip multi-family techniques

        # Check if provenance suggests wrong family
        # SWIRecon output should be GRE, not EP
        if ctx.provenance == "SWIRecon" and expected_family != "GRE":
            return self._create_violation(
                f"Technique '{ctx.technique}' (expected {expected_family}) "
                f"classified under SWI reconstruction (expects GRE family)",
                {"technique": ctx.technique, "expected_family": expected_family, "provenance": ctx.provenance},
            )

        # Check constructs for family hints
        constructs = (ctx.construct_csv or "").lower()
        if expected_family == "SE":
            # SE techniques shouldn't have GRE-specific constructs
            if "swi" in constructs or "qsm" in constructs:
                return self._create_violation(
                    f"SE technique '{ctx.technique}' has GRE-specific construct ({ctx.construct_csv})",
                    {"technique": ctx.technique, "expected_family": expected_family, "constructs": ctx.construct_csv},
                )

        return None


class TechniqueMissingRule(QCRule):
    """Rule: Technique should be classified for most scans."""

    rule_id = "technique_missing"
    category = RuleCategory.TECHNIQUE
    name = "Missing Technique"
    description = "Flags scans without technique classification"
    severity = RuleSeverity.WARNING

    def evaluate(self, ctx: RuleContext) -> Optional[RuleViolation]:
        # Skip localizers and excluded
        if ctx.localizer == 1 or ctx.directory_type in ("localizer", "excluded"):
            return None

        # Skip if provenance handles technique
        if ctx.provenance in ("SyMRI", "SWIRecon", "DTIRecon"):
            return None

        if not ctx.technique and ctx.base:
            return self._create_violation(
                f"Base contrast '{ctx.base}' classified but technique is missing",
                {"base": ctx.base},
            )

        return None


# =============================================================================
# Body Part Rules
# =============================================================================


class BrainAspectRatioRule(QCRule):
    """
    Rule: Brain scans should have ~1:1 aspect ratio.

    Brain FOV is typically square (220x220mm to 256x256mm).
    Elongated aspect ratios (>1.4 or <0.7) suggest spine or other body part.
    """

    rule_id = "brain_aspect_ratio_anomaly"
    category = RuleCategory.BODY_PART
    name = "Brain Aspect Ratio Anomaly"
    description = "Brain scans should have roughly square aspect ratio"
    severity = RuleSeverity.WARNING

    # Thresholds
    MIN_RATIO = 0.7   # Below this is too wide
    MAX_RATIO = 1.4   # Above this is too tall (suggests spine)

    def evaluate(self, ctx: RuleContext) -> Optional[RuleViolation]:
        # Only check anatomical brain scans
        if ctx.directory_type != "anat":
            return None

        # Skip if already marked as spine
        if ctx.spinal_cord == 1:
            return None

        # Skip localizers
        if ctx.localizer == 1:
            return None

        if ctx.aspect_ratio is None:
            return None

        if ctx.aspect_ratio > self.MAX_RATIO:
            return self._create_violation(
                f"Aspect ratio {ctx.aspect_ratio:.2f} is elongated (>{self.MAX_RATIO}), "
                f"suggesting spine rather than brain",
                {"aspect_ratio": ctx.aspect_ratio, "threshold": self.MAX_RATIO},
            )

        if ctx.aspect_ratio < self.MIN_RATIO:
            return self._create_violation(
                f"Aspect ratio {ctx.aspect_ratio:.2f} is unusually wide (<{self.MIN_RATIO}) for brain anatomy",
                {"aspect_ratio": ctx.aspect_ratio, "threshold": self.MIN_RATIO},
            )

        return None


class SpineAspectRatioRule(QCRule):
    """
    Rule: Spine scans should have elongated aspect ratio.

    Spine FOV is typically rectangular (e.g., 280x400mm).
    Near-square ratios (0.85-1.15) suggest brain, not spine.
    """

    rule_id = "spine_aspect_ratio_anomaly"
    category = RuleCategory.BODY_PART
    name = "Spine Aspect Ratio Anomaly"
    description = "Spine scans should have elongated (non-square) aspect ratio"
    severity = RuleSeverity.WARNING

    # Thresholds - if marked as spine but ratio is square
    MIN_SPINE_RATIO = 1.3  # Spine should be at least this elongated

    def evaluate(self, ctx: RuleContext) -> Optional[RuleViolation]:
        # Only check if explicitly marked as spine
        if ctx.spinal_cord != 1:
            return None

        if ctx.aspect_ratio is None:
            return None

        if ctx.aspect_ratio < self.MIN_SPINE_RATIO:
            return self._create_violation(
                f"Marked as spine but aspect ratio {ctx.aspect_ratio:.2f} is nearly square "
                f"(<{self.MIN_SPINE_RATIO}), suggesting brain",
                {"aspect_ratio": ctx.aspect_ratio, "threshold": self.MIN_SPINE_RATIO},
            )

        return None


class LocalizerSliceCountRule(QCRule):
    """
    Rule: Localizers should have few slices.

    Localizers (scouts) typically have 3-15 slices.
    High slice counts (>20) suggest a full acquisition, not a localizer.
    """

    rule_id = "localizer_slice_count"
    category = RuleCategory.BODY_PART
    name = "Localizer Slice Count"
    description = "Localizers should have few slices (typically <20)"
    severity = RuleSeverity.WARNING

    MAX_LOCALIZER_SLICES = 20

    def evaluate(self, ctx: RuleContext) -> Optional[RuleViolation]:
        if ctx.localizer != 1:
            return None

        if ctx.slices_count is None:
            return None

        if ctx.slices_count > self.MAX_LOCALIZER_SLICES:
            return self._create_violation(
                f"Localizer has {ctx.slices_count} slices (expected <{self.MAX_LOCALIZER_SLICES}), "
                f"may be misclassified",
                {"slices_count": ctx.slices_count, "threshold": self.MAX_LOCALIZER_SLICES},
            )

        return None


class NonLocalizerLowSliceCountRule(QCRule):
    """
    Rule: Non-localizer anatomical scans should have reasonable slice count.

    Full brain coverage typically requires 100+ slices for 3D or 20+ for 2D.
    Very low slice counts (<10) for non-localizers are suspicious.
    """

    rule_id = "non_localizer_low_slices"
    category = RuleCategory.BODY_PART
    name = "Low Slice Count for Anatomy"
    description = "Non-localizer anatomical scans should have adequate slice coverage"
    severity = RuleSeverity.INFO

    MIN_SLICES = 10

    def evaluate(self, ctx: RuleContext) -> Optional[RuleViolation]:
        # Only check anatomical non-localizers
        if ctx.localizer == 1 or ctx.directory_type != "anat":
            return None

        if ctx.slices_count is None:
            return None

        if ctx.slices_count < self.MIN_SLICES:
            return self._create_violation(
                f"Anatomical scan has only {ctx.slices_count} slices "
                f"(expected >={self.MIN_SLICES}), may be a localizer",
                {"slices_count": ctx.slices_count, "threshold": self.MIN_SLICES},
            )

        return None


# =============================================================================
# Provenance Rules
# =============================================================================


class ProvenanceMismatchRule(QCRule):
    """
    Rule: Provenance should match constructs.

    For example:
    - SWIRecon should have SWI-related constructs (SWI, QSM, Phase, etc.)
    - DTIRecon should have diffusion constructs (ADC, FA, etc.)
    """

    rule_id = "provenance_construct_mismatch"
    category = RuleCategory.PROVENANCE
    name = "Provenance-Construct Mismatch"
    description = "Provenance should match the type of derived constructs"
    severity = RuleSeverity.WARNING

    PROVENANCE_EXPECTED_CONSTRUCTS = {
        "SWIRecon": {"swi", "qsm", "phase", "magnitude", "minip", "mip"},
        "DTIRecon": {"adc", "fa", "trace", "md", "ad", "rd", "eadc"},
        "SyMRI": {"t1map", "t2map", "pdmap", "myelin", "synthetic"},
        "PerfusionRecon": {"cbf", "cbv", "mtt", "tmax", "ttp"},
    }

    def evaluate(self, ctx: RuleContext) -> Optional[RuleViolation]:
        if not ctx.provenance:
            return None

        expected = self.PROVENANCE_EXPECTED_CONSTRUCTS.get(ctx.provenance)
        if not expected:
            return None

        constructs_lower = (ctx.construct_csv or "").lower()
        if not constructs_lower:
            return None

        # Check if any expected construct is present
        found = any(exp in constructs_lower for exp in expected)

        if not found and constructs_lower:
            return self._create_violation(
                f"Provenance '{ctx.provenance}' doesn't match constructs '{ctx.construct_csv}'",
                {"provenance": ctx.provenance, "constructs": ctx.construct_csv, "expected_any": list(expected)},
            )

        return None


# =============================================================================
# Contrast Rules
# =============================================================================


class ContrastUndeterminedRule(QCRule):
    """
    Rule: Contrast status should be determined for T1w scans.

    T1w scans are commonly acquired pre and post gadolinium.
    Unknown contrast status may affect downstream analysis.
    """

    rule_id = "contrast_undetermined"
    category = RuleCategory.CONTRAST
    name = "Contrast Status Undetermined"
    description = "T1w scans should have known contrast status"
    severity = RuleSeverity.INFO

    def evaluate(self, ctx: RuleContext) -> Optional[RuleViolation]:
        # Only relevant for T1w anatomical scans
        if ctx.base != "T1w" or ctx.directory_type != "anat":
            return None

        if ctx.post_contrast is None:
            return self._create_violation(
                "T1w anatomical scan has unknown contrast status (pre/post gadolinium)",
                {"base": ctx.base},
            )

        return None


# =============================================================================
# Base Classification Rules
# =============================================================================


class BaseMissingRule(QCRule):
    """Rule: Base contrast should be classified for most anatomical scans."""

    rule_id = "base_missing"
    category = RuleCategory.BASE
    name = "Missing Base Contrast"
    description = "Anatomical scans should have base contrast classification"
    severity = RuleSeverity.WARNING

    def evaluate(self, ctx: RuleContext) -> Optional[RuleViolation]:
        # Skip localizers, excluded, and non-anatomical
        if ctx.localizer == 1 or ctx.directory_type in ("localizer", "excluded", "fmap"):
            return None

        # Skip if has constructs (derived maps don't need base)
        if ctx.construct_csv:
            return None

        # Skip provenances that handle their own classification
        if ctx.provenance in ("SyMRI", "SWIRecon", "DTIRecon", "PerfusionRecon"):
            return None

        if not ctx.base and ctx.directory_type == "anat":
            return self._create_violation(
                "Anatomical scan without base contrast classification",
                {"directory_type": ctx.directory_type},
            )

        return None


# =============================================================================
# Rules Engine
# =============================================================================


class QCRulesEngine:
    """Orchestrates rule evaluation across all categories."""

    def __init__(self):
        self._rules: dict[str, list[QCRule]] = {
            "base": [],
            "provenance": [],
            "technique": [],
            "body_part": [],
            "contrast": [],
        }
        self._load_builtin_rules()

    def _load_builtin_rules(self) -> None:
        """Load all built-in rules."""
        # Base rules
        self._rules["base"].append(BaseMissingRule())

        # Provenance rules
        self._rules["provenance"].append(ProvenanceMismatchRule())

        # Technique rules
        self._rules["technique"].append(TechniqueFamilyMismatchRule())
        self._rules["technique"].append(TechniqueMissingRule())

        # Body part rules
        self._rules["body_part"].append(BrainAspectRatioRule())
        self._rules["body_part"].append(SpineAspectRatioRule())
        self._rules["body_part"].append(LocalizerSliceCountRule())
        self._rules["body_part"].append(NonLocalizerLowSliceCountRule())

        # Contrast rules
        self._rules["contrast"].append(ContrastUndeterminedRule())

    def register_rule(self, rule: QCRule) -> None:
        """Register a custom rule."""
        category = rule.category.value
        if category not in self._rules:
            self._rules[category] = []
        self._rules[category].append(rule)

    def get_rules(self, category: Optional[str] = None) -> list[QCRule]:
        """Get all rules, optionally filtered by category."""
        if category:
            return self._rules.get(category, [])
        return [rule for rules in self._rules.values() for rule in rules]

    def evaluate(
        self,
        ctx: RuleContext,
        category: Optional[str] = None,
    ) -> list[RuleViolation]:
        """
        Evaluate rules against context.

        Args:
            ctx: Rule evaluation context with classification data
            category: Optional category to filter rules

        Returns:
            List of rule violations found
        """
        violations = []
        categories = [category] if category else self._rules.keys()

        for cat in categories:
            for rule in self._rules.get(cat, []):
                if not rule.enabled:
                    continue

                try:
                    violation = rule.evaluate(ctx)
                    if violation:
                        violations.append(violation)
                except Exception:
                    # Log but don't fail on rule errors
                    pass

        return violations

    def evaluate_dict(
        self,
        classification: dict,
        category: Optional[str] = None,
    ) -> list[dict]:
        """
        Evaluate rules against classification dictionary.

        Convenience method that creates RuleContext from dict.

        Returns:
            List of violation dictionaries
        """
        ctx = RuleContext.from_classification(classification)
        violations = self.evaluate(ctx, category)
        return [v.to_dict() for v in violations]


# Global engine instance
rules_engine = QCRulesEngine()
