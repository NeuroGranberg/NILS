"""
Body Part Detector (Multi-Category, YAML-Driven)

Detects body part category from text-based keyword matching.
Keywords for each category are defined in the YAML config file.

Resolution order (most specific wins):
  spine  >  brain-neck  >  neck  >  brain  >  unknown

Combination rules:
  spine + anything     -> spine    (triggers review)
  neck  + brain        -> brain-neck (triggers review)
  neck  only           -> neck    (triggers review)
  brain only           -> brain   (no review)
  nothing              -> unknown

Version: 3.1.0
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
class _CategoryConfig:
    """Parsed category configuration from YAML."""
    name: str
    triggers_review: bool
    review_reason: Optional[str]
    confidence: float
    keywords: List[str]


@dataclass
class BodyPartResult:
    """
    Result of body part detection.

    Attributes:
        body_part: Detected category ("brain", "spine", "neck", "brain-neck") or None
        confidence: Detection confidence
        detection_method: How it was detected
        matched_keyword: The keyword that matched
        matched_category: Which YAML category matched
        triggers_review: Whether this should trigger manual review
        evidence: Evidence list
        has_conflict: Whether conflicting categories were found
        conflict_reason: Human-readable conflict description
    """
    body_part: Optional[str]
    confidence: float
    detection_method: str
    matched_keyword: Optional[str] = None
    matched_category: Optional[str] = None
    triggers_review: bool = False
    evidence: List[Evidence] = field(default_factory=list)
    has_conflict: bool = False
    conflict_reason: Optional[str] = None

    # ------------------------------------------------------------------
    # Backward-compatibility helpers
    # ------------------------------------------------------------------

    @property
    def spinal_cord(self) -> Optional[int]:
        """Backward compat: 1 if spine/neck, 0 if brain/brain-neck, None if unknown."""
        if self.body_part in ("spine", "neck"):
            return 1
        if self.body_part in ("brain", "brain-neck"):
            return 0
        return None

    @property
    def value(self) -> Optional[str]:
        """Alias for body_part."""
        return self.body_part

    @property
    def is_spine(self) -> bool:
        return self.body_part == "spine"

    @property
    def is_neck(self) -> bool:
        return self.body_part == "neck"

    @property
    def is_brain_neck(self) -> bool:
        return self.body_part == "brain-neck"

    @property
    def is_brain(self) -> bool:
        return self.body_part == "brain"

    @property
    def is_brain_only(self) -> bool:
        return self.body_part == "brain"

    @property
    def is_unknown(self) -> bool:
        return self.body_part is None

    # ------------------------------------------------------------------
    # Conversion
    # ------------------------------------------------------------------

    def to_axis_result(self) -> AxisResult:
        return AxisResult(
            value=self.body_part or "unknown",
            confidence=self.confidence,
            evidence=self.evidence,
            alternatives=[],
            has_conflict=self.has_conflict,
            conflict_target=self.conflict_reason,
        )


class BodyPartDetector(BaseDetector):
    """
    YAML-driven multi-category body part detector.

    Keywords are loaded from the YAML config. The resolution logic is:

      1. Check text_search_blob for keyword matches in each category.
      2. Apply combination rules:
         - spine found (with or without anything else) -> spine
         - neck + brain found (no spine) -> brain-neck
         - neck only -> neck
         - brain only -> brain
         - nothing -> unknown (None)
    """

    YAML_FILENAME = "body_part-detection.yaml"
    CONFIDENCE_UNKNOWN = 0.0

    # Aspect ratio threshold for portrait-sagittal spine heuristic.
    # In validated data (49K stacks): zero brain sagittal scans above 1.4,
    # all portrait sagittal scans >= 1.4 are spine.
    PORTRAIT_SPINE_AR_THRESHOLD = 1.4
    PORTRAIT_SPINE_CONFIDENCE = 0.55

    def __init__(
        self,
        yaml_dir: Optional[Path] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        yaml_path = None
        if yaml_dir:
            yaml_path = Path(yaml_dir) / self.YAML_FILENAME
        else:
            yaml_path = Path(__file__).parent.parent / "detection_yaml" / self.YAML_FILENAME

        super().__init__(yaml_path, config=config)

        self._categories: Dict[str, _CategoryConfig] = {}
        for name, cfg in (self.config.get("categories") or {}).items():
            self._categories[name] = _CategoryConfig(
                name=name,
                triggers_review=cfg.get("triggers_review", False),
                review_reason=cfg.get("review_reason"),
                confidence=cfg.get("confidence", 0.65),
                keywords=cfg.get("keywords", []),
            )

        self._default_body_part: Optional[str] = self.config.get("default_body_part")

        detection_cfg = self.config.get("detection") or {}
        self._conflict_triggers_review: bool = detection_cfg.get("conflict_triggers_review", True)
        self._conflict_review_reason: str = detection_cfg.get("conflict_review_reason", "bodypart:conflict")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def axis_name(self) -> str:
        return "body_part"

    def detect(self, ctx: ClassificationContext) -> AxisResult:
        result = self.detect_body_part(ctx)
        return result.to_axis_result()

    def detect_body_part(
        self,
        ctx: ClassificationContext,
    ) -> BodyPartResult:
        """Detect body part category from classification context."""
        text_blob = ctx.text_search_blob

        if not text_blob:
            return BodyPartResult(
                body_part=None,
                confidence=self.CONFIDENCE_UNKNOWN,
                detection_method="unknown",
            )

        text_lower = text_blob.lower()

        # Find keyword matches for each real category (spine, neck, brain)
        matches: Dict[str, str] = {}  # category_name -> first matched keyword
        for name, cat in self._categories.items():
            if not cat.keywords:
                continue
            kw = self._match_keywords(text_lower, cat.keywords)
            if kw is not None:
                matches[name] = kw

        if not matches:
            # Fallback: portrait sagittal FOV heuristic for spine
            ar_result = self._detect_by_aspect_ratio(ctx)
            if ar_result is not None:
                return ar_result
            return BodyPartResult(
                body_part=self._default_body_part,
                confidence=self.CONFIDENCE_UNKNOWN,
                detection_method="default" if self._default_body_part else "unknown",
            )

        has_spine = "spine" in matches
        has_neck = "neck" in matches
        has_brain = "brain" in matches

        # ---- Resolution: spine > brain-neck > neck > brain ----

        if has_spine:
            return self._make_result(
                body_part="spine",
                matched_keyword=matches["spine"],
                matched_category="spine",
                has_conflict=has_brain,
                conflict_reason=f"spine:'{matches['spine']}' vs brain:'{matches.get('brain', '')}'" if has_brain else None,
                extra_brain_kw=matches.get("brain"),
            )

        if has_neck and has_brain:
            brain_neck_cat = self._categories.get("brain-neck")
            return BodyPartResult(
                body_part="brain-neck",
                confidence=brain_neck_cat.confidence if brain_neck_cat else 0.65,
                detection_method="text_keyword",
                matched_keyword=matches["neck"],
                matched_category="brain-neck",
                triggers_review=brain_neck_cat.triggers_review if brain_neck_cat else True,
                evidence=[
                    Evidence(
                        source=EvidenceSource.TEXT_SEARCH,
                        field="text_search_blob",
                        value=matches["neck"],
                        target="neck",
                        weight=0.65,
                        description=f"Neck keyword '{matches['neck']}' found",
                    ),
                    Evidence(
                        source=EvidenceSource.TEXT_SEARCH,
                        field="text_search_blob",
                        value=matches["brain"],
                        target="brain",
                        weight=0.65,
                        description=f"Brain keyword '{matches['brain']}' found",
                    ),
                ],
                has_conflict=False,
            )

        if has_neck:
            return self._make_result(
                body_part="neck",
                matched_keyword=matches["neck"],
                matched_category="neck",
            )

        if has_brain:
            return self._make_result(
                body_part="brain",
                matched_keyword=matches["brain"],
                matched_category="brain",
            )

        # Only non-standard categories matched (future-proofing)
        first_name = next(iter(matches))
        return self._make_result(
            body_part=first_name,
            matched_keyword=matches[first_name],
            matched_category=first_name,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_result(
        self,
        *,
        body_part: str,
        matched_keyword: str,
        matched_category: str,
        has_conflict: bool = False,
        conflict_reason: Optional[str] = None,
        extra_brain_kw: Optional[str] = None,
    ) -> BodyPartResult:
        cat = self._categories.get(matched_category)
        confidence = cat.confidence if cat else 0.65
        triggers_review = (cat.triggers_review if cat else False) or (has_conflict and self._conflict_triggers_review)

        evidence: List[Evidence] = [Evidence(
            source=EvidenceSource.TEXT_SEARCH,
            field="text_search_blob",
            value=matched_keyword,
            target=body_part,
            weight=confidence if not has_conflict else confidence * 0.75,
            description=f"{body_part.title()} keyword '{matched_keyword}' found",
        )]
        if has_conflict and extra_brain_kw:
            evidence.append(Evidence(
                source=EvidenceSource.TEXT_SEARCH,
                field="text_search_blob",
                value=extra_brain_kw,
                target="brain",
                weight=0.5,
                description=f"Brain keyword '{extra_brain_kw}' also found (conflict)",
            ))

        return BodyPartResult(
            body_part=body_part,
            confidence=confidence,
            detection_method="text_keyword",
            matched_keyword=matched_keyword,
            matched_category=matched_category,
            triggers_review=triggers_review,
            evidence=evidence,
            has_conflict=has_conflict,
            conflict_reason=conflict_reason,
        )

    def _detect_by_aspect_ratio(
        self, ctx: ClassificationContext
    ) -> Optional[BodyPartResult]:
        """Fallback spine detection using portrait sagittal FOV geometry.

        A sagittal image with fov_y >> fov_x (portrait, AR >= 1.4) is
        almost certainly a spinal acquisition.  Validated against 49K+
        stacks: zero brain sagittal scans exceed this threshold.
        """
        orientation = (ctx.stack_orientation or "").lower()
        if orientation != "sagittal":
            return None
        fov_x = ctx.fov_x
        fov_y = ctx.fov_y
        if fov_x is None or fov_y is None or fov_x <= 0:
            return None
        if fov_y <= fov_x:
            return None
        ar = fov_y / fov_x
        if ar < self.PORTRAIT_SPINE_AR_THRESHOLD:
            return None

        evidence = Evidence(
            source=EvidenceSource.PHYSICS_DISTINCT,
            field="fov_y/fov_x",
            value=f"AR={ar:.2f} (fov {fov_x:.0f}×{fov_y:.0f}mm, sagittal portrait)",
            target="spine",
            weight=self.PORTRAIT_SPINE_CONFIDENCE,
            description=(
                f"Sagittal portrait FOV (AR={ar:.2f} >= {self.PORTRAIT_SPINE_AR_THRESHOLD}) "
                f"indicates spinal acquisition"
            ),
        )
        return BodyPartResult(
            body_part="spine",
            confidence=self.PORTRAIT_SPINE_CONFIDENCE,
            detection_method="aspect_ratio",
            matched_keyword=None,
            matched_category="spine",
            triggers_review=True,
            evidence=[evidence],
        )

    @staticmethod
    def _match_keywords(text: str, keywords: List[str]) -> Optional[str]:
        for keyword in keywords:
            if keyword in text:
                return keyword
        return None

    # ------------------------------------------------------------------
    # Convenience / introspection
    # ------------------------------------------------------------------

    @property
    def categories(self) -> List[str]:
        """Return list of category names."""
        return list(self._categories.keys())

    def get_category_keywords(self, category: str) -> List[str]:
        cat = self._categories.get(category)
        return cat.keywords.copy() if cat else []

    def get_review_reason(self, category: Optional[str] = None) -> str:
        if category:
            cat = self._categories.get(category)
            if cat:
                return cat.review_reason or ""
        return "bodypart:spine_detected"

    def get_conflict_review_reason(self) -> str:
        return self._conflict_review_reason

    def explain_detection(self, ctx: ClassificationContext) -> Dict[str, Any]:
        result = self.detect_body_part(ctx)
        return {
            "body_part": result.body_part,
            "confidence": result.confidence,
            "detection_method": result.detection_method,
            "matched_keyword": result.matched_keyword,
            "matched_category": result.matched_category,
            "triggers_review": result.triggers_review,
            "has_conflict": result.has_conflict,
            "conflict_reason": result.conflict_reason,
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

    def debug_text_matching(self, text: str) -> Dict[str, Any]:
        text_lower = text.lower()

        category_matches: Dict[str, List[str]] = {}
        for name, cat in self._categories.items():
            matched = [kw for kw in cat.keywords if kw in text_lower]
            if matched:
                category_matches[name] = matched

        has_spine = "spine" in category_matches
        has_neck = "neck" in category_matches
        has_brain = "brain" in category_matches

        if has_spine:
            detection = f"spine (matched: {category_matches['spine'][0]})"
            would_trigger_review = True
        elif has_neck and has_brain:
            detection = f"brain-neck (neck:'{category_matches['neck'][0]}' + brain:'{category_matches['brain'][0]}')"
            would_trigger_review = True
        elif has_neck:
            detection = f"neck (matched: {category_matches['neck'][0]})"
            would_trigger_review = True
        elif has_brain:
            detection = f"brain (matched: {category_matches['brain'][0]})"
            would_trigger_review = False
        else:
            detection = "unknown (no keywords matched)"
            would_trigger_review = False

        return {
            "input_text": text,
            "normalized_text": text_lower,
            "category_matches": category_matches,
            "detection_result": detection,
            "would_trigger_review": would_trigger_review,
        }
