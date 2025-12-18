"""
Base Contrast Detector

Detects the fundamental tissue contrast weighting of an MRI series.
Base contrast answers: "What physical contrast is this series about?"

Detection uses a four-tier approach (priority order):
1. Technique inference - Some techniques ALWAYS imply a base (MPRAGE → T1w)
2. Exclusive flags - Definitive unified_flags (is_dwi → DWI)
3. Keywords - Text search matches ("t1w" → T1w)
4. Physics ranges - TR/TE/TI thresholds by technique family (edge cases)

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
class BaseContrastResult:
    """
    Result of base contrast detection.

    Attributes:
        base: Detected base contrast (e.g., "T1w", "T2w", "DWI")
        confidence: Detection confidence (0.0 - 1.0)
        detection_method: How it was detected
        evidence: List of evidence that contributed to detection
        has_conflict: Whether text evidence contradicts the prediction
        conflicting_base: The conflicting base found in text (if any)
    """
    base: str
    confidence: float
    detection_method: str
    evidence: List[Evidence] = field(default_factory=list)
    has_conflict: bool = False
    conflicting_base: Optional[str] = None

    def to_axis_result(self) -> AxisResult:
        """Convert to AxisResult for integration with ClassificationResult."""
        return AxisResult(
            value=self.base,
            confidence=self.confidence,
            evidence=self.evidence,
            alternatives=[],
            has_conflict=self.has_conflict,
            conflict_target=self.conflicting_base,
        )


class BaseContrastDetector(BaseDetector):
    """
    Detect MRI base tissue contrast.
    
    Detection uses a four-tier approach:
    1. Technique inference - Technique implies base (MPRAGE → T1w)
    2. Exclusive flags - unified_flags that definitively indicate base
    3. Keywords - Text search in series description
    4. Physics ranges - TR/TE/TI thresholds for edge cases
    
    Special handling for:
    - FLAIR: Can be T1-FLAIR or T2-FLAIR (differentiated by TE)
    - Dual-echo PD+T2: Stack-split by TE, use TE to determine contrast
    """
    
    YAML_FILENAME = "base-detection.yaml"
    
    # Confidence thresholds
    CONFIDENCE_THRESHOLDS = {
        "technique_inference": 0.95,
        "exclusive_flag": 0.90,
        "keywords": 0.85,
        "flair_te": 0.85,
        "dual_echo_te": 0.85,
        "physics": 0.70,
        "fallback": 0.50,
    }
    
    def __init__(self, yaml_dir: Optional[Path] = None):
        """
        Initialize base contrast detector.
        
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
        
        # Load configuration
        self._bases: Dict[str, Dict[str, Any]] = self.config.get("bases", {})
        self._technique_inference: Dict[str, List] = self.config.get("technique_inference", {})
        self._flair_rules: Dict[str, Any] = self.config.get("flair_rules", {})
        self._dual_echo_rules: Dict[str, Any] = self.config.get("dual_echo_rules", {})
        self._physics_rules: Dict[str, Any] = self.config.get("physics_rules", {})
        self._rules: Dict[str, Any] = self.config.get("rules", {})
        self._priority_order: List[str] = self._rules.get("priority_order", [])
    
    @property
    def axis_name(self) -> str:
        return "base"
    
    def detect(self, ctx: ClassificationContext, technique: Optional[str] = None) -> AxisResult:
        """
        Detect base contrast.
        
        Args:
            ctx: Classification context with fingerprint data
            technique: Optional detected technique (for inference)
        
        Returns:
            AxisResult with base contrast value, confidence, and evidence
        """
        result = self.detect_base(ctx, technique)
        return result.to_axis_result()
    
    # Detection methods that are authoritative (no conflict check needed)
    # Technique inference and exclusive flags are physics-based and definitive
    AUTHORITATIVE_METHODS = {"technique_inference", "exclusive_flag"}

    def detect_base(
        self,
        ctx: ClassificationContext,
        technique: Optional[str] = None
    ) -> BaseContrastResult:
        """
        Detect base contrast with full result details.

        Args:
            ctx: Classification context
            technique: Optional detected technique for inference

        Returns:
            BaseContrastResult with detection details
        """
        uf = ctx.unified_flags
        text_blob = (ctx.text_search_blob or "").lower()

        # =====================================================================
        # TIER 1: TECHNIQUE INFERENCE (authoritative - no conflict check)
        # =====================================================================
        # Some techniques physics-lock to a specific base contrast
        if technique and technique in self._technique_inference:
            inferred = self._technique_inference[technique]
            if inferred and inferred[0]:  # [base, confidence]
                base, conf = inferred[0], inferred[1]
                evidence = Evidence(
                    source=EvidenceSource.TECHNIQUE_INFERENCE,
                    field="technique",
                    value=technique,
                    target=base,
                    weight=conf,
                    description=f"Technique {technique} implies {base} base contrast",
                )
                return BaseContrastResult(
                    base=base,
                    confidence=conf,
                    detection_method="technique_inference",
                    evidence=[evidence],
                )

        # =====================================================================
        # TIER 2: EXCLUSIVE FLAGS (authoritative - no conflict check)
        # =====================================================================
        # Check unified_flags that definitively indicate base contrast
        exclusive_result = self._check_exclusive_flags(uf)
        if exclusive_result:
            return exclusive_result

        # =====================================================================
        # TIER 3: FLAIR SPECIAL HANDLING
        # =====================================================================
        # FLAIR can be T1-FLAIR or T2-FLAIR - use TE to differentiate
        if uf.get("is_flair") or "flair" in text_blob:
            flair_result = self._detect_flair_base(ctx, text_blob)
            if flair_result:
                # Check for conflict (non-authoritative method)
                return self._add_conflict_check(flair_result, text_blob)

        # =====================================================================
        # TIER 4: DUAL-ECHO SPECIAL HANDLING
        # =====================================================================
        # PD+T2 series - use TE to determine which echo
        dual_echo_result = self._detect_dual_echo_base(ctx, text_blob)
        if dual_echo_result:
            # Check for conflict (non-authoritative method)
            return self._add_conflict_check(dual_echo_result, text_blob)

        # =====================================================================
        # TIER 5: KEYWORD MATCHING
        # =====================================================================
        keyword_result = self._detect_by_keywords(text_blob)
        if keyword_result:
            # No conflict check needed - keywords are from text itself
            return keyword_result

        # =====================================================================
        # TIER 6: PHYSICS-BASED INFERENCE
        # =====================================================================
        physics_result = self._detect_by_physics(ctx, uf)
        if physics_result:
            # Check for conflict (non-authoritative method)
            return self._add_conflict_check(physics_result, text_blob)

        # =====================================================================
        # FALLBACK: Unknown
        # =====================================================================
        return BaseContrastResult(
            base="Unknown",
            confidence=self.CONFIDENCE_THRESHOLDS["fallback"],
            detection_method="fallback",
            evidence=[],
        )

    def _add_conflict_check(
        self, result: BaseContrastResult, text_blob: str
    ) -> BaseContrastResult:
        """
        Check for text conflict and update result if found.

        Args:
            result: Detection result to check
            text_blob: Lowercase text search blob

        Returns:
            Updated result with conflict info if applicable
        """
        conflicting_base = self._check_text_conflict(result.base, text_blob)
        if conflicting_base:
            result.has_conflict = True
            result.conflicting_base = conflicting_base
        return result

    def _check_text_conflict(
        self, predicted_base: str, text_blob: str
    ) -> Optional[str]:
        """
        Check if text_search_blob conflicts with predicted base.

        Conflict = text contains OTHER base keyword AND does NOT contain
        predicted base keyword.

        This catches cases where the series description mentions a different
        base contrast than what was detected (e.g., physics said T1w but
        text says "t2w").

        Args:
            predicted_base: The base we predicted (e.g., "T1w")
            text_blob: Lowercase text search blob

        Returns:
            Name of conflicting base if conflict detected, None otherwise
        """
        if not text_blob or not predicted_base or predicted_base == "Unknown":
            return None

        # Find the base_id for predicted base (need to map name -> id)
        predicted_base_id = None
        for base_id, base_config in self._bases.items():
            if base_config.get("name") == predicted_base:
                predicted_base_id = base_id
                break

        if not predicted_base_id:
            return None  # Unknown base, can't check conflict

        # Check if predicted base keywords are in text (if so, no conflict)
        predicted_keywords = self._bases[predicted_base_id].get("keywords", [])
        if any(kw in text_blob for kw in predicted_keywords):
            return None  # Our prediction is mentioned, no conflict

        # Check if any OTHER base keywords are in text
        for base_id, base_config in self._bases.items():
            if base_id == predicted_base_id:
                continue
            if base_id == "Unknown":
                continue  # Skip Unknown base

            keywords = base_config.get("keywords", [])
            if any(kw in text_blob for kw in keywords):
                return base_config.get("name", base_id)  # Found conflicting base

        return None
    
    def _check_exclusive_flags(self, uf: Dict[str, bool]) -> Optional[BaseContrastResult]:
        """
        Check for exclusive unified_flags that definitively indicate base.
        
        Args:
            uf: unified_flags dictionary
        
        Returns:
            BaseContrastResult if exclusive flag found, None otherwise
        """
        # DWI indicators
        if uf.get("is_dwi") or uf.get("has_adc") or uf.get("has_fa") or uf.get("has_trace"):
            flags_found = [f for f in ["is_dwi", "has_adc", "has_fa", "has_trace"] if uf.get(f)]
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value=", ".join(flags_found),
                target="DWI",
                weight=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                description=f"Diffusion flags: {', '.join(flags_found)}",
            )
            return BaseContrastResult(
                base="DWI",
                confidence=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                detection_method="exclusive_flag",
                evidence=[evidence],
            )
        
        # PWI indicators
        perfusion_flags = ["is_perfusion", "is_asl", "has_cbf", "has_cbv", "has_mtt", "has_tmax", "has_ttp"]
        found_perfusion = [f for f in perfusion_flags if uf.get(f)]
        if found_perfusion:
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value=", ".join(found_perfusion),
                target="PWI",
                weight=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                description=f"Perfusion flags: {', '.join(found_perfusion)}",
            )
            return BaseContrastResult(
                base="PWI",
                confidence=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                detection_method="exclusive_flag",
                evidence=[evidence],
            )
        
        # SWI indicators
        if uf.get("is_swi") or uf.get("has_swi"):
            flags_found = [f for f in ["is_swi", "has_swi"] if uf.get(f)]
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value=", ".join(flags_found),
                target="SWI",
                weight=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                description=f"SWI flags: {', '.join(flags_found)}",
            )
            return BaseContrastResult(
                base="SWI",
                confidence=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                detection_method="exclusive_flag",
                evidence=[evidence],
            )
        
        # MTw indicator
        if uf.get("has_mtc"):
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value="has_mtc",
                target="MTw",
                weight=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                description="Magnetization transfer contrast flag",
            )
            return BaseContrastResult(
                base="MTw",
                confidence=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                detection_method="exclusive_flag",
                evidence=[evidence],
            )
        
        # =====================================================================
        # SYNTHETIC MRI OUTPUTS
        # =====================================================================
        # Synthetic images have a base contrast (T1w, T2w, PDw, etc.)
        # These are derived from quantitative maps but represent tissue contrast
        
        # Synthetic T1w
        if uf.get("has_t1_synthetic"):
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value="has_t1_synthetic",
                target="T1w",
                weight=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                description="Synthetic T1-weighted image",
            )
            return BaseContrastResult(
                base="T1w",
                confidence=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                detection_method="exclusive_flag",
                evidence=[evidence],
            )
        
        # Synthetic T2w
        if uf.get("has_t2_synthetic"):
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value="has_t2_synthetic",
                target="T2w",
                weight=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                description="Synthetic T2-weighted image",
            )
            return BaseContrastResult(
                base="T2w",
                confidence=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                detection_method="exclusive_flag",
                evidence=[evidence],
            )
        
        # Synthetic FLAIR (usually T2-FLAIR)
        if uf.get("has_flair_synthetic"):
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value="has_flair_synthetic",
                target="T2w",
                weight=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                description="Synthetic FLAIR image (T2-weighted base)",
            )
            return BaseContrastResult(
                base="T2w",
                confidence=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                detection_method="exclusive_flag",
                evidence=[evidence],
            )
        
        # Synthetic PDw
        if uf.get("has_pd_synthetic"):
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value="has_pd_synthetic",
                target="PDw",
                weight=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                description="Synthetic PD-weighted image",
            )
            return BaseContrastResult(
                base="PDw",
                confidence=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                detection_method="exclusive_flag",
                evidence=[evidence],
            )
        
        # Synthetic DIR (Double IR nulls CSF+WM → T2w-like base)
        if uf.get("is_dir") and uf.get("is_synthetic"):
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value="is_dir + is_synthetic",
                target="T2w",
                weight=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                description="Synthetic DIR image (T2-weighted base)",
            )
            return BaseContrastResult(
                base="T2w",
                confidence=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                detection_method="exclusive_flag",
                evidence=[evidence],
            )
        
        # Synthetic PSIR (Phase-sensitive IR → T1w base)
        if uf.get("has_psir") and uf.get("is_synthetic"):
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value="has_psir + is_synthetic",
                target="T1w",
                weight=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                description="Synthetic PSIR image (T1-weighted base)",
            )
            return BaseContrastResult(
                base="T1w",
                confidence=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                detection_method="exclusive_flag",
                evidence=[evidence],
            )
        
        # =====================================================================
        # QUANTITATIVE MAPS
        # =====================================================================
        # T1 map, T2 map, R1, R2 are parameter maps but still have implied base
        # T1 map → measured T1, related to T1w contrast
        # T2 map → measured T2, related to T2w contrast
        
        # T1 map → T1w base (it's measuring T1 relaxation)
        if uf.get("has_t1_map") or uf.get("has_r1"):
            flags_found = [f for f in ["has_t1_map", "has_r1"] if uf.get(f)]
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value=", ".join(flags_found),
                target="T1w",
                weight=self.CONFIDENCE_THRESHOLDS["keywords"],  # Lower confidence for maps
                description=f"T1 relaxometry map: {', '.join(flags_found)}",
            )
            return BaseContrastResult(
                base="T1w",
                confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                detection_method="exclusive_flag",
                evidence=[evidence],
            )
        
        # T2 map → T2w base (it's measuring T2 relaxation)
        if uf.get("has_t2_map") or uf.get("has_r2"):
            flags_found = [f for f in ["has_t2_map", "has_r2"] if uf.get(f)]
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value=", ".join(flags_found),
                target="T2w",
                weight=self.CONFIDENCE_THRESHOLDS["keywords"],
                description=f"T2 relaxometry map: {', '.join(flags_found)}",
            )
            return BaseContrastResult(
                base="T2w",
                confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                detection_method="exclusive_flag",
                evidence=[evidence],
            )
        
        # PD map from QMAP - check ImageType tokens
        if uf.get("is_qmap"):
            # Get parsed_image_type tokens to check for PD
            # This is a bit of a workaround since we don't have a direct flag
            # We'll check in keyword detection for this case
            pass

        # =====================================================================
        # MP2RAGE OUTPUTS
        # =====================================================================
        # MP2RAGE is fundamentally a T1-weighted technique.
        # All outputs (INV1, INV2, UNI, UNI-DEN) get T1w base contrast.
        # The specific output type (INV1, INV2, etc.) is captured as a construct.
        # T1map is already handled by has_t1_map above.

        # MP2RAGE INV1 or INV2 - both are T1w base
        if uf.get("is_mp2rage_inv1") or uf.get("is_mp2rage_inv2"):
            flag = "is_mp2rage_inv1" if uf.get("is_mp2rage_inv1") else "is_mp2rage_inv2"
            inv_type = "INV1" if uf.get("is_mp2rage_inv1") else "INV2"
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value=flag,
                target="T1w",
                weight=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                description=f"MP2RAGE {inv_type} → T1-weighted (technique is T1w)",
            )
            return BaseContrastResult(
                base="T1w",
                confidence=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                detection_method="exclusive_flag",
                evidence=[evidence],
            )

        # MP2RAGE Uniform (UNI/UNI-DEN) - bias-corrected T1w
        if uf.get("has_uniform") or uf.get("is_uniform_denoised"):
            flags_found = [f for f in ["has_uniform", "is_uniform_denoised"] if uf.get(f)]
            evidence = Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value=", ".join(flags_found),
                target="T1w",
                weight=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                description="MP2RAGE Uniform image → T1-weighted (bias-corrected)",
            )
            return BaseContrastResult(
                base="T1w",
                confidence=self.CONFIDENCE_THRESHOLDS["exclusive_flag"],
                detection_method="exclusive_flag",
                evidence=[evidence],
            )

        return None
    
    def _detect_flair_base(
        self,
        ctx: ClassificationContext,
        text_blob: str
    ) -> Optional[BaseContrastResult]:
        """
        Detect T1-FLAIR vs T2-FLAIR using TE.
        
        T1-FLAIR: TE < 40ms (short TE for T1 weighting)
        T2-FLAIR: TE >= 40ms (long TE for T2 weighting)
        
        Args:
            ctx: Classification context
            text_blob: Lowercase text search blob
        
        Returns:
            BaseContrastResult if FLAIR detected, None otherwise
        """
        flair_rules = self._flair_rules
        te_threshold = flair_rules.get("te_threshold", 40)
        
        # Check for T1 or T2 keyword in text (prioritize text over TE)
        # Data analysis shows: 97% of "t1"-keyword FLAIR have TE<40ms, 99.9% of "t2"-keyword FLAIR have TE>=80ms
        if "t1" in text_blob:
            evidence = Evidence(
                source=EvidenceSource.TEXT_SEARCH,
                field="text_search_blob",
                value="t1",
                target="T1w",
                weight=self.CONFIDENCE_THRESHOLDS["keywords"],
                description="T1 keyword in FLAIR series → T1-FLAIR",
            )
            return BaseContrastResult(
                base="T1w",
                confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                detection_method="keywords",
                evidence=[evidence],
            )

        if "t2" in text_blob:
            evidence = Evidence(
                source=EvidenceSource.TEXT_SEARCH,
                field="text_search_blob",
                value="t2",
                target="T2w",
                weight=self.CONFIDENCE_THRESHOLDS["keywords"],
                description="T2 keyword in FLAIR series → T2-FLAIR",
            )
            return BaseContrastResult(
                base="T2w",
                confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                detection_method="keywords",
                evidence=[evidence],
            )
        
        # Use TE to differentiate generic FLAIR
        if ctx.mr_te is not None:
            if ctx.mr_te < te_threshold:
                evidence = Evidence(
                    source=EvidenceSource.PHYSICS_DISTINCT,
                    field="mr_te",
                    value=f"{ctx.mr_te:.1f}ms",
                    target="T1w",
                    weight=self.CONFIDENCE_THRESHOLDS["flair_te"],
                    description=f"FLAIR with TE={ctx.mr_te:.1f}ms < {te_threshold}ms → T1-FLAIR",
                )
                return BaseContrastResult(
                    base="T1w",
                    confidence=self.CONFIDENCE_THRESHOLDS["flair_te"],
                    detection_method="flair_te",
                    evidence=[evidence],
                )
            else:
                evidence = Evidence(
                    source=EvidenceSource.PHYSICS_DISTINCT,
                    field="mr_te",
                    value=f"{ctx.mr_te:.1f}ms",
                    target="T2w",
                    weight=self.CONFIDENCE_THRESHOLDS["flair_te"],
                    description=f"FLAIR with TE={ctx.mr_te:.1f}ms >= {te_threshold}ms → T2-FLAIR",
                )
                return BaseContrastResult(
                    base="T2w",
                    confidence=self.CONFIDENCE_THRESHOLDS["flair_te"],
                    detection_method="flair_te",
                    evidence=[evidence],
                )
        
        # Default FLAIR without TE → assume T2-FLAIR (most common)
        default_rules = flair_rules.get("default", {})
        evidence = Evidence(
            source=EvidenceSource.TEXT_SEARCH,
            field="text_search_blob",
            value="flair",
            target=default_rules.get("base", "T2w"),
            weight=default_rules.get("confidence", 0.70),
            description="Generic FLAIR without TE → default to T2-FLAIR",
        )
        return BaseContrastResult(
            base=default_rules.get("base", "T2w"),
            confidence=default_rules.get("confidence", 0.70),
            detection_method="flair_default",
            evidence=[evidence],
        )
    
    def _detect_dual_echo_base(
        self,
        ctx: ClassificationContext,
        text_blob: str
    ) -> Optional[BaseContrastResult]:
        """
        Detect PD vs T2 in dual-echo series using TE.

        PD echo: TE < 40ms
        T2 echo: TE >= 40ms

        Args:
            ctx: Classification context
            text_blob: Lowercase text search blob

        Returns:
            BaseContrastResult if dual-echo detected, None otherwise
        """
        dual_echo_rules = self._dual_echo_rules

        # Robust dual-echo detection: check if BOTH PD and T2 keywords appear
        # This handles any separator/ordering (pd+t2, t2/pd, proton-density + t2w, etc.)
        pd_keywords = dual_echo_rules.get("pd_keywords", [])
        t2_keywords = dual_echo_rules.get("t2_keywords", [])

        has_pd = any(kw in text_blob for kw in pd_keywords)
        has_t2 = any(kw in text_blob for kw in t2_keywords)

        # Only trigger dual-echo logic if BOTH contrasts mentioned
        is_dual_echo = has_pd and has_t2

        if not is_dual_echo:
            return None
        
        te_threshold = dual_echo_rules.get("te_threshold", 40)
        
        if ctx.mr_te is not None:
            if ctx.mr_te < te_threshold:
                evidence = Evidence(
                    source=EvidenceSource.PHYSICS_DISTINCT,
                    field="mr_te",
                    value=f"{ctx.mr_te:.1f}ms",
                    target="PDw",
                    weight=self.CONFIDENCE_THRESHOLDS["dual_echo_te"],
                    description=f"Dual-echo PD+T2: TE={ctx.mr_te:.1f}ms < {te_threshold}ms → PD echo",
                )
                return BaseContrastResult(
                    base="PDw",
                    confidence=self.CONFIDENCE_THRESHOLDS["dual_echo_te"],
                    detection_method="dual_echo_te",
                    evidence=[evidence],
                )
            else:
                evidence = Evidence(
                    source=EvidenceSource.PHYSICS_DISTINCT,
                    field="mr_te",
                    value=f"{ctx.mr_te:.1f}ms",
                    target="T2w",
                    weight=self.CONFIDENCE_THRESHOLDS["dual_echo_te"],
                    description=f"Dual-echo PD+T2: TE={ctx.mr_te:.1f}ms >= {te_threshold}ms → T2 echo",
                )
                return BaseContrastResult(
                    base="T2w",
                    confidence=self.CONFIDENCE_THRESHOLDS["dual_echo_te"],
                    detection_method="dual_echo_te",
                    evidence=[evidence],
                )
        
        # No TE available for dual-echo → can't determine
        return None
    
    def _detect_by_keywords(self, text_blob: str) -> Optional[BaseContrastResult]:
        """
        Detect base contrast using keyword matching.
        
        Args:
            text_blob: Lowercase text search blob
        
        Returns:
            BaseContrastResult if keyword match found, None otherwise
        """
        # Check in priority order
        for base_id in self._priority_order:
            if base_id not in self._bases:
                continue
            
            base_config = self._bases[base_id]
            keywords = base_config.get("keywords", [])
            
            if not keywords:
                continue
            
            matched = match_any_keyword(text_blob, keywords)
            if matched:
                # Use the 'name' field from config (e.g., "T2*w") not the dict key (e.g., "T2starw")
                base_name = base_config.get("name", base_id)
                evidence = Evidence(
                    source=EvidenceSource.TEXT_SEARCH,
                    field="text_search_blob",
                    value=matched,
                    target=base_name,
                    weight=self.CONFIDENCE_THRESHOLDS["keywords"],
                    description=f"Keyword '{matched}' matched for {base_name}",
                )
                return BaseContrastResult(
                    base=base_name,
                    confidence=self.CONFIDENCE_THRESHOLDS["keywords"],
                    detection_method="keywords",
                    evidence=[evidence],
                )
        
        return None
    
    def _detect_by_physics(
        self,
        ctx: ClassificationContext,
        uf: Dict[str, bool]
    ) -> Optional[BaseContrastResult]:
        """
        Detect base contrast using TR/TE/TI physics ranges.
        
        Thresholds are technique-family-specific.
        
        Args:
            ctx: Classification context
            uf: unified_flags dictionary
        
        Returns:
            BaseContrastResult if physics match, None otherwise
        """
        physics_rules = self._physics_rules
        tr = ctx.mr_tr
        te = ctx.mr_te
        ti = ctx.mr_ti
        
        # Determine technique family
        is_se = uf.get("has_se", False)
        is_gre = uf.get("has_gre", False)
        is_ir = uf.get("has_ir", False)
        is_epi = uf.get("has_epi", False)
        
        # === IR FAMILY (check first - has TI) ===
        if is_ir and ti is not None:
            ir_rules = physics_rules.get("ir", {})
            
            # Short TI → STIR-like (fat nulling, T2-weighted base)
            stir_rules = ir_rules.get("stir_like", {})
            if ti < stir_rules.get("ti_max", 300):
                evidence = Evidence(
                    source=EvidenceSource.PHYSICS_DISTINCT,
                    field="mr_ti",
                    value=f"{ti:.0f}ms",
                    target=stir_rules.get("base", "T2w"),
                    weight=stir_rules.get("confidence", 0.75),
                    description=f"IR with TI={ti:.0f}ms < 300ms → STIR-like (T2w base)",
                )
                return BaseContrastResult(
                    base=stir_rules.get("base", "T2w"),
                    confidence=stir_rules.get("confidence", 0.75),
                    detection_method="physics",
                    evidence=[evidence],
                )
            
            # Medium TI → standard IR (T1w)
            std_ir = ir_rules.get("standard_ir", {})
            ti_min = std_ir.get("ti_min", 300)
            ti_max = std_ir.get("ti_max", 1500)
            if ti_min <= ti <= ti_max:
                evidence = Evidence(
                    source=EvidenceSource.PHYSICS_DISTINCT,
                    field="mr_ti",
                    value=f"{ti:.0f}ms",
                    target=std_ir.get("base", "T1w"),
                    weight=std_ir.get("confidence", 0.70),
                    description=f"IR with TI={ti:.0f}ms in {ti_min}-{ti_max}ms → standard IR (T1w)",
                )
                return BaseContrastResult(
                    base=std_ir.get("base", "T1w"),
                    confidence=std_ir.get("confidence", 0.70),
                    detection_method="physics",
                    evidence=[evidence],
                )
            
            # Long TI → FLAIR-like (handled in _detect_flair_base)
            # Fall through to SE/GRE rules
        
        # === SE FAMILY (Spin Echo) ===
        if is_se and not is_epi:
            se_rules = physics_rules.get("se", {})
            
            # T1w: TR < 1000ms AND TE < 30ms
            t1_rules = se_rules.get("t1w", {})
            if tr is not None and te is not None:
                if tr < t1_rules.get("tr_max", 1000) and te < t1_rules.get("te_max", 30):
                    evidence = Evidence(
                        source=EvidenceSource.PHYSICS_DISTINCT,
                        field="mr_tr, mr_te",
                        value=f"TR={tr:.0f}ms, TE={te:.1f}ms",
                        target="T1w",
                        weight=t1_rules.get("confidence", 0.70),
                        description=f"SE with TR={tr:.0f}ms < 1000 and TE={te:.1f}ms < 30 → T1w",
                    )
                    return BaseContrastResult(
                        base="T1w",
                        confidence=t1_rules.get("confidence", 0.70),
                        detection_method="physics",
                        evidence=[evidence],
                    )
            
            # T2w: TR > 2000ms AND TE > 50ms
            t2_rules = se_rules.get("t2w", {})
            if tr is not None and te is not None:
                if tr > t2_rules.get("tr_min", 2000) and te > t2_rules.get("te_min", 50):
                    evidence = Evidence(
                        source=EvidenceSource.PHYSICS_DISTINCT,
                        field="mr_tr, mr_te",
                        value=f"TR={tr:.0f}ms, TE={te:.1f}ms",
                        target="T2w",
                        weight=t2_rules.get("confidence", 0.70),
                        description=f"SE with TR={tr:.0f}ms > 2000 and TE={te:.1f}ms > 50 → T2w",
                    )
                    return BaseContrastResult(
                        base="T2w",
                        confidence=t2_rules.get("confidence", 0.70),
                        detection_method="physics",
                        evidence=[evidence],
                    )
            
            # PDw: TR > 2000ms AND TE < 30ms
            pd_rules = se_rules.get("pdw", {})
            if tr is not None and te is not None:
                if tr > pd_rules.get("tr_min", 2000) and te < pd_rules.get("te_max", 30):
                    evidence = Evidence(
                        source=EvidenceSource.PHYSICS_DISTINCT,
                        field="mr_tr, mr_te",
                        value=f"TR={tr:.0f}ms, TE={te:.1f}ms",
                        target="PDw",
                        weight=pd_rules.get("confidence", 0.65),
                        description=f"SE with TR={tr:.0f}ms > 2000 and TE={te:.1f}ms < 30 → PDw",
                    )
                    return BaseContrastResult(
                        base="PDw",
                        confidence=pd_rules.get("confidence", 0.65),
                        detection_method="physics",
                        evidence=[evidence],
                    )
        
        # === GRE FAMILY (Gradient Echo) ===
        if is_gre and not is_epi:
            gre_rules = physics_rules.get("gre", {})
            
            # T1w: TE < 10ms
            t1_rules = gre_rules.get("t1w", {})
            if te is not None and te < t1_rules.get("te_max", 10):
                evidence = Evidence(
                    source=EvidenceSource.PHYSICS_DISTINCT,
                    field="mr_te",
                    value=f"TE={te:.1f}ms",
                    target="T1w",
                    weight=t1_rules.get("confidence", 0.70),
                    description=f"GRE with TE={te:.1f}ms < 10 → T1w",
                )
                return BaseContrastResult(
                    base="T1w",
                    confidence=t1_rules.get("confidence", 0.70),
                    detection_method="physics",
                    evidence=[evidence],
                )
            
            # T2*w: TE > 15ms
            t2star_rules = gre_rules.get("t2starw", {})
            if te is not None and te > t2star_rules.get("te_min", 15):
                evidence = Evidence(
                    source=EvidenceSource.PHYSICS_DISTINCT,
                    field="mr_te",
                    value=f"TE={te:.1f}ms",
                    target="T2*w",
                    weight=t2star_rules.get("confidence", 0.70),
                    description=f"GRE with TE={te:.1f}ms > 15 → T2*w",
                )
                return BaseContrastResult(
                    base="T2*w",
                    confidence=t2star_rules.get("confidence", 0.70),
                    detection_method="physics",
                    evidence=[evidence],
                )
        
        return None
    
    def get_all_bases(self) -> List[str]:
        """
        Get list of all valid base contrast values from YAML config.

        Returns:
            List of base contrast names (e.g., ["T1w", "T2w", "PDw", ...])
        """
        return [config.get("name", base_id) for base_id, config in self._bases.items()]

    def explain_detection(
        self,
        ctx: ClassificationContext,
        technique: Optional[str] = None
    ) -> str:
        """
        Generate human-readable explanation of base detection.
        
        Args:
            ctx: Classification context
            technique: Optional detected technique
        
        Returns:
            Multi-line explanation string
        """
        result = self.detect_base(ctx, technique)
        
        lines = [
            f"Base Contrast Detection: {result.base}",
            f"Confidence: {result.confidence:.0%}",
            f"Method: {result.detection_method}",
            "",
            "Evidence:",
        ]
        
        for ev in result.evidence:
            lines.append(f"  - {ev.description}")
        
        if not result.evidence:
            lines.append("  (no explicit evidence - fallback)")
        
        return "\n".join(lines)
