"""
Classification Pipeline

Main orchestrator for the provenance-first classification system.

Architecture:
The pipeline uses THIN BRANCH ORCHESTRATORS that override only base contrast
and construct detection for specific provenance types. All other detectors
(Technique, Modifier, Acceleration) run normally for all branches.

Flow:
1. Stage 0: Exclusion check
2. Stage 1: Provenance detection → determines branch routing
3. Stage 2: Technique detection (runs for ALL branches)
4. Stage 3: Branch logic (base/construct override for SWI, SyMRI, EPIMix)
5. Stage 4: Modifier detection (runs for ALL branches)
6. Stage 5: Acceleration detection (runs for ALL branches)
7. Stage 6: Contrast agent detection (pre/post contrast)
8. Stage 7: Body part detection (spinal cord focus)
9. Stage 8: Intent synthesis (directory_type)
10. Stage 9: Review flag aggregation

Version: 4.2.0

Changelog:
- 4.2.0: Added EPIMix branch for multicontrast EPI classification
- 4.1.0: Added ContrastDetector and BodyPartDetector
         Stage 6: Contrast agent detection (structured + text)
         Stage 7: Body part detection (spinal cord triggers review)
- 4.0.0: Implemented thin branch orchestrators for SWI and SyMRI
         All standard detectors now run for all branches
- 3.2.0: Initial provenance-first branching (placeholder branches)
"""

from pathlib import Path
from typing import Optional

from .core.context import ClassificationContext
from .core.output import ClassificationResult, create_excluded_result
from .branches import apply_swi_logic, apply_symri_logic, apply_epimix_logic, BranchResult
from .detectors import (
    ProvenanceDetector,
    TechniqueDetector,
    ModifierDetector,
    BaseContrastDetector,
    AccelerationDetector,
    ConstructDetector,
    ContrastDetector,
    BodyPartDetector,
)


class ClassificationPipeline:
    """
    Main classification orchestrator.
    
    Routes classification through provenance-first branching
    with evidence-based confidence tracking.
    
    Branch Architecture:
    - SWI branch (provenance=SWIRecon): Override base/construct for SWI outputs
    - SyMRI branch (provenance=SyMRI): Override base/construct for SyMRI outputs
    - EPIMix branch (provenance=EPIMix): Override base/construct/technique for EPIMix outputs
    - RawRecon branch (all others): Use standard detectors

    All branches get full Technique, Modifier, and Acceleration detection.
    """
    
    def __init__(self, detection_yaml_dir: Optional[str] = None):
        """
        Initialize the classification pipeline.
        
        Args:
            detection_yaml_dir: Path to YAML detection config files.
                              If None, uses default location.
        """
        if detection_yaml_dir:
            self.yaml_dir = Path(detection_yaml_dir)
        else:
            self.yaml_dir = Path(__file__).parent / "detection_yaml"
        
        # Initialize all detectors
        self.provenance_detector = ProvenanceDetector(self.yaml_dir)
        self.technique_detector = TechniqueDetector(self.yaml_dir)
        self.modifier_detector = ModifierDetector(self.yaml_dir)
        self.base_detector = BaseContrastDetector(self.yaml_dir)
        self.acceleration_detector = AccelerationDetector(self.yaml_dir)
        self.construct_detector = ConstructDetector(self.yaml_dir)
        self.contrast_detector = ContrastDetector(self.yaml_dir)
        self.body_part_detector = BodyPartDetector(self.yaml_dir)
    
    def classify(self, ctx: ClassificationContext) -> ClassificationResult:
        """
        Classify a series stack.
        
        Args:
            ctx: Classification context with all fingerprint data
        
        Returns:
            ClassificationResult with all axes and flags
        """
        # =====================================================================
        # Stage 0: Exclusion check
        # =====================================================================
        if ctx.should_exclude():
            pit = ctx.parsed_image_type
            if pit["is_screenshot"]:
                return create_excluded_result("screenshot")
            if pit["is_secondary"] and not pit["is_primary"]:
                return create_excluded_result("secondary_reformat")
            if pit["is_error"]:
                return create_excluded_result("error_map")
            return create_excluded_result("unknown")
        
        # Create result object
        result = ClassificationResult()
        
        # =====================================================================
        # Stage 1: Provenance detection
        # =====================================================================
        prov_result = self.provenance_detector.detect(ctx)
        result.set_provenance(prov_result.provenance)
        result.set_axis_result("provenance", prov_result.to_axis_result())
        
        # Get branch for routing
        branch = prov_result.branch
        
        # =====================================================================
        # Stage 2: Technique detection (runs for ALL branches)
        # =====================================================================
        tech_result = self.technique_detector.detect_technique(ctx)
        result.technique = tech_result.name  # Use display name for output
        # NOTE: Don't call set_axis_result yet - wait until after branch check
        # to avoid adding conflict review reason that branch will override

        # =====================================================================
        # Stage 3: Branch-specific base/construct/technique detection
        # =====================================================================
        branch_result = self._apply_branch_logic(branch, ctx)

        # Apply technique override from branch (e.g., EPIMix sets SE-EPI/GRE-EPI)
        # When branch overrides technique, skip conflict check (branch knows best)
        if branch_result.skip_technique_detection and branch_result.technique:
            result.technique = branch_result.technique
            # Create axis result without conflict since branch overrides
            tech_axis_result = tech_result.to_axis_result()
            tech_axis_result.has_conflict = False  # Branch knows best
            tech_axis_result.conflict_target = None
            result.set_axis_result("technique", tech_axis_result)
        else:
            # Use standard technique result with potential conflict
            result.set_axis_result("technique", tech_result.to_axis_result())

        # Apply base from branch OR standard detector
        if branch_result.skip_base_detection:
            result.base = branch_result.base
            result._confidences["base"] = branch_result.confidence
        else:
            base_result = self.base_detector.detect_base(ctx, tech_result.technique)
            result.base = base_result.base if base_result.base != "Unknown" else None
            result.set_axis_result("base", base_result.to_axis_result())
        
        # Apply construct from branch OR standard detector
        if branch_result.skip_construct_detection:
            result.construct_csv = branch_result.construct
            result._confidences["construct"] = branch_result.confidence
        else:
            construct_result = self.construct_detector.detect(ctx)
            result.construct_csv = construct_result.construct_csv
            # Note: No constructs detected is a valid, confident result
            # (means this is an original acquisition, not a derived construct)
            result._confidences["construct"] = max(
                [c.confidence for c in construct_result.constructs],
                default=0.8  # Confident that no constructs apply
            )
        
        # =====================================================================
        # Stage 4: Modifier detection (runs for ALL branches)
        # =====================================================================
        modifier_result = self.modifier_detector.detect_modifiers(ctx)
        modifiers = modifier_result.modifiers.copy()
        
        # Add branch-specific modifiers
        if branch_result.modifiers_add:
            for mod in branch_result.modifiers_add:
                if mod not in modifiers:
                    modifiers.append(mod)
        
        result.set_modifiers(modifiers)
        result.set_axis_result("modifier", modifier_result.to_axis_result())
        
        # =====================================================================
        # Stage 5: Acceleration detection (runs for ALL branches)
        # =====================================================================
        accel_result = self.acceleration_detector.detect(ctx)
        result.set_acceleration(accel_result.values)
        
        # =====================================================================
        # Stage 6: Contrast agent detection
        # =====================================================================
        contrast_result = self.contrast_detector.detect_contrast(ctx)
        result.post_contrast = contrast_result.post_contrast
        result.set_axis_result("contrast", contrast_result.to_axis_result())
        
        # =====================================================================
        # Stage 7: Body part detection (spinal cord focus)
        # Pass technique and geometry for heuristic spine scan detection
        # =====================================================================
        body_part_result = self.body_part_detector.detect_body_part(
            ctx,
            technique=tech_result.name,
            aspect_ratio=ctx.aspect_ratio,
            slices_count=ctx.stack_n_instances,
        )
        result.spinal_cord = body_part_result.spinal_cord
        result.set_axis_result("body_part", body_part_result.to_axis_result())

        # Add review flag if spine detected (via keyword or heuristic)
        if body_part_result.triggers_review:
            if body_part_result.has_conflict:
                # Heuristic triggered - add specific reason
                result.add_review_reason("body_part:heuristic")
            else:
                # Keyword match - use standard reason
                result.add_review_reason(self.body_part_detector.get_review_reason())
        
        # =====================================================================
        # Stage 8: Intent synthesis (directory_type)
        # =====================================================================
        # Handle localizer provenance
        if prov_result.provenance == "Localizer":
            result.localizer = 1
            result.directory_type = "localizer"
        elif branch_result.directory_type:
            # Branch override
            result.directory_type = branch_result.directory_type
        else:
            # Synthesize from classification axes
            result.directory_type = self._synthesize_intent(ctx, result, prov_result.provenance)
        
        # =====================================================================
        # Stage 9: Review flag aggregation
        # =====================================================================
        self._aggregate_review_flags(result, branch_result)
        
        return result
    
    def _apply_branch_logic(
        self,
        branch: str,
        ctx: ClassificationContext
    ) -> BranchResult:
        """
        Apply branch-specific classification logic.

        Args:
            branch: Branch name ("symri", "swi", "epimix", "rawrecon")
            ctx: Classification context

        Returns:
            BranchResult with any overrides
        """
        if branch == "symri":
            return apply_symri_logic(ctx)
        elif branch == "swi":
            return apply_swi_logic(ctx)
        elif branch == "epimix":
            return apply_epimix_logic(ctx)
        else:
            # RawRecon: no overrides, use standard detectors
            return BranchResult()
    
    def _synthesize_intent(
        self,
        ctx: ClassificationContext,
        result: ClassificationResult,
        provenance: str
    ) -> str:
        """
        Synthesize directory_type from detected axes.
        
        Maps classification results to BIDS-like intent per mri_intent.md:
        - anat: Anatomical (T1w, T2w, FLAIR, SWI) - NO perfusion/diffusion modifiers
        - dwi: Diffusion-weighted (base=DWI OR diffusion constructs)
        - func: Functional (BOLD time-series, keywords, provenance)
        - perf: Perfusion (base=PWI OR perfusion modifiers/constructs)
        - fmap: Field maps (B0map, PhaseMap, InPhase/OutPhase dual-echo)
        - localizer: Scout/localizer (handled separately)
        - misc: Unknown/unclassified
        
        Priority order:
        1. Provenance-based (strongest signal)
        2. Construct-based (derived maps are definitive)
        3. Base + modifier combination
        4. Text-based hints (BOLD, fMRI keywords)
        5. Default to misc
        """
        # Parse constructs and modifiers for reuse
        constructs = self._parse_csv(result.construct_csv)
        modifiers = self._parse_csv(result.modifier_csv)
        base = result.base
        technique = result.technique
        text_blob = (ctx.text_search_blob or "").lower()
        
        # =====================================================================
        # Priority 1: Provenance-based routing (strongest signal)
        # =====================================================================
        if provenance == "DTIRecon":
            return "dwi"
        if provenance in ("PerfusionRecon", "ASLRecon"):
            return "perf"
        if provenance == "BOLDRecon":
            return "func"
        # SWIRecon, SyMRI, ProjectionDerived → fall through to construct/base checks
        
        # =====================================================================
        # Priority 2: Construct-based routing (derived maps are definitive)
        # =====================================================================
        
        # Diffusion constructs → dwi
        diffusion_constructs = {"ADC", "eADC", "FA", "Trace", "MD", "AD", "RD"}
        if constructs & diffusion_constructs:
            return "dwi"
        
        # Perfusion constructs → perf
        perfusion_constructs = {"CBF", "CBV", "MTT", "Tmax", "TTP"}
        if constructs & perfusion_constructs:
            return "perf"
        
        # Field map constructs → fmap
        # B0map, PhaseMap (not SWI phase), or dual-echo InPhase/OutPhase pairs
        fmap_constructs = {"B0map"}
        if constructs & fmap_constructs:
            return "fmap"
        
        # PhaseMap + InPhase/OutPhase → fmap (dual-echo field mapping)
        # But NOT if it's SWI-related (SWIProcessed, QSM, MinIP present)
        swi_indicators = {"SWIProcessed", "QSM", "MinIP"}
        if "PhaseMap" in constructs and not (constructs & swi_indicators):
            # Check if it's dual-echo (InPhase + OutPhase) or standalone phase map
            if constructs & {"InPhase", "OutPhase"}:
                return "fmap"
            # Standalone PhaseMap without SWI context → likely fmap
            if provenance != "SWIRecon":
                return "fmap"
        
        # InPhase + OutPhase together (Dixon for field mapping) → fmap
        # But only if no other strong signals (not fat/water separation)
        if {"InPhase", "OutPhase"} <= constructs:
            # If also has Water/Fat, it's Dixon separation → anat
            if not (constructs & {"Water", "Fat", "FatFraction"}):
                return "fmap"
        
        # =====================================================================
        # Priority 3: Functional detection (BOLD, fMRI)
        # =====================================================================
        # Check for BOLD/fMRI keywords in text
        func_keywords = {"bold", "fmri", "resting state", "resting-state", 
                         "task-", "reti", "retinotopy", "functional"}
        if any(kw in text_blob for kw in func_keywords):
            # Additional validation: should be EPI-based technique
            epi_techniques = {"EPI", "GRE-EPI", "SE-EPI", "MS-EPI", "DWI"}
            if technique in epi_techniques or "epi" in text_blob:
                return "func"
        
        # =====================================================================
        # Priority 4: Base + modifier combination
        # =====================================================================
        
        # DWI base → dwi
        if base == "DWI":
            return "dwi"
        
        # PWI base → perf
        if base == "PWI":
            return "perf"
        
        # Anatomical bases → anat (but NOT if perfusion modifiers present)
        anat_bases = {"T1w", "T2w", "PDw", "T2*w", "SWI", "MTw", "T1rho", "T2rho"}
        if base in anat_bases:
            # Perfusion modifiers would override anatomical intent
            # (Currently no DSC/DCE/ASL modifiers defined, but future-proofing)
            perfusion_modifiers = {"DSC", "DCE", "ASL"}
            if not (modifiers & perfusion_modifiers):
                return "anat"
        
        # =====================================================================
        # Priority 5: Remaining construct-based routing
        # =====================================================================
        
        # SWI-related constructs → anat
        swi_constructs = {"SWIProcessed", "QSM", "MinIP", "Magnitude"}
        if constructs & swi_constructs:
            return "anat"
        
        # Projection/reformats → anat
        projection_constructs = {"MIP", "MPR"}
        if constructs & projection_constructs:
            return "anat"
        
        # Quantitative maps → anat (T1map, T2map, etc.)
        quant_constructs = {"T1map", "T2map", "PDmap", "R1map", "R2map", "B1map"}
        if constructs & quant_constructs:
            return "anat"
        
        # Synthetic weighted images → anat
        if any("Synthetic" in c for c in constructs):
            return "anat"
        
        # Dixon water/fat → anat
        dixon_constructs = {"Water", "Fat", "FatFraction"}
        if constructs & dixon_constructs:
            return "anat"
        
        # =====================================================================
        # Priority 6: Provenance fallback (weaker signals)
        # =====================================================================
        if provenance in ("SWIRecon", "SyMRI", "ProjectionDerived", "SubtractionDerived"):
            return "anat"
        
        # =====================================================================
        # Default: misc
        # =====================================================================
        return "misc"
    
    def _parse_csv(self, csv_string: str) -> set:
        """Parse comma-separated string into a set of values."""
        if not csv_string:
            return set()
        return {v.strip() for v in csv_string.split(",") if v.strip()}
    
    # Standardized confidence threshold for review flagging
    CONFIDENCE_THRESHOLD = 0.6

    def _aggregate_review_flags(
        self,
        result: ClassificationResult,
        branch_result: BranchResult
    ) -> None:
        """
        Aggregate review flags from all detections.

        Sets manual_review_required if any detection has issues.
        """
        # Check if any axis has low confidence
        for axis, confidence in result._confidences.items():
            if confidence < self.CONFIDENCE_THRESHOLD:
                result.add_review_reason(f"{axis}:low_confidence")

        # Check for missing required fields
        # Exclude func (BOLD has no base by design - measures hemodynamic signal, not tissue contrast)
        if result.base is None and result.construct_csv == "" and result.localizer == 0:
            if result.directory_type not in ("excluded", "localizer", "func"):
                result.add_review_reason("base:missing")

        # Check branch confidence (use same threshold)
        if branch_result.has_override and branch_result.confidence < self.CONFIDENCE_THRESHOLD:
            result.add_review_reason("branch:low_confidence")
    
    # =========================================================================
    # Convenience Methods
    # =========================================================================
    
    def explain_classification(self, ctx: ClassificationContext) -> dict:
        """
        Generate detailed explanation of classification decision.
        
        Useful for debugging and understanding why a stack was classified
        a certain way.
        
        Args:
            ctx: Classification context
        
        Returns:
            Dict with detailed explanation of each stage
        """
        explanation = {
            "stages": [],
            "final_result": None,
        }
        
        # Stage 0: Exclusion
        if ctx.should_exclude():
            explanation["stages"].append({
                "stage": 0,
                "name": "Exclusion",
                "result": "EXCLUDED",
                "reason": self._get_exclusion_reason(ctx),
            })
            return explanation
        
        # Stage 1: Provenance
        prov_result = self.provenance_detector.detect(ctx)
        explanation["stages"].append({
            "stage": 1,
            "name": "Provenance",
            "result": prov_result.provenance,
            "branch": prov_result.branch,
            "confidence": prov_result.confidence,
            "method": prov_result.detection_method,
        })
        
        # Stage 2: Technique
        tech_result = self.technique_detector.detect_technique(ctx)
        explanation["stages"].append({
            "stage": 2,
            "name": "Technique",
            "result": tech_result.technique,
            "family": tech_result.family,
            "confidence": tech_result.confidence,
            "method": tech_result.detection_method,
        })
        
        # Stage 3: Branch logic
        branch_result = self._apply_branch_logic(prov_result.branch, ctx)
        explanation["stages"].append({
            "stage": 3,
            "name": "Branch Logic",
            "branch": prov_result.branch,
            "base_override": branch_result.base if branch_result.skip_base_detection else "(none)",
            "construct_override": branch_result.construct if branch_result.skip_construct_detection else "(none)",
            "confidence": branch_result.confidence,
        })
        
        # Run full classification
        result = self.classify(ctx)
        explanation["final_result"] = result.to_dict()
        
        return explanation
    
    def _get_exclusion_reason(self, ctx: ClassificationContext) -> str:
        """Get reason why context was excluded."""
        pit = ctx.parsed_image_type
        if pit["is_screenshot"]:
            return "screenshot"
        if pit["is_secondary"] and not pit["is_primary"]:
            return "secondary_reformat"
        if pit["is_error"]:
            return "error_map"
        return "unknown"
