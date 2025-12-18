"""
Branch Common Module

Shared data structures and utilities for classification branches.

A branch is a thin orchestrator that overrides ONLY base contrast and
construct detection for specific provenance types. All other detectors
(Technique, Modifier, Acceleration) run normally for all branches.

Version: 1.0.0
"""

from dataclasses import dataclass, field
from typing import List, Optional

from ..core.evidence import Evidence


@dataclass
class BranchResult:
    """
    Result from a classification branch.
    
    Branches return this to indicate what should be overridden
    in the pipeline. Only non-None values are used.
    
    Attributes:
        base: Override base contrast (e.g., "T1w", "T2w", None for no base)
        construct: Override construct CSV (e.g., "ADC", "T1map,T2map")
        modifiers_add: Additional modifiers to add (branch-specific)
        skip_base_detection: If True, skip BaseContrastDetector entirely
        skip_construct_detection: If True, skip ConstructDetector entirely
        directory_type: Override directory type intent (e.g., "anat", "dwi")
        confidence: Confidence in this branch result (0.0 - 1.0)
        evidence: List of evidence that led to this result
        
    Usage:
        # SWI MinIP output - override base and construct
        return BranchResult(
            base="SWI",
            construct="MinIP",
            skip_base_detection=True,
            skip_construct_detection=True,
            directory_type="anat",
            confidence=0.95,
        )
        
        # SyMRI T1map - no base contrast, just construct
        return BranchResult(
            base=None,  # Explicitly no base
            construct="T1map",
            skip_base_detection=True,
            skip_construct_detection=True,
            directory_type="anat",
            confidence=0.95,
        )
        
        # Let standard detectors handle it
        return BranchResult()  # All defaults, nothing overridden
    """
    
    # === Override Values ===
    base: Optional[str] = None
    construct: str = ""
    modifiers_add: Optional[List[str]] = None
    technique: Optional[str] = None  # Override technique (e.g., "GRE", "EPI")

    # === Skip Flags ===
    skip_base_detection: bool = False
    skip_construct_detection: bool = False
    skip_technique_detection: bool = False  # If True, use technique override

    # === Intent Override ===
    directory_type: Optional[str] = None
    
    # === Confidence & Evidence ===
    confidence: float = 0.0
    evidence: List[Evidence] = field(default_factory=list)
    
    @property
    def has_override(self) -> bool:
        """Check if this result has any overrides."""
        return (
            self.base is not None or
            self.construct != "" or
            self.modifiers_add is not None or
            self.technique is not None or
            self.skip_base_detection or
            self.skip_construct_detection or
            self.skip_technique_detection or
            self.directory_type is not None
        )
    
    @property
    def base_is_set(self) -> bool:
        """
        Check if base was explicitly set (even to None).
        
        This is important because base=None means "no base contrast"
        which is different from "didn't set base" (let detector run).
        
        When skip_base_detection=True, we use the base value as-is.
        When skip_base_detection=False, base is ignored.
        """
        return self.skip_base_detection
    
    def get_modifiers_to_add(self) -> List[str]:
        """Get modifiers to add (empty list if None)."""
        return self.modifiers_add or []


# =============================================================================
# Branch Output Types
# =============================================================================

# Output type definitions for SWI branch
# All SWI outputs have base="SWI" to indicate the contrast type
SWI_OUTPUT_TYPES = {
    "magnitude": {
        "base": "SWI",
        "construct": "Magnitude",
        "description": "SWI magnitude source image (T2*-weighted)",
    },
    "phase": {
        "base": "SWI",
        "construct": "Phase",
        "description": "SWI phase map (iron/calcium differentiation)",
    },
    "swi": {
        "base": "SWI",
        "construct": "SWI",
        "description": "Processed SWI image (magnitude × phase mask)",
    },
    "minip": {
        "base": "SWI",
        "construct": "MinIP",
        "description": "Minimum intensity projection (venogram)",
    },
    "mip": {
        "base": "SWI",
        "construct": "MIP",
        "description": "Maximum intensity projection",
    },
    "qsm": {
        "base": "SWI",
        "construct": "QSM",
        "description": "Quantitative susceptibility map (ppm)",
    },
}

# Output type definitions for SyMRI branch
SYMRI_OUTPUT_TYPES = {
    # Raw source components (Magnitude and Phase)
    "magnitude": {
        "base": None,
        "construct": "Magnitude",
        "description": "SyMRI raw magnitude source",
    },
    "phase": {
        "base": None,
        "construct": "Phase",
        "description": "SyMRI raw phase source",
    },
    "source_mdme": {
        "base": None,
        "construct": "Magnitude",
        "description": "MDME raw acquisition data",
    },
    "source_qalas": {
        "base": None,
        "construct": "Magnitude",
        "description": "3D-QALAS raw acquisition data",
    },

    # Quantitative maps
    "multi_qmap": {
        "base": None,
        "construct": "MultiQmap",
        "description": "Combined T1/T2/PD quantitative maps (GE MAGiC bundled)",
    },
    "t1_map": {
        "base": None,
        "construct": "T1map",
        "description": "T1 relaxation time map",
    },
    "t2_map": {
        "base": None,
        "construct": "T2map",
        "description": "T2 relaxation time map",
    },
    "pd_map": {
        "base": None,
        "construct": "PDmap",
        "description": "Proton density map",
    },
    "r1_map": {
        "base": None,
        "construct": "R1map",
        "description": "R1 (1/T1) map",
    },
    "r2_map": {
        "base": None,
        "construct": "R2map",
        "description": "R2 (1/T2) map",
    },
    "b1_map": {
        "base": None,
        "construct": "B1map",
        "description": "B1 field map",
    },
    
    # Synthetic weighted
    "synthetic_t1w": {
        "base": "T1w",
        "construct": "SyntheticT1w",
        "description": "Synthetic T1-weighted image",
    },
    "synthetic_t2w": {
        "base": "T2w",
        "construct": "SyntheticT2w",
        "description": "Synthetic T2-weighted image",
    },
    "synthetic_pdw": {
        "base": "PDw",
        "construct": "SyntheticPDw",
        "description": "Synthetic PD-weighted image",
    },
    "synthetic_flair": {
        "base": "T2w",
        "construct": "SyntheticFLAIR",
        "modifiers": ["FLAIR"],
        "description": "Synthetic FLAIR image",
    },
    "synthetic_dir": {
        "base": "T2w",
        "construct": "SyntheticDIR",
        "modifiers": ["DIR"],
        "description": "Synthetic DIR image",
    },
    "synthetic_psir": {
        "base": "T1w",
        "construct": "SyntheticPSIR",
        "modifiers": ["PSIR"],
        "description": "Synthetic PSIR image",
    },
    "synthetic_stir": {
        "base": "T2w",
        "construct": "SyntheticSTIR",
        "modifiers": ["STIR"],
        "description": "Synthetic STIR image",
    },
    
    # Myelin
    "myelin_map": {
        "base": None,
        "construct": "MyelinMap",
        "description": "Myelin water fraction map",
    },
}

# Output type definitions for EPIMix/NeuroMix branch
# EPIMix produces 6 contrasts, NeuroMix adds SSFSE/FSE/3D-EPI variants
EPIMIX_OUTPUT_TYPES = {
    # === SHARED (EPIMix + NeuroMix) - EPI readouts ===
    "t1_flair": {
        "base": "T1w",           # From base-detection.yaml
        "construct": "",
        "modifiers": ["FLAIR"],  # From modifier-detection.yaml
        "technique": "SE-EPI",   # From technique-detection.yaml (MIXED family)
        "directory_type": "anat",
        "description": "T1-FLAIR (CSF-nulled T1w, short TI)",
    },
    "t2star": {
        "base": "T2*w",          # From base-detection.yaml (T2starw key, name="T2*w")
        "construct": "",
        "modifiers": None,
        "technique": "GRE-EPI",  # From technique-detection.yaml (MIXED family)
        "directory_type": "anat",
        "description": "T2*-w (gradient echo, no refocusing)",
    },
    "iso_dwi": {
        "base": "DWI",           # From base-detection.yaml
        "construct": "isoDWI",
        "modifiers": None,
        "technique": "SE-EPI",   # From technique-detection.yaml (MIXED family)
        "directory_type": "dwi",
        "description": "Isotropic DWI (averaged 3-direction)",
    },
    "adc": {
        "base": "DWI",           # From base-detection.yaml
        "construct": "ADC",
        "modifiers": None,
        "technique": "SE-EPI",   # From technique-detection.yaml (MIXED family)
        "directory_type": "dwi",
        "description": "ADC map",
    },
    "t2w_b0": {
        "base": "T2w",           # From base-detection.yaml
        "construct": "",
        "modifiers": None,
        "technique": "SE-EPI",   # From technique-detection.yaml (MIXED family)
        "directory_type": "anat",
        "description": "T2-w (b=0 from DWI block)",
    },

    # === EPIMIX-ONLY (EPI readout for T2-FLAIR) ===
    "t2_flair_epi": {
        "base": "T2w",           # From base-detection.yaml
        "construct": "",
        "modifiers": ["FLAIR"],  # From modifier-detection.yaml
        "technique": "SE-EPI",   # From technique-detection.yaml (MIXED family)
        "directory_type": "anat",
        "description": "EPIMix T2-FLAIR (CSF-nulled T2w, EPI readout)",
    },
    "t2w_epi": {
        "base": "T2w",           # From base-detection.yaml
        "construct": "",
        "modifiers": None,
        "technique": "SE-EPI",   # From technique-detection.yaml (MIXED family)
        "directory_type": "anat",
        "description": "EPIMix T2-w (EPI readout)",
    },

    # === NEUROMIX-ONLY (SSFSE readout - no EPI distortions) ===
    "t2_flair_ssfse": {
        "base": "T2w",           # From base-detection.yaml
        "construct": "",
        "modifiers": ["FLAIR"],  # From modifier-detection.yaml
        "technique": "HASTE",    # From technique-detection.yaml: SS-TSE name="HASTE" (SSFSE is GE name)
        "directory_type": "anat",
        "description": "NeuroMix T2-FLAIR (SSFSE readout, no EPI distortion)",
    },
    "t2w_ssfse": {
        "base": "T2w",           # From base-detection.yaml
        "construct": "",
        "modifiers": None,
        "technique": "HASTE",    # From technique-detection.yaml: SS-TSE name="HASTE"
        "directory_type": "anat",
        "description": "NeuroMix T2w (SSFSE readout)",
    },

    # === NEUROMIX OPTIONAL (FSE multishot) ===
    "t2w_fse": {
        "base": "T2w",           # From base-detection.yaml
        "construct": "",
        "modifiers": None,
        "technique": "TSE",      # From technique-detection.yaml: TSE name="TSE" (FSE is GE name)
        "directory_type": "anat",
        "description": "NeuroMix T2w (multishot FSE, high resolution)",
    },

    # === NEUROMIX OPTIONAL (3D-EPI) ===
    "t1w_3depi": {
        "base": "T1w",           # From base-detection.yaml
        "construct": "",
        "modifiers": None,
        "technique": "EPI",      # From technique-detection.yaml: EPI (generic 3D-EPI)
        "directory_type": "anat",
        "description": "NeuroMix T1w (3D-EPI, isotropic resolution)",
    },
    "swi_3depi": {
        "base": "SWI",           # From base-detection.yaml
        "construct": "SWI",
        "modifiers": None,
        "technique": "EPI",      # From technique-detection.yaml: EPI (generic 3D-EPI)
        "directory_type": "anat",
        "description": "NeuroMix SWI (3D-EPI)",
    },
}

# Output type definitions for Dixon (handled in rawrecon branch)
DIXON_OUTPUT_TYPES = {
    "water": {
        "base": None,
        "construct": "Water",
        "description": "Dixon water-only image",
    },
    "fat": {
        "base": None,
        "construct": "Fat",
        "description": "Dixon fat-only image",
    },
    "in_phase": {
        "base": None,
        "construct": "InPhase",
        "description": "Dixon in-phase image",
    },
    "out_phase": {
        "base": None,
        "construct": "OutPhase",
        "description": "Dixon opposed-phase image",
    },
    "fat_fraction": {
        "base": None,
        "construct": "FatFraction",
        "description": "Proton density fat fraction map",
    },
}
