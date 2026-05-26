"""
STAGE Branch Logic

Classification branch for STrategically Acquired Gradient Echo (STAGE) outputs.

STAGE is a rapid multi-parametric 3D GRE protocol that acquires two
dual-echo scans at different flip angles (e.g. 6° and 24°, TR=25ms):

Raw outputs (what the scanner produces and what we classify here):
- Magnitude images at each FA/TE combination
- Phase images at each FA/TE combination

Derived outputs (post-processing, not typically stored as DICOM):
- T1WE (enhanced T1 contrast from FA subtraction)
- SWI / tSWI (from low-FA phase data)
- QSM (from multi-echo phase unwrapping)
- T1 map, PD map, R2* map

Key design decisions:
- base is NOT overridden — the standard BaseContrastDetector resolves
  PDw vs T1w from text_search_blob keywords ("proton-density" / "t1w")
- technique IS overridden to "STAGE" (replacing VIBE/FLASH)
- construct IS overridden to Magnitude or Phase (from ImageType flags)
- directory_type = "anat"

Reference:
  Chen et al. (2018) "STrategically Acquired Gradient Echo (STAGE)
  imaging, part I" Magnetic Resonance Imaging 46:130-139
  doi:10.1016/j.mri.2017.10.005

Version: 1.0.0
"""

from typing import Optional

from ..core.context import ClassificationContext
from ..core.evidence import Evidence, EvidenceSource
from .common import BranchResult, STAGE_OUTPUT_TYPES


def apply_stage_logic(ctx: ClassificationContext) -> BranchResult:
    """
    Apply STAGE-specific classification logic.

    Unlike SWI/SyMRI branches, STAGE does NOT override base detection.
    The raw echoes genuinely have tissue contrast (PDw at low FA, T1w at
    high FA) that the standard BaseContrastDetector resolves correctly
    from text_search_blob keywords.

    Only technique (→ STAGE) and construct (→ Magnitude/Phase) are set.

    Args:
        ctx: Classification context with fingerprint data

    Returns:
        BranchResult with technique and construct overrides
    """
    uf = ctx.unified_flags

    # Determine output type from ImageType flags
    has_phase = uf.get("has_phase") and not uf.get("has_magnitude")
    has_magnitude = uf.get("has_magnitude")

    if has_phase:
        construct = "Phase"
        output_key = "phase"
        evidence_value = "has_phase"
        description = "STAGE phase image (for SWI/QSM processing)"
    elif has_magnitude:
        construct = "Magnitude"
        output_key = "magnitude"
        evidence_value = "has_magnitude"
        description = "STAGE magnitude image (tissue-weighted)"
    else:
        # Fallback — most STAGE stacks are magnitude
        construct = "Magnitude"
        output_key = "magnitude"
        evidence_value = "fallback"
        description = "STAGE output type unclear → default to Magnitude"

    return BranchResult(
        base=None,                      # Let BaseContrastDetector resolve PDw/T1w
        construct=construct,
        technique="STAGE",
        skip_base_detection=False,      # ← key: base comes from standard detector
        skip_construct_detection=True,
        skip_technique_detection=True,
        directory_type="anat",
        confidence=0.90,
        evidence=[Evidence(
            source=EvidenceSource.HIGH_VALUE_TOKEN,
            field="unified_flags" if evidence_value != "fallback" else "provenance",
            value=evidence_value,
            target=construct,
            weight=0.90,
            description=description,
        )],
    )


def detect_stage_output_type(ctx: ClassificationContext) -> Optional[str]:
    """
    Detect which STAGE output type this stack represents.

    Args:
        ctx: Classification context

    Returns:
        Output type key from STAGE_OUTPUT_TYPES
    """
    uf = ctx.unified_flags

    if uf.get("has_phase") and not uf.get("has_magnitude"):
        return "phase"
    return "magnitude"


def get_stage_output_info(output_type: str) -> dict:
    """
    Get information about a STAGE output type.

    Args:
        output_type: Key from STAGE_OUTPUT_TYPES

    Returns:
        Dict with construct, description
    """
    return STAGE_OUTPUT_TYPES.get(output_type, {
        "construct": "Magnitude",
        "description": "STAGE output (type unspecified)",
    })
