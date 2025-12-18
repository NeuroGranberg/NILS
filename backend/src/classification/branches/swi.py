"""
SWI Branch Logic

Classification branch for Susceptibility-Weighted Imaging (SWI) outputs.

SWI acquisitions produce multiple output types from a single scan:
1. Magnitude - Source magnitude image (T2*-weighted)
2. Phase - Filtered phase map (iron/calcium differentiation)
3. SWI - Processed image (magnitude × phase_mask^n)
4. MinIP - Minimum intensity projection (venogram)
5. MIP - Maximum intensity projection
6. QSM - Quantitative susceptibility map (optional)

Physics Note:
SWI requires gradient echo (T2*) readout. The 180° refocusing pulse in
spin echo nullifies susceptibility-induced phase shifts. SWI can be:
- Standard GRE-SWI: High resolution, 4-6 min
- EPI-SWI: Rapid acquisition, 1-2 min (geometric distortion trade-off)
- Multi-echo SWI/SWAN: Multiple echoes for better SNR/coverage

All SWI outputs have base="SWI" to indicate the contrast type.
The technique axis (GRE or EPI) indicates the acquisition method.

Version: 2.0.0 - Updated construct names, added MIP, text_search_blob checks
"""

from typing import Optional

from ..core.context import ClassificationContext
from ..core.evidence import Evidence, EvidenceSource
from .common import BranchResult, SWI_OUTPUT_TYPES


def apply_swi_logic(ctx: ClassificationContext) -> BranchResult:
    """
    Apply SWI-specific classification logic.

    Determines the output type (Magnitude, Phase, SWI, MinIP, MIP, QSM) and
    returns appropriate base contrast and construct overrides.

    All SWI outputs get base="SWI". The construct indicates the specific output type.

    Args:
        ctx: Classification context with fingerprint data

    Returns:
        BranchResult with base/construct overrides for SWI outputs

    Detection Priority (first match wins):
        1. QSM - Quantitative susceptibility map (highest specificity)
        2. MinIP - Minimum intensity projection
        3. MIP - Maximum intensity projection
        4. Phase - Phase map (has_phase flag, no magnitude)
        5. SWI Processed - has_swi token in ImageType (key indicator!)
        6. Magnitude - Fallback (has M but no SWI token)

    Key insight: The SWI token in ImageType indicates processed SWI output,
    regardless of ORIGINAL/DERIVED flag. Example:
        - ORIGINAL\PRIMARY\M\SWI\... = Processed SWI (has SWI token)
        - ORIGINAL\PRIMARY\M\...     = Magnitude source (no SWI token)

    Detection uses:
        - unified_flags: DICOM-derived flags (is_minip, has_phase, has_swi, etc.)
        - text_search_blob: Normalized text for keyword matching
    """
    uf = ctx.unified_flags
    text_blob = (ctx.text_search_blob or "").lower()

    # =========================================================================
    # Determine technique family (GRE or EPI) for all SWI outputs
    # =========================================================================
    # SWI is a provenance/processing method, not a technique.
    # The actual acquisition is either GRE (standard) or EPI (fast).
    #
    # Detection priority:
    # 1. has_epi flag (from DICOM tags) → EPI
    # 2. has_gre flag (from DICOM tags) → GRE
    # 3. Text fallback for ambiguous cases (e.g., GE "RM" research mode)
    #    - Check text_search_blob for "epi" keyword → EPI
    # 4. Default → GRE (standard SWI)
    if uf.get("has_epi"):
        swi_technique = "EPI"
    elif uf.get("has_gre"):
        swi_technique = "GRE"
    elif "epi" in text_blob or "3depi" in text_blob:
        # Fallback: GE scanners use "RM" (Research Mode) in scanning_sequence
        # but include "EPI" or "3DEPI" in series description (e.g., "3DEPIks")
        swi_technique = "EPI"
    else:
        swi_technique = "GRE"

    # =========================================================================
    # 1. QSM - Quantitative Susceptibility Mapping
    # =========================================================================
    # QSM is derived from phase data using dipole inversion
    # Highest specificity - check first
    if uf.get("has_qsm") or "qsm" in text_blob:
        return BranchResult(
            base="SWI",
            construct="QSM",
            technique=swi_technique,
            skip_base_detection=True,
            skip_construct_detection=True,
            skip_technique_detection=True,
            directory_type="anat",
            confidence=0.95,
            evidence=[Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags" if uf.get("has_qsm") else "text_search_blob",
                value="has_qsm" if uf.get("has_qsm") else "qsm keyword",
                target="QSM",
                weight=0.95,
                description="QSM detected → Quantitative Susceptibility Map",
            )],
        )

    # =========================================================================
    # 2. MinIP - Minimum Intensity Projection
    # =========================================================================
    # MinIP is always derived, created by taking minimum across slices
    # Used for venogram visualization (veins appear dark)
    if uf.get("is_minip") or "minip" in text_blob or "min ip" in text_blob:
        return BranchResult(
            base="SWI",
            construct="MinIP",
            technique=swi_technique,
            skip_base_detection=True,
            skip_construct_detection=True,
            skip_technique_detection=True,
            directory_type="anat",
            confidence=0.95,
            evidence=[Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags" if uf.get("is_minip") else "text_search_blob",
                value="is_minip" if uf.get("is_minip") else "minip keyword",
                target="MinIP",
                weight=0.95,
                description="MinIP detected → Minimum Intensity Projection",
            )],
        )

    # =========================================================================
    # 3. MIP - Maximum Intensity Projection
    # =========================================================================
    # MIP is derived, created by taking maximum across slices
    # Check for "mip" but NOT "minip" (which was caught above)
    is_mip_flag = uf.get("is_mip")
    is_mip_text = "mip" in text_blob and "minip" not in text_blob
    if is_mip_flag or is_mip_text:
        return BranchResult(
            base="SWI",
            construct="MIP",
            technique=swi_technique,
            skip_base_detection=True,
            skip_construct_detection=True,
            skip_technique_detection=True,
            directory_type="anat",
            confidence=0.90,
            evidence=[Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags" if is_mip_flag else "text_search_blob",
                value="is_mip" if is_mip_flag else "mip keyword",
                target="MIP",
                weight=0.90,
                description="MIP detected → Maximum Intensity Projection",
            )],
        )

    # =========================================================================
    # 4. Phase - Phase Map
    # =========================================================================
    # Phase images show phase angle, used for iron/calcium differentiation
    # Check flag OR text keyword (semantic normalizer expands "pha" → "phase")
    is_phase_flag = uf.get("has_phase") and not uf.get("has_magnitude")
    is_phase_text = "phase" in text_blob and "magnitude" not in text_blob
    if is_phase_flag or is_phase_text:
        return BranchResult(
            base="SWI",
            construct="Phase",
            technique=swi_technique,
            skip_base_detection=True,
            skip_construct_detection=True,
            skip_technique_detection=True,
            directory_type="anat",
            confidence=0.90,
            evidence=[Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags" if is_phase_flag else "text_search_blob",
                value="has_phase" if is_phase_flag else "phase keyword",
                target="Phase",
                weight=0.90,
                description="Phase detected → SWI Phase output",
            )],
        )

    # =========================================================================
    # 5. SWI - Processed/Combined SWI (by ImageType flag)
    # =========================================================================
    # The SWI token in ImageType indicates processed SWI output
    if uf.get("has_swi"):
        return BranchResult(
            base="SWI",
            construct="SWI",
            technique=swi_technique,
            skip_base_detection=True,
            skip_construct_detection=True,
            skip_technique_detection=True,
            directory_type="anat",
            confidence=0.90,
            evidence=[Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags",
                value="has_swi",
                target="SWI",
                weight=0.90,
                description="SWI token in ImageType → Processed SWI output",
            )],
        )

    # =========================================================================
    # 5.5. PROJECTION Fallback - Projections default to SWI (not Magnitude)
    # =========================================================================
    # PROJECTION images in SWI context are always processed outputs.
    # If not caught by MinIP/MIP/Phase/SWI checks above, they should be SWI.
    # They should NEVER fall through to Magnitude (which is for source data).
    if uf.get("is_projection"):
        return BranchResult(
            base="SWI",
            construct="SWI",
            technique=swi_technique,
            skip_base_detection=True,
            skip_construct_detection=True,
            skip_technique_detection=True,
            directory_type="anat",
            confidence=0.80,
            evidence=[Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="is_projection",
                value="PROJECTION IMAGE",
                target="SWI",
                weight=0.80,
                description="PROJECTION IMAGE in SWI context → Processed SWI output (fallback)",
            )],
        )

    # =========================================================================
    # 6. Magnitude - Source Magnitude Image
    # =========================================================================
    # Original magnitude is T2*-weighted, the source data before SWI processing
    # Only match when has_magnitude but NOT has_swi (SWI token = processed output)
    # The magnitude source has M token but no SWI token in ImageType
    has_magnitude_no_swi = uf.get("has_magnitude") and not uf.get("has_swi")
    if has_magnitude_no_swi or "magnitude" in text_blob:
        return BranchResult(
            base="SWI",
            construct="Magnitude",
            technique=swi_technique,
            skip_base_detection=True,
            skip_construct_detection=True,
            skip_technique_detection=True,
            directory_type="anat",
            confidence=0.85,
            evidence=[Evidence(
                source=EvidenceSource.HIGH_VALUE_TOKEN,
                field="unified_flags" if has_magnitude_no_swi else "text_search_blob",
                value="has_magnitude (no SWI token)" if has_magnitude_no_swi else "magnitude keyword",
                target="Magnitude",
                weight=0.85,
                description="Magnitude detected → SWI Magnitude source",
            )],
        )

    # =========================================================================
    # Fallback: Unknown SWI output → default to SWI
    # =========================================================================
    # If we reach here, we're in SWI branch but can't determine specific type
    # Default to SWI (not Magnitude) since we know it's SWI data
    return BranchResult(
        base="SWI",
        construct="SWI",
        technique=swi_technique,
        skip_base_detection=True,
        skip_construct_detection=True,
        skip_technique_detection=True,
        directory_type="anat",
        confidence=0.70,
        evidence=[Evidence(
            source=EvidenceSource.HIGH_VALUE_TOKEN,
            field="provenance",
            value="SWIRecon",
            target="SWI",
            weight=0.70,
            description="SWI branch, output type unclear → default to SWI",
        )],
    )


def detect_swi_output_type(ctx: ClassificationContext) -> Optional[str]:
    """
    Detect which SWI output type this stack represents.

    Args:
        ctx: Classification context

    Returns:
        Output type key from SWI_OUTPUT_TYPES, or None if unknown
    """
    uf = ctx.unified_flags
    text_blob = (ctx.text_search_blob or "").lower()

    # Check in priority order (same as apply_swi_logic)
    if uf.get("has_qsm") or "qsm" in text_blob:
        return "qsm"
    if uf.get("is_minip") or "minip" in text_blob:
        return "minip"
    if uf.get("is_mip") or ("mip" in text_blob and "minip" not in text_blob):
        return "mip"
    if uf.get("has_phase") and not uf.get("has_magnitude"):
        return "phase"
    if "phase" in text_blob and "magnitude" not in text_blob:
        return "phase"
    # SWI: has_swi flag in ImageType
    if uf.get("has_swi"):
        return "swi"
    # PROJECTION fallback: projections are SWI if not MinIP/MIP/Phase
    if uf.get("is_projection"):
        return "swi"
    # Magnitude: has M but no SWI token, or explicit "magnitude" keyword
    if (uf.get("has_magnitude") and not uf.get("has_swi")) or "magnitude" in text_blob:
        return "magnitude"

    return "swi"  # Default to SWI (we're in SWI branch, so it's SWI data)


def get_swi_output_info(output_type: str) -> dict:
    """
    Get information about an SWI output type.

    Args:
        output_type: Key from SWI_OUTPUT_TYPES

    Returns:
        Dict with base, construct, description
    """
    return SWI_OUTPUT_TYPES.get(output_type, {
        "base": "SWI",
        "construct": "SWI",
        "description": "SWI output (type unspecified)",
    })
