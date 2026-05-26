"""
SWI Branch Logic

Classification branch for Susceptibility-Weighted Imaging (SWI) and
Quantitative Susceptibility Mapping (QSM) outputs.

SWI/QSM acquisitions produce multiple output types from a single
multi-echo GRE scan:
1. Magnitude - Source magnitude image (T2*-weighted)
2. Phase - Filtered phase map (iron/calcium differentiation)
3. SWI - Processed image (magnitude × phase_mask^n)
4. MinIP - Minimum intensity projection (venogram)
5. MIP - Maximum intensity projection
6. QSM - Quantitative susceptibility map (derived from phase via dipole inversion)
7. R2* - Transverse relaxation rate map (derived from magnitude decay fitting)

Physics Note:
SWI and QSM both require gradient echo (T2*) readout. The 180° refocusing
pulse in spin echo nullifies susceptibility-induced phase shifts. Variants:
- Standard GRE-SWI: High resolution, 4-6 min
- EPI-SWI: Rapid acquisition, 1-2 min (geometric distortion trade-off)
- Multi-echo GRE (QSM/me, SWAN): Multiple echoes for QSM/R2*/SWI

All outputs have base="SWI" to indicate the contrast family.
The technique axis (GRE or EPI) indicates the acquisition method.

Version: 3.0.0 - Added R2*, reordered rules to handle GE psd/QSM/me outputs
"""

from typing import Optional

from ..core.context import ClassificationContext
from ..core.evidence import Evidence, EvidenceSource
from .common import BranchResult, SWI_OUTPUT_TYPES


# Text tokens that identify a specific output type in GE-style descriptions
# like "MAG | psd/QSM/me | EFGRE3D" where the leading label before "|"
# determines the output type. These are checked BEFORE generic "qsm" matching
# to prevent the PSD name "psd/QSM/me" from making everything construct=QSM.
_SPECIFIC_OUTPUT_TOKENS = frozenset([
    "magnitude", "phase", "swi", "mip", "minip", "r2star",
])


def _has_specific_output_token(text_blob: str) -> bool:
    """Check if text contains a specific output-type indicator."""
    return any(tok in text_blob for tok in _SPECIFIC_OUTPUT_TOKENS)


def _make_result(
    construct: str,
    technique: str,
    confidence: float,
    evidence_field: str,
    evidence_value: str,
    evidence_desc: str,
) -> BranchResult:
    """Helper to build a standard SWI BranchResult."""
    return BranchResult(
        base="SWI",
        construct=construct,
        technique=technique,
        skip_base_detection=True,
        skip_construct_detection=True,
        skip_technique_detection=True,
        directory_type="anat",
        confidence=confidence,
        evidence=[Evidence(
            source=EvidenceSource.HIGH_VALUE_TOKEN,
            field=evidence_field,
            value=evidence_value,
            target=construct,
            weight=confidence,
            description=evidence_desc,
        )],
    )


def apply_swi_logic(ctx: ClassificationContext) -> BranchResult:
    """
    Apply SWI/QSM-specific classification logic.

    Determines the output type and returns base contrast/construct overrides.
    All outputs get base="SWI". The construct distinguishes the output type.

    Args:
        ctx: Classification context with fingerprint data

    Returns:
        BranchResult with base/construct overrides for SWI/QSM outputs

    Detection Priority (first match wins):
        1. MinIP  - Minimum intensity projection (venogram)
        2. MIP    - Maximum intensity projection
        3. Phase  - Phase map (has_phase flag, "phase" text from pha/phs normalizer)
        4. SWI    - Processed SWI (has_swi ImageType token, or "swi" leading label)
        5. R2*    - R2* map (has_r2star flag, or "r2star" text)
        6. Magnitude - Source magnitude (has_magnitude flag, "magnitude" text)
        7. QSM    - Quantitative susceptibility map (narrow: only when no other
                    specific output token present, or has_qsm ImageType flag)
        8. Fallback - default to SWI

    Key design: QSM moved to rule 7 (narrow) instead of rule 1 (greedy).
    GE multi-echo QSM sequences put "psd/QSM/me" in ALL output descriptions,
    so "qsm" appears in the text blob for MAG, PHS, SWI, mIP, R2*, QSM, and
    source echoes alike. Specific output tokens (magnitude, phase, swi, mip,
    r2star) must be checked BEFORE the generic "qsm" fallback.
    """
    uf = ctx.unified_flags
    text_blob = (ctx.text_search_blob or "").lower()

    # =========================================================================
    # Determine technique family (GRE or EPI) for all SWI outputs
    # =========================================================================
    if uf.get("has_epi"):
        swi_technique = "EPI"
    elif uf.get("has_gre"):
        swi_technique = "GRE"
    elif "epi" in text_blob or "3depi" in text_blob:
        swi_technique = "EPI"
    else:
        swi_technique = "GRE"

    # =========================================================================
    # 1. MinIP - Minimum Intensity Projection
    # =========================================================================
    if uf.get("is_minip") or "minip" in text_blob or "min ip" in text_blob:
        return _make_result(
            "MinIP", swi_technique, 0.95,
            "unified_flags" if uf.get("is_minip") else "text_search_blob",
            "is_minip" if uf.get("is_minip") else "minip keyword",
            "MinIP detected → Minimum Intensity Projection",
        )

    # =========================================================================
    # 2. MIP - Maximum Intensity Projection
    # =========================================================================
    is_mip_flag = uf.get("is_mip")
    is_mip_text = "mip" in text_blob and "minip" not in text_blob
    if is_mip_flag or is_mip_text:
        return _make_result(
            "MIP", swi_technique, 0.90,
            "unified_flags" if is_mip_flag else "text_search_blob",
            "is_mip" if is_mip_flag else "mip keyword",
            "MIP detected → Maximum Intensity Projection",
        )

    # =========================================================================
    # 3. Phase - Phase Map
    # =========================================================================
    # Normalizer expands "pha"→"phase" and "phs"→"phase" so GE PHS outputs match.
    # Skip if has_qsm ImageType flag is set — QSM token takes priority over P token
    # (e.g. DERIVED\PRIMARY\QSM\P should be QSM, not Phase).
    is_phase_flag = uf.get("has_phase") and not uf.get("has_magnitude") and not uf.get("has_qsm")
    is_phase_text = "phase" in text_blob and "magnitude" not in text_blob and not uf.get("has_qsm")
    if is_phase_flag or is_phase_text:
        # Phase from a QSM acquisition also gets QSM construct
        construct = "Phase,QSM" if "qsm" in text_blob else "Phase"
        return _make_result(
            construct, swi_technique, 0.90,
            "unified_flags" if is_phase_flag else "text_search_blob",
            "has_phase" if is_phase_flag else "phase keyword",
            f"{construct} detected → SWI/QSM Phase output",
        )

    # =========================================================================
    # 4. SWI - Processed/Combined SWI
    # =========================================================================
    # The SWI token in ImageType indicates processed SWI output.
    # Also match "swi" as a leading output label in text when it appears
    # alongside other specific tokens (not just as the sequence family name).
    if uf.get("has_swi"):
        return _make_result(
            "SWI", swi_technique, 0.90,
            "unified_flags", "has_swi",
            "SWI token in ImageType → Processed SWI output",
        )

    # =========================================================================
    # 4.5. PROJECTION Fallback
    # =========================================================================
    if uf.get("is_projection"):
        return _make_result(
            "SWI", swi_technique, 0.80,
            "is_projection", "PROJECTION IMAGE",
            "PROJECTION IMAGE in SWI context → Processed SWI output (fallback)",
        )

    # =========================================================================
    # 5. R2* - Transverse Relaxation Rate Map
    # =========================================================================
    # R2* is derived from multi-echo magnitude signal decay fitting.
    # Distinct from QSM (which comes from phase). Can co-occur with QSM
    # if both keywords are present → comma-separated construct.
    has_r2star = uf.get("has_r2star") or "r2star" in text_blob
    if has_r2star:
        # R2* from a QSM acquisition also gets QSM construct (comma-separated).
        # "qsm" in text from psd/QSM/me or explicit QSM ImageType token.
        has_qsm = uf.get("has_qsm") or "qsm" in text_blob
        construct = "QSM,R2starmap" if has_qsm else "R2starmap"
        return _make_result(
            construct, swi_technique, 0.90,
            "unified_flags" if uf.get("has_r2star") else "text_search_blob",
            "has_r2star" if uf.get("has_r2star") else "r2star keyword",
            f"{construct} detected → R2* transverse relaxation rate map",
        )

    # =========================================================================
    # 6. Magnitude - Source Magnitude Image
    # =========================================================================
    # T2*-weighted source data before SWI/QSM processing.
    # has_magnitude flag (M token without SWI token) or "magnitude" text
    # (normalizer expands "mag" → "magnitude").
    # When "qsm" is also present in text, use "Magnitude,QSM" to indicate
    # this is magnitude data from a QSM acquisition (not generic SWI mag).
    has_magnitude_no_swi = uf.get("has_magnitude") and not uf.get("has_swi")
    if has_magnitude_no_swi or "magnitude" in text_blob:
        construct = "Magnitude,QSM" if "qsm" in text_blob else "Magnitude"
        return _make_result(
            construct, swi_technique, 0.85,
            "unified_flags" if has_magnitude_no_swi else "text_search_blob",
            "has_magnitude (no SWI token)" if has_magnitude_no_swi else "magnitude keyword",
            f"{construct} detected → SWI/QSM source magnitude",
        )

    # =========================================================================
    # 7. QSM - Quantitative Susceptibility Map (narrow match)
    # =========================================================================
    # Only fires when no specific output token was matched above.
    # This handles:
    # - Stacks with has_qsm ImageType flag (e.g. DERIVED\PRIMARY\QSM)
    # - Stacks where "qsm" is the ONLY meaningful keyword (e.g. "QSM | psd/QSM/me")
    # - Multi-echo source echoes (e.g. "QSM_Ax_10TE_2mm...") that have "qsm"
    #   but no specific output token → these are source magnitudes for QSM
    has_qsm_flag = uf.get("has_qsm")
    has_qsm_text = "qsm" in text_blob

    if has_qsm_flag or has_qsm_text:
        # Distinguish actual QSM maps from multi-echo source data.
        # Source echoes have acquisition-style names (e.g. "qsm ax 10te 2mm...")
        # while derived QSM maps have short output labels (e.g. just "qsm").
        # Heuristic: if text contains multi-echo acquisition indicators
        # AND no QSM ImageType flag, it's source magnitude data.
        is_source_echo = (
            not has_qsm_flag
            and any(tok in text_blob for tok in ["10te", "8te", "6te", "5te", "4te", "3te"])
            and "psd" in text_blob
        )
        if is_source_echo:
            return _make_result(
                "Magnitude,QSM", swi_technique, 0.80,
                "text_search_blob", "multi-echo QSM source",
                "Multi-echo QSM source echoes → Magnitude,QSM (raw T2*-weighted data)",
            )
        return _make_result(
            "QSM", swi_technique, 0.95,
            "unified_flags" if has_qsm_flag else "text_search_blob",
            "has_qsm" if has_qsm_flag else "qsm keyword (no other output token)",
            "QSM detected → Quantitative Susceptibility Map",
        )

    # =========================================================================
    # 8. Fallback: Unknown SWI output → default to SWI
    # =========================================================================
    return _make_result(
        "SWI", swi_technique, 0.70,
        "provenance", "SWIRecon",
        "SWI branch, output type unclear → default to SWI",
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

    # Priority order matches apply_swi_logic
    if uf.get("is_minip") or "minip" in text_blob or "min ip" in text_blob:
        return "minip"
    if uf.get("is_mip") or ("mip" in text_blob and "minip" not in text_blob):
        return "mip"
    if uf.get("has_phase") and not uf.get("has_magnitude"):
        return "phase"
    if "phase" in text_blob and "magnitude" not in text_blob:
        return "phase"
    if uf.get("has_swi"):
        return "swi"
    if uf.get("is_projection"):
        return "swi"
    if uf.get("has_r2star") or "r2star" in text_blob:
        return "r2star"
    if (uf.get("has_magnitude") and not uf.get("has_swi")) or "magnitude" in text_blob:
        return "magnitude"
    if uf.get("has_qsm") or "qsm" in text_blob:
        return "qsm"

    return "swi"


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
