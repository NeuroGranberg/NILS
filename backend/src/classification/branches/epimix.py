"""
EPIMix/NeuroMix Branch Logic

Classification branch for EPIMix and NeuroMix multicontrast outputs.

EPIMix is a rapid brain MRI sequence that produces 6 distinct contrasts
from a single ~1 minute acquisition:

1. T1-FLAIR - T1w with CSF nulling (SE-EPI with short TI ~580-670ms)
2. T2-FLAIR - T2w with CSF nulling (SE-EPI with long TI ~2460-3120ms)
3. T2-w - T2-weighted (b=0 from DWI block, SE-EPI)
4. isoDWI - Isotropic diffusion-weighted (averaged 3-direction DWI)
5. ADC - Apparent diffusion coefficient map
6. T2*-w - Gradient echo (GRE-EPI, no 180° refocusing pulse)

NeuroMix is an evolution of EPIMix that replaces some EPI readouts with
SSFSE (Single-Shot Fast Spin Echo) for reduced geometric distortion:

- T2-FLAIR: SSFSE readout instead of SE-EPI (no EPI distortion)
- T2w: SSFSE or FSE readout options
- Optional: 3D-EPI T1w and SWI outputs

Physics Note:
- EPIMix: All outputs use EPI readout (SE-EPI or GRE-EPI)
- NeuroMix: T2-FLAIR/T2w use SSFSE (HASTE), others use EPI
- SSFSE = Single-Shot FSE = HASTE (vendor names differ)

Detection Strategy:
- Primary: Text keywords in text_search_blob
- SSFSE keyword differentiates NeuroMix from EPIMix for T2-FLAIR/T2w
- Fallback: Physics parameters (TI/TE ranges) when keywords unavailable

Version: 2.0.0
"""

from typing import Optional

from ..core.context import ClassificationContext
from ..core.evidence import Evidence, EvidenceSource
from .common import BranchResult, EPIMIX_OUTPUT_TYPES


def _is_neuromix(text_blob: str) -> bool:
    """
    Check if this is a NeuroMix sequence (vs standard EPIMix).

    NeuroMix-specific keywords: neuromix, mix2, mix3
    """
    return any(kw in text_blob for kw in ["neuromix", "mix2", "mix3"])


def _has_ssfse_readout(text_blob: str) -> bool:
    """
    Check if text indicates SSFSE (Single-Shot FSE) readout.

    SSFSE/HASTE is the key differentiator for NeuroMix T2-FLAIR/T2w.
    """
    return any(kw in text_blob for kw in ["ssfse", "ss-fse", "haste", "single shot fse", "single-shot fse"])


def _has_fse_readout(text_blob: str) -> bool:
    """
    Check if text indicates multishot FSE readout.
    """
    # Check for FSE but exclude SSFSE
    if "fse" in text_blob and not _has_ssfse_readout(text_blob):
        return True
    return False


def _has_3depi_readout(text_blob: str) -> bool:
    """
    Check if text indicates 3D-EPI readout (NeuroMix T1w/SWI).
    """
    return "3d-epi" in text_blob or "3depi" in text_blob or "3d epi" in text_blob


def _has_flair(text_blob: str) -> bool:
    """Check if text indicates FLAIR preparation."""
    return "flair" in text_blob


def _has_t1_indicator(text_blob: str) -> bool:
    """
    Check if text indicates T1 weighting.

    Patterns: t1w, t1 (with space/boundary), t1flair
    """
    import re
    # Match t1w, t1flair, or t1 followed by space/punctuation/end
    return bool(re.search(r'\bt1(?:w|flair|\s|$|-)', text_blob))


def _has_t2_indicator(text_blob: str) -> bool:
    """
    Check if text indicates T2 weighting.

    Patterns: t2w, t2 (with space/boundary), t2flair, t2star
    Note: Excludes t2* patterns (handled separately)
    """
    import re
    # Match t2w, t2flair, or t2 followed by space/punctuation/end (but not t2star/t2*)
    return bool(re.search(r'\bt2(?:w|flair|\s|$|-)', text_blob)) and "t2star" not in text_blob and "t2*" not in text_blob


def _has_t2star_indicator(text_blob: str) -> bool:
    """Check if text indicates T2* weighting."""
    return "t2star" in text_blob or "t2*" in text_blob or "gradient-echo" in text_blob or "gradient echo" in text_blob


def apply_epimix_logic(ctx: ClassificationContext) -> BranchResult:
    """
    Apply EPIMix/NeuroMix-specific classification logic.

    Determines the output type and returns appropriate base, construct,
    modifier, and technique overrides.

    Args:
        ctx: Classification context with fingerprint data

    Returns:
        BranchResult with overrides for EPIMix/NeuroMix outputs

    Detection Priority:
        1. Text keywords (HIGH confidence 0.90) - most reliable
        2. Physics fallback (MEDIUM confidence 0.75) - TI/TE based
    """
    text_blob = (ctx.text_search_blob or "").lower()
    mr_ti = ctx.mr_ti or 0
    mr_te = ctx.mr_te or 0

    is_neuromix = _is_neuromix(text_blob)
    has_ssfse = _has_ssfse_readout(text_blob)
    has_fse = _has_fse_readout(text_blob)
    has_3depi = _has_3depi_readout(text_blob)

    # Detect contrast indicators separately
    has_flair = _has_flair(text_blob)
    has_t1 = _has_t1_indicator(text_blob)
    has_t2 = _has_t2_indicator(text_blob)
    has_t2star = _has_t2star_indicator(text_blob)

    # =========================================================================
    # Priority 1: Text keyword detection (most reliable)
    # =========================================================================

    # --- T1-FLAIR ---
    # Same for both EPIMix and NeuroMix (SE-EPI readout)
    # Detected when both T1 indicator AND FLAIR are present
    if has_t1 and has_flair:
        return _make_result(
            output_type="t1_flair",
            evidence_field="text_search_blob",
            evidence_value="t1 + flair",
            description="T1-FLAIR detected via keyword",
            confidence=0.90,
        )

    # --- T2-FLAIR ---
    # NeuroMix uses SSFSE readout, EPIMix uses SE-EPI
    # Detected when both T2 indicator AND FLAIR are present (but not T1)
    if has_t2 and has_flair and not has_t1:
        if has_ssfse or (is_neuromix and "epi" not in text_blob):
            # NeuroMix T2-FLAIR with SSFSE readout
            return _make_result(
                output_type="t2_flair_ssfse",
                evidence_field="text_search_blob",
                evidence_value="t2 + flair + ssfse/neuromix",
                description="NeuroMix T2-FLAIR (SSFSE readout) detected",
                confidence=0.90,
            )
        else:
            # EPIMix T2-FLAIR with SE-EPI readout
            return _make_result(
                output_type="t2_flair_epi",
                evidence_field="text_search_blob",
                evidence_value="t2 + flair",
                description="EPIMix T2-FLAIR (EPI readout) detected",
                confidence=0.90,
            )

    # --- SWI (NeuroMix only, 3D-EPI) ---
    if "swi" in text_blob:
        return _make_result(
            output_type="swi_3depi",
            evidence_field="text_search_blob",
            evidence_value="swi keyword",
            description="NeuroMix SWI (3D-EPI) detected",
            confidence=0.90,
        )

    # --- ADC ---
    # Same for both EPIMix and NeuroMix
    if " adc" in text_blob or "adc " in text_blob or text_blob.startswith("adc"):
        return _make_result(
            output_type="adc",
            evidence_field="text_search_blob",
            evidence_value="adc keyword",
            description="ADC detected via keyword",
            confidence=0.90,
        )

    # --- isoDWI ---
    # Same for both EPIMix and NeuroMix
    if "iso dwi" in text_blob or "isodwi" in text_blob:
        return _make_result(
            output_type="iso_dwi",
            evidence_field="text_search_blob",
            evidence_value="iso dwi keyword",
            description="isoDWI detected via keyword",
            confidence=0.90,
        )

    # --- T2*-w (Gradient Echo) ---
    # Same for both EPIMix and NeuroMix (GRE-EPI)
    if has_t2star:
        return _make_result(
            output_type="t2star",
            evidence_field="text_search_blob",
            evidence_value="t2star/gradient-echo keyword",
            description="T2*-w detected via keyword",
            confidence=0.90,
        )

    # --- T1w 3D-EPI (NeuroMix only) ---
    if has_3depi and has_t1:
        return _make_result(
            output_type="t1w_3depi",
            evidence_field="text_search_blob",
            evidence_value="t1 + 3d-epi",
            description="NeuroMix T1w (3D-EPI) detected",
            confidence=0.90,
        )

    # --- T2-w (without FLAIR) ---
    # NeuroMix can use SSFSE or FSE, EPIMix uses SE-EPI
    if has_t2 and not has_flair:
        if has_ssfse:
            return _make_result(
                output_type="t2w_ssfse",
                evidence_field="text_search_blob",
                evidence_value="t2w + ssfse",
                description="NeuroMix T2w (SSFSE readout) detected",
                confidence=0.90,
            )
        elif has_fse:
            return _make_result(
                output_type="t2w_fse",
                evidence_field="text_search_blob",
                evidence_value="t2w + fse",
                description="NeuroMix T2w (FSE readout) detected",
                confidence=0.90,
            )
        else:
            # Standard EPIMix T2-w or NeuroMix T2-w with EPI
            return _make_result(
                output_type="t2w_epi",
                evidence_field="text_search_blob",
                evidence_value="t2w keyword",
                description="T2-w (EPI readout) detected",
                confidence=0.90,
            )

    # =========================================================================
    # Priority 2: Physics-based fallback (when keywords unavailable)
    # =========================================================================
    # Based on database analysis of 216 EPIMix stacks:
    # - T2-FLAIR: TI 2461-3119ms (avg 2753)
    # - T1-FLAIR: TI 548-671ms (avg 604)
    # - T2*-w: TI=0, TE 16-51ms (avg 34) - shorter than T2-w
    # - T2-w/ADC/isoDWI: TI=0, TE 35-120ms (avg 93) - identical physics

    # T2-FLAIR: Long TI > 2000ms
    if mr_ti > 2000:
        # For NeuroMix, default to SSFSE; for EPIMix, use EPI
        output_type = "t2_flair_ssfse" if is_neuromix else "t2_flair_epi"
        return _make_result(
            output_type=output_type,
            evidence_field="physics",
            evidence_value=f"TI={mr_ti}ms > 2000",
            description="T2-FLAIR inferred from long TI",
            confidence=0.75,
        )

    # T1-FLAIR: TI 500-750ms AND TE < 40ms
    if 500 <= mr_ti <= 750 and mr_te < 40:
        return _make_result(
            output_type="t1_flair",
            evidence_field="physics",
            evidence_value=f"TI={mr_ti}ms, TE={mr_te}ms",
            description="T1-FLAIR inferred from TI/TE ranges",
            confidence=0.75,
        )

    # T2*-w: TI=0 AND TE < 60ms (gradient echo has shorter TE)
    if mr_ti == 0 and 0 < mr_te < 60:
        return _make_result(
            output_type="t2star",
            evidence_field="physics",
            evidence_value=f"TI=0, TE={mr_te}ms < 60",
            description="T2*-w inferred from short TE, no TI",
            confidence=0.75,
        )

    # T2-w: TI=0 AND TE >= 60ms (same physics as DWI/ADC, default to T2-w)
    if mr_ti == 0 and mr_te >= 60:
        # For NeuroMix, default to SSFSE; for EPIMix, use EPI
        output_type = "t2w_ssfse" if is_neuromix else "t2w_epi"
        return _make_result(
            output_type=output_type,
            evidence_field="physics",
            evidence_value=f"TI=0, TE={mr_te}ms >= 60",
            description="T2-w inferred from long TE (ambiguous with DWI/ADC)",
            confidence=0.70,
        )

    # =========================================================================
    # Fallback: Unknown EPIMix/NeuroMix output
    # =========================================================================
    # If we can't determine the specific output type, return a generic result
    # with T2-w as default (most common output in clinical use)
    return BranchResult(
        base="T2w",
        construct="",
        technique="SE-EPI",
        skip_base_detection=True,
        skip_construct_detection=True,
        skip_technique_detection=True,
        directory_type="anat",
        confidence=0.60,
        evidence=[Evidence(
            source=EvidenceSource.TEXT_SEARCH,
            field="provenance",
            value="EPIMix/NeuroMix",
            target="T2w",
            weight=0.60,
            description="EPIMix/NeuroMix branch, output type unclear → default to T2-w",
        )],
    )


def _make_result(
    output_type: str,
    evidence_field: str,
    evidence_value: str,
    description: str,
    confidence: float,
) -> BranchResult:
    """
    Create BranchResult for a specific EPIMix/NeuroMix output type.

    Args:
        output_type: Key from EPIMIX_OUTPUT_TYPES (in common.py)
        evidence_field: Field that provided evidence (e.g., "text_search_blob")
        evidence_value: Value that matched (e.g., "t1w flair keyword")
        description: Human-readable description
        confidence: Detection confidence (0.0 - 1.0)

    Returns:
        Configured BranchResult
    """
    config = EPIMIX_OUTPUT_TYPES[output_type]

    return BranchResult(
        base=config["base"],
        construct=config["construct"],
        modifiers_add=config["modifiers"],
        technique=config["technique"],
        skip_base_detection=True,
        skip_construct_detection=True,
        skip_technique_detection=True,
        directory_type=config["directory_type"],
        confidence=confidence,
        evidence=[Evidence(
            source=EvidenceSource.TEXT_SEARCH if "text" in evidence_field else EvidenceSource.PHYSICS_DISTINCT,
            field=evidence_field,
            value=evidence_value,
            target=config["base"],
            weight=confidence,
            description=description,
        )],
    )


def detect_epimix_output_type(ctx: ClassificationContext) -> Optional[str]:
    """
    Detect which EPIMix/NeuroMix output type this stack represents.

    Args:
        ctx: Classification context

    Returns:
        Output type key from EPIMIX_OUTPUT_TYPES, or None if unknown
    """
    text_blob = (ctx.text_search_blob or "").lower()
    mr_ti = ctx.mr_ti or 0
    mr_te = ctx.mr_te or 0

    is_neuromix = _is_neuromix(text_blob)
    has_ssfse = _has_ssfse_readout(text_blob)
    has_fse = _has_fse_readout(text_blob)
    has_3depi = _has_3depi_readout(text_blob)

    # Detect contrast indicators separately
    has_flair = _has_flair(text_blob)
    has_t1 = _has_t1_indicator(text_blob)
    has_t2 = _has_t2_indicator(text_blob)
    has_t2star = _has_t2star_indicator(text_blob)

    # Text keyword detection (same priority as apply_epimix_logic)
    if has_t1 and has_flair:
        return "t1_flair"

    if has_t2 and has_flair and not has_t1:
        if has_ssfse or (is_neuromix and "epi" not in text_blob):
            return "t2_flair_ssfse"
        return "t2_flair_epi"

    if "swi" in text_blob:
        return "swi_3depi"

    if " adc" in text_blob or "adc " in text_blob or text_blob.startswith("adc"):
        return "adc"

    if "iso dwi" in text_blob or "isodwi" in text_blob:
        return "iso_dwi"

    if has_t2star:
        return "t2star"

    if has_3depi and has_t1:
        return "t1w_3depi"

    if has_t2 and not has_flair:
        if has_ssfse:
            return "t2w_ssfse"
        elif has_fse:
            return "t2w_fse"
        return "t2w_epi"

    # Physics fallback
    if mr_ti > 2000:
        return "t2_flair_ssfse" if is_neuromix else "t2_flair_epi"
    if 500 <= mr_ti <= 750 and mr_te < 40:
        return "t1_flair"
    if mr_ti == 0 and 0 < mr_te < 60:
        return "t2star"
    if mr_ti == 0 and mr_te >= 60:
        return "t2w_ssfse" if is_neuromix else "t2w_epi"

    return None


def get_epimix_output_info(output_type: str) -> dict:
    """
    Get information about an EPIMix/NeuroMix output type.

    Args:
        output_type: Key from EPIMIX_OUTPUT_TYPES

    Returns:
        Dict with base, construct, modifiers, technique, description
    """
    return EPIMIX_OUTPUT_TYPES.get(output_type, {
        "base": "T2w",
        "construct": "",
        "modifiers": None,
        "technique": "SE-EPI",
        "directory_type": "anat",
        "description": "EPIMix/NeuroMix output (type unspecified)",
    })
