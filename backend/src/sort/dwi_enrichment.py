"""DWI stack enrichment: derive b-value, PE direction, and N-directions per stack.

Called during Sort Step 4 (Phase 4D) for all stacks classified as directory_type='dwi'.

Three output columns in series_classification_cache:
  dwi_b_value      - max non-zero b-value in the stack (the shell, used for naming)
  dwi_b_values_csv - all unique b-values in the stack, comma-separated sorted
  dwi_pe_direction - anatomical phase-encoding direction (AP/PA/RL/LR/IS/SI)
  dwi_n_directions - number of unique gradient directions (b0 volumes excluded)

Source priority for each field:
  b-value:
    1. Manufacturer private tag (Siemens (0019,100C), GE (0043,1039), Philips (2001,1003))
    2. Standard DiffusionBValue tag (mri_series_details.diffusion_b_value)
    3. Text regex on series_description / protocol_name / series_comments
  PE direction:
    1. Siemens: CSA PhaseEncodingDirectionPositive + IOP + InPlanePhaseEncodingDirection
    2. Text regex on concatenated text fields (all manufacturers)
  N-directions:
    1. GE: (0043,1030) direct count tag
    2. Siemens + Philips: count unique non-zero gradient vectors from
       mri_series_details.diffusion_gradient_orientation per stack
    3. Text regex on concatenated text fields
"""

from __future__ import annotations

import logging
import re
import struct
from typing import Any

logger = logging.getLogger(__name__)

# Philips sentinel for non-DW volumes (~1.7e+38); filter any value above this
_PHILIPS_SENTINEL: float = 1e37

# Maximum plausible b-value; higher values are almost certainly noise/sentinels
_BVAL_MAX: int = 10_000


# ---------------------------------------------------------------------------
# b-value text patterns
# ---------------------------------------------------------------------------
# All real-world patterns observed across Siemens / GE / Philips datasets:
#   b1000, b=1000, b-1000, b 1000       (b-prefix, various separators)
#   1000b, 1000B, 1000 b                 (b-suffix)
#   B-1000, B=1000, B 1000              (uppercase B-prefix with explicit separator)
#   B0-500-1000                          (multi-b-value range string)
#   iso b1000, isob1000, iso_b1000      (Philips isotropic ADC variants)
#   *ep_b1000t, *re_b1000t              (Siemens seq name: Trace has trailing 't')
#   *ep_b1000#32                        (Siemens seq name: direction index after '#')

_BVAL_PATTERNS: list[re.Pattern] = [
    # Multi-b range: B0-500-1000, b0-500-1000 — capture full match, split numerics afterward
    # Handled specially in extract_bvalue_from_text via _BVAL_RANGE_RE
    # b-prefix with optional separator: b1000, b=1000, b-1000, b 1000, b0
    # Uses lookbehind so separator is not consumed (allows overlapping matches like _b0-b1000)
    # Allows optional trailing letter (e.g. 't' for Trace in Siemens seq names like *ep_b1000t)
    # and optional '#N' direction index (e.g. *ep_b1000#32)
    re.compile(r"(?:(?<=^)|(?<=[_\s=\-:\/\*]))b[\s=\-]?(\d{1,4})(?:[t]|#\d+)?(?=[^a-zA-Z0-9]|$)", re.IGNORECASE),
    # iso-prefix variants: isob1000, isoB1000, iso b1000, iso_b1000
    re.compile(r"\biso\s*b(\d{3,4})\b", re.IGNORECASE),
    # b-suffix: 1000b, 1000 b, 1000B
    re.compile(r"(?:^|[^a-zA-Z])(\d{3,4})[\s]?b(?:[^a-zA-Z0-9]|$)", re.IGNORECASE),
    # Uppercase B with mandatory separator: B-1000, B=1000, B 1000
    re.compile(r"\bB[\s=\-](\d{3,4})\b"),
]

# Separate pattern for multi-b ranges like B0-500-1000 or *ep_b0_1000 (Siemens uses '_')
_BVAL_RANGE_RE = re.compile(r"(?:^|[*_\s])b[\s]?(\d+)(?:[\-\/_]\d+)+", re.IGNORECASE)


def extract_bvalue_from_text(text_blobs: list[str]) -> set[int]:
    """Return all unique b-values found across the provided text strings.

    Applies a cascade of regex patterns. Handles multi-b range strings
    (e.g. B0-500-1000) by splitting on separators. Performs sanity filtering
    (0 ≤ v ≤ _BVAL_MAX).
    """
    combined = " ".join(b for b in text_blobs if b)
    if not combined:
        return set()
    found: set[int] = set()

    # First pass: multi-b range strings like "B0-500-1000"
    for m in _BVAL_RANGE_RE.finditer(combined):
        # Extract the full matched portion and split all numeric parts
        segment = m.group(0)
        for num_str in re.findall(r"\d+", segment):
            try:
                v = int(num_str)
                if 0 <= v <= _BVAL_MAX:
                    found.add(v)
            except ValueError:
                pass

    # Second pass: single b-value patterns
    for pat in _BVAL_PATTERNS:
        for m in pat.finditer(combined):
            for group in m.groups():
                if group is not None:
                    try:
                        v = int(group)
                        if 0 <= v <= _BVAL_MAX:
                            found.add(v)
                    except ValueError:
                        pass

    return found


def aggregate_b_values(
    instance_rows: list[dict[str, Any]],
    manufacturer: str,
    text_blobs: list[str],
) -> tuple[float | None, str | None]:
    """Compute (dwi_b_value, dwi_b_values_csv) for a single DWI stack.

    dwi_b_value      = max non-zero b-value (the acquisition shell, used for naming)
    dwi_b_values_csv = all unique b-values sorted ascending, comma-separated

    A pure b=0 stack (fieldmap/reference) returns (0.0, "0").

    Args:
        instance_rows: rows from mri_series_details joined via instance for this stack
        manufacturer:  study.manufacturer string (e.g. 'SIEMENS', 'GE MEDICAL SYSTEMS')
        text_blobs:    list of text fields (series_description, protocol_name, etc.)
    """
    b_values: set[int] = set()
    mfr = (manufacturer or "").upper()

    # --- 1. Manufacturer private tags ---
    for row in instance_rows:
        v: float | int | None = None
        if "SIEMENS" in mfr or "SIEMENS HEALTHINEERS" in mfr:
            v = row.get("dwi_siemens_b_value")
        elif "GE" in mfr:
            v = row.get("dwi_ge_b_value")
        elif "PHILIPS" in mfr:
            raw = row.get("dwi_philips_b_value")
            v = raw if (raw is not None and raw <= _PHILIPS_SENTINEL) else None
        if v is not None:
            try:
                iv = int(round(float(v)))
                if 0 <= iv <= _BVAL_MAX:
                    b_values.add(iv)
            except (ValueError, TypeError):
                pass

    # --- 2. Standard DiffusionBValue tag (already in mri_series_details) ---
    if not b_values:
        for row in instance_rows:
            raw = row.get("diffusion_b_value")
            if not raw:
                continue
            try:
                for part in str(raw).split("\\"):
                    fv = float(part)
                    if fv <= _PHILIPS_SENTINEL:
                        iv = int(round(fv))
                        if 0 <= iv <= _BVAL_MAX:
                            b_values.add(iv)
            except (ValueError, TypeError):
                pass

    # --- 3. Text regex fallback ---
    # Always run when b_values is empty OR contains only b=0.
    # b=0 alone from a private tag is ambiguous: derived images (Trace, ADC) have
    # b=0 written by the scanner even though the source shell was b>0. The sequence
    # name encodes the real value (e.g. *re_b1000t). Merging gives {0, 1000} and
    # max_nonzero correctly resolves to 1000.
    if not b_values or b_values == {0}:
        b_values |= extract_bvalue_from_text(text_blobs)

    if not b_values:
        return None, None

    csv = ",".join(str(v) for v in sorted(b_values))
    max_nonzero = max((v for v in b_values if v > 0), default=0)
    return float(max_nonzero), csv


# ---------------------------------------------------------------------------
# PE direction patterns
# ---------------------------------------------------------------------------
# Covers observed formats across manufacturers:
#   _AP, _PA, -AP, -PA, /AP, /PA, space-bounded
#   DWI_AP, DWI_PA  (Philips)
#   ep2d_diff_..._AP  (Siemens HCP protocols)

_PE_PATTERNS: list[re.Pattern] = [
    # Delimiter-bounded: _AP, -AP, /AP, space AP, or AP at start/end of token
    re.compile(r"(?:^|[_\-/\s])(AP|PA|RL|LR)(?:[_\-/\s]|$)"),
    # At end of full string: ...b1000_AP
    re.compile(r"_(AP|PA|RL|LR)\s*$", re.IGNORECASE),
    # DWI_AP prefix pattern (Philips series names)
    re.compile(r"DWI[_\-](AP|PA|RL|LR)", re.IGNORECASE),
]


def _compute_siemens_pe_direction(
    iop_string: str,
    ipe: str,
    pe_positive: int,
) -> str | None:
    """Compute anatomical PE direction from Siemens IOP + InPlanePhaseEncDir + CSA positive flag.

    Algorithm:
      1. Parse IOP into row_cosine (IOP[0:3]) and col_cosine (IOP[3:6])
      2. Select phase_cosine based on InPlanePhaseEncodingDirection (COL or ROW)
      3. Find dominant anatomical axis (R/A/S) and its sign
      4. Apply PhaseEncodingDirectionPositive to determine final direction
      5. Map to BIDS-style label: AP/PA/RL/LR/IS/SI

    Returns None if IOP is malformed or has fewer than 6 components.
    """
    try:
        # Tolerate both backslash and comma-separated IOP strings
        iop = [float(p) for p in iop_string.replace(",", "\\").split("\\") if p.strip()]
        if len(iop) < 6:
            return None
    except (ValueError, TypeError):
        return None

    row_cosine = iop[:3]
    col_cosine = iop[3:6]
    phase_cosine = col_cosine if "COL" in ipe.upper() else row_cosine

    dominant_idx = max(range(3), key=lambda i: abs(phase_cosine[i]))
    dominant_sign = 1 if phase_cosine[dominant_idx] >= 0 else -1
    effective_sign = dominant_sign if pe_positive == 1 else -dominant_sign

    axes = ["R", "A", "S"]
    direction_map = {
        ("R", 1): "RL", ("R", -1): "LR",
        ("A", 1): "AP", ("A", -1): "PA",
        ("S", 1): "IS", ("S", -1): "SI",
    }
    return direction_map.get((axes[dominant_idx], effective_sign))


def derive_pe_direction(
    instance_rows: list[dict[str, Any]],
    iop_string: str | None,
    text_blobs: list[str],
    manufacturer: str,
) -> str | None:
    """Derive anatomical phase-encoding direction for a DWI stack.

    Returns one of: 'AP', 'PA', 'RL', 'LR', 'IS', 'SI', or None.

    Priority:
      1. Siemens CSA computation (requires pe_dir_positive + IOP + ipe)
      2. Text regex across all text fields (all manufacturers)

    Args:
        instance_rows: rows from mri_series_details for this stack
        iop_string:    ImageOrientationPatient from series_stack (backslash-separated)
        text_blobs:    concatenated text fields for text fallback
        manufacturer:  study.manufacturer string
    """
    mfr = (manufacturer or "").upper()

    # --- 1. Siemens CSA-based computation ---
    if ("SIEMENS" in mfr) and iop_string:
        for row in instance_rows:
            pe_pos = row.get("dwi_siemens_pe_dir_positive")
            if pe_pos is None:
                continue
            # mri_series_details.phase_encoding_direction holds standard (0018,1312) = ROW or COL
            ipe = row.get("phase_encoding_direction") or "COL"
            result = _compute_siemens_pe_direction(iop_string, ipe, pe_pos)
            if result:
                return result

    # --- 2. Text regex fallback (all manufacturers) ---
    combined = " ".join(b for b in text_blobs if b)
    for pat in _PE_PATTERNS:
        m = pat.search(combined)
        if m:
            return m.group(1).upper()

    return None


# ---------------------------------------------------------------------------
# N-directions patterns
# ---------------------------------------------------------------------------

_NDIR_PATTERNS: list[re.Pattern] = [
    # "32 directions", "32dir", "32 riktningar", "32 rikt.", "32 riktn", "32R"
    re.compile(r"\b(\d{1,3})\s*(?:dir(?:ections?)?|rikt(?:ningar?|n(?:ingar?)?)?(?:\b|\.)|r\b)", re.IGNORECASE),
    # "dir32", "dir_32", "dir 32"
    re.compile(r"\bdir[\s_]?(\d{1,3})\b", re.IGNORECASE),
    # Siemens HCP protocol naming: "_32_", "_73_" between non-digit delimiters
    re.compile(r"_(\d{2,3})_"),
]


def count_gradient_directions(
    instance_rows: list[dict[str, Any]],
    manufacturer: str,
    text_blobs: list[str],
) -> int | None:
    """Count unique gradient directions (b0 volumes excluded) for a DWI stack.

    Priority:
      1. GE (0043,1030) direct count tag — takes the max across all instances
      2. Siemens / Philips: count unique non-zero gradient vectors from
         mri_series_details.diffusion_gradient_orientation
      3. Text regex fallback

    Returns an integer ≥ 1 or None if no source yields a result.
    """
    mfr = (manufacturer or "").upper()

    # --- 1. GE: direct count tag ---
    if "GE" in mfr:
        counts = [
            row["dwi_ge_n_directions"]
            for row in instance_rows
            if row.get("dwi_ge_n_directions") is not None
        ]
        if counts:
            return max(counts)

    # --- 2. Count unique non-zero gradient vectors ---
    unique_gradients: set[tuple] = set()
    for row in instance_rows:
        # Skip b0 volumes
        directionality = (
            row.get("dwi_siemens_directionality")
            or row.get("diffusion_directionality")
            or ""
        )
        if any(k in directionality.upper() for k in ("NONE", "ISOTROPIC")):
            continue
        grad_str = row.get("diffusion_gradient_orientation")
        if not grad_str:
            continue
        try:
            parts = [round(float(p), 4) for p in str(grad_str).split("\\")]
            if len(parts) == 3 and any(abs(p) > 0.001 for p in parts):
                unique_gradients.add(tuple(parts))
        except (ValueError, TypeError):
            continue
    if unique_gradients:
        return len(unique_gradients)

    # --- 3. Text regex fallback ---
    combined = " ".join(b for b in text_blobs if b)
    for pat in _NDIR_PATTERNS:
        m = pat.search(combined)
        if m:
            try:
                v = int(m.group(1))
                if 2 <= v <= 256:  # sanity bounds
                    return v
            except (ValueError, TypeError):
                continue

    return None
