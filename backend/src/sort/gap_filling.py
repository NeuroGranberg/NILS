"""
Gap Filling Utilities for Step 4: Completion

Provides physics-based similarity matching and gap filling logic
for stacks missing base, technique, or acquisition type.

Version: 1.0.0
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from typing import Any, Optional


# =============================================================================
# Constants
# =============================================================================

# Techniques that are inherently 3D
TECHNIQUE_IS_3D = {
    "MPRAGE", "MEMPRAGE", "MP2RAGE", "3D-TSE", "CISS", "bSSFP",
    "TOF-MRA", "PC-MRA", "SWI", "QALAS", "SPACE", "MDME",
}

# Techniques that are inherently 2D
TECHNIQUE_IS_2D = {
    "TSE", "SE", "SS-TSE", "EPI", "GRE-EPI", "SE-EPI", "FLASH",
    "DWI-EPI", "BOLD-EPI", "GRASE", "GRE", "IR-TSE", "ME-SE",
}

# Text patterns indicating 3D acquisition
TEXT_PATTERNS_3D = [
    "3d", "space", "mprage", "cube", "vista", "ciss", "vibe",
    "3d-tse", "3d tse", "isotropic", "mp2rage", "bravo",
]

# Text patterns indicating 2D acquisition
TEXT_PATTERNS_2D = [
    "2d", "haste", "blade", "propeller", "tse2d", "flash2d",
    "single shot", "single-shot", "ss-tse", "sstse",
]

# Orientation confidence threshold for flagging
ORIENTATION_CONFIDENCE_THRESHOLD = 0.85

# Minimum matches required for similarity-based filling
MIN_SIMILARITY_MATCHES = 2


# =============================================================================
# Physics Binning
# =============================================================================

@dataclass(frozen=True)
class PhysicsKey:
    """
    Binned physics signature for grouping similar stacks.
    
    Uses rounded values to create discrete bins for efficient lookup.
    Immutable and hashable for use as dict key.
    """
    tr_bin: Optional[int]      # TR rounded to 100ms bins
    te_bin: Optional[int]      # TE rounded to 5ms bins
    ti_bin: Optional[int]      # TI rounded to 100ms bins
    fa_bin: Optional[int]      # Flip angle rounded to 5° bins
    slices_bin: Optional[int]  # Slice count grouped into 20-slice bands


def compute_physics_key(
    tr: Optional[float],
    te: Optional[float],
    ti: Optional[float],
    fa: Optional[float],
    n_instances: Optional[int],
) -> PhysicsKey:
    """
    Compute binned physics key for a stack.
    
    Args:
        tr: Repetition time in ms
        te: Echo time in ms
        ti: Inversion time in ms
        fa: Flip angle in degrees
        n_instances: Number of slices
    
    Returns:
        PhysicsKey with binned values
    """
    return PhysicsKey(
        tr_bin=int(round(tr / 100) * 100) if tr is not None else None,
        te_bin=int(round(te / 5) * 5) if te is not None else None,
        ti_bin=int(round(ti / 100) * 100) if ti is not None else None,
        fa_bin=int(round(fa / 5) * 5) if fa is not None else None,
        slices_bin=(
            int(math.ceil(n_instances / 20) * 20)
            if (n_instances is not None and n_instances > 0)
            else None
        ),
    )


def get_adjacent_keys(key: PhysicsKey, distance: int = 1) -> list[PhysicsKey]:
    """
    Generate adjacent physics keys within a given distance.

    Only varies non-None dimensions. For distance=1, generates keys
    where one dimension is ±1 bin from the original.

    Args:
        key: Original physics key
        distance: How many bins to expand (1 or 2)

    Returns:
        List of adjacent PhysicsKey objects
    """
    adjacent = []

    # Define bin step sizes
    steps = {
        "tr": 100,
        "te": 5,
        "ti": 100,
        "fa": 5,
        "slices": 20,
    }

    # For each dimension, create variations
    for dim in ["tr", "te", "ti", "fa", "slices"]:
        current_val = getattr(key, f"{dim}_bin")
        if current_val is None:
            continue

        step = steps[dim]
        for delta in range(-distance, distance + 1):
            if delta == 0:
                continue

            new_val = current_val + (delta * step)
            if new_val < 0:
                continue

            # Create new key with this dimension varied
            new_key = PhysicsKey(
                tr_bin=new_val if dim == "tr" else key.tr_bin,
                te_bin=new_val if dim == "te" else key.te_bin,
                ti_bin=new_val if dim == "ti" else key.ti_bin,
                fa_bin=new_val if dim == "fa" else key.fa_bin,
                slices_bin=new_val if dim == "slices" else key.slices_bin,
            )
            adjacent.append(new_key)

    return adjacent


def get_multi_dim_adjacent_keys(key: PhysicsKey, distance: int = 1) -> list[PhysicsKey]:
    """
    Generate adjacent physics keys varying multiple dimensions simultaneously.

    Creates variations for specific dimension pairs that commonly co-vary:
    - TE + TI: For IR sequences (FLAIR, STIR) where both echo and inversion times vary
    - TR + FA: For GRE sequences where TR and flip angle are often adjusted together

    Args:
        key: Original physics key
        distance: How many bins to expand (1 or 2)

    Returns:
        List of adjacent PhysicsKey objects with multi-dimension variations
    """
    adjacent = []
    seen = set()  # Avoid duplicates

    # Define bin step sizes
    steps = {
        "tr": 100,
        "te": 5,
        "ti": 100,
        "fa": 5,
        "slices": 20,
    }

    # Define dimension pairs to vary together
    # These are physics parameters that commonly co-vary in MRI protocols
    dim_pairs = [
        ("te", "ti"),   # IR sequences: TE and TI often vary together
        ("tr", "fa"),   # GRE sequences: TR and flip angle relationship
    ]

    for dim1, dim2 in dim_pairs:
        val1 = getattr(key, f"{dim1}_bin")
        val2 = getattr(key, f"{dim2}_bin")

        # Skip if either dimension is None
        if val1 is None or val2 is None:
            continue

        step1 = steps[dim1]
        step2 = steps[dim2]

        # Generate all combinations of variations for this pair
        for delta1 in range(-distance, distance + 1):
            for delta2 in range(-distance, distance + 1):
                # Skip if both are zero (that's the original key)
                # Also skip if only one varies (covered by single-dim expansion)
                if delta1 == 0 or delta2 == 0:
                    continue

                new_val1 = val1 + (delta1 * step1)
                new_val2 = val2 + (delta2 * step2)

                # Skip negative values
                if new_val1 < 0 or new_val2 < 0:
                    continue

                # Build new key with both dimensions varied
                new_key = PhysicsKey(
                    tr_bin=new_val1 if dim1 == "tr" else (new_val2 if dim2 == "tr" else key.tr_bin),
                    te_bin=new_val1 if dim1 == "te" else (new_val2 if dim2 == "te" else key.te_bin),
                    ti_bin=new_val1 if dim1 == "ti" else (new_val2 if dim2 == "ti" else key.ti_bin),
                    fa_bin=new_val1 if dim1 == "fa" else (new_val2 if dim2 == "fa" else key.fa_bin),
                    slices_bin=key.slices_bin,  # Don't vary slices in multi-dim
                )

                # Avoid duplicates
                if new_key not in seen:
                    seen.add(new_key)
                    adjacent.append(new_key)

    return adjacent


def get_relaxed_ti_keys(
    key: PhysicsKey,
    max_ti_distance: int = 6,
    max_te_distance: int = 4
) -> list[PhysicsKey]:
    """
    Generate physics keys with relaxed TI and TE matching for IR sequences.

    FLAIR/STIR sequences can have TI ranging from 1800-3000ms and TE from
    80-160ms depending on vendor and field strength. This function allows
    wider TI and TE search while keeping TR, FA, and slices fixed.

    Args:
        key: Original physics key (must have ti_bin set)
        max_ti_distance: Maximum TI bins to search (default 6 = ±600ms)
        max_te_distance: Maximum TE bins to search (default 4 = ±20ms)

    Returns:
        List of PhysicsKey objects with varied TI and/or TE
    """
    if key.ti_bin is None:
        return []

    adjacent = []
    seen = set()
    ti_step = 100  # 100ms bins
    te_step = 5    # 5ms bins

    # Generate combinations of TI and TE variations
    for ti_delta in range(-max_ti_distance, max_ti_distance + 1):
        for te_delta in range(-max_te_distance, max_te_distance + 1):
            # Skip if both are zero (original key, already tried)
            # Skip if only TE varies (covered by single-dim expansion)
            if ti_delta == 0:
                continue

            new_ti = key.ti_bin + (ti_delta * ti_step)
            if new_ti < 0:
                continue

            new_te = key.te_bin
            if key.te_bin is not None and te_delta != 0:
                new_te = key.te_bin + (te_delta * te_step)
                if new_te < 0:
                    continue

            new_key = PhysicsKey(
                tr_bin=key.tr_bin,
                te_bin=new_te,
                ti_bin=new_ti,
                fa_bin=key.fa_bin,
                slices_bin=key.slices_bin,
            )

            if new_key not in seen:
                seen.add(new_key)
                adjacent.append(new_key)

    return adjacent


# =============================================================================
# Reference Database
# =============================================================================

@dataclass
class ReferenceStack:
    """Reference stack with classification and physics."""
    series_stack_id: int
    base: str
    technique: str
    tr: Optional[float]
    te: Optional[float]
    ti: Optional[float]
    fa: Optional[float]
    n_instances: Optional[int]


class ReferenceDatabase:
    """
    In-memory database of classified stacks for similarity matching.
    
    Groups stacks by physics key for efficient lookup.
    """
    
    def __init__(self):
        self._by_key: dict[PhysicsKey, list[ReferenceStack]] = {}
        self._total_count = 0
    
    def add(self, stack: ReferenceStack) -> None:
        """Add a reference stack to the database."""
        key = compute_physics_key(
            stack.tr, stack.te, stack.ti, stack.fa, stack.n_instances
        )
        if key not in self._by_key:
            self._by_key[key] = []
        self._by_key[key].append(stack)
        self._total_count += 1
    
    def get_matches(self, key: PhysicsKey) -> list[ReferenceStack]:
        """Get all stacks matching the exact physics key."""
        return self._by_key.get(key, [])
    
    def get_expanded_matches(
        self,
        key: PhysicsKey,
        max_distance: int = 2
    ) -> tuple[list[ReferenceStack], str]:
        """
        Get matches, expanding search if exact bin is empty.

        Search strategy:
        1. Try exact bin match
        2. Try single-dimension expansion (±1 bin in one dimension)
        3. Try multi-dimension expansion (±1 bin in TE+TI or TR+FA pairs)
        4. Repeat steps 2-3 with distance=2 if still no matches
        5. For IR sequences (TI present): try relaxed TI search (±6 bins = ±600ms)

        Returns:
            Tuple of (matches, method) where method is:
            - "exact_bin": Exact physics match
            - "expanded_single": Single dimension expanded
            - "expanded_multi": Multi-dimension expanded (TE+TI or TR+FA)
            - "expanded_relaxed_ti": Relaxed TI search for IR sequences
            - "no_match": No matches found
        """
        # Try exact match first
        matches = self.get_matches(key)
        if matches:
            return matches, "exact_bin"

        # Expand search progressively
        for distance in range(1, max_distance + 1):
            # First try single-dimension expansion
            adjacent_keys = get_adjacent_keys(key, distance)
            for adj_key in adjacent_keys:
                matches.extend(self.get_matches(adj_key))

            if matches:
                return matches, "expanded_single"

            # Then try multi-dimension expansion (TE+TI, TR+FA pairs)
            multi_dim_keys = get_multi_dim_adjacent_keys(key, distance)
            for adj_key in multi_dim_keys:
                matches.extend(self.get_matches(adj_key))

            if matches:
                return matches, "expanded_multi"

        # Last resort for IR sequences: relaxed TI search
        # FLAIR/STIR sequences can have TI ranging from 1800-3000ms across vendors
        # Try wider TI range while keeping other params fixed
        if key.ti_bin is not None:
            relaxed_ti_keys = get_relaxed_ti_keys(key, max_ti_distance=6)
            for adj_key in relaxed_ti_keys:
                matches.extend(self.get_matches(adj_key))

            if matches:
                return matches, "expanded_relaxed_ti"

        return [], "no_match"
    
    @property
    def total_count(self) -> int:
        return self._total_count
    
    @property
    def bin_count(self) -> int:
        return len(self._by_key)


def build_reference_database(rows: list[dict[str, Any]]) -> ReferenceDatabase:
    """
    Build reference database from query results.
    
    Args:
        rows: List of dicts with series_stack_id, base, technique, mr_tr, mr_te, etc.
    
    Returns:
        Populated ReferenceDatabase
    """
    db = ReferenceDatabase()
    
    for row in rows:
        stack = ReferenceStack(
            series_stack_id=row["series_stack_id"],
            base=row["base"],
            technique=row["technique"],
            tr=row.get("mr_tr"),
            te=row.get("mr_te"),
            ti=row.get("mr_ti"),
            fa=row.get("mr_flip_angle"),
            n_instances=row.get("stack_n_instances"),
        )
        db.add(stack)
    
    return db


# =============================================================================
# Similarity-Based Gap Filling
# =============================================================================

# Technique to pulse family mapping
# Used to validate that predicted technique is compatible with scanning_sequence
TECHNIQUE_PULSE_FAMILY: dict[str, str] = {
    # SE family (spin echo based)
    "SE": "SE",
    "TSE": "SE",
    "HASTE": "SE",
    "RESTORE": "SE",
    "MESE": "SE",
    "MDME": "SE",
    "SPACE": "SE",
    "TIRM": "SE",  # IR-TSE is SE-based
    "3D-TSE": "SE",
    "SS-TSE": "SE",
    "IR-TSE": "SE",
    "ME-SE": "SE",
    "VFA-TSE": "SE",
    # GRE family (gradient echo based)
    "GRE": "GRE",
    "FLASH": "GRE",
    "MPRAGE": "GRE",
    "MEMPRAGE": "GRE",
    "MP2RAGE": "GRE",
    "TOF-MRA": "GRE",
    "PC-MRA": "GRE",
    "SWI": "GRE",
    "CISS": "GRE",
    "bSSFP": "GRE",
    "FIESTA": "GRE",
    "DESS": "GRE",
    "SSFP": "GRE",
    "QALAS": "GRE",
    "VI-GRE": "GRE",
    "SP-GRE": "GRE",
    "SS-GRE": "GRE",
    "FSP-GRE": "GRE",
    "VFA-GRE": "GRE",
    "ME-GRE": "GRE",
    # EPI family (echo planar)
    "EPI": "EPI",
    "DWI-EPI": "EPI",
    "BOLD-EPI": "EPI",
    "GRE-EPI": "EPI",
    "SE-EPI": "EPI",
    "GRASE": "EPI",
    # Special/Other
    "ASL": "OTHER",
    "Perfusion": "OTHER",
    "MRF": "OTHER",
    "BOLD": "OTHER",
}


def is_technique_compatible_with_scanning_sequence(
    technique: str,
    scanning_sequence: Optional[str],
) -> bool:
    """
    Check if a technique is compatible with the scanning_sequence.

    This prevents assigning EPI-based techniques to non-EPI sequences, etc.

    Args:
        technique: Predicted technique (e.g., "DWI-EPI", "TSE", "TIRM")
        scanning_sequence: DICOM ScanningSequence value (e.g., "EP", "SE", "IR", "RM")

    Returns:
        True if compatible or uncertain (RM, None), False if definitely incompatible
    """
    if not scanning_sequence or not technique:
        return True  # Can't validate, allow

    # Normalize scanning_sequence
    seq_str = str(scanning_sequence).upper()

    # RM (Research Mode) is ambiguous - allow anything
    if "RM" in seq_str and "TIRM" not in seq_str:
        # But if it's ONLY RM (no other indicators), be more cautious with EPI
        if seq_str == "RM" or seq_str == "['RM']":
            # For pure RM, don't allow EPI techniques unless there's EP indicator
            pulse_family = TECHNIQUE_PULSE_FAMILY.get(technique, "OTHER")
            if pulse_family == "EPI":
                return False  # Reject EPI for pure RM sequences
        return True

    # Get technique's pulse family
    pulse_family = TECHNIQUE_PULSE_FAMILY.get(technique, "OTHER")

    # Check compatibility
    has_ep = "EP" in seq_str
    has_se = "SE" in seq_str or "FSE" in seq_str
    has_ir = "IR" in seq_str
    has_gr = "GR" in seq_str

    if pulse_family == "EPI":
        # EPI techniques require EP in scanning_sequence
        return has_ep

    if pulse_family == "SE":
        # SE techniques are compatible with SE, IR, or combinations
        # IR sequences (FLAIR, STIR) use SE-based readout
        return has_se or has_ir or (not has_ep and not has_gr)

    if pulse_family == "GRE":
        # GRE techniques should have GR, or no conflicting SE/EP
        return has_gr or (not has_se and not has_ep)

    # OTHER or unknown - allow
    return True


@dataclass
class SimilarityResult:
    """Result of similarity-based gap filling."""
    base: Optional[str] = None
    technique: Optional[str] = None
    method: str = "no_match"
    match_count: int = 0
    total_in_bin: int = 0


def find_best_match(
    ref_db: ReferenceDatabase,
    tr: Optional[float],
    te: Optional[float],
    ti: Optional[float],
    fa: Optional[float],
    n_instances: Optional[int],
    scanning_sequence: Optional[str] = None,
) -> SimilarityResult:
    """
    Find the best (base, technique) classification via similarity.

    Strategy:
    1. Compute physics key for the target stack
    2. Find all stacks in the same bin (or expand search)
    3. Count (base, technique) pairs
    4. Validate technique compatibility with scanning_sequence
    5. Return the most frequent compatible pair if count >= MIN_SIMILARITY_MATCHES

    Args:
        ref_db: Reference database of classified stacks
        tr, te, ti, fa, n_instances: Physics parameters of target stack
        scanning_sequence: DICOM ScanningSequence for validation (optional)

    Returns:
        SimilarityResult with best match or no_match
    """
    key = compute_physics_key(tr, te, ti, fa, n_instances)

    matches, method = ref_db.get_expanded_matches(key, max_distance=2)

    if not matches:
        return SimilarityResult(method="no_match")

    # Count (base, technique) pairs
    pair_counts = Counter((m.base, m.technique) for m in matches)

    # Find best compatible pair
    for (best_base, best_technique), count in pair_counts.most_common():
        # Validate technique is compatible with scanning_sequence
        if not is_technique_compatible_with_scanning_sequence(best_technique, scanning_sequence):
            continue  # Skip incompatible techniques

        # Require minimum matches for confidence
        if count < MIN_SIMILARITY_MATCHES:
            return SimilarityResult(
                method="insufficient_matches",
                match_count=count,
                total_in_bin=len(matches),
            )

        return SimilarityResult(
            base=best_base,
            technique=best_technique,
            method=method,
            match_count=count,
            total_in_bin=len(matches),
        )

    # No compatible technique found
    return SimilarityResult(
        method="no_compatible_match",
        match_count=0,
        total_in_bin=len(matches),
    )


# =============================================================================
# Acquisition Type Filling
# =============================================================================

def infer_acquisition_type(
    mr_acquisition_type: Optional[str],
    unified_flags: dict[str, bool],
    text_search_blob: Optional[str],
    technique: Optional[str],
) -> tuple[Optional[str], str]:
    """
    Infer MR acquisition type (2D/3D) from available signals.
    
    Args:
        mr_acquisition_type: Current value (may be None)
        unified_flags: Unified flags from classification context
        text_search_blob: Searchable text blob
        technique: Detected technique
    
    Returns:
        Tuple of (inferred_value, detection_method)
        Returns (None, "already_set") if value already exists
        Returns (None, "unknown") if cannot infer
    """
    # Already set
    if mr_acquisition_type:
        return None, "already_set"
    
    # 1. Check unified_flags
    if unified_flags.get("is_3d"):
        return "3D", "unified_flag"
    if unified_flags.get("is_2d"):
        return "2D", "unified_flag"
    
    # 2. Check text patterns
    text = (text_search_blob or "").lower()
    
    # Check 3D patterns first (more specific)
    for pattern in TEXT_PATTERNS_3D:
        if pattern in text:
            return "3D", "text_pattern"
    
    # Check 2D patterns
    for pattern in TEXT_PATTERNS_2D:
        if pattern in text:
            return "2D", "text_pattern"
    
    # 3. Infer from technique
    if technique in TECHNIQUE_IS_3D:
        return "3D", "technique_inference"
    if technique in TECHNIQUE_IS_2D:
        return "2D", "technique_inference"
    
    return None, "unknown"


# =============================================================================
# Intent Re-synthesis (simplified version)
# =============================================================================

def synthesize_directory_type(
    base: Optional[str],
    technique: Optional[str],
    construct_csv: str,
    provenance: Optional[str],
    localizer: int,
) -> str:
    """
    Simplified intent synthesis for gap-filled stacks.
    
    This is a simplified version of the full pipeline's _synthesize_intent().
    Used to re-compute directory_type after filling base/technique.
    
    Args:
        base: Base contrast (T1w, T2w, DWI, etc.)
        technique: Technique name
        construct_csv: Comma-separated constructs
        provenance: Provenance type
        localizer: 1 if localizer, 0 otherwise
    
    Returns:
        Directory type string
    """
    # Localizer check
    if localizer == 1:
        return "localizer"
    
    # Parse constructs
    constructs = set(c.strip() for c in construct_csv.split(",") if c.strip())
    
    # Provenance-based routing
    if provenance == "DTIRecon":
        return "dwi"
    if provenance in ("PerfusionRecon", "ASLRecon"):
        return "perf"
    if provenance == "BOLDRecon":
        return "func"
    
    # Construct-based routing
    diffusion_constructs = {"ADC", "eADC", "FA", "Trace", "MD", "AD", "RD"}
    if constructs & diffusion_constructs:
        return "dwi"
    
    perfusion_constructs = {"CBF", "CBV", "MTT", "Tmax", "TTP"}
    if constructs & perfusion_constructs:
        return "perf"
    
    fmap_constructs = {"B0map", "PhaseMap", "B1map"}
    if constructs & fmap_constructs:
        return "fmap"
    
    # Base-based routing
    if base == "DWI":
        return "dwi"
    if base == "PWI":
        return "perf"
    
    anat_bases = {"T1w", "T2w", "PDw", "T2*w", "SWI", "MTw", "T1rho", "T2rho"}
    if base in anat_bases:
        return "anat"
    
    # Provenance fallback
    if provenance in ("SWIRecon", "SyMRI", "ProjectionDerived", "SubtractionDerived"):
        return "anat"
    
    # Still misc
    return "misc"


# =============================================================================
# Review Reason Helpers
# =============================================================================

def add_review_reason(existing_csv: str, new_reason: str) -> str:
    """
    Add a review reason to an existing CSV string.

    Args:
        existing_csv: Existing comma-separated reasons (may be empty)
        new_reason: New reason to add

    Returns:
        Updated CSV string with new reason
    """
    existing = set(r.strip() for r in existing_csv.split(",") if r.strip())
    existing.add(new_reason)
    return ",".join(sorted(existing))


def remove_review_reason(existing_csv: str, reason_to_remove: str) -> str:
    """
    Remove a review reason from an existing CSV string.

    Args:
        existing_csv: Existing comma-separated reasons (may be empty)
        reason_to_remove: Reason to remove (exact match)

    Returns:
        Updated CSV string with reason removed
    """
    if not existing_csv:
        return ""
    existing = set(r.strip() for r in existing_csv.split(",") if r.strip())
    existing.discard(reason_to_remove)
    return ",".join(sorted(existing))
