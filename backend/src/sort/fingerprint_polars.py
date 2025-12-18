"""Polars-based fingerprint generation pipeline.

This module provides high-performance fingerprint generation using Polars
LazyFrame for memory-efficient, vectorized data processing combined with
PostgreSQL COPY for bulk insert.

Performance improvements over row-by-row approach:
- Single JOIN query instead of multiple round-trips
- Vectorized transformations (10-100x faster than Python loops)
- Bulk COPY + UPSERT instead of individual INSERT statements
- Batched commits to prevent PostgreSQL OOM

Expected performance: ~45-60 seconds for 450K stacks (vs OOM with old approach)
"""

from __future__ import annotations

import logging
import re
from io import StringIO
from tempfile import NamedTemporaryFile
from pathlib import Path
from typing import Any, Callable

import polars as pl
from rapidfuzz import fuzz, process
from sqlalchemy import text
from sqlalchemy.engine import Connection

from .semantic_normalizer import normalize_text_blob

logger = logging.getLogger(__name__)


# =============================================================================
# Schema Definitions
# =============================================================================

# Schema for the combined source query result
FINGERPRINT_SOURCE_SCHEMA = {
    # Stack identifiers
    "series_stack_id": pl.Int64,
    "series_id": pl.Int64,
    "stack_modality": pl.Utf8,
    "stack_n_instances": pl.Int32,
    "stack_image_orientation": pl.Utf8,
    "stack_image_type": pl.Utf8,
    
    # MR stack fields
    "stack_echo_time": pl.Float64,
    "stack_repetition_time": pl.Float64,
    "stack_inversion_time": pl.Float64,
    "stack_flip_angle": pl.Float64,
    "stack_echo_train_length": pl.Int32,
    "stack_echo_numbers": pl.Utf8,
    
    # CT stack fields
    "stack_kvp": pl.Float64,
    "stack_tube_current": pl.Float64,
    
    # PET stack fields
    "stack_pet_bed_index": pl.Int32,
    "stack_pet_frame_type": pl.Utf8,
    
    # Series fields
    "sequence_name": pl.Utf8,
    "series_description": pl.Utf8,
    "protocol_name": pl.Utf8,
    "body_part_examined": pl.Utf8,
    "series_comments": pl.Utf8,
    "scanning_sequence": pl.Utf8,
    "sequence_variant": pl.Utf8,
    "scan_options": pl.Utf8,
    "contrast_bolus_agent": pl.Utf8,
    "contrast_bolus_route": pl.Utf8,
    "contrast_bolus_total_dose": pl.Float64,
    "contrast_bolus_start_time": pl.Utf8,
    "contrast_bolus_volume": pl.Float64,
    "contrast_flow_rate": pl.Float64,
    "contrast_flow_duration": pl.Float64,
    
    # Study fields
    "manufacturer": pl.Utf8,
    "manufacturer_model_name": pl.Utf8,
    
    # MRI details
    "mr_acquisition_type": pl.Utf8,
    "angio_flag": pl.Utf8,
    "b1rms": pl.Utf8,
    "diffusion_b_value": pl.Utf8,
    "parallel_acquisition_technique": pl.Utf8,
    "temporal_position_identifier": pl.Utf8,
    "phase_contrast": pl.Utf8,
    
    # CT details
    "ct_exposure_time": pl.Float64,
    "convolution_kernel": pl.Utf8,
    "revolution_time": pl.Float64,
    "filter_type": pl.Utf8,
    "calcium_scoring_mass_factor_patient": pl.Float64,
    "calcium_scoring_mass_factor_device": pl.Float64,
    
    # PET details
    "radiopharmaceutical": pl.Utf8,
    "reconstruction_method": pl.Utf8,
    "suv_type": pl.Utf8,
    "pet_units": pl.Utf8,
    "pet_units_type": pl.Utf8,
    "pet_series_type": pl.Utf8,
    "attenuation_correction_method": pl.Utf8,
    "counts_source": pl.Utf8,
    "radionuclide_total_dose": pl.Float64,
    "radionuclide_half_life": pl.Float64,
    
    # Instance data for FOV (from window function)
    "pixel_spacing": pl.Utf8,
    "rows": pl.Int32,
    "columns": pl.Int32,
    "image_comments": pl.Utf8,
}

# Schema for the output fingerprint table
FINGERPRINT_OUTPUT_SCHEMA = {
    "series_stack_id": pl.Int64,
    "modality": pl.Utf8,
    "manufacturer": pl.Utf8,
    "manufacturer_model": pl.Utf8,
    "stack_sequence_name": pl.Utf8,
    "text_search_blob": pl.Utf8,
    "contrast_search_blob": pl.Utf8,
    "stack_orientation": pl.Utf8,
    "fov_x": pl.Float64,
    "fov_y": pl.Float64,
    "aspect_ratio": pl.Float64,
    "image_type": pl.Utf8,
    "scanning_sequence": pl.Utf8,
    "sequence_variant": pl.Utf8,
    "scan_options": pl.Utf8,
    "mr_te": pl.Float64,
    "mr_tr": pl.Float64,
    "mr_ti": pl.Float64,
    "mr_flip_angle": pl.Float64,
    "mr_echo_train_length": pl.Int32,
    "mr_echo_number": pl.Utf8,
    "mr_acquisition_type": pl.Utf8,
    "mr_angio_flag": pl.Utf8,
    "mr_b1rms": pl.Utf8,
    "mr_diffusion_b_value": pl.Utf8,
    "mr_parallel_acquisition_technique": pl.Utf8,
    "mr_temporal_position_identifier": pl.Utf8,
    "mr_phase_contrast": pl.Utf8,
    "ct_kvp": pl.Float64,
    "ct_exposure_time": pl.Float64,
    "ct_tube_current": pl.Float64,
    "ct_convolution_kernel": pl.Utf8,
    "ct_revolution_time": pl.Float64,
    "ct_filter_type": pl.Utf8,
    "ct_is_calcium_score": pl.Boolean,
    "pet_bed_index": pl.Int32,
    "pet_frame_type": pl.Utf8,
    "pet_tracer": pl.Utf8,
    "pet_reconstruction_method": pl.Utf8,
    "pet_suv_type": pl.Utf8,
    "pet_units": pl.Utf8,
    "pet_units_type": pl.Utf8,
    "pet_series_type": pl.Utf8,
    "pet_corrected_image": pl.Utf8,
    "pet_counts_source": pl.Utf8,
    "pet_is_attenuation_corrected": pl.Boolean,
    "pet_radionuclide_total_dose": pl.Float64,
    "pet_radionuclide_half_life": pl.Float64,
    "stack_n_instances": pl.Int32,
}


# =============================================================================
# SQL Query - Single JOIN to gather all data
# =============================================================================

QUERY_FINGERPRINT_ALL_DATA = """
WITH instance_first AS (
    -- Get first instance per stack for FOV calculation
    SELECT DISTINCT ON (series_stack_id)
        series_stack_id,
        pixel_spacing,
        rows,
        columns,
        image_comments
    FROM instance
    WHERE series_stack_id = ANY(:series_stack_ids)
      AND series_stack_id IS NOT NULL
    ORDER BY series_stack_id, instance_id
)
SELECT 
    -- Stack identifiers
    ss.series_stack_id,
    ss.series_id,
    ss.stack_modality,
    ss.stack_n_instances,
    ss.stack_image_orientation,
    ss.stack_image_type,
    
    -- MR stack fields
    ss.stack_echo_time,
    ss.stack_repetition_time,
    ss.stack_inversion_time,
    ss.stack_flip_angle,
    ss.stack_echo_train_length,
    ss.stack_echo_numbers,
    
    -- CT stack fields
    ss.stack_kvp,
    ss.stack_tube_current,
    
    -- PET stack fields
    ss.stack_pet_bed_index,
    ss.stack_pet_frame_type,
    
    -- Series fields
    ser.sequence_name,
    ser.series_description,
    ser.protocol_name,
    ser.body_part_examined,
    ser.series_comments,
    ser.scanning_sequence,
    ser.sequence_variant,
    ser.scan_options,
    ser.contrast_bolus_agent,
    ser.contrast_bolus_route,
    ser.contrast_bolus_total_dose,
    ser.contrast_bolus_start_time::text,
    ser.contrast_bolus_volume,
    ser.contrast_flow_rate,
    ser.contrast_flow_duration,
    
    -- Study fields
    st.manufacturer,
    st.manufacturer_model_name,
    
    -- MRI details (may be NULL for non-MR)
    mri.mr_acquisition_type,
    mri.angio_flag,
    mri.b1rms,
    mri.diffusion_b_value,
    mri.parallel_acquisition_technique,
    mri.temporal_position_identifier,
    mri.phase_contrast,
    
    -- CT details (may be NULL for non-CT)
    ct.exposure_time AS ct_exposure_time,
    ct.convolution_kernel,
    ct.revolution_time,
    ct.filter_type,
    ct.calcium_scoring_mass_factor_patient,
    ct.calcium_scoring_mass_factor_device,
    
    -- PET details (may be NULL for non-PET)
    pet.radiopharmaceutical,
    pet.reconstruction_method,
    pet.suv_type,
    pet.units AS pet_units,
    pet.units_type AS pet_units_type,
    pet.series_type AS pet_series_type,
    pet.attenuation_correction_method,
    pet.counts_source,
    pet.radionuclide_total_dose,
    pet.radionuclide_half_life,
    
    -- Instance data for FOV (from first instance per stack)
    inst.pixel_spacing,
    inst.rows,
    inst.columns,
    inst.image_comments

FROM series_stack ss
JOIN series ser ON ss.series_id = ser.series_id
JOIN study st ON ser.study_id = st.study_id
LEFT JOIN mri_series_details mri ON ser.series_id = mri.series_id
LEFT JOIN ct_series_details ct ON ser.series_id = ct.series_id
LEFT JOIN pet_series_details pet ON ser.series_id = pet.series_id
LEFT JOIN instance_first inst ON ss.series_stack_id = inst.series_stack_id
WHERE ss.series_stack_id = ANY(:series_stack_ids)
"""


# =============================================================================
# Normalization Functions (for map_elements)
# =============================================================================

CANONICAL_MANUFACTURERS = ["GE", "SIEMENS", "PHILIPS", "CANON", "FUJI", "HITACHI"]
MANUFACTURER_MATCH_THRESHOLD = 70

CANONICAL_TRACERS = [
    "FDG", "FLT", "CHOLINE", "PSMA", "DOTATATE",
    "NH3", "RB82", "AMYLOID", "TAU", "FMISO",
]
TRACER_MATCH_THRESHOLD = 65


def _normalize_manufacturer(raw: str | None) -> str | None:
    """Normalize manufacturer name using fuzzy matching."""
    if not raw:
        return None
    
    cleaned = raw.upper().strip()
    
    # Special case: Toshiba → Canon
    if "TOSHIBA" in cleaned:
        return "CANON"
    
    # Exact substring match
    for canonical in CANONICAL_MANUFACTURERS:
        if canonical in cleaned:
            return canonical
    
    # Fuzzy matching
    match = process.extractOne(
        cleaned,
        CANONICAL_MANUFACTURERS,
        scorer=fuzz.partial_ratio,
        score_cutoff=MANUFACTURER_MATCH_THRESHOLD,
    )
    
    return match[0] if match else cleaned


def _normalize_pet_tracer(raw: str | None) -> str | None:
    """Normalize PET tracer using fuzzy matching."""
    if not raw:
        return None
    
    cleaned = raw.upper().strip()
    
    # Exact substring match
    for canonical in CANONICAL_TRACERS:
        if canonical in cleaned:
            return canonical
    
    # Fuzzy matching
    match = process.extractOne(
        cleaned,
        CANONICAL_TRACERS,
        scorer=fuzz.partial_ratio,
        score_cutoff=TRACER_MATCH_THRESHOLD,
    )
    
    return match[0] if match else cleaned


def _normalize_mr_acquisition_type(raw: str | None) -> str | None:
    """Normalize MR acquisition type to 2D or 3D."""
    if not raw:
        return None
    upper = raw.upper().strip()
    if "3D" in upper or upper == "3":
        return "3D"
    if "2D" in upper or upper == "2":
        return "2D"
    return None


def _normalize_yes_no_flag(raw: str | None) -> str | None:
    """Normalize Y/N flags."""
    if not raw:
        return None
    upper = raw.upper().strip()
    if upper in ("Y", "YES", "1", "TRUE"):
        return "Y"
    if upper in ("N", "NO", "0", "FALSE"):
        return "N"
    return None


def _build_text_blob(
    series_description: str | None,
    protocol_name: str | None,
    sequence_name: str | None,
    body_part_examined: str | None,
    series_comments: str | None,
    image_comments: str | None,
) -> str | None:
    """
    Build normalized text search blob with semantic tokenization.
    
    Uses SemanticNormalizer to:
    1. Replace meaningful characters (e.g., * → star for T2*)
    2. Replace separators with spaces
    3. Remove noise characters
    4. Tokenize and deduplicate
    5. Replace ambiguous tokens with canonical forms (e.g., ir → inversion-recovery)
    6. Apply conditional replacements (e.g., mpr + 3d → mprage)
    
    This ensures EXACT keyword matching works reliably in classification.
    """
    parts = [
        series_description,
        protocol_name,
        sequence_name,
        body_part_examined,
        series_comments,
        image_comments,
    ]
    
    text = " ".join(p for p in parts if p)
    if not text:
        return None
    
    # Apply semantic normalization
    return normalize_text_blob(text)


def _build_contrast_blob(
    agent: str | None,
    route: str | None,
    dose: float | None,
    start_time: str | None,
    volume: float | None,
    rate: float | None,
    duration: float | None,
) -> str | None:
    """Build normalized contrast search blob."""
    parts = []
    if agent:
        parts.append(f"agent:{agent}")
    if route:
        parts.append(f"route:{route}")
    if dose is not None:
        parts.append(f"dose:{dose}")
    if start_time:
        parts.append(f"start:{start_time}")
    if volume is not None:
        parts.append(f"volume:{volume}")
    if rate is not None:
        parts.append(f"rate:{rate}")
    if duration is not None:
        parts.append(f"duration:{duration}")
    
    return " ".join(parts).lower() if parts else None


# =============================================================================
# Main Pipeline Functions
# =============================================================================


def load_fingerprint_source_data(
    conn: Connection,
    series_stack_ids: list[int],
    log_callback: Callable[[str], None] | None = None,
) -> pl.DataFrame:
    """
    Load all fingerprint source data in a single query.
    
    Args:
        conn: SQLAlchemy connection
        series_stack_ids: List of series_stack_id to process
        log_callback: Optional callback for logging
        
    Returns:
        Polars DataFrame with source data
    """
    if log_callback:
        log_callback(f"Loading source data for {len(series_stack_ids):,} stacks...")
    
    # Execute query
    result = conn.execute(
        text(QUERY_FINGERPRINT_ALL_DATA),
        {"series_stack_ids": series_stack_ids}
    )
    
    # Fetch all rows
    rows = result.fetchall()
    columns = result.keys()
    
    if log_callback:
        log_callback(f"Fetched {len(rows):,} rows from database")
    
    if not rows:
        # Return empty DataFrame with schema
        return pl.DataFrame(schema=FINGERPRINT_SOURCE_SCHEMA)
    
    # Convert to list of dicts
    data = [dict(zip(columns, row)) for row in rows]
    
    # Create DataFrame with explicit schema
    df = pl.DataFrame(data, schema_overrides=FINGERPRINT_SOURCE_SCHEMA)
    
    if log_callback:
        log_callback(f"Created DataFrame: {df.height:,} rows, {df.estimated_size('mb'):.1f} MB")
    
    return df


def transform_fingerprints(
    df: pl.DataFrame,
    log_callback: Callable[[str], None] | None = None,
) -> pl.DataFrame:
    """
    Apply all fingerprint transformations using vectorized Polars operations.
    
    Args:
        df: Source DataFrame
        log_callback: Optional callback for logging
        
    Returns:
        Transformed DataFrame ready for insert
    """
    if df.height == 0:
        return pl.DataFrame(schema=FINGERPRINT_OUTPUT_SCHEMA)
    
    if log_callback:
        log_callback("Applying transformations...")
    
    # =========================================================================
    # Normalize manufacturer (using map_elements for fuzzy matching)
    # =========================================================================
    df = df.with_columns(
        pl.col("manufacturer")
        .map_elements(_normalize_manufacturer, return_dtype=pl.Utf8)
        .alias("manufacturer_normalized")
    )
    
    if log_callback:
        log_callback("  - Normalized manufacturers")
    
    # =========================================================================
    # Normalize MR fields
    # =========================================================================
    df = df.with_columns([
        pl.col("mr_acquisition_type")
        .map_elements(_normalize_mr_acquisition_type, return_dtype=pl.Utf8)
        .alias("mr_acquisition_type_normalized"),
        
        pl.col("angio_flag")
        .map_elements(_normalize_yes_no_flag, return_dtype=pl.Utf8)
        .alias("mr_angio_flag_normalized"),
        
        pl.col("phase_contrast")
        .map_elements(_normalize_yes_no_flag, return_dtype=pl.Utf8)
        .alias("mr_phase_contrast_normalized"),
    ])
    
    if log_callback:
        log_callback("  - Normalized MR fields")
    
    # =========================================================================
    # Normalize PET tracer
    # =========================================================================
    df = df.with_columns(
        pl.col("radiopharmaceutical")
        .map_elements(_normalize_pet_tracer, return_dtype=pl.Utf8)
        .alias("pet_tracer_normalized")
    )
    
    if log_callback:
        log_callback("  - Normalized PET tracer")
    
    # =========================================================================
    # Compute FOV from pixel_spacing, rows, columns
    # Pure Polars expressions (no Python UDFs)
    # =========================================================================
    df = df.with_columns([
        # Parse pixel_spacing: "row_sp\col_sp" -> extract row_sp and col_sp
        pl.col("pixel_spacing")
        .str.split("\\")
        .list.get(0)
        .cast(pl.Float64)
        .alias("_row_sp"),
        
        pl.col("pixel_spacing")
        .str.split("\\")
        .list.get(1)
        .cast(pl.Float64)
        .alias("_col_sp"),
    ])
    
    df = df.with_columns([
        # FOV = pixel_spacing * matrix dimension
        (pl.col("_col_sp") * pl.col("columns").cast(pl.Float64)).round(2).alias("fov_x"),
        (pl.col("_row_sp") * pl.col("rows").cast(pl.Float64)).round(2).alias("fov_y"),
    ])
    
    df = df.with_columns(
        # Aspect ratio: max/min (always >= 1.0)
        pl.when((pl.col("fov_x") > 0) & (pl.col("fov_y") > 0))
        .then(
            (pl.max_horizontal("fov_x", "fov_y") / pl.min_horizontal("fov_x", "fov_y")).round(3)
        )
        .otherwise(pl.lit(None))
        .alias("aspect_ratio")
    )
    
    if log_callback:
        log_callback("  - Computed FOV")
    
    # =========================================================================
    # Build text search blob using struct + map_elements
    # =========================================================================
    df = df.with_columns(
        pl.struct([
            "series_description",
            "protocol_name",
            "sequence_name",
            "body_part_examined",
            "series_comments",
            "image_comments",
        ])
        .map_elements(
            lambda s: _build_text_blob(
                s["series_description"],
                s["protocol_name"],
                s["sequence_name"],
                s["body_part_examined"],
                s["series_comments"],
                s["image_comments"],
            ),
            return_dtype=pl.Utf8,
        )
        .alias("text_search_blob")
    )
    
    if log_callback:
        log_callback("  - Built text search blobs")
    
    # =========================================================================
    # Build contrast search blob
    # =========================================================================
    df = df.with_columns(
        pl.struct([
            "contrast_bolus_agent",
            "contrast_bolus_route",
            "contrast_bolus_total_dose",
            "contrast_bolus_start_time",
            "contrast_bolus_volume",
            "contrast_flow_rate",
            "contrast_flow_duration",
        ])
        .map_elements(
            lambda s: _build_contrast_blob(
                s["contrast_bolus_agent"],
                s["contrast_bolus_route"],
                s["contrast_bolus_total_dose"],
                s["contrast_bolus_start_time"],
                s["contrast_bolus_volume"],
                s["contrast_flow_rate"],
                s["contrast_flow_duration"],
            ),
            return_dtype=pl.Utf8,
        )
        .alias("contrast_search_blob")
    )
    
    if log_callback:
        log_callback("  - Built contrast search blobs")
    
    # =========================================================================
    # Detect calcium score CT
    # =========================================================================
    df = df.with_columns(
        pl.when(
            pl.col("calcium_scoring_mass_factor_patient").is_not_null() |
            pl.col("calcium_scoring_mass_factor_device").is_not_null()
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(None))
        .alias("ct_is_calcium_score")
    )
    
    # =========================================================================
    # Detect PET attenuation correction
    # =========================================================================
    df = df.with_columns(
        pl.when(
            (pl.col("stack_modality") == "PT") &
            pl.col("attenuation_correction_method").is_not_null()
        )
        .then(pl.lit(True))
        .when(pl.col("stack_modality") == "PT")
        .then(pl.lit(False))
        .otherwise(pl.lit(None))
        .alias("pet_is_attenuation_corrected")
    )
    
    if log_callback:
        log_callback("  - Detected CT calcium score and PET attenuation")
    
    # =========================================================================
    # Select and rename final columns
    # =========================================================================
    result = df.select([
        pl.col("series_stack_id"),
        pl.col("stack_modality").alias("modality"),
        pl.col("manufacturer_normalized").alias("manufacturer"),
        pl.col("manufacturer_model_name").alias("manufacturer_model"),
        pl.col("sequence_name").alias("stack_sequence_name"),
        pl.col("text_search_blob"),
        pl.col("contrast_search_blob"),
        pl.col("stack_image_orientation").alias("stack_orientation"),
        pl.col("fov_x"),
        pl.col("fov_y"),
        pl.col("aspect_ratio"),
        pl.col("stack_image_type").alias("image_type"),
        pl.col("scanning_sequence"),
        pl.col("sequence_variant"),
        pl.col("scan_options"),
        # MR fields
        pl.col("stack_echo_time").alias("mr_te"),
        pl.col("stack_repetition_time").alias("mr_tr"),
        pl.col("stack_inversion_time").alias("mr_ti"),
        pl.col("stack_flip_angle").alias("mr_flip_angle"),
        pl.col("stack_echo_train_length").alias("mr_echo_train_length"),
        pl.col("stack_echo_numbers").alias("mr_echo_number"),
        pl.col("mr_acquisition_type_normalized").alias("mr_acquisition_type"),
        pl.col("mr_angio_flag_normalized").alias("mr_angio_flag"),
        pl.col("b1rms").alias("mr_b1rms"),
        pl.col("diffusion_b_value").alias("mr_diffusion_b_value"),
        pl.col("parallel_acquisition_technique").alias("mr_parallel_acquisition_technique"),
        pl.col("temporal_position_identifier").alias("mr_temporal_position_identifier"),
        pl.col("mr_phase_contrast_normalized").alias("mr_phase_contrast"),
        # CT fields
        pl.col("stack_kvp").alias("ct_kvp"),
        pl.col("ct_exposure_time"),
        pl.col("stack_tube_current").alias("ct_tube_current"),
        pl.col("convolution_kernel").alias("ct_convolution_kernel"),
        pl.col("revolution_time").alias("ct_revolution_time"),
        pl.col("filter_type").alias("ct_filter_type"),
        pl.col("ct_is_calcium_score"),
        # PET fields
        pl.col("stack_pet_bed_index").alias("pet_bed_index"),
        pl.col("stack_pet_frame_type").alias("pet_frame_type"),
        pl.col("pet_tracer_normalized").alias("pet_tracer"),
        pl.col("reconstruction_method").alias("pet_reconstruction_method"),
        pl.col("suv_type").alias("pet_suv_type"),
        pl.col("pet_units"),
        pl.col("pet_units_type"),
        pl.col("pet_series_type"),
        pl.col("attenuation_correction_method").alias("pet_corrected_image"),
        pl.col("counts_source").alias("pet_counts_source"),
        pl.col("pet_is_attenuation_corrected"),
        pl.col("radionuclide_total_dose").alias("pet_radionuclide_total_dose"),
        pl.col("radionuclide_half_life").alias("pet_radionuclide_half_life"),
        pl.col("stack_n_instances"),
    ])
    
    if log_callback:
        log_callback(f"Transformation complete: {result.height:,} fingerprints")
    
    return result


def compute_metrics_from_dataframe(df: pl.DataFrame) -> dict[str, Any]:
    """
    Compute Step 2 metrics from the transformed DataFrame.
    
    Args:
        df: Transformed fingerprint DataFrame
        
    Returns:
        Dict of metrics for Step 2
    """
    if df.height == 0:
        return {
            "total_fingerprints_created": 0,
            "stacks_processed": 0,
            "breakdown_by_manufacturer": {},
            "breakdown_by_modality": {},
        }
    
    # Manufacturer breakdown
    manufacturer_counts = (
        df.group_by("manufacturer")
        .len()
        .filter(pl.col("manufacturer").is_not_null())
        .to_dicts()
    )
    manufacturer_breakdown = {r["manufacturer"]: r["len"] for r in manufacturer_counts}
    
    # Modality breakdown
    modality_counts = df.group_by("modality").len().to_dicts()
    modality_breakdown = {r["modality"]: r["len"] for r in modality_counts}
    
    # FOV stats
    stacks_with_fov = df.filter(pl.col("fov_x").is_not_null()).height
    stacks_missing_fov = df.height - stacks_with_fov
    
    # Contrast stats
    stacks_with_contrast = df.filter(pl.col("contrast_search_blob").is_not_null()).height
    
    # MR stats
    mr_df = df.filter(pl.col("modality") == "MR")
    mr_3d_count = mr_df.filter(pl.col("mr_acquisition_type") == "3D").height
    mr_diffusion_count = mr_df.filter(pl.col("mr_diffusion_b_value").is_not_null()).height
    
    # CT stats
    ct_calcium_count = df.filter(pl.col("ct_is_calcium_score") == True).height
    
    # PET stats
    pet_attn_count = df.filter(pl.col("pet_is_attenuation_corrected") == True).height
    
    return {
        "total_fingerprints_created": df.height,
        "stacks_processed": df.height,
        "breakdown_by_manufacturer": manufacturer_breakdown,
        "breakdown_by_modality": modality_breakdown,
        "stacks_with_fov": stacks_with_fov,
        "stacks_with_missing_fov": stacks_missing_fov,
        "stacks_with_contrast": stacks_with_contrast,
        "mr_stacks_with_3d": mr_3d_count,
        "mr_stacks_with_diffusion": mr_diffusion_count,
        "ct_stacks_calcium_score": ct_calcium_count,
        "pet_stacks_attn_corrected": pet_attn_count,
    }


def bulk_upsert_fingerprints(
    conn: Connection,
    df: pl.DataFrame,
    batch_size: int = 50_000,
    log_callback: Callable[[str], None] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> int:
    """
    Bulk UPSERT fingerprints using COPY + temp table pattern.
    
    Processes in batches to prevent PostgreSQL OOM and allow progress updates.
    
    Args:
        conn: SQLAlchemy connection
        df: Transformed fingerprint DataFrame
        batch_size: Number of rows per batch
        log_callback: Optional callback for logging
        progress_callback: Optional callback (processed, total) for progress
        
    Returns:
        Total rows upserted
    """
    if df.height == 0:
        if log_callback:
            log_callback("No fingerprints to insert")
        return 0
    
    total_rows = df.height
    processed = 0
    
    if log_callback:
        log_callback(f"Starting bulk UPSERT of {total_rows:,} fingerprints in batches of {batch_size:,}")
    
    # Get raw connection for COPY
    raw = conn.connection
    dbapi_conn = getattr(raw, "driver_connection", raw)
    
    # Column list for COPY (exclude auto-generated fingerprint_id)
    columns = df.columns
    columns_str = ", ".join(columns)
    
    # Update columns for ON CONFLICT (all except series_stack_id)
    update_columns = [c for c in columns if c != "series_stack_id"]
    update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_columns])
    
    # Process in batches
    num_batches = (total_rows + batch_size - 1) // batch_size
    
    for batch_idx in range(num_batches):
        start_idx = batch_idx * batch_size
        end_idx = min(start_idx + batch_size, total_rows)
        batch_df = df.slice(start_idx, end_idx - start_idx)
        
        if log_callback:
            log_callback(f"Processing batch {batch_idx + 1}/{num_batches} ({batch_df.height:,} rows)")
        
        # Write batch to CSV in memory
        csv_buffer = StringIO()
        batch_df.write_csv(csv_buffer, include_header=False, null_value="")
        csv_buffer.seek(0)
        
        cursor = dbapi_conn.cursor()
        try:
            # Create temp table
            temp_table = f"fingerprint_staging_{batch_idx}"
            cursor.execute(f"""
                CREATE TEMP TABLE {temp_table} (LIKE stack_fingerprint INCLUDING DEFAULTS)
                ON COMMIT DROP
            """)
            
            # COPY data into temp table
            copy_sql = f"""
                COPY {temp_table} ({columns_str})
                FROM STDIN
                WITH (FORMAT CSV, NULL '')
            """
            
            # Use psycopg3-style copy
            with cursor.copy(copy_sql) as copy:
                while data := csv_buffer.read(8192):
                    copy.write(data)
            
            # UPSERT from temp to real table
            upsert_sql = f"""
                INSERT INTO stack_fingerprint ({columns_str})
                SELECT {columns_str} FROM {temp_table}
                ON CONFLICT (series_stack_id)
                DO UPDATE SET {update_set}, updated_at = CURRENT_TIMESTAMP
            """
            cursor.execute(upsert_sql)
            
            # Commit this batch to release memory
            dbapi_conn.commit()
            
            processed += batch_df.height
            
            if progress_callback:
                progress_callback(processed, total_rows)
            
            if log_callback:
                log_callback(f"  Batch {batch_idx + 1} committed: {processed:,}/{total_rows:,} total")
                
        except Exception as e:
            dbapi_conn.rollback()
            logger.error("Batch %d failed: %s", batch_idx + 1, e)
            raise
        finally:
            cursor.close()
    
    if log_callback:
        log_callback(f"Bulk UPSERT complete: {processed:,} fingerprints")
    
    return processed
