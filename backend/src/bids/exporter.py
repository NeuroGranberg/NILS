"""Unified BIDS exporter for DICOM and NIfTI outputs.

Features:
- Filters by intent (directory_type) and provenance.
- Provenance routing (SyMRI under anat/SyMRI, SWI in anat, projections optionally excluded).
- Collision-safe naming with time-ordered suffixes.
- Parallel copy (DICOM) and parallel dcm2niix conversion (NIfTI).
"""

from __future__ import annotations

import os
import shutil
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable, Optional, Sequence, Callable

from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import text

from metadata_db.session import SessionLocal as MetadataSessionLocal

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

# Provenances that cannot be converted to NIfTI format.
# These sequences have special DICOM structures incompatible with dcm2niix:
# - SyMRI: Multi-parameter synthetic MRI (TI×TE×complex dimensions)
NIFTI_INCOMPATIBLE_PROVENANCES = frozenset({"SyMRI"})

# --------------------------------------------------------------------------- #
# Config models
# --------------------------------------------------------------------------- #


class OutputMode(str, Enum):
    DCM = "dcm"
    NII = "nii"
    NII_GZ = "nii.gz"


class Layout(str, Enum):
    BIDS = "bids"
    FLAT = "flat"


class OverwriteMode(str, Enum):
    PROMPT = "prompt"
    CLEAN = "clean"
    OVERWRITE = "overwrite"
    SKIP = "skip"


class BidsExportConfig(BaseModel):
    output_modes: list[OutputMode] = Field(default_factory=lambda: [OutputMode.DCM])
    layout: Layout = Layout.BIDS
    overwrite_mode: OverwriteMode = OverwriteMode.SKIP

    include_intents: list[str] = Field(default_factory=list)
    include_provenance: list[str] = Field(default_factory=list)
    exclude_provenance: list[str] = Field(default_factory=list)
    group_symri: bool = True

    copy_workers: int = Field(8, ge=1, le=64)
    convert_workers: int = Field(8, ge=1, le=64)

    bids_dcm_root_name: str = "bids-dcm"
    bids_nifti_root_name: str = "bids-nifti"
    flat_dcm_root_name: str = "flat-dcm"
    flat_nifti_root_name: str = "flat-nifti"
    dcm2niix_path: str = "dcm2niix"

    # Subject identifier selection: "subject_code" (default) or an id_type_id integer
    # When set to an id_type_id, uses subject_other_identifiers lookup
    subject_identifier_source: str | int = "subject_code"

    # Cohort filter: when set, only exports stacks from this cohort
    # This dramatically improves query performance for large databases
    cohort_name: str | None = None

    # Field strength filter: when set, only exports stacks from specified field strengths (Tesla)
    # Empty list = include all; [3.0, 7.0] = only 3T and 7T
    include_field_strengths: list[float] = Field(default_factory=list)

    # When False, acceleration_csv is excluded from exported file names
    include_acceleration_in_name: bool = True

    # Stack ID filter: when set, only exports these specific stacks
    # Used by standalone export to export a resolved manifest subset
    include_stack_ids: list[int] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _coerce_output_modes(cls, values: dict) -> dict:
        if not isinstance(values, dict):
            return values

        # Accept legacy single output_mode/outputMode and normalize to output_modes.
        if "output_modes" not in values:
            legacy = values.get("output_mode") or values.get("outputMode")
            if legacy:
                values["output_modes"] = [legacy]

        if "output_modes" not in values or values["output_modes"] in (None, []):
            values["output_modes"] = [OutputMode.DCM]

        return values

    @model_validator(mode="before")
    @classmethod
    def _normalize_flat_root_names(cls, values: dict) -> dict:
        if not isinstance(values, dict):
            return values

        if values.get("flat_dcm_root_name") == "dcm-flat":
            values["flat_dcm_root_name"] = "flat-dcm"
        if values.get("flat_nifti_root_name") == "nii-flat":
            values["flat_nifti_root_name"] = "flat-nifti"

        # Accept camelCase variants from frontend state
        if values.get("flatDcmRootName") == "dcm-flat":
            values["flatDcmRootName"] = "flat-dcm"
        if values.get("flatNiftiRootName") == "nii-flat":
            values["flatNiftiRootName"] = "flat-nifti"

        return values

    @model_validator(mode="before")
    @classmethod
    def _normalize_overwrite_mode(cls, values: dict) -> dict:
        if not isinstance(values, dict):
            return values

        raw = values.get("overwrite_mode") or values.get("overwriteMode")
        if raw is None or raw == "":
            values["overwrite_mode"] = OverwriteMode.SKIP
            return values

        # Treat legacy "prompt" as skip to avoid blocking existing outputs
        if str(raw) == OverwriteMode.PROMPT.value:
            values["overwrite_mode"] = OverwriteMode.SKIP
            return values

        values["overwrite_mode"] = raw
        return values

    @field_validator("output_modes", mode="after")
    @classmethod
    def _validate_outputs(cls, modes: list[OutputMode | str]) -> list[OutputMode]:
        if not modes:
            raise ValueError("At least one output mode must be selected")

        normalized: list[OutputMode] = []
        seen = set()
        nifti_modes = set()
        for mode in modes:
            coerced = mode if isinstance(mode, OutputMode) else OutputMode(mode)
            if coerced in seen:
                continue
            seen.add(coerced)
            normalized.append(coerced)
            if coerced in (OutputMode.NII, OutputMode.NII_GZ):
                nifti_modes.add(coerced)

        if len(nifti_modes) > 1:
            raise ValueError("Choose either .nii or .nii.gz (not both)")

        return normalized

    @model_validator(mode="after")
    def _validate_root_names(self) -> "BidsExportConfig":
        # DICOM root must be non-empty and not use reserved names
        reserved_dcm = {"dcm-original", "dcm-raw"}
        if self.has_dicom:
            if not self.bids_dcm_root_name:
                raise ValueError("BIDS DICOM root name cannot be empty")
            if self.bids_dcm_root_name in reserved_dcm:
                raise ValueError(
                    "BIDS DICOM root name cannot be 'dcm-original' or 'dcm-raw'"
                )
            if not self.flat_dcm_root_name:
                raise ValueError("Flat DICOM root name cannot be empty")
            if self.flat_dcm_root_name in reserved_dcm:
                raise ValueError(
                    "Flat DICOM root name cannot be 'dcm-original' or 'dcm-raw'"
                )

        # NIfTI roots: allow empty to target raw root; otherwise use provided names
        if not self.bids_nifti_root_name:
            object.__setattr__(self, "bids_nifti_root_name", "")
        if not self.flat_nifti_root_name:
            object.__setattr__(self, "flat_nifti_root_name", "")

        return self

    @property
    def has_dicom(self) -> bool:
        return OutputMode.DCM in self.output_modes

    @property
    def nifti_mode(self) -> OutputMode | None:
        for mode in self.output_modes:
            if mode in (OutputMode.NII, OutputMode.NII_GZ):
                return mode
        return None

    @property
    def is_nifti(self) -> bool:
        return self.nifti_mode is not None

    @property
    def compression_flag(self) -> str:
        return "y" if self.nifti_mode == OutputMode.NII_GZ else "n"

    @field_validator("include_intents", "include_provenance", "exclude_provenance")
    @classmethod
    def _dedupe(cls, value: list[str]) -> list[str]:
        # Preserve order, drop duplicates
        seen = set()
        result = []
        for item in value:
            if item not in seen:
                seen.add(item)
                result.append(item)
        return result

    @field_validator("include_field_strengths")
    @classmethod
    def _validate_field_strengths(cls, value: list[float]) -> list[float]:
        # Valid normalized field strengths in Tesla
        valid = {0.5, 1.0, 1.5, 3.0, 7.0}
        # Dedupe and filter to valid values only
        return [v for v in dict.fromkeys(value) if v in valid]


# --------------------------------------------------------------------------- #
# Data structures
# --------------------------------------------------------------------------- #


ORIENT_ABBREV = {
    "Axial": "Ax",
    "Coronal": "Cor",
    "Sagittal": "Sag",
}


@dataclass
class StackRecord:
    series_stack_id: int
    series_id: int
    series_instance_uid: str
    stack_index: int
    stack_key: Optional[str]
    subject_code: str
    study_date: str
    series_time: Optional[str]
    directory_type: Optional[str]
    base: Optional[str]
    acquisition_type: Optional[str]  # 2D or 3D
    technique: Optional[str]
    modifier_csv: Optional[str]
    construct_csv: Optional[str]
    provenance: Optional[str]
    acceleration_csv: Optional[str]
    post_contrast: Optional[int]
    spinal_cord: Optional[int]
    body_part: Optional[str]
    stack_orientation: Optional[str]
    dicom_files: list[str]
    dwi_b_value: Optional[float] = None
    dwi_pe_direction: Optional[str] = None
    dwi_n_directions: Optional[int] = None

    # Acquisition parameters (from stack_fingerprint)
    echo_time: Optional[float] = None
    repetition_time: Optional[float] = None
    inversion_time: Optional[float] = None
    flip_angle: Optional[float] = None

    # Computed fields (filled later)
    dest_rel_dir: Optional[Path] = None
    dest_name: Optional[str] = None


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _build_stack_name(
    stack: StackRecord,
    is_multi_stack_series: bool = False,
    include_acceleration: bool = True,
) -> str:
    """Build the base name for a stack.

    Args:
        stack: The stack record
        is_multi_stack_series: If True, this series has multiple stacks and we should
            add an echo/inversion suffix based on stack_key
    """
    orient = stack.stack_orientation or ""
    orient_part = ORIENT_ABBREV.get(orient, orient[:3]) if orient else ""
    base = stack.base or ""
    acq_type = stack.acquisition_type or ""  # 2D or 3D
    mods = (stack.modifier_csv or "").replace(",", "-")
    tech = stack.technique or ""
    accel = (stack.acceleration_csv or "").replace(",", "-") if include_acceleration else ""
    construct = (stack.construct_csv or "").replace(",", "-")

    # Order: orientation, base, acquisition_type, modifiers, technique, acceleration, construct
    parts = [
        p for p in (orient_part, base, acq_type, mods, tech, accel, construct) if p
    ]
    _BODY_PART_PREFIX = {"spine": "SC", "neck": "Neck", "brain-neck": "BrainNeck"}
    bp_prefix = _BODY_PART_PREFIX.get(stack.body_part or "")
    if bp_prefix:
        parts.insert(0, bp_prefix)
    elif stack.spinal_cord and not stack.body_part:
        parts.insert(0, "SC")
    name = "_".join(parts) if parts else "unknown"

    # Mark contrast-enhanced series
    if stack.post_contrast:
        name = f"{name}_CE"

    # Append DWI suffix: _b1000_AP_32dir (each part omitted if NULL)
    # Skip b=0 for derived constructs (Trace/ADC): scanner writes b=0 as artifact;
    # genuine b=0 acquisitions (field maps) have no construct_csv.
    if stack.directory_type == "dwi":
        dwi_parts = []
        constructs = {c.strip().lower() for c in (stack.construct_csv or "").split(",") if c.strip()}
        derived = constructs & {"trace", "adc", "fa", "colfa", "isodwi"}
        skip_b0 = derived and stack.dwi_b_value == 0
        if stack.dwi_b_value is not None and not skip_b0:
            dwi_parts.append(f"b{int(stack.dwi_b_value)}")
        if stack.dwi_pe_direction:
            dwi_parts.append(stack.dwi_pe_direction)
        if stack.dwi_n_directions:
            dwi_parts.append(f"{stack.dwi_n_directions}dir")
        if dwi_parts:
            name = f"{name}_{'_'.join(dwi_parts)}"

    # Add echo/inversion suffix for multi-stack series
    if is_multi_stack_series and stack.stack_key:
        echo_num = stack.stack_index + 1
        if stack.stack_key == "multi_echo":
            name = f"{name}_e{echo_num}"
        elif stack.stack_key == "multi_ti":
            name = f"{name}_ti{echo_num}"
        # For multi_orientation and image_type_variation, the orientation prefix
        # or base name differences should handle disambiguation

    return name


def _destination_subfolder(stack: StackRecord, config: BidsExportConfig) -> str:
    intent = stack.directory_type or "misc"

    # Provenance routing
    if config.group_symri and stack.provenance == "SyMRI":
        return "anat/SyMRI"
    if stack.provenance == "SWIRecon":
        return "anat"
    if stack.provenance == "STAGE":
        return "anat"
    if stack.provenance == "ProjectionDerived":
        return "anat"

    return intent


def _format_subject(subject_code: str) -> str:
    cleaned = subject_code or "unknown"
    if cleaned.lower().startswith("sub-"):
        cleaned = cleaned[4:]
    return f"sub-{cleaned}"


def _format_session(study_date: str) -> str:
    cleaned = (study_date or "unknown").replace("-", "")
    return f"ses-{cleaned}"


def _apply_filters(
    stacks: Sequence[StackRecord], config: BidsExportConfig
) -> list[StackRecord]:
    result: list[StackRecord] = []
    include_intents = set(config.include_intents or [])
    include_provs = set(config.include_provenance or [])
    exclude_provs = set(config.exclude_provenance or [])
    selectable_provs = {"SyMRI", "SWIRecon", "EPIMix", "STAGE", "ProjectionDerived"}

    for stack in stacks:
        if include_intents and (stack.directory_type or "misc") not in include_intents:
            continue
        prov = stack.provenance or ""
        if include_provs and prov in selectable_provs and prov not in include_provs:
            continue
        if prov in exclude_provs:
            continue
        result.append(stack)

    return result


def _assign_unique_names(stacks: list[StackRecord], config: BidsExportConfig) -> None:
    """Assign collision-safe names with time-based ordering.

    Naming strategy:
    1. Build base name from classification fields (orientation, base, technique, etc.)
    2. For multi-stack series (same series_id), add echo/inversion suffix based on stack_key
    3. Only add collision numbers (_1, _2, etc.) when names still clash within a session/folder
    """
    # First pass: identify which series have multiple stacks
    series_stack_counts: dict[int, int] = defaultdict(int)
    for stack in stacks:
        if stack.series_id is not None:
            series_stack_counts[stack.series_id] += 1

    # Group stacks by (subject, session, destination folder)
    grouped: dict[tuple[str, str, str], list[StackRecord]] = defaultdict(list)

    for stack in stacks:
        dest_sub = _destination_subfolder(stack, config)
        key = (stack.subject_code, stack.study_date, dest_sub)
        grouped[key].append(stack)

    for (_, _, dest_sub), group in grouped.items():
        # Sort by series time then stack index
        group.sort(key=lambda s: ((s.series_time or "zzz"), s.stack_index))

        # Build names with echo/inversion suffix where applicable
        for stack in group:
            is_multi_stack = (
                stack.series_id is not None and series_stack_counts[stack.series_id] > 1
            )
            stack.dest_name = _build_stack_name(
                stack,
                is_multi_stack_series=is_multi_stack,
                include_acceleration=config.include_acceleration_in_name,
            )
            stack.dest_rel_dir = Path(dest_sub)

        # Check for name collisions and add numbered suffix only where needed
        name_counts: dict[str, list[StackRecord]] = defaultdict(list)
        for stack in group:
            name_counts[stack.dest_name].append(stack)

        # Provenance groups where collision numbering should be
        # synchronized by acquisition parameters (TE/TR/TI/FA) so that
        # e.g. magnitude_1 and phase_1 refer to the same echo.
        _PARAM_SORT_PROVENANCES = {"SyMRI", "SWIRecon", "EPIMix"}

        def _acq_params_key(s: StackRecord) -> tuple:
            return (
                s.echo_time or 0,
                s.repetition_time or 0,
                s.inversion_time or 0,
                s.flip_angle or 0,
            )

        for name, colliding_stacks in name_counts.items():
            if len(colliding_stacks) > 1:
                # For SyMRI/SWI/EPIMix: sort by acquisition parameters
                # so related series get matching collision numbers.
                if any(s.provenance in _PARAM_SORT_PROVENANCES for s in colliding_stacks):
                    colliding_stacks.sort(key=_acq_params_key)
                # Add numbered suffix
                for idx, stack in enumerate(colliding_stacks, start=1):
                    stack.dest_name = f"{name}_{idx}"


def _ensure_empty_or_handle(path: Path, mode: OverwriteMode) -> None:
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        return
    if mode == OverwriteMode.SKIP:
        # Ensure path exists but keep existing contents for per-stack skipping logic
        path.mkdir(parents=True, exist_ok=True)
        return
    if mode == OverwriteMode.CLEAN:
        for child in path.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink(missing_ok=True)
    elif mode == OverwriteMode.PROMPT:
        # If directory has contents, raise
        try:
            next(path.iterdir())
        except StopIteration:
            return
        raise RuntimeError(f"Output path {path} is not empty. Choose overwrite/clean.")
    else:
        path.mkdir(parents=True, exist_ok=True)


def _clean_root_preserve_child(root: Path, keep_child: str) -> None:
    """
    Clean all children under `root` except the specified `keep_child` directory.
    Creates `root` if missing. Silently ignores missing paths.
    """
    root.mkdir(parents=True, exist_ok=True)
    for child in root.iterdir():
        if child.name == keep_child:
            continue
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            try:
                child.unlink(missing_ok=True)
            except Exception:
                pass


# --------------------------------------------------------------------------- #
# Core export routines
# --------------------------------------------------------------------------- #


def _build_fetch_stacks_sql(config: BidsExportConfig) -> tuple[str, dict]:
    """Build SQL query for fetching stacks, with optional alternative identifier and cohort filter.

    Performance optimization: When cohort_name is set, filters stacks at the SQL level
    using the dicom_origin_cohort column. This dramatically reduces query time for large
    databases by avoiding full table scans.
    """
    use_alt_id = isinstance(config.subject_identifier_source, int)

    if use_alt_id:
        # Use subject_other_identifiers with specific id_type_id
        subject_select = "COALESCE(soi.other_identifier, subj.subject_code, 'unknown') AS subject_code"
        subject_join = """
        LEFT JOIN subject subj ON s.subject_id = subj.subject_id
        LEFT JOIN subject_other_identifiers soi ON subj.subject_id = soi.subject_id
            AND soi.id_type_id = :id_type_id"""
        params: dict = {"id_type_id": config.subject_identifier_source}
    else:
        # Default: use subject.subject_code
        subject_select = "COALESCE(subj.subject_code, 'unknown') AS subject_code"
        subject_join = "LEFT JOIN subject subj ON s.subject_id = subj.subject_id"
        params = {}

    # Build WHERE clause with multiple optional filters
    where_parts = []
    if config.cohort_name:
        where_parts.append("scc.dicom_origin_cohort = :cohort_name")
        params["cohort_name"] = config.cohort_name
    if config.include_field_strengths:
        placeholders = ", ".join(
            f":fs_{i}" for i in range(len(config.include_field_strengths))
        )
        where_parts.append(f"msd.magnetic_field_strength IN ({placeholders})")
        for i, fs in enumerate(config.include_field_strengths):
            params[f"fs_{i}"] = fs
    if config.include_stack_ids:
        placeholders = ", ".join(
            f":sid_{i}" for i in range(len(config.include_stack_ids))
        )
        where_parts.append(f"scc.series_stack_id IN ({placeholders})")
        for i, sid in enumerate(config.include_stack_ids):
            params[f"sid_{i}"] = sid

    where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    sql = f"""
        SELECT
            scc.series_stack_id,
            ss.series_id,
            scc.series_instance_uid,
            COALESCE(ss.stack_index, 0) AS stack_index,
            ss.stack_key,
            {subject_select},
            COALESCE(st.study_date::text, 'unknown') AS study_date,
            s.series_time,
            scc.directory_type,
            scc.base,
            scc.technique,
            scc.modifier_csv,
            scc.construct_csv,
            scc.provenance,
            scc.acceleration_csv,
            scc.post_contrast,
            scc.spinal_cord,
            scc.body_part,
            sf.stack_orientation,
            sf.mr_acquisition_type,
            msd.magnetic_field_strength,
            scc.dwi_b_value,
            scc.dwi_pe_direction,
            scc.dwi_n_directions,
            ss.stack_echo_time,
            ss.stack_repetition_time,
            ss.stack_inversion_time,
            ss.stack_flip_angle,
            paths.dicom_files
        FROM series_classification_cache scc
        JOIN series s ON scc.series_instance_uid = s.series_instance_uid
        JOIN study st ON s.study_id = st.study_id
        {subject_join}
        LEFT JOIN series_stack ss ON scc.series_stack_id = ss.series_stack_id
        LEFT JOIN stack_fingerprint sf ON scc.series_stack_id = sf.series_stack_id
        LEFT JOIN mri_series_details msd ON s.series_id = msd.series_id
        -- Aggregate instance file paths per stack in a correlated subquery keyed
        -- on series_stack_id. This avoids exploding the outer query to one row per
        -- instance (millions) and sorting them under a wide GROUP BY, which caused
        -- export timeouts on large cohorts. The (series_stack_id, instance_number,
        -- dicom_file_path) covering index serves this aggregation directly.
        LEFT JOIN LATERAL (
            SELECT ARRAY_AGG(i.dicom_file_path ORDER BY i.instance_number NULLS LAST) AS dicom_files
            FROM instance i
            WHERE i.series_stack_id = scc.series_stack_id
        ) paths ON true
        {where_clause}
    """
    return sql, params


def fetch_stacks(config: BidsExportConfig) -> list[StackRecord]:
    """Fetch classified stacks with instance file paths."""
    sql, params = _build_fetch_stacks_sql(config)

    with MetadataSessionLocal() as meta_db:
        rows = meta_db.execute(text(sql), params).fetchall()

    stacks: list[StackRecord] = []
    for row in rows:
        files = [f for f in row.dicom_files or [] if f]
        stacks.append(
            StackRecord(
                series_stack_id=row.series_stack_id,
                series_id=row.series_id,
                series_instance_uid=row.series_instance_uid,
                stack_index=row.stack_index,
                stack_key=row.stack_key,
                subject_code=row.subject_code,
                study_date=row.study_date,
                series_time=row.series_time.isoformat() if row.series_time is not None else None,
                directory_type=row.directory_type,
                base=row.base,
                acquisition_type=row.mr_acquisition_type,
                technique=row.technique,
                modifier_csv=row.modifier_csv,
                construct_csv=row.construct_csv,
                provenance=row.provenance,
                acceleration_csv=row.acceleration_csv,
                post_contrast=row.post_contrast,
                spinal_cord=row.spinal_cord,
                body_part=getattr(row, "body_part", None),
                stack_orientation=row.stack_orientation,
                dicom_files=files,
                dwi_b_value=getattr(row, "dwi_b_value", None),
                dwi_pe_direction=getattr(row, "dwi_pe_direction", None),
                dwi_n_directions=getattr(row, "dwi_n_directions", None),
                echo_time=getattr(row, "stack_echo_time", None),
                repetition_time=getattr(row, "stack_repetition_time", None),
                inversion_time=getattr(row, "stack_inversion_time", None),
                flip_angle=getattr(row, "stack_flip_angle", None),
            )
        )

    return _apply_filters(stacks, config)


def _fetch_cohort_raw_roots() -> list[Path]:
    """Fetch all cohort dcm-raw paths from the metadata database.

    Used as fallback roots when resolving DICOM file paths for subjects
    that span multiple cohorts (e.g., a subject extracted via iAID whose
    sessions are also part of an NMOSD export).
    """
    sql = "SELECT path FROM cohort WHERE path IS NOT NULL AND path != ''"
    with MetadataSessionLocal() as meta_db:
        rows = meta_db.execute(text(sql)).fetchall()
    return [Path(row.path) for row in rows if row.path]


def _resolve_source(
    path: str, raw_root: Path, fallback_roots: Sequence[Path] = ()
) -> Path:
    p = Path(path)
    if p.is_absolute():
        return p
    candidate = raw_root / p
    if candidate.exists():
        return candidate
    # Fallback: some datasets store dcm-raw under sub-<id>/... while stack paths omit the sub- prefix.
    parts = p.parts
    if parts:
        alt = (
            raw_root / ("sub-" + parts[0]) / Path(*parts[1:])
            if len(parts) > 1
            else raw_root / ("sub-" + parts[0])
        )
        if alt.exists():
            return alt
    # Cross-cohort fallback: try other cohort raw roots for subjects
    # that were originally extracted from a different cohort.
    for fb_root in fallback_roots:
        if fb_root == raw_root:
            continue
        fb_candidate = fb_root / p
        if fb_candidate.exists():
            return fb_candidate
    return candidate


def _compute_destinations(stacks: list[StackRecord], config: BidsExportConfig) -> None:
    _assign_unique_names(stacks, config)

    for stack in stacks:
        if stack.dest_name is None or stack.dest_rel_dir is None:
            raise RuntimeError("Destination naming failed")


def _copy_stack(
    stack: StackRecord,
    raw_root: Path,
    dest_dir: Path,
    fallback_roots: Sequence[Path] = (),
) -> tuple[int, int, Optional[str]]:
    """Copy one stack; returns (copied_files, skipped_files, error)."""
    copied = 0
    skipped = 0
    dest_dir.mkdir(parents=True, exist_ok=True)
    for src in stack.dicom_files:
        src_path = _resolve_source(src, raw_root, fallback_roots)
        if not src_path.exists():
            skipped += 1
            continue
        try:
            shutil.copy2(src_path, dest_dir / src_path.name)
            copied += 1
        except Exception as exc:  # pragma: no cover - defensive
            return copied, skipped, str(exc)
    return copied, skipped, None


def _convert_stack(
    stack: StackRecord,
    raw_root: Path,
    dest_dir: Path,
    filename: str,
    config: BidsExportConfig,
    fallback_roots: Sequence[Path] = (),
) -> tuple[bool, Optional[str]]:
    """Convert a stack's DICOM files to NIfTI using dcm2niix.

    Uses dcm2niix's text file mode (-s y) to convert only the specific DICOM
    files belonging to this stack, rather than an entire directory. This ensures
    each stack produces exactly one NIfTI file, even when multiple stacks share
    the same source directory (e.g., multi-echo sequences).
    """
    import tempfile

    dest_dir.mkdir(parents=True, exist_ok=True)
    if not stack.dicom_files:
        return False, "No DICOM files to convert"

    # Create a temporary file listing the specific DICOM files for this stack.
    # dcm2niix with -s y reads this file and converts only the listed files.
    file_list_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            for dicom_file in stack.dicom_files:
                src_path = _resolve_source(dicom_file, raw_root, fallback_roots)
                f.write(f"{src_path}\n")
            file_list_path = f.name

        cmd = [
            config.dcm2niix_path,
            "-s",
            "y",  # Text file mode: read file list from input path
            "-z",
            config.compression_flag,
            "-b",
            "y",
            "--terse",  # Omit filename post-fixes (_e2, _ph, etc.) - we handle naming ourselves
            "-f",
            filename,
            "-o",
            str(dest_dir),
            file_list_path,
        ]
        completed = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if completed.returncode != 0:
            stderr = (completed.stderr or "").strip()
            stdout = (completed.stdout or "").strip()
            detail = stderr or stdout or f"exit code {completed.returncode}"
            return False, f"dcm2niix failed ({detail})"
        return True, None
    finally:
        if file_list_path:
            os.unlink(file_list_path)


@dataclass
class ExportResult:
    """Result of a BIDS export operation.

    Attributes:
        total_stacks: Total number of stacks considered for export.
        exported_stacks: Number of stacks successfully exported (DICOM or NIfTI).
        copied_files: Total DICOM files copied.
        skipped_files: Files skipped (already exist, missing source, etc.).
        errors: Critical errors that prevented export of specific stacks.
        warnings: Non-critical issues (data quality, partial failures, etc.).
        skipped_nifti_provenances: Counts of stacks skipped for NIfTI by provenance.
            Example: {"SyMRI": 5, "DTIRecon": 3}
        nifti_conversion_errors: Count of dcm2niix failures.
        dicom_copy_errors: Count of DICOM copy failures.
    """

    total_stacks: int = 0
    exported_stacks: int = 0
    copied_files: int = 0
    skipped_files: int = 0

    # Critical errors that prevented export
    errors: list[str] = None  # type: ignore[assignment]

    # Non-critical warnings (data quality issues, etc.)
    warnings: list[str] = None  # type: ignore[assignment]

    # Stacks intentionally skipped for NIfTI due to incompatible provenance
    # Still exported as DICOM if DICOM export is enabled
    skipped_nifti_provenances: dict[str, int] = None  # type: ignore[assignment]

    # Detailed error counts by category
    nifti_conversion_errors: int = 0
    dicom_copy_errors: int = 0

    def __post_init__(self) -> None:
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
        if self.skipped_nifti_provenances is None:
            self.skipped_nifti_provenances = {}


def run_bids_export(
    raw_root: Path,
    derivatives_root: Path,
    config: BidsExportConfig,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> ExportResult:
    """
    Execute export given a config and prepared derivatives roots.
    """
    stacks = fetch_stacks(config)
    _compute_destinations(stacks, config)

    # Fetch all cohort raw roots for cross-cohort path resolution.
    # Subjects that span multiple cohorts may have dicom_file_path entries
    # relative to a different cohort's dcm-raw directory.
    fallback_roots = _fetch_cohort_raw_roots()

    if not config.has_dicom and not config.is_nifti:
        raise RuntimeError("No outputs selected: enable DICOM and/or NIfTI")

    # Determine output roots
    cohort_root = derivatives_root.parent
    if config.layout == Layout.BIDS:
        dcm_root = derivatives_root / config.bids_dcm_root_name
        nifti_root = (
            derivatives_root / config.bids_nifti_root_name
            if config.bids_nifti_root_name
            else cohort_root
        )
    else:
        dcm_root = derivatives_root / config.flat_dcm_root_name
        nifti_root = (
            derivatives_root / config.flat_nifti_root_name
            if config.flat_nifti_root_name
            else cohort_root
        )

    # Ensure destinations are ready for the selected outputs
    if config.has_dicom:
        _ensure_empty_or_handle(dcm_root, config.overwrite_mode)
    if config.is_nifti:
        # Special clean behavior when writing directly to cohort root (no nifti subfolder)
        if nifti_root == cohort_root and config.overwrite_mode == OverwriteMode.CLEAN:
            _clean_root_preserve_child(cohort_root, keep_child=derivatives_root.name)
        else:
            _ensure_empty_or_handle(nifti_root, config.overwrite_mode)

    result = ExportResult(total_stacks=len(stacks))

    dcm_tasks: list[tuple[StackRecord, Path]] = []
    nifti_tasks: list[tuple[StackRecord, Path, str]] = []
    skip_events: list[int] = []

    for stack in stacks:
        subject = _format_subject(stack.subject_code)
        session = _format_session(stack.study_date)

        # Check if this stack's provenance is incompatible with NIfTI conversion
        is_nifti_incompatible = stack.provenance in NIFTI_INCOMPATIBLE_PROVENANCES

        if config.layout == Layout.BIDS:
            dest_base_dcm = dcm_root / subject / session / stack.dest_rel_dir
            dest_base_nifti = nifti_root / subject / session / stack.dest_rel_dir
            if config.has_dicom:
                dest_dir_dcm = dest_base_dcm / stack.dest_name
                if (
                    config.overwrite_mode == OverwriteMode.SKIP
                    and dest_dir_dcm.exists()
                ):
                    skip_events.append(len(stack.dicom_files))
                else:
                    dcm_tasks.append((stack, dest_dir_dcm))
            if config.is_nifti:
                if is_nifti_incompatible:
                    # Track skipped NIfTI conversions by provenance
                    prov = stack.provenance or "Unknown"
                    result.skipped_nifti_provenances[prov] = (
                        result.skipped_nifti_provenances.get(prov, 0) + 1
                    )
                else:
                    target_file = (
                        dest_base_nifti
                        / f"{stack.dest_name}.{'nii.gz' if config.nifti_mode == OutputMode.NII_GZ else 'nii'}"
                    )
                    if (
                        config.overwrite_mode == OverwriteMode.SKIP
                        and target_file.exists()
                    ):
                        skip_events.append(1)
                    else:
                        nifti_tasks.append((stack, dest_base_nifti, stack.dest_name))
        else:
            flat_name = f"{subject}_{session}_{stack.dest_name}"
            if config.has_dicom:
                dest_dir_dcm = dcm_root / flat_name
                if (
                    config.overwrite_mode == OverwriteMode.SKIP
                    and dest_dir_dcm.exists()
                ):
                    skip_events.append(len(stack.dicom_files))
                else:
                    dcm_tasks.append((stack, dest_dir_dcm))
            if config.is_nifti:
                if is_nifti_incompatible:
                    # Track skipped NIfTI conversions by provenance
                    prov = stack.provenance or "Unknown"
                    result.skipped_nifti_provenances[prov] = (
                        result.skipped_nifti_provenances.get(prov, 0) + 1
                    )
                else:
                    target_file = (
                        nifti_root
                        / f"{flat_name}.{'nii.gz' if config.nifti_mode == OutputMode.NII_GZ else 'nii'}"
                    )
                    if (
                        config.overwrite_mode == OverwriteMode.SKIP
                        and target_file.exists()
                    ):
                        skip_events.append(1)
                    else:
                        nifti_tasks.append((stack, nifti_root, flat_name))

    processed = 0
    total_tasks = len(dcm_tasks) + len(nifti_tasks) + len(skip_events)

    # Account for skipped tasks up-front so progress reflects them
    for skipped_files in skip_events:
        processed += 1
        result.skipped_files += skipped_files
        if progress_cb:
            progress_cb(processed, total_tasks)

    if config.is_nifti and nifti_tasks:
        import multiprocessing as mp
        from concurrent.futures import ProcessPoolExecutor, as_completed

        # Use 'spawn' context to avoid fork() memory issues.
        # With vm.overcommit_memory=2 (strict), fork() must reserve the parent's
        # entire virtual address space (~12GB) per worker, which can exceed the
        # system's commit limit. Spawn creates fresh interpreters without this overhead.
        ctx = mp.get_context("spawn")
        with ProcessPoolExecutor(
            max_workers=config.convert_workers, mp_context=ctx
        ) as pool:
            futures = {
                pool.submit(
                    _convert_stack, stack, raw_root, dest_dir, filename, config, fallback_roots
                ): (stack, dest_dir)
                for stack, dest_dir, filename in nifti_tasks
            }
            for fut in as_completed(futures):
                stack, _dest = futures[fut]
                ok, err = fut.result()
                processed += 1
                if progress_cb:
                    progress_cb(processed, total_tasks)
                if ok:
                    result.exported_stacks += 1
                else:
                    result.nifti_conversion_errors += 1
                    result.errors.append(f"Stack {stack.series_stack_id}: {err}")

    if config.has_dicom and dcm_tasks:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=config.copy_workers) as pool:
            futures = {
                pool.submit(_copy_stack, stack, raw_root, dest_dir, fallback_roots): stack
                for stack, dest_dir in dcm_tasks
            }
            for fut in as_completed(futures):
                stack = futures[fut]
                copied, skipped, err = fut.result()
                processed += 1
                if progress_cb:
                    progress_cb(processed, total_tasks)
                result.copied_files += copied
                result.skipped_files += skipped
                if err:
                    result.dicom_copy_errors += 1
                    result.errors.append(f"Stack {stack.series_stack_id}: {err}")
                else:
                    result.exported_stacks += 1

    return result


__all__ = [
    "BidsExportConfig",
    "OutputMode",
    "Layout",
    "OverwriteMode",
    "run_bids_export",
    "ExportResult",
]
