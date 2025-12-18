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
                raise ValueError("BIDS DICOM root name cannot be 'dcm-original' or 'dcm-raw'")
            if not self.flat_dcm_root_name:
                raise ValueError("Flat DICOM root name cannot be empty")
            if self.flat_dcm_root_name in reserved_dcm:
                raise ValueError("Flat DICOM root name cannot be 'dcm-original' or 'dcm-raw'")

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
    stack_orientation: Optional[str]
    dicom_files: list[str]

    # Computed fields (filled later)
    dest_rel_dir: Optional[Path] = None
    dest_name: Optional[str] = None


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _build_stack_name(stack: StackRecord, is_multi_stack_series: bool = False) -> str:
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
    accel = (stack.acceleration_csv or "").replace(",", "-")
    construct = (stack.construct_csv or "").replace(",", "-")

    # Order: orientation, base, acquisition_type, modifiers, technique, acceleration, construct
    parts = [p for p in (orient_part, base, acq_type, mods, tech, accel, construct) if p]
    if stack.spinal_cord:
        parts.insert(0, "SC")
    name = "_".join(parts) if parts else "unknown"

    # Mark contrast-enhanced series
    if stack.post_contrast:
        name = f"{name}_CE"

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
    if stack.provenance == "ProjectionDerived":
        return "anat"

    return intent


def _format_subject(subject_code: str) -> str:
    cleaned = subject_code or "unknown"
    return f"sub-{cleaned}"


def _format_session(study_date: str) -> str:
    cleaned = (study_date or "unknown").replace("-", "")
    return f"ses-{cleaned}"


def _apply_filters(stacks: Sequence[StackRecord], config: BidsExportConfig) -> list[StackRecord]:
    result: list[StackRecord] = []
    include_intents = set(config.include_intents or [])
    include_provs = set(config.include_provenance or [])
    exclude_provs = set(config.exclude_provenance or [])
    selectable_provs = {"SyMRI", "SWIRecon", "EPIMix", "ProjectionDerived"}

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
                stack.series_id is not None
                and series_stack_counts[stack.series_id] > 1
            )
            stack.dest_name = _build_stack_name(stack, is_multi_stack_series=is_multi_stack)
            stack.dest_rel_dir = Path(dest_sub)

        # Check for name collisions and add numbered suffix only where needed
        name_counts: dict[str, list[StackRecord]] = defaultdict(list)
        for stack in group:
            name_counts[stack.dest_name].append(stack)

        for name, colliding_stacks in name_counts.items():
            if len(colliding_stacks) > 1:
                # Multiple stacks have the same name - add numbered suffix
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
    """Build SQL query for fetching stacks, with optional alternative identifier."""
    use_alt_id = isinstance(config.subject_identifier_source, int)

    if use_alt_id:
        # Use subject_other_identifiers with specific id_type_id
        subject_select = "COALESCE(soi.other_identifier, subj.subject_code, 'unknown') AS subject_code"
        subject_join = """
        LEFT JOIN subject subj ON s.subject_id = subj.subject_id
        LEFT JOIN subject_other_identifiers soi ON subj.subject_id = soi.subject_id
            AND soi.id_type_id = :id_type_id"""
        group_by_subject = "soi.other_identifier, subj.subject_code"
        params = {"id_type_id": config.subject_identifier_source}
    else:
        # Default: use subject.subject_code
        subject_select = "COALESCE(subj.subject_code, 'unknown') AS subject_code"
        subject_join = "LEFT JOIN subject subj ON s.subject_id = subj.subject_id"
        group_by_subject = "subj.subject_code"
        params = {}

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
            sf.stack_orientation,
            sf.mr_acquisition_type,
            ARRAY_AGG(i.dicom_file_path ORDER BY i.instance_number NULLS LAST) AS dicom_files
        FROM series_classification_cache scc
        JOIN series s ON scc.series_instance_uid = s.series_instance_uid
        JOIN study st ON s.study_id = st.study_id
        {subject_join}
        LEFT JOIN series_stack ss ON scc.series_stack_id = ss.series_stack_id
        LEFT JOIN stack_fingerprint sf ON scc.series_stack_id = sf.series_stack_id
        LEFT JOIN instance i ON i.series_stack_id = scc.series_stack_id
        GROUP BY
            scc.series_stack_id,
            ss.series_id,
            scc.series_instance_uid,
            ss.stack_index,
            ss.stack_key,
            {group_by_subject},
            st.study_date,
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
            sf.stack_orientation,
            sf.mr_acquisition_type
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
                series_time=row.series_time,
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
                stack_orientation=row.stack_orientation,
                dicom_files=files,
            )
        )

    return _apply_filters(stacks, config)


def _resolve_source(path: str, raw_root: Path) -> Path:
    p = Path(path)
    if p.is_absolute():
        return p
    candidate = raw_root / p
    if candidate.exists():
        return candidate
    # Fallback: some datasets store dcm-raw under sub-<id>/... while stack paths omit the sub- prefix.
    parts = p.parts
    if parts:
        alt = raw_root / ("sub-" + parts[0]) / Path(*parts[1:]) if len(parts) > 1 else raw_root / ("sub-" + parts[0])
        if alt.exists():
            return alt
    return candidate


def _compute_destinations(stacks: list[StackRecord], config: BidsExportConfig) -> None:
    _assign_unique_names(stacks, config)

    for stack in stacks:
        if stack.dest_name is None or stack.dest_rel_dir is None:
            raise RuntimeError("Destination naming failed")


def _copy_stack(stack: StackRecord, raw_root: Path, dest_dir: Path) -> tuple[int, int, Optional[str]]:
    """Copy one stack; returns (copied_files, skipped_files, error)."""
    copied = 0
    skipped = 0
    dest_dir.mkdir(parents=True, exist_ok=True)
    for src in stack.dicom_files:
        src_path = _resolve_source(src, raw_root)
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
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            for dicom_file in stack.dicom_files:
                src_path = _resolve_source(dicom_file, raw_root)
                f.write(f"{src_path}\n")
            file_list_path = f.name

        cmd = [
            config.dcm2niix_path,
            "-s", "y",  # Text file mode: read file list from input path
            "-z", config.compression_flag,
            "-b", "y",
            "--terse",  # Omit filename post-fixes (_e2, _ph, etc.) - we handle naming ourselves
            "-f", filename,
            "-o", str(dest_dir),
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
    total_stacks: int = 0
    exported_stacks: int = 0
    copied_files: int = 0
    skipped_files: int = 0
    errors: list[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.errors is None:
            self.errors = []


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

    if not config.has_dicom and not config.is_nifti:
        raise RuntimeError("No outputs selected: enable DICOM and/or NIfTI")

    # Determine output roots
    cohort_root = derivatives_root.parent
    if config.layout == Layout.BIDS:
        dcm_root = derivatives_root / config.bids_dcm_root_name
        nifti_root = (
            derivatives_root / config.bids_nifti_root_name if config.bids_nifti_root_name else cohort_root
        )
    else:
        dcm_root = derivatives_root / config.flat_dcm_root_name
        nifti_root = (
            derivatives_root / config.flat_nifti_root_name if config.flat_nifti_root_name else cohort_root
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

        if config.layout == Layout.BIDS:
            dest_base_dcm = dcm_root / subject / session / stack.dest_rel_dir
            dest_base_nifti = nifti_root / subject / session / stack.dest_rel_dir
            if config.has_dicom:
                dest_dir_dcm = dest_base_dcm / stack.dest_name
                if config.overwrite_mode == OverwriteMode.SKIP and dest_dir_dcm.exists():
                    skip_events.append(len(stack.dicom_files))
                else:
                    dcm_tasks.append((stack, dest_dir_dcm))
            if config.is_nifti:
                target_file = dest_base_nifti / f"{stack.dest_name}.{ 'nii.gz' if config.nifti_mode == OutputMode.NII_GZ else 'nii' }"
                if config.overwrite_mode == OverwriteMode.SKIP and target_file.exists():
                    skip_events.append(1)
                else:
                    nifti_tasks.append((stack, dest_base_nifti, stack.dest_name))
        else:
            flat_name = f"{subject}_{session}_{stack.dest_name}"
            if config.has_dicom:
                dest_dir_dcm = dcm_root / flat_name
                if config.overwrite_mode == OverwriteMode.SKIP and dest_dir_dcm.exists():
                    skip_events.append(len(stack.dicom_files))
                else:
                    dcm_tasks.append((stack, dest_dir_dcm))
            if config.is_nifti:
                target_file = nifti_root / f"{flat_name}.{ 'nii.gz' if config.nifti_mode == OutputMode.NII_GZ else 'nii' }"
                if config.overwrite_mode == OverwriteMode.SKIP and target_file.exists():
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
        from concurrent.futures import ProcessPoolExecutor, as_completed

        with ProcessPoolExecutor(max_workers=config.convert_workers) as pool:
            futures = {
                pool.submit(_convert_stack, stack, raw_root, dest_dir, filename, config): (stack, dest_dir)
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
                    result.errors.append(f"Stack {stack.series_stack_id}: {err}")

    if config.has_dicom and dcm_tasks:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=config.copy_workers) as pool:
            futures = {
                pool.submit(_copy_stack, stack, raw_root, dest_dir): stack
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

