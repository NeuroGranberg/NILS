"""Core anonymization engine aligned with the reference CLI script."""

from __future__ import annotations

import csv
import hashlib
import logging
import os
import re
import time
import warnings
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future, wait, FIRST_COMPLETED, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Callable, Deque, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

import pydicom
from dateutil.relativedelta import relativedelta
from pydicom.datadict import dictionary_description
from pydicom.tag import Tag


warnings.filterwarnings(
    "ignore",
    message=r"The value length .* exceeds the maximum length .* allowed for VR",
    module="pydicom",
)
warnings.filterwarnings(
    "ignore",
    message=r"Incorrect value for Specific Character Set",
    module="pydicom",
)

from .config import (
    AnonymizeConfig,
    AnonymizeResult,
    AuditExportFormat,
    CsvMappingConfig,
    CsvMissingMode,
    DeterministicIdConfig,
    FolderIdConfig,
    PatientIdConfig,
    PatientIdStrategyType,
    SequentialDiscoveryMode,
    SequentialIdConfig,
)
from .exporter import StudyAuditAggregator, export_csv, export_encrypted_excel
from .store import (
    load_leaf_summaries_for_cohort,
    mark_study_audit_complete,
    record_leaf_audit_summary,
    study_audit_exists,
)


logger = logging.getLogger(__name__)


# Progress reporting callback: done_files, total_files, subjects_seen (top-level folders) or None
ProgressCallback = Callable[[int, int, Optional[int]], None]


# ---------------------------------------------------------------------------
# Static tag definitions (inlined to minimise module sprawl)
# ---------------------------------------------------------------------------

PATIENT_ID_TAG = (0x0010, 0x0020)
STUDY_DATE_TAG = (0x0008, 0x0020)
STUDY_INSTANCE_UID_TAG = (0x0020, 0x000D)

MANDATORY_TAGS: Tuple[Tuple[int, int], ...] = (
    (0x0008, 0x0016),
    (0x0008, 0x0018),
)

ANONYMIZATION_TAGS: Dict[str, Tuple[Tuple[int, int], ...]] = {
    "Patient_Information": (
        (0x0010, 0x0010),
        (0x0010, 0x0030),
        (0x0010, 0x0032),
        (0x0010, 0x0040),
        (0x0010, 0x1010),
        (0x0010, 0x1020),
        (0x0010, 0x1030),
        (0x0010, 0x1040),
        (0x0010, 0x0050),
        (0x0010, 0x1090),
        (0x0010, 0x1080),
        (0x0010, 0x1081),
        (0x0010, 0x2000),
        (0x0010, 0x2110),
        (0x0010, 0x2150),
        (0x0010, 0x2152),
        (0x0010, 0x2154),
        (0x0010, 0x2160),
        (0x0010, 0x2180),
        (0x0010, 0x21A0),
        (0x0010, 0x21B0),
        (0x0010, 0x21C0),
        (0x0010, 0x21D0),
        (0x0010, 0x1000),
        (0x0010, 0x1001),
        (0x0010, 0x1002),
        (0x0010, 0x0101),
        (0x0010, 0x0102),
        (0x0010, 0x2297),
        (0x0010, 0x2298),
        (0x0010, 0x0021),
        (0x0010, 0x1060),
        (0x0010, 0x21F0),
        (0x0010, 0x4000),
    ),
    "Clinical_Trial_Information": (
        (0x0012, 0x0010),
        (0x0012, 0x0020),
        (0x0012, 0x0021),
        (0x0012, 0x0030),
        (0x0012, 0x0031),
        (0x0012, 0x0040),
        (0x0012, 0x0042),
        (0x0012, 0x0050),
        (0x0012, 0x0051),
        (0x0012, 0x0060),
        (0x0012, 0x0071),
        (0x0012, 0x0072),
        (0x0012, 0x0081),
        (0x0012, 0x0082),
        (0x0012, 0x0083),
        (0x0012, 0x0084),
        (0x0012, 0x0085),
        (0x0012, 0x0086),
        (0x0012, 0x0087),
        (0x0012, 0x0088),
        (0x0012, 0x0089),
        (0x0012, 0x0090),
        (0x0012, 0x0091),
    ),
    "Healthcare_Provider_Information": (
        (0x0008, 0x0090),
        (0x0008, 0x0092),
        (0x0008, 0x0094),
        (0x0008, 0x0096),
        (0x0008, 0x1060),
        (0x0008, 0x106E),
        (0x0008, 0x1048),
        (0x0008, 0x1049),
        (0x0008, 0x1062),
        (0x0008, 0x1050),
        (0x0008, 0x1052),
        (0x0008, 0x1070),
        (0x0008, 0x1072),
        (0x0008, 0x2111),
        (0x0008, 0x1080),
        (0x0032, 0x1032),
        (0x0032, 0x1033),
        (0x0032, 0x1060),
        (0x0040, 0x1001),
        (0x0040, 0x0007),
        (0x0040, 0x0009),
        (0x0040, 0x0006),
        (0x0040, 0x000B),
        (0x0040, 0x0254),
        (0x0040, 0x0253),
        (0x0040, 0xA075),
        (0x0040, 0xA073),
        (0x0040, 0x0275),
        (0x0040, 0x0260),
        (0x0040, 0xA730),
        (0x0040, 0x1102),
        (0x0040, 0x1103),
        (0x0040, 0x1104),
        (0x0400, 0x0561),
        (0x0040, 0x1002),
        (0x0040, 0x1400),
        (0x0070, 0x0084),
        (0x0070, 0x0086),
    ),
    "Institution_Information": (
        (0x0008, 0x1010),
        (0x0008, 0x0080),
        (0x0008, 0x0081),
        (0x0008, 0x1040),
        (0x0008, 0x1041),
    ),
    "Time_And_Date_Information": (
        (0x0008, 0x0021),
        (0x0008, 0x0022),
        (0x0008, 0x0023),
        (0x0008, 0x0030),
        (0x0008, 0x0031),
        (0x0008, 0x0032),
        (0x0008, 0x0033),
        (0x0008, 0x0012),
        (0x0008, 0x0013),
        (0x0032, 0x1050),
        (0x0032, 0x1051),
        (0x0040, 0x0244),
        (0x0040, 0x0245),
        (0x0040, 0x0250),
        (0x0040, 0x0251),
        (0x0040, 0x2004),
        (0x0040, 0x2005),
        (0x0040, 0xA030),
        (0x0040, 0xA032),
    ),
}


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------


def _is_dicom_candidate(entry: os.DirEntry[str]) -> bool:
    try:
        if not entry.is_file():
            return False
    except FileNotFoundError:
        return False
    name = entry.name.lower()
    if name.endswith(".dcm"):
        return True
    if not os.path.splitext(name)[1]:
        return True
    return False


def iter_dicom_files(root: Path, max_workers: int = 16, group_by_leaf: bool = False) -> Iterator[Path]:
    """
    Yield DICOM-ish files (endswith .dcm or extensionless) under *root*.
    
    Args:
        root: Root directory to scan
        max_workers: Number of parallel workers for directory scanning
        group_by_leaf: If True, yields files grouped by parent directory (leaf) to improve
                      cache locality and enable per-leaf optimizations. Slightly higher
                      memory usage as it collects all files before yielding.
    """

    def walk(directory: Path) -> Tuple[List[Path], List[Path]]:
        files: List[Path] = []
        dirs: List[Path] = []
        with os.scandir(directory) as it:
            for entry in it:
                try:
                    if _is_dicom_candidate(entry):
                        files.append(Path(entry.path))
                    elif entry.is_dir():
                        dirs.append(Path(entry.path))
                except FileNotFoundError:
                    continue
        return files, dirs

    if group_by_leaf:
        # Collect all files first, then sort by parent directory
        all_files: List[Path] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(walk, root)]
            while futures:
                future = futures.pop()
                files, dirs = future.result()
                all_files.extend(files)
                for dir_path in dirs:
                    futures.append(executor.submit(walk, dir_path))
        
        # Sort by parent directory to group files from same leaf together
        logger.info(f"Grouping {len(all_files)} files by leaf directory for optimized processing")
        all_files.sort(key=lambda p: (p.parent, p.name))
        for file_path in all_files:
            yield file_path
    else:
        # Original streaming behavior
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(walk, root)]
            while futures:
                future = futures.pop()
                files, dirs = future.result()
                for file_path in files:
                    yield file_path
                for dir_path in dirs:
                    futures.append(executor.submit(walk, dir_path))


def iter_dicom_files_streaming_by_leaf(
    root: Path, 
    max_workers: int = 16,
    buffer_leaves: int = 200
) -> Iterator[Path]:
    """
    Stream files grouped by leaf directory without collecting everything.
    
    Yields files in batches of N leaves at a time, providing immediate
    processing start while maintaining leaf grouping.
    
    Args:
        root: Root directory to scan
        max_workers: Parallel workers for directory scanning
        buffer_leaves: Number of leaves to buffer before yielding (default 200)
    """
    
    def walk(directory: Path) -> Tuple[List[Path], List[Path]]:
        files: List[Path] = []
        dirs: List[Path] = []
        with os.scandir(directory) as it:
            for entry in it:
                try:
                    if _is_dicom_candidate(entry):
                        files.append(Path(entry.path))
                    elif entry.is_dir():
                        dirs.append(Path(entry.path))
                except FileNotFoundError:
                    continue
        return files, dirs
    
    # Buffer for batching
    file_buffer: List[Path] = []
    leaves_seen: Set[Path] = set()
    total_files = 0
    total_leaves = 0
    
    # Original breadth-first scan (yields files as found)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(walk, root)]
        while futures:
            future = futures.pop()
            files, dirs = future.result()
            
            # Add files to buffer
            file_buffer.extend(files)
            for f in files:
                leaves_seen.add(f.parent)
            
            # When buffer reaches threshold, sort and yield batch
            if len(leaves_seen) >= buffer_leaves:
                file_buffer.sort(key=lambda p: (p.parent, p.name))
                total_files += len(file_buffer)
                total_leaves += len(leaves_seen)
                logger.info(
                    f"Yielding batch: {len(file_buffer)} files from {len(leaves_seen)} leaves "
                    f"(total: {total_files} files, {total_leaves} leaves)"
                )
                for file_path in file_buffer:
                    yield file_path
                
                file_buffer.clear()
                leaves_seen.clear()
            
            # Continue scanning
            for dir_path in dirs:
                futures.append(executor.submit(walk, dir_path))
    
    # Yield remaining files
    if file_buffer:
        file_buffer.sort(key=lambda p: (p.parent, p.name))
        total_files += len(file_buffer)
        total_leaves += len(leaves_seen)
        logger.info(
            f"Yielding final batch: {len(file_buffer)} files from {len(leaves_seen)} leaves "
            f"(total: {total_files} files, {total_leaves} leaves)"
        )
        for file_path in file_buffer:
            yield file_path


def iter_dicom_files_depth_first(
    root: Path,
    max_workers: int = 16,
    batch_size: Optional[int] = None,
) -> Iterator[Path]:
    """
    Depth-first traversal that yields files grouped naturally by leaf.
    
    Processes directory tree in depth-first manner, completing all files
    in each leaf before moving to next. Provides best possible leaf grouping
    without any sorting.
    
    Args:
        root: Root directory to scan
        max_workers: Parallel workers (applied to sibling directories)
        batch_size: Number of sibling dirs to process in parallel (default: max_workers)
    """
    if batch_size is None:
        batch_size = max_workers
    
    def collect_leaf_files(leaf_dir: Path) -> List[Path]:
        """Collect all files in a single leaf directory."""
        files = []
        try:
            with os.scandir(leaf_dir) as it:
                for entry in it:
                    if _is_dicom_candidate(entry):
                        files.append(Path(entry.path))
        except (FileNotFoundError, PermissionError):
            pass
        files.sort()  # Deterministic order within leaf
        return files
    
    def traverse_depth_first(directory: Path) -> Iterator[Path]:
        """Recursively traverse directory tree depth-first."""
        # Yield all files in THIS directory first (if it's a leaf)
        leaf_files = collect_leaf_files(directory)
        for file_path in leaf_files:
            yield file_path
        
        # Then recursively process subdirectories
        subdirs = []
        try:
            with os.scandir(directory) as it:
                for entry in it:
                    if entry.is_dir():
                        subdirs.append(Path(entry.path))
        except (FileNotFoundError, PermissionError):
            pass
        
        subdirs.sort()  # Deterministic order
        
        # Process subdirectories in parallel batches
        for i in range(0, len(subdirs), batch_size):
            batch = subdirs[i:i + batch_size]
            
            if len(batch) == 1:
                # Single directory - no need for threading overhead
                for file_path in traverse_depth_first(batch[0]):
                    yield file_path
            else:
                # Multiple directories - process in parallel
                with ThreadPoolExecutor(max_workers=min(len(batch), max_workers)) as executor:
                    # Submit each subdirectory traversal
                    futures = {
                        executor.submit(lambda d: list(traverse_depth_first(d)), subdir): subdir
                        for subdir in batch
                    }
                    
                    # Yield results as they complete
                    for future in futures:
                        try:
                            for file_path in future.result():
                                yield file_path
                        except Exception as exc:
                            logger.warning(f"Failed to traverse {futures[future]}: {exc}")
    
    # Start depth-first traversal from root
    logger.info(f"Starting depth-first traversal from {root}")
    file_count = 0
    for file_path in traverse_depth_first(root):
        file_count += 1
        if file_count % 10000 == 0:
            logger.info(f"Depth-first: yielded {file_count} files...")
        yield file_path
    
    logger.info(f"Depth-first traversal complete: {file_count} files")


def collect_source_stats(root: Path) -> tuple[int, int]:
    def scan(directory: Path) -> tuple[int, int]:
        files_count = 0
        bytes_count = 0
        try:
            with os.scandir(directory) as it:
                for entry in it:
                    try:
                        if _is_dicom_candidate(entry):
                            files_count += 1
                            try:
                                bytes_count += entry.stat(follow_symlinks=False).st_size
                            except OSError:
                                continue
                        elif entry.is_dir():
                            sub_files, sub_bytes = scan(Path(entry.path))
                            files_count += sub_files
                            bytes_count += sub_bytes
                    except FileNotFoundError:
                        continue
        except FileNotFoundError:
            return 0, 0
        return files_count, bytes_count

    return scan(root)


def _relative_parts(path: Path, root: Path) -> List[str]:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return list(path.parts)
    return list(relative.parts)


def _leaf_relative_path(leaf: Path, root: Path) -> str:
    parts = _relative_parts(leaf, root)
    if parts:
        return "/".join(parts)
    return leaf.name


def _top_level_name(path: Path, root: Path) -> Optional[str]:
    parts = _relative_parts(path, root)
    if not parts:
        return None
    return parts[0]


# ---------------------------------------------------------------------------
# ID strategy helpers
# ---------------------------------------------------------------------------


def _tag_name(tag: Tag) -> str:
    try:
        return dictionary_description(tag)
    except Exception:  # pragma: no cover - defensive
        return "Unknown"


def _format_tag(tag: Tag) -> str:
    return f"({tag.group:04X},{tag.element:04X})"


def _id_from_pattern(pattern: str, number: int) -> str:
    width = pattern.count("X")
    if width <= 0:
        return f"{pattern}{number:04d}"
    return pattern.replace("X" * width, f"{number:0{width}d}")


def _dedupe(items: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    ordered: List[str] = []
    for value in items:
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _rename_from_leaf_states(
    output_root: Path,
    leaf_states: Dict[Path, "_LeafState"],
) -> List[str]:
    """
    Rename top-level patient folders based on processed leaf states.
    
    This is much faster than scanning the entire tree since we already know
    which directories were touched during processing.
    """
    errors: List[str] = []
    if not output_root.exists():
        return errors

    # Extract unique top-level renames from leaf states
    renames: Dict[str, str] = {}  # original_top_dir -> mapped_top_dir
    for state in leaf_states.values():
        if not state.patient_id_original or not state.patient_id_updated:
            continue
        if state.patient_id_original == state.patient_id_updated:
            continue
        # Assuming top-level directories are named after patient IDs
        renames[state.patient_id_original] = state.patient_id_updated

    if not renames:
        return errors

    # Rename the directories
    for original_name, new_name in renames.items():
        source_dir = output_root / original_name
        target_dir = output_root / new_name
        
        if not source_dir.exists():
            # Already renamed in a previous run, or doesn't exist
            continue
        
        if target_dir.exists():
            errors.append(
                f"Cannot rename '{source_dir}' to '{target_dir}': target already exists"
            )
            continue
        
        try:
            source_dir.rename(target_dir)
            logger.info(f"Renamed patient folder: {original_name} → {new_name}")
        except Exception as exc:
            errors.append(f"Failed to rename '{source_dir}' to '{target_dir}': {exc}")

    return errors


def _rename_patient_folders(output_root: Path, pid_strategy: "IDStrategy", fallback_map: Dict[str, str], max_workers: int = 16) -> List[str]:
    errors: List[str] = []
    if not output_root.exists():
        return errors

    mapping: Dict[str, str] = {}
    for old, new in pid_strategy.iter_mappings():
        if old and new and old != new:
            mapping[old] = new
    for old, new in fallback_map.items():
        if old and new and old != new and old not in mapping:
            mapping[old] = new

    if not mapping:
        return errors

    replacements: List[Tuple[str, str]] = sorted(
        mapping.items(), key=lambda pair: (-len(pair[0]), pair[0])
    )

    def name_needs_rename(name: str) -> bool:
        for old, _ in replacements:
            if old in name:
                return True
        return False

    def scan_children(parent: Path, depth: int) -> List[Tuple[Path, int, bool]]:
        children: List[Tuple[Path, int, bool]] = []
        try:
            with os.scandir(parent) as it:
                for entry in it:
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    child_path = Path(entry.path)
                    child_depth = depth + 1
                    children.append((child_path, child_depth, name_needs_rename(entry.name)))
        except FileNotFoundError:
            return children
        except Exception as exc:  # pragma: no cover - filesystem errors are rare
            errors.append(f"Failed to scan '{parent}': {exc}")
        return children

    dirs_by_depth: Dict[int, List[Path]] = defaultdict(list)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures: Dict[Future, Tuple[Path, int]] = {}

        def submit(target: Path, depth: int) -> None:
            futures[executor.submit(scan_children, target, depth)] = (target, depth)

        submit(output_root, 0)
        while futures:
            done, _ = wait(list(futures.keys()), return_when=FIRST_COMPLETED)
            for future in done:
                path, depth = futures.pop(future)
                try:
                    children = future.result()
                except Exception as exc:  # pragma: no cover - defensive
                    errors.append(f"Failed to enumerate '{path}': {exc}")
                    continue
                for child_path, child_depth, needs in children:
                    submit(child_path, child_depth)
                    if needs:
                        dirs_by_depth[child_depth].append(child_path)

    if not dirs_by_depth:
        return errors

    def rename_directory(directory: Path) -> Optional[str]:
        if not directory.exists():
            return None
        original_name = directory.name
        new_name = original_name
        for old, new in replacements:
            if old in new_name:
                new_name = new_name.replace(old, new)
        if new_name == original_name:
            return None
        target = directory.with_name(new_name)
        if target.exists():
            return f"Skipped renaming '{directory}' because '{target}' already exists"
        try:
            directory.rename(target)
        except Exception as exc:  # pragma: no cover - filesystem errors are rare
            return f"Failed to rename '{directory}' to '{target}': {exc}"
        return None

    for depth in sorted(dirs_by_depth.keys(), reverse=True):
        candidates = dirs_by_depth[depth]
        if not candidates:
            continue
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(rename_directory, path) for path in candidates]
            for future in futures:
                result = future.result()
                if result:
                    errors.append(result)

    return errors


def _safe_value_preview(value: object, max_length: int = 512) -> str:
    if value is None:
        return ""
    try:
        if isinstance(value, bytes):
            return f"<bytes:{len(value)}>"
        text = str(value)
    except Exception:  # pragma: no cover - defensive
        return "<unrepr>"
    text = " | ".join(text.splitlines()).strip()
    if len(text) <= max_length:
        return text
    return text[: max_length - 3] + "..."


def _element_is_uid(dataset: pydicom.Dataset, tag: Tag) -> bool:
    try:
        return tag in dataset and dataset[tag].VR == "UI"
    except Exception:
        return False


def _name_contains_uid_or_reference(tag: Tag) -> bool:
    name = _tag_name(tag).lower()
    return "uid" in name or ("referenc" in name and "sequence" in name)


def _compute_timepoint(first: datetime, study: datetime) -> str:
    delta = relativedelta(study, first)
    months = delta.years * 12 + delta.months + delta.days / 30.44
    rounded = round(abs(months))
    if rounded == 0:
        return "M00"
    nearest6 = 6 * round(rounded / 6)
    if nearest6 == 0:
        nearest6 = 6
    if abs(rounded - nearest6) <= 1:
        rounded = nearest6
    return f"M{rounded:02d}"


def _anonymization_tag_set(categories: Sequence[str]) -> List[Tag]:
    tags: Set[Tag] = {Tag(v) for v in MANDATORY_TAGS}
    for category in categories:
        for tag_tuple in ANONYMIZATION_TAGS.get(category, ()):  # pragma: no branch - small lists
            tags.add(Tag(tag_tuple))
    return sorted(tags, key=lambda t: (t.group, t.element))


def _light_read_pid(path: Path) -> Optional[str]:
    try:
        ds = pydicom.dcmread(str(path), specific_tags=[Tag(PATIENT_ID_TAG)], stop_before_pixels=True, force=False)
    except Exception:
        return None
    element = ds.get(Tag(PATIENT_ID_TAG))
    value = getattr(element, "value", "") if element else ""
    return str(value).strip() or None


def _light_read_uid_and_pid(path: Path) -> tuple[Optional[str], Optional[str]]:
    tags = [Tag(STUDY_INSTANCE_UID_TAG), Tag(PATIENT_ID_TAG)]
    try:
        ds = pydicom.dcmread(str(path), specific_tags=tags, stop_before_pixels=True, force=False)
    except Exception:
        return None, None
    uid_element = ds.get(Tag(STUDY_INSTANCE_UID_TAG))
    pid_element = ds.get(Tag(PATIENT_ID_TAG))
    uid = getattr(uid_element, "value", "") if uid_element else ""
    pid = getattr(pid_element, "value", "") if pid_element else ""
    uid_text = str(uid).strip()
    pid_text = str(pid).strip()
    return (uid_text or None, pid_text or None)


def _light_read_pid_date(path: Path) -> Optional[tuple[str, datetime]]:
    try:
        ds = pydicom.dcmread(
            str(path), specific_tags=[Tag(PATIENT_ID_TAG), Tag(STUDY_DATE_TAG)], stop_before_pixels=True, force=False
        )
    except Exception:
        return None
    pid = getattr(ds.get(Tag(PATIENT_ID_TAG)), "value", "")
    study_date = getattr(ds.get(Tag(STUDY_DATE_TAG)), "value", "")
    if not pid or not study_date:
        return None
    try:
        parsed = datetime.strptime(str(study_date), "%Y%m%d")
    except ValueError:
        return None
    return str(pid), parsed


def _discover_pids_by_top_folder(root: Path) -> List[str]:
    top_dirs = [path for path in root.iterdir() if path.is_dir()]
    top_dirs.sort(key=lambda p: p.name)

    discovered: List[str] = []
    for folder in top_dirs:
        for entry in folder.rglob("*"):
            if entry.is_file() and (entry.suffix.lower() == ".dcm" or not entry.suffix):
                pid = _light_read_pid(entry)
                if pid:
                    discovered.append(pid)
                    break
    return _dedupe(discovered)


def _discover_pids_one_per_study(root: Path, max_workers: int = 16) -> List[str]:
    paths = list(iter_dicom_files(root, max_workers=max_workers))
    seen: Set[str] = set()
    ordered: List[str] = []
    for path in paths:
        try:
            ds = pydicom.dcmread(str(path), specific_tags=[Tag(STUDY_INSTANCE_UID_TAG), Tag(PATIENT_ID_TAG)], stop_before_pixels=True)
        except Exception:
            continue
        uid = getattr(ds.get(Tag(STUDY_INSTANCE_UID_TAG)), "value", None)
        pid = getattr(ds.get(Tag(PATIENT_ID_TAG)), "value", None)
        if uid and pid and uid not in seen:
            seen.add(str(uid))
            ordered.append(str(pid))
    return _dedupe(ordered)


def _discover_pids_all(root: Path, max_workers: int = 16) -> List[str]:
    paths = list(iter_dicom_files(root, max_workers=max_workers))
    observed: Set[str] = set()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for pid in executor.map(_light_read_pid, paths):
            if pid:
                observed.add(pid)
    return sorted(observed)


# ---------------------------------------------------------------------------
# ID strategy implementations (localised here to minimise modules)
# ---------------------------------------------------------------------------


class IDStrategy:
    def map(self, old_id: str, filepath: Path) -> str:  # pragma: no cover - base method
        return old_id

    def lookup_replacement(self, old_id: str) -> Optional[str]:  # pragma: no cover - base method
        return None

    def iter_mappings(self) -> Iterable[Tuple[str, str]]:  # pragma: no cover - base method
        return ()


class SequentialStrategy(IDStrategy):
    def __init__(self, mapping: Dict[str, str]):
        self._mapping = mapping

    def map(self, old_id: str, filepath: Path) -> str:
        return self._mapping.get(old_id, old_id)

    def lookup_replacement(self, old_id: str) -> Optional[str]:
        mapped = self._mapping.get(old_id)
        if mapped and mapped != old_id:
            return mapped
        return None

    def iter_mappings(self) -> Iterable[Tuple[str, str]]:
        return self._mapping.items()


class PathStrategy(IDStrategy):
    def __init__(self, input_root: Path, depth_after_root: int, regex: str, pattern: str):
        self._root = input_root.resolve()
        self._depth = depth_after_root
        self._regex = re.compile(regex)
        self._pattern = pattern
        self._width = pattern.count("X") if "X" in pattern else 4

    def map(self, old_id: str, filepath: Path) -> str:
        try:
            relative = filepath.resolve().relative_to(self._root)
        except Exception:
            return old_id
        parts = list(relative.parts)
        index = self._depth - 1
        if not (0 <= index < len(parts)):
            return old_id
        segment = parts[index]
        match = self._regex.search(segment)
        token = match.group(1) if match and match.groups() else match.group(0) if match else segment

        if token.isdigit():
            number = int(token)
            return _id_from_pattern(self._pattern, number)

        placeholder_width = self._width
        placeholder = "X" * placeholder_width if placeholder_width > 0 else ""

        if placeholder_width > 0 and placeholder in self._pattern:
            return self._pattern.replace(placeholder, token)
        if placeholder_width == 0 and self._pattern:
            return f"{self._pattern}{token}"
        if placeholder_width == 0:
            return token

        hashed = int(hashlib.blake2b(token.encode(), digest_size=4).hexdigest(), 16)
        return _id_from_pattern(self._pattern, hashed % (10 ** self._width))


class DeterministicStrategy(IDStrategy):
    def __init__(self, pattern: str, salt: str):
        self._pattern = pattern
        self._salt = salt
        self._width = pattern.count("X") if "X" in pattern else 4

    def map(self, old_id: str, filepath: Path) -> str:
        key = f"{self._salt}|{old_id}"
        digest = hashlib.blake2b(key.encode(), digest_size=4).hexdigest()
        value = int(digest, 16)
        return _id_from_pattern(self._pattern, value % (10 ** self._width))

    def lookup_replacement(self, old_id: str) -> Optional[str]:
        mapped = self.map(old_id, Path("."))
        if mapped and mapped != old_id:
            return mapped
        return None


class CSVStrategy(IDStrategy):
    def __init__(self, mapping: Dict[str, str]):
        self._mapping = mapping

    def map(self, old_id: str, filepath: Path) -> str:
        return self._mapping.get(old_id, old_id)

    def lookup_replacement(self, old_id: str) -> Optional[str]:
        mapped = self._mapping.get(old_id)
        if mapped and mapped != old_id:
            return mapped
        return None


class CSVPlusDeterministicFallback(IDStrategy):
    def __init__(self, mapping: Dict[str, str], pattern: str, salt: str):
        self._mapping = mapping
        self._fallback = DeterministicStrategy(pattern, salt)

    def map(self, old_id: str, filepath: Path) -> str:
        mapped = self._mapping.get(old_id)
        if mapped:
            return mapped
        return self._fallback.map(old_id, filepath)

    def lookup_replacement(self, old_id: str) -> Optional[str]:
        mapped = self._mapping.get(old_id)
        if mapped and mapped != old_id:
            return mapped
        fallback_value = self._fallback.lookup_replacement(old_id)
        if fallback_value and fallback_value != old_id:
            return fallback_value
        return None

    def iter_mappings(self) -> Iterable[Tuple[str, str]]:
        return self._mapping.items()


def _build_id_strategy(config: AnonymizeConfig, files: Sequence[Path], max_workers: int = 16) -> IDStrategy:
    pid_config: PatientIdConfig = config.patient_id
    if not pid_config.enabled:
        return IDStrategy()

    strategy = pid_config.strategy
    if strategy == PatientIdStrategyType.NONE:
        return IDStrategy()
    if strategy == PatientIdStrategyType.FOLDER and pid_config.folder:
        folder: FolderIdConfig = pid_config.folder
        return PathStrategy(
            config.source_root,
            folder.depth_after_root,
            folder.regex,
            folder.fallback_template,
        )

    if strategy == PatientIdStrategyType.CSV and pid_config.csv_mapping:
        csv_cfg: CsvMappingConfig = pid_config.csv_mapping
        mapping: Dict[str, str] = {}
        with open(csv_cfg.path, "r", newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            source_col = csv_cfg.source_column
            target_col = csv_cfg.target_column
            for row in reader:
                src = (row.get(source_col) or "").strip()
                dst = (row.get(target_col) or "").strip()
                if src:
                    mapping[src] = dst

        if csv_cfg.missing_mode == CsvMissingMode.HASH:
            return CSVPlusDeterministicFallback(mapping, csv_cfg.missing_pattern, csv_cfg.missing_salt)

        fallback_ids = _discover_pids_by_top_folder(config.source_root)
        if not csv_cfg.preserve_top_folder_order:
            fallback_ids = sorted(set(fallback_ids))

        next_counter = csv_cfg.starting_number_for_missing(mapping)
        for pid in _dedupe(fallback_ids):
            if pid in mapping and mapping[pid]:
                continue
            mapping[pid] = _id_from_pattern(csv_cfg.missing_pattern, next_counter)
            next_counter += 1
        return CSVStrategy(mapping)

    if strategy == PatientIdStrategyType.DETERMINISTIC and pid_config.deterministic:
        det: DeterministicIdConfig = pid_config.deterministic
        return DeterministicStrategy(det.pattern, det.salt)

    if strategy == PatientIdStrategyType.SEQUENTIAL and pid_config.sequential:
        seq: SequentialIdConfig = pid_config.sequential
        discovered: List[str]
        if seq.discovery == SequentialDiscoveryMode.PER_TOP_FOLDER:
            discovered = _discover_pids_by_top_folder(config.source_root)
        elif seq.discovery == SequentialDiscoveryMode.ONE_PER_STUDY:
            discovered = _discover_pids_one_per_study(config.source_root, max_workers)
        else:
            discovered = _discover_pids_all(config.source_root, max_workers)
        mapping = {
            pid: _id_from_pattern(seq.pattern, index)
            for index, pid in enumerate(discovered, start=seq.starting_number)
        }
        return SequentialStrategy(mapping)

    return IDStrategy()


# ---------------------------------------------------------------------------
# Core execution helpers
# ---------------------------------------------------------------------------


def _collect_first_dates(config: AnonymizeConfig, files: Iterable[Path]) -> Dict[str, datetime]:
    if not config.study_dates.enabled:
        return {}

    first_dates: Dict[str, datetime] = {}
    for path in files:
        result = _light_read_pid_date(path)
        if not result:
            continue
        pid, study = result
        if pid not in first_dates or study < first_dates[pid]:
            first_dates[pid] = study
    return first_dates


def _parse_tag(value: str) -> Optional[Tag]:
    cleaned = value.strip().replace("(", "").replace(")", "").replace(",", "").replace(" ", "")
    if len(cleaned) != 8:
        return None
    try:
        group = int(cleaned[:4], 16)
        element = int(cleaned[4:], 16)
    except ValueError:
        return None
    return Tag(group, element)


@dataclass
class _Options:
    source_root: Path
    output_root: Path
    scrub_tags: List[Tag]
    exclude_tags: Set[Tag]
    anonymize_patient_id: bool
    map_timepoints: bool
    preserve_uids: bool
    rename_patient_folders: bool
    resume: bool = False
    audit_resume_per_leaf: bool = False
    cohort_name: Optional[str] = None


class _LeafProcessMode(str, Enum):
    FULL = "full"
    OUTPUT_ONLY = "output_only"
    SKIP = "skip"


@dataclass
class _LeafState:
    leaf_rel_path: str
    study_uid: Optional[str] = None
    audit_done: Optional[bool] = None
    processed_for_audit: bool = False
    had_error: bool = False
    leaf_skipped_recorded: bool = False
    pid_hint: Optional[str] = None
    mapped_patient_id: Optional[str] = None
    audit_anchor_done: bool = False
    files_seen: int = 0
    files_written: int = 0
    files_existing: int = 0
    errors: list[str] = field(default_factory=list)
    patient_id_original: Optional[str] = None
    patient_id_updated: Optional[str] = None
    anchor_rel_path: Optional[str] = None
    audit_tags: Dict[str, Dict[str, Optional[str]]] = field(default_factory=dict)
    audit_persisted_this_run: bool = False


@dataclass
class _ResumeMetrics:
    leaves_skipped: int = 0
    leaves_completed: int = 0
    files_reused: int = 0
    files_output_only: int = 0


@dataclass
class PatientProgressResult:
    """Result from processing a partition of patient folders with progress tracking."""
    total_files: int
    updated_files: int
    skipped_files: int
    errors: List[str]
    completed_patients: List[str]  # Patient folder names for logging
    patient_count: int             # Number of patients processed


def _merge_leaf_audit_event(state: _LeafState, event: dict) -> None:
    tag_code = event.get("tag")
    if not tag_code:
        return
    entry = state.audit_tags.setdefault(
        tag_code,
        {
            "tag": tag_code,
            "tag_name": event.get("tag_name"),
            "action": event.get("action"),
            "old_value": event.get("old_value"),
            "new_value": event.get("new_value"),
        },
    )
    if not entry.get("tag_name") and event.get("tag_name"):
        entry["tag_name"] = event.get("tag_name")
    if not entry.get("action") and event.get("action"):
        entry["action"] = event.get("action")
    if not entry.get("old_value") and event.get("old_value"):
        entry["old_value"] = event.get("old_value")
    if event.get("new_value"):
        entry["new_value"] = event.get("new_value")


def _build_leaf_audit_payload(state: _LeafState) -> Optional[dict]:
    if not state.audit_tags:
        return None
    tags = [
        {
            "tag": tag_code,
            "tag_name": details.get("tag_name"),
            "action": details.get("action"),
            "old_value": details.get("old_value"),
            "new_value": details.get("new_value"),
        }
        for tag_code, details in sorted(state.audit_tags.items())
    ]
    return {
        "anchor_rel_path": state.anchor_rel_path or state.leaf_rel_path,
        "tags": tags,
    }


def _events_from_summaries(summaries: Iterable[dict]) -> List[dict]:
    events: List[dict] = []
    for row in summaries:
        summary_payload = row.get("summary") or {}
        audit = summary_payload.get("audit") or {}
        rel_path = audit.get("anchor_rel_path") or row.get("leaf_rel_path")
        tags = audit.get("tags") or []
        for tag in tags:
            events.append(
                {
                    "study_uid": row.get("study_uid"),
                    "rel_path": rel_path,
                    "tag": tag.get("tag"),
                    "tag_name": tag.get("tag_name"),
                    "action": tag.get("action"),
                    "old_value": tag.get("old_value"),
                    "new_value": tag.get("new_value"),
                }
            )
    return events


_WORKER_OPTIONS: Optional["_Options"] = None
_WORKER_PID_STRATEGY: Optional["IDStrategy"] = None
_WORKER_FIRST_DATES: Dict[str, datetime] = {}


def _is_dicom_candidate_from_path(path: Path) -> bool:
    """Check if path looks like a DICOM file without filesystem call."""
    name = path.name.lower()
    if name.endswith(".dcm"):
        return True
    if not path.suffix:  # Extensionless
        return True
    return False


def _persist_aggregated_leaf_audit(
    leaf_uid: str,
    leaf_files: List[Path],
    leaf_results: List[Tuple[Path, Dict[str, object]]],
    leaf_errors: List[str],
    options: _Options,
) -> int:
    """
    Persist audit summary for entire leaf based on processing all files.
    
    Returns: Number of successfully processed files
    """
    # Aggregate audit events from all files
    all_audit_events = []
    files_written = 0
    files_reused = 0
    files_with_errors = 0
    patient_ids = set()
    
    for file_path, result in leaf_results:
        if result.get("error"):
            files_with_errors += 1
            continue
            
        # Count output files
        if result.get("wrote_output"):
            files_written += 1
        elif result.get("output_preexisting"):  
            files_reused += 1
            
        # Collect audit events
        audit_events = result.get("audit_events", [])
        all_audit_events.extend(audit_events)
        
        # Collect patient IDs
        if result.get("patient_id_original"):
            patient_ids.add(result["patient_id_original"])
    
    # Build aggregated audit payload
    audit_tags = {}
    for event in all_audit_events:
        tag_code = event.get("tag")
        if tag_code and tag_code not in audit_tags:  # Avoid duplicates
            audit_tags[tag_code] = event
    
    # Use first successful result for representative data
    representative_result = next((r for _, r in leaf_results if not r.get("error")), {})
    
    audit_payload = {
        "anchor_rel_path": representative_result.get("rel_path"),
        "tags": list(audit_tags.values()),
        "files_processed": len(leaf_results) - files_with_errors,
    }
    
    # Write to DB
    leaf_rel_path = _leaf_relative_path(leaf_files[0].parent, options.source_root)
    
    record_leaf_audit_summary(
        leaf_uid,
        cohort_name=options.cohort_name,
        leaf_rel_path=leaf_rel_path,
        files_total=len(leaf_files),
        files_written=files_written,
        files_reused=files_reused,
        files_with_errors=files_with_errors,
        patient_id_original=list(patient_ids)[0] if patient_ids else None,
        patient_id_updated=representative_result.get("patient_id_updated"),
        errors=leaf_errors[:10],  # Limit error list size
        audit_payload=audit_payload,
    )
    
    mark_study_audit_complete(
        leaf_uid,
        leaf_rel_path=leaf_rel_path,
        cohort_name=options.cohort_name,
    )
    
    return len(leaf_results) - files_with_errors  # Successfully processed files


def _process_patient_partition(
    patient_folder: Path,
    options: _Options,
    pid_strategy: "IDStrategy",
    first_dates: Dict[str, datetime],
) -> Tuple[int, int, int, List[str]]:
    """
    Process all files in ONE patient folder.
    
    NEW APPROACH: Group files by leaf, then process ALL files in each leaf.
    Each worker owns exclusive patient folders - no race conditions.
    DB is single source of truth for leaf processing status.
    
    Returns: (total_files, updated_files, skipped_files, errors)
    """
    total_files = 0
    updated_files = 0
    skipped_files = 0
    errors: List[str] = []
    
    # Phase 1: Group files by leaf (StudyInstanceUID)
    files_by_leaf: Dict[str, List[Path]] = defaultdict(list)
    
    for file_path in patient_folder.rglob("*"):
        if not file_path.is_file():
            continue
        if not _is_dicom_candidate_from_path(file_path):
            continue
        
        total_files += 1
        
        # Extract leaf identifier (StudyUID)
        try:
            leaf_uid, _ = _light_read_uid_and_pid(file_path)
            if leaf_uid:
                files_by_leaf[leaf_uid].append(file_path)
            else:
                skipped_files += 1
        except Exception:
            skipped_files += 1
            continue
    
    # Phase 2: Process each leaf as a unit
    for leaf_uid, leaf_files in files_by_leaf.items():
        # Skip if already processed (DB check)
        if study_audit_exists(leaf_uid):
            skipped_files += len(leaf_files)
            logger.debug(f"Skipping leaf {leaf_uid} (already in DB): {len(leaf_files)} files")
            continue
            
        # Process ALL files in this leaf
        leaf_results = []
        leaf_errors = []
        
        for file_path in leaf_files:
            try:
                result = _process_single_file(file_path, options, pid_strategy, first_dates)
                leaf_results.append((file_path, result))
                
                if result.get("error"):
                    leaf_errors.append(f"{file_path}: {result['error']}")
            except Exception as exc:
                leaf_errors.append(f"{file_path}: {exc}")
        
        # Phase 3: Aggregate and persist audit for entire leaf
        try:
            success_count = _persist_aggregated_leaf_audit(
                leaf_uid, 
                leaf_files, 
                leaf_results, 
                leaf_errors, 
                options
            )
            
            updated_files += success_count
            skipped_files += (len(leaf_files) - success_count)
            errors.extend(leaf_errors)
            
            logger.debug(f"Processed leaf {leaf_uid} from {patient_folder.name}: {success_count}/{len(leaf_files)} files successful")
            
        except Exception as exc:
            error_msg = f"Failed to persist audit for leaf {leaf_uid}: {exc}"
            errors.append(error_msg)
            skipped_files += len(leaf_files)
            logger.error(error_msg)
    
    return total_files, updated_files, skipped_files, errors


def _partition_patients(
    source_root: Path,
    num_workers: int,
) -> List[List[Path]]:
    """
    Partition top-level patient folders across workers.
    
    Returns: List of lists, one per worker, containing patient folders.
    """
    # Get all top-level folders
    try:
        patient_folders = [p for p in source_root.iterdir() if p.is_dir()]
    except Exception as exc:
        logger.error(f"Failed to list patient folders: {exc}")
        return [[] for _ in range(num_workers)]
    
    patient_folders.sort()  # Deterministic order
    
    if not patient_folders:
        logger.warning(f"No patient folders found in {source_root}")
        return [[] for _ in range(num_workers)]
    
    # Round-robin distribution for load balancing
    partitions: List[List[Path]] = [[] for _ in range(num_workers)]
    for idx, folder in enumerate(patient_folders):
        worker_idx = idx % num_workers
        partitions[worker_idx].append(folder)
    
    logger.info(f"Partitioned {len(patient_folders)} patients across {num_workers} workers")
    for i, partition in enumerate(partitions):
        if partition:
            logger.debug(f"  Worker {i}: {len(partition)} patients")
    
    return partitions


def _process_single_file(
    path: Path,
    options: _Options,
    pid_strategy: "IDStrategy",
    first_dates: Dict[str, datetime],
) -> Dict[str, object]:
    audit_events: List[Dict[str, object]] = []
    id_mapping: Dict[str, str] = {}

    pid_tag = Tag(PATIENT_ID_TAG)
    date_tag = Tag(STUDY_DATE_TAG)
    uid_tag = Tag(STUDY_INSTANCE_UID_TAG)

    try:
        ds = pydicom.dcmread(str(path), force=True, stop_before_pixels=False)
    except Exception as exc:
        error_message = str(exc)
        return {
            "audit_events": audit_events,
            "id_mapping": id_mapping,
            "error": error_message,
            "study_uid": None,
            "patient_id_original": None,
            "patient_id_updated": None,
            "rel_path": None,
            "wrote_output": False,
            "output_preexisting": False,
        }

    rel_parts = _relative_parts(path, options.source_root)
    rel_path = "/".join(rel_parts) if rel_parts else path.name
    original_pid = getattr(ds.get(pid_tag), "value", "")
    original_date = getattr(ds.get(date_tag), "value", "")
    study_uid = getattr(ds.get(uid_tag), "value", "")

    new_pid = original_pid
    if options.anonymize_patient_id and original_pid:
        mapped = pid_strategy.map(str(original_pid), path)
        if mapped and mapped != original_pid and pid_tag in ds:
            ds[pid_tag].value = mapped
            new_pid = mapped
            id_mapping[str(original_pid)] = str(mapped)
            audit_events.append(
                {
                    "rel_path": rel_path,
                    "study_uid": str(study_uid),
                    "tag": _format_tag(pid_tag),
                    "tag_name": _tag_name(pid_tag),
                    "action": "replaced",
                    "old_value": _safe_value_preview(original_pid),
                    "new_value": _safe_value_preview(mapped),
                }
            )

    new_date_value = original_date
    study_date_logged = False
    if options.map_timepoints and original_pid and original_date:
        try:
            study_dt = datetime.strptime(str(original_date), "%Y%m%d")
            first_dt = first_dates.get(str(original_pid))
            if first_dt:
                label = _compute_timepoint(first_dt, study_dt)
                if label:
                    ds[date_tag] = pydicom.DataElement(date_tag, "DA", label)
                    new_date_value = label
                    study_date_logged = True
                    audit_events.append(
                        {
                            "rel_path": rel_path,
                            "study_uid": str(study_uid),
                            "tag": _format_tag(date_tag),
                            "tag_name": _tag_name(date_tag),
                            "action": "replaced" if original_date else "added",
                            "old_value": _safe_value_preview(original_date),
                            "new_value": _safe_value_preview(label),
                        }
                    )
        except ValueError:
            pass
    if not study_date_logged and original_date:
        audit_events.append(
            {
                "rel_path": rel_path,
                "study_uid": str(study_uid),
                "tag": _format_tag(date_tag),
                "tag_name": _tag_name(date_tag),
                "action": "retained",
                "old_value": _safe_value_preview(original_date),
                "new_value": "",
            }
        )

    _scrub_dataset(
        ds,
        options,
        rel_path=rel_path,
        study_uid=str(study_uid),
        audit_events=audit_events,
    )

    target_path = _target_path(path, options)
    mapped_target: Optional[Path] = None
    if options.rename_patient_folders and rel_parts and new_pid:
        mapped_top = str(new_pid)
        if mapped_top and mapped_top != rel_parts[0]:
            mapped_parts = list(rel_parts)
            mapped_parts[0] = mapped_top
            mapped_target = options.output_root.joinpath(*mapped_parts)

    skip_write = False
    if target_path.exists():
        skip_write = True
    elif mapped_target and mapped_target.exists():
        skip_write = True

    wrote_output = False
    error_message: Optional[str]
    if not skip_write:
        try:
            _save_dataset(ds, path, options, mapped_pid=new_pid if options.rename_patient_folders else None)
        except Exception as exc:  # pragma: no cover - defensive
            error_message = str(exc)
        else:
            error_message = None
            wrote_output = True
    else:
        error_message = None

    return {
        "audit_events": audit_events,
        "id_mapping": id_mapping,
        "error": error_message,
        "study_uid": str(study_uid) if study_uid else None,
        "patient_id_original": str(original_pid) if original_pid else None,
        "patient_id_updated": str(new_pid) if new_pid else None,
        "rel_path": rel_path,
        "wrote_output": wrote_output,
        "output_preexisting": skip_write,
    }


def _worker_init(options: _Options, pid_strategy: "IDStrategy", first_dates: Dict[str, datetime]) -> None:
    """
    Initialize worker process after fork.
    
    CRITICAL: Must dispose database engine to avoid prepared statement conflicts.
    When forking, child processes inherit parent's DB connections/prepared statements,
    which PostgreSQL treats as conflicts. Disposing forces fresh connections per worker.
    """
    global _WORKER_OPTIONS, _WORKER_PID_STRATEGY, _WORKER_FIRST_DATES
    
    # Dispose inherited database connections - prevents prepared statement conflicts
    from db.session import engine
    engine.dispose()
    
    _WORKER_OPTIONS = options
    _WORKER_PID_STRATEGY = pid_strategy
    _WORKER_FIRST_DATES = first_dates


def _worker_process_file(path_str: str) -> Dict[str, object]:
    if _WORKER_OPTIONS is None or _WORKER_PID_STRATEGY is None:
        raise RuntimeError("Worker not initialized")
    path = Path(path_str)
    return _process_single_file(path, _WORKER_OPTIONS, _WORKER_PID_STRATEGY, _WORKER_FIRST_DATES)


def _accumulate_worker_output(
    result: Dict[str, object],
    *,
    aggregator: "StudyAuditAggregator",
    id_manifest: Dict[str, str],
    errors: List[str],
) -> Tuple[int, int]:
    aggregator.add_events(result["audit_events"])
    id_manifest.update(result["id_mapping"])
    error_message = result["error"]
    if error_message:
        errors.append(error_message)
        return 0, 1
    return (1 if result.get("wrote_output") else 0), 0


def _should_cache_files(config: AnonymizeConfig) -> bool:
    return config.study_dates.enabled or (
        config.patient_id.enabled
        and config.patient_id.strategy
        in {
            PatientIdStrategyType.SEQUENTIAL,
            PatientIdStrategyType.FOLDER,
        }
    )


def _process_files_streaming(
    file_iterator: Iterable[Path],
    options: _Options,
    pid_strategy: "IDStrategy",
    first_dates: Dict[str, datetime],
    progress: Optional[ProgressCallback],
    aggregator: "StudyAuditAggregator",
    id_manifest: Dict[str, str],
    errors: List[str],
    concurrent_processes: int,
) -> Tuple[int, int, int, int, Optional[_ResumeMetrics], int, Dict[Path, _LeafState]]:
    total_files = 0
    updated_files = 0
    skipped_files = 0

    seen_subjects: set[str] = set()
    subjects_processed = 0

    def _update_subjects(path: Path) -> None:
        nonlocal subjects_processed
        name = _top_level_name(path, options.source_root)
        if name and name not in seen_subjects:
            seen_subjects.add(name)
            subjects_processed += 1

    leaf_states: Dict[Path, _LeafState] = {} if options.audit_resume_per_leaf else {}
    resume_metrics: Optional[_ResumeMetrics] = _ResumeMetrics() if options.audit_resume_per_leaf else None
    study_audit_cache: Dict[str, bool] = {}

    def _handle_result(
        result: Dict[str, object],
        mode: _LeafProcessMode,
        state: Optional[_LeafState],
    ) -> None:
        nonlocal updated_files, skipped_files
        error_message = result.get("error")
        wrote_output = bool(result.get("wrote_output"))
        reused_output = bool(result.get("output_preexisting"))
        if state:
            state.processed_for_audit = True
            events = result.get("audit_events", []) or []
            if state.anchor_rel_path is None and result.get("rel_path"):
                state.anchor_rel_path = result.get("rel_path")
            for event in events:
                _merge_leaf_audit_event(state, event)
            if error_message:
                state.had_error = True
                state.errors.append(error_message)
            else:
                if mode != _LeafProcessMode.OUTPUT_ONLY and not state.audit_anchor_done:
                    state.audit_anchor_done = True
            if wrote_output:
                state.files_written += 1
            elif reused_output:
                state.files_existing += 1
            if not state.patient_id_original and result.get("patient_id_original"):
                state.patient_id_original = result.get("patient_id_original")
            if not state.patient_id_updated and result.get("patient_id_updated"):
                state.patient_id_updated = result.get("patient_id_updated")

        if mode == _LeafProcessMode.OUTPUT_ONLY:
            if state:
                state.audit_done = True
            id_manifest.update(result.get("id_mapping", {}))
            if error_message:
                errors.append(error_message)
                skipped_files += 1
                if state:
                    state.had_error = True
            else:
                updated_files += 1
                if resume_metrics:
                    resume_metrics.files_output_only += 1
        else:
            delta_updated, delta_skipped = _accumulate_worker_output(
                result,
                aggregator=aggregator,
                id_manifest=id_manifest,
                errors=errors,
            )
            updated_files += delta_updated
            skipped_files += delta_skipped

        # Persist audit immediately after successfully processing first file in leaf
        if state and mode == _LeafProcessMode.FULL and state.files_seen == 1 and not state.had_error:
            success = _persist_leaf_audit_now(state, options, resume_metrics, study_audit_cache)
            if success:
                logger.debug(f"Persisted audit for leaf: {state.leaf_rel_path} (study: {state.study_uid})")

    def _should_process(mode: _LeafProcessMode, state: Optional[_LeafState], outputs_exist: bool) -> bool:
        if mode == _LeafProcessMode.OUTPUT_ONLY:
            return not outputs_exist
        if mode == _LeafProcessMode.FULL:
            if state is None:
                return not outputs_exist
            if not state.audit_anchor_done:
                return True
            return not outputs_exist
        return False

    def _prepare_file(path: Path) -> Optional[Tuple[_LeafProcessMode, Optional[_LeafState]]]:
        nonlocal total_files, skipped_files
        total_files += 1
        _update_subjects(path)
        mode, state = _determine_leaf_process_mode(
            path,
            options,
            pid_strategy,
            leaf_states,
            study_audit_cache,
            resume_metrics,
        )
        if state:
            state.files_seen += 1

        if mode == _LeafProcessMode.SKIP:
            skipped_files += 1
            if resume_metrics:
                resume_metrics.files_reused += 1
            if state:
                state.files_existing += 1
            if progress:
                progress(updated_files + skipped_files, total_files, subjects_processed)
            return None

        outputs_exist = _outputs_exist_for_leaf_file(path, options, pid_strategy, state)
        if not _should_process(mode, state, outputs_exist):
            skipped_files += 1
            if resume_metrics:
                resume_metrics.files_reused += 1
            if state and outputs_exist:
                state.files_existing += 1
            if progress:
                progress(updated_files + skipped_files, total_files, subjects_processed)
            return None

        return mode, state

    if concurrent_processes > 1:
        max_pending = concurrent_processes * 2  # 2x buffer for better feedback loop
        pending: Deque[Tuple[Future, _LeafProcessMode, Optional[_LeafState]]] = deque()
        with ProcessPoolExecutor(
            max_workers=concurrent_processes,
            initializer=_worker_init,
            initargs=(options, pid_strategy, first_dates),
        ) as pool:
            for path in file_iterator:
                prepared = _prepare_file(path)
                if not prepared:
                    continue
                mode, state = prepared
                future = pool.submit(_worker_process_file, str(path))
                pending.append((future, mode, state))
                if len(pending) >= max_pending:
                    future, future_mode, future_state = pending.popleft()
                    result = future.result()
                    _handle_result(result, future_mode, future_state)
                    if progress:
                        progress(updated_files + skipped_files, total_files, subjects_processed)

            while pending:
                future, future_mode, future_state = pending.popleft()
                result = future.result()
                _handle_result(result, future_mode, future_state)
                if progress:
                    progress(updated_files + skipped_files, total_files, subjects_processed)
    else:
        for path in file_iterator:
            prepared = _prepare_file(path)
            if not prepared:
                continue
            mode, state = prepared
            result = _process_single_file(path, options, pid_strategy, first_dates)
            _handle_result(result, mode, state)
            if progress:
                progress(updated_files + skipped_files, total_files, subjects_processed)
    completion_count = _finalize_leaf_audit_states(leaf_states, options, resume_metrics)
    return total_files, updated_files, skipped_files, subjects_processed, resume_metrics, completion_count, leaf_states

def _determine_leaf_process_mode(
    path: Path,
    options: _Options,
    pid_strategy: "IDStrategy",
    leaf_states: Dict[Path, _LeafState],
    study_audit_cache: Dict[str, bool],
    resume_metrics: Optional[_ResumeMetrics],
) -> Tuple[_LeafProcessMode, Optional[_LeafState]]:
    if not options.audit_resume_per_leaf:
        return _LeafProcessMode.FULL, None

    leaf_dir = path.parent
    state = leaf_states.get(leaf_dir)
    if state is None:
        state = _LeafState(leaf_rel_path=_leaf_relative_path(leaf_dir, options.source_root))
        leaf_states[leaf_dir] = state

    uid, pid = _light_read_uid_and_pid(path)
    if state.study_uid is None and uid:
        state.study_uid = uid
    if state.pid_hint is None and pid:
        state.pid_hint = pid
    if options.rename_patient_folders and pid and state.mapped_patient_id is None:
        mapped_pid = pid_strategy.map(pid, path)
        if mapped_pid:
            state.mapped_patient_id = mapped_pid

    if state.study_uid and state.audit_done is None:
        cached = study_audit_cache.get(state.study_uid)
        if cached is None:
            cached = study_audit_exists(state.study_uid)
            study_audit_cache[state.study_uid] = cached
        state.audit_done = cached

    # If audit is already complete (from DB or this run), skip the entire leaf
    if state.audit_done:
        if resume_metrics and not state.leaf_skipped_recorded:
            resume_metrics.leaves_skipped += 1
            state.leaf_skipped_recorded = True
        outputs_exist = _outputs_exist_for_path(path, options, pid_strategy, pid)
        if outputs_exist:
            return _LeafProcessMode.SKIP, state
        return _LeafProcessMode.OUTPUT_ONLY, state

    # First file in leaf: process it fully to capture audit
    if state.files_seen == 0:
        state.processed_for_audit = True
        return _LeafProcessMode.FULL, state

    # Subsequent files in leaf after audit was persisted: skip them
    if state.audit_persisted_this_run:
        outputs_exist = _outputs_exist_for_path(path, options, pid_strategy, pid)
        if outputs_exist:
            return _LeafProcessMode.SKIP, state
        return _LeafProcessMode.OUTPUT_ONLY, state

    # Fallback: process for output only
    return _LeafProcessMode.OUTPUT_ONLY, state


def _persist_leaf_audit_now(
    state: _LeafState,
    options: _Options,
    resume_metrics: Optional[_ResumeMetrics],
    study_audit_cache: Dict[str, bool],
) -> bool:
    """Persist leaf audit immediately after processing first file. Returns True on success."""
    if not state.study_uid:
        return False
    if not state.processed_for_audit:
        return False
    if state.had_error:
        return False
    if state.audit_persisted_this_run:
        return False

    try:
        record_leaf_audit_summary(
            state.study_uid,
            cohort_name=options.cohort_name,
            leaf_rel_path=state.leaf_rel_path,
            files_total=state.files_seen,
            files_written=state.files_written,
            files_reused=state.files_existing,
            files_with_errors=len(state.errors),
            patient_id_original=state.patient_id_original,
            patient_id_updated=state.patient_id_updated,
            errors=state.errors,
            audit_payload=_build_leaf_audit_payload(state),
        )

        if not state.audit_done:
            mark_study_audit_complete(
                state.study_uid,
                leaf_rel_path=state.leaf_rel_path,
                cohort_name=options.cohort_name,
            )
            if resume_metrics:
                resume_metrics.leaves_completed += 1

        state.audit_done = True
        state.audit_persisted_this_run = True
        study_audit_cache[state.study_uid] = True
        return True
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to persist leaf audit for %s: %s", state.leaf_rel_path, exc)
        return False


def _finalize_leaf_audit_states(
    leaf_states: Dict[Path, _LeafState],
    options: _Options,
    resume_metrics: Optional[_ResumeMetrics],
) -> int:
    completed = 0
    for state in leaf_states.values():
        # Skip states already persisted immediately
        if state.audit_persisted_this_run:
            continue
        
        if not state.study_uid:
            continue
        if not state.processed_for_audit:
            continue
        if state.had_error:
            continue

        record_leaf_audit_summary(
            state.study_uid,
            cohort_name=options.cohort_name,
            leaf_rel_path=state.leaf_rel_path,
            files_total=state.files_seen,
            files_written=state.files_written,
            files_reused=state.files_existing,
            files_with_errors=len(state.errors),
            patient_id_original=state.patient_id_original,
            patient_id_updated=state.patient_id_updated,
            errors=state.errors,
            audit_payload=_build_leaf_audit_payload(state),
        )

        if not state.audit_done:
            mark_study_audit_complete(
                state.study_uid,
                leaf_rel_path=state.leaf_rel_path,
                cohort_name=options.cohort_name,
            )
            state.audit_done = True
            if resume_metrics:
                resume_metrics.leaves_completed += 1
        completed += 1

    return completed


def _target_path(input_path: Path, opts: _Options) -> Path:
    relative = input_path.resolve().relative_to(opts.source_root.resolve())
    return opts.output_root / relative


def _outputs_exist_for_path(
    input_path: Path,
    opts: _Options,
    pid_strategy: "IDStrategy",
    pid_hint: Optional[str] = None,
) -> bool:
    target = _target_path(input_path, opts)
    if target.exists():
        return True
    if not opts.rename_patient_folders:
        return False
    if pid_hint is None:
        pid_hint = _light_read_pid(input_path)
    if not pid_hint:
        return False
    mapped = pid_strategy.map(pid_hint, input_path)
    if not mapped:
        return False
    rel_parts = _relative_parts(input_path, opts.source_root)
    if not rel_parts:
        return False
    if rel_parts[0] == mapped:
        return False
    mapped_parts = list(rel_parts)
    mapped_parts[0] = mapped
    mapped_target = opts.output_root.joinpath(*mapped_parts)
    return mapped_target.exists()


def _outputs_exist_for_leaf_file(
    input_path: Path,
    opts: _Options,
    pid_strategy: "IDStrategy",
    state: Optional[_LeafState],
) -> bool:
    target = _target_path(input_path, opts)
    if target.exists():
        return True
    if not opts.rename_patient_folders:
        return False

    rel_parts = _relative_parts(input_path, opts.source_root)
    if not rel_parts:
        return False

    mapped_top: Optional[str] = None
    if state and state.mapped_patient_id:
        mapped_top = state.mapped_patient_id
    elif state and state.pid_hint:
        mapped_top = pid_strategy.map(state.pid_hint, input_path)
        if mapped_top:
            state.mapped_patient_id = mapped_top
    else:
        pid_hint = _light_read_pid(input_path)
        if pid_hint:
            mapped_top = pid_strategy.map(pid_hint, input_path)

    if not mapped_top or rel_parts[0] == mapped_top:
        return False

    mapped_parts = list(rel_parts)
    mapped_parts[0] = mapped_top
    mapped_target = opts.output_root.joinpath(*mapped_parts)
    if mapped_target.exists():
        if state and not state.mapped_patient_id:
            state.mapped_patient_id = mapped_top
        return True
    return False


def _save_dataset(
    ds: pydicom.Dataset,
    input_path: Path,
    opts: _Options,
    mapped_pid: Optional[str] = None,
) -> Path:
    """
    Save dataset to output, using the active directory (renamed if it exists, original otherwise).
    
    This handles resume scenarios where folders may have been renamed in a previous run.
    """
    target = _target_path(input_path, opts)
    
    # Check if we should write to a renamed directory instead
    if opts.rename_patient_folders and mapped_pid:
        rel_parts = _relative_parts(input_path, opts.source_root)
        if rel_parts and rel_parts[0] != mapped_pid:
            # Check if renamed directory already exists from a previous run
            renamed_top_dir = opts.output_root / mapped_pid
            if renamed_top_dir.exists():
                # Use the renamed path since the directory was already renamed
                mapped_parts = list(rel_parts)
                mapped_parts[0] = mapped_pid
                target = opts.output_root.joinpath(*mapped_parts)
    
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(target.suffix + ".tmp")
    ds.save_as(str(temp), enforce_file_format=not opts.preserve_uids)
    os.replace(str(temp), str(target))
    return target


def _scrub_dataset(
    ds: pydicom.Dataset,
    options: _Options,
    *,
    rel_path: str,
    study_uid: str,
    audit_events: List[Dict[str, object]],
) -> int:
    pid_tag = Tag(PATIENT_ID_TAG)
    date_tag = Tag(STUDY_DATE_TAG)
    uid_tag = Tag(STUDY_INSTANCE_UID_TAG)
    removed = 0
    for tag in options.scrub_tags:
        if tag in options.exclude_tags:
            continue
        if tag in (pid_tag, date_tag, uid_tag):
            continue
        if tag not in ds:
            continue
        if _element_is_uid(ds, tag) or _name_contains_uid_or_reference(tag):
            continue
        value = ds[tag].value
        audit_events.append(
            {
                "rel_path": rel_path,
                "study_uid": study_uid,
                "tag": _format_tag(tag),
                "tag_name": _tag_name(tag),
                "action": "removed",
                "old_value": _safe_value_preview(value),
                "new_value": "",
            }
        )
        try:
            del ds[tag]
        except Exception:
            continue
        removed += 1
    return removed


def _process_partition_worker(patient_folders: List[Path]) -> PatientProgressResult:
    """
    Worker function to process a partition of patient folders.
    
    Must be module-level (not nested) for pickle serialization in multiprocessing.
    Uses worker-initialized globals (_WORKER_OPTIONS, etc.) set by _worker_init.
    
    Returns PatientProgressResult for smooth progress tracking.
    """
    total = 0
    updated = 0
    skipped = 0
    all_errors = []
    completed_patient_names = []
    
    # Use worker-initialized globals (set by ProcessPoolExecutor initializer)
    if _WORKER_OPTIONS is None or _WORKER_PID_STRATEGY is None:
        raise RuntimeError("Worker not initialized - globals not set")
    
    for folder in patient_folders:
        t, u, s, errs = _process_patient_partition(
            folder,
            _WORKER_OPTIONS,
            _WORKER_PID_STRATEGY,
            _WORKER_FIRST_DATES,
        )
        total += t
        updated += u
        skipped += s
        all_errors.extend(errs)
        
        # Track patient completion for progress reporting
        completed_patient_names.append(folder.name)
        logger.info(f"Completed patient {folder.name}: {u} leaves processed, {s} skipped, {t} files total")
    
    return PatientProgressResult(
        total_files=total,
        updated_files=updated,
        skipped_files=skipped,
        errors=all_errors,
        completed_patients=completed_patient_names,
        patient_count=len(patient_folders),
    )


def _run_partitioned_processing(
    config: AnonymizeConfig,
    options: _Options,
    pid_strategy: "IDStrategy",
    first_dates: Dict[str, datetime],
    progress: Optional[ProgressCallback],
) -> Tuple[int, int, int, List[str]]:
    """
    Partition patients and process in parallel.
    
    Each worker gets exclusive patient folders - no race conditions.
    DB is single source of truth.
    """
    
    # Count patients upfront for smooth progress reporting
    total_patients = 0
    try:
        patient_folders = [p for p in config.source_root.iterdir() if p.is_dir()]
        total_patients = len(patient_folders)
        logger.info(f"Found {total_patients} patient folders to process")
    except Exception as exc:
        logger.warning(f"Could not count patient folders: {exc}")
        total_patients = 0
    
    # Partition work across workers
    partitions = _partition_patients(
        config.source_root,
        config.concurrent_processes,
    )
    
    # Execute in parallel with patient-level progress tracking
    total_files = 0
    updated_files = 0
    skipped_files = 0
    all_errors: List[str] = []
    completed_patients = 0
    
    if config.concurrent_processes > 1:
        logger.info(f"Starting parallel processing with {config.concurrent_processes} workers")
        with ProcessPoolExecutor(
            max_workers=config.concurrent_processes,
            initializer=_worker_init,
            initargs=(options, pid_strategy, first_dates),
        ) as pool:
            # Submit all partitions for processing
            futures = [pool.submit(_process_partition_worker, partition) for partition in partitions]
            
            # Process results as they complete (enables real-time patient-level progress updates)
            for future in as_completed(futures):
                result = future.result()  # PatientProgressResult
                total_files += result.total_files
                updated_files += result.updated_files
                skipped_files += result.skipped_files
                all_errors.extend(result.errors)
                
                # Update patient progress
                completed_patients += result.patient_count
                
                if progress and total_patients > 0:
                    # Smooth per-patient progress!
                    progress(completed_patients, total_patients, completed_patients)
                    logger.debug(f"Progress: {completed_patients}/{total_patients} patients ({100*completed_patients/total_patients:.1f}%)")
                elif progress:
                    # Fallback to file-based progress if patient count unavailable
                    progress(updated_files + skipped_files, total_files, updated_files)
    else:
        # Single-threaded mode (no worker init needed)
        logger.info("Starting single-threaded processing")
        # Set globals for single-threaded mode
        global _WORKER_OPTIONS, _WORKER_PID_STRATEGY, _WORKER_FIRST_DATES
        _WORKER_OPTIONS = options
        _WORKER_PID_STRATEGY = pid_strategy
        _WORKER_FIRST_DATES = first_dates
        
        for partition in partitions:
            result = _process_partition_worker(partition)  # PatientProgressResult
            total_files += result.total_files
            updated_files += result.updated_files
            skipped_files += result.skipped_files
            all_errors.extend(result.errors)
            
            # Update patient progress
            completed_patients += result.patient_count
            
            if progress and total_patients > 0:
                # Smooth per-patient progress!
                progress(completed_patients, total_patients, completed_patients)
                logger.debug(f"Progress: {completed_patients}/{total_patients} patients ({100*completed_patients/total_patients:.1f}%)")
            elif progress:
                # Fallback to file-based progress if patient count unavailable
                progress(updated_files + skipped_files, total_files, updated_files)
    
    logger.info(f"Processing complete: {updated_files} leaves processed, {skipped_files} skipped, {total_files} files total")
    
    return total_files, updated_files, skipped_files, all_errors


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_anonymization(
    config: AnonymizeConfig,
    *,
    progress: Optional[ProgressCallback] = None,
    job_id: Optional[int] = None,
) -> AnonymizeResult:
    start_time = time.monotonic()

    scrub_tags: List[Tag]
    if config.scrub_tags:
        scrub_tags = [Tag(tag) for tag in config.scrub_tags]
    else:
        scrub_tags = _anonymization_tag_set(config.anonymize_categories)

    exclude_tags = {tag for tag_str in config.scrub_exclude_tags if (tag := _parse_tag(tag_str))}

    # For strategies that need full file discovery upfront (sequential, folder-based)
    need_cached_files = _should_cache_files(config)
    if need_cached_files:
        # Collect files for ID strategy discovery only
        files = list(iter_dicom_files(config.source_root, max_workers=config.worker_threads))
        first_dates = _collect_first_dates(config, files) if config.study_dates.enabled else {}
        pid_strategy = _build_id_strategy(config, files, max_workers=config.worker_threads)
    else:
        first_dates = {}
        pid_strategy = _build_id_strategy(config, [], max_workers=config.worker_threads)

    options = _Options(
        source_root=config.source_root,
        output_root=config.output_root,
        scrub_tags=scrub_tags,
        exclude_tags=exclude_tags,
        anonymize_patient_id=config.patient_id.enabled and bool(config.patient_id.strategy),
        map_timepoints=config.study_dates.enabled,
        preserve_uids=config.preserve_uids,
        rename_patient_folders=config.rename_patient_folders,
        resume=config.resume,
        audit_resume_per_leaf=config.audit_resume_per_leaf,
        cohort_name=config.cohort_name,
    )

    # NEW: Simple partitioned processing - DB-driven, no complex modes
    total_files, updated_files, skipped_files, errors = _run_partitioned_processing(
        config,
        options,
        pid_strategy,
        first_dates,
        progress,
    )

    aggregator_db = StudyAuditAggregator(config.source_root, config.cohort_name or "cohort")
    persisted_summaries = load_leaf_summaries_for_cohort(config.cohort_name)
    aggregator_db.add_events(_events_from_summaries(persisted_summaries))
    aggregated_df = aggregator_db.build_dataframe()

    export_path: Optional[Path] = None
    if config.audit_export.enabled and not aggregated_df.is_empty():
        filename = config.audit_export.filename or (
            "anonymize_audit.xlsx"
            if config.audit_export.format == AuditExportFormat.ENCRYPTED_EXCEL
            else "anonymize_audit.csv"
        )
        destination = (config.source_root / filename).resolve()
        if config.audit_export.format == AuditExportFormat.ENCRYPTED_EXCEL:
            export_path = export_encrypted_excel(
                aggregated_df,
                destination,
                config.audit_export.excel_password or "password",
            )
        else:
            export_path = export_csv(aggregated_df, destination)

    # Rename patient folders if configured
    if config.rename_patient_folders:
        try:
            # Build ID mapping from DB summaries
            id_manifest: Dict[str, str] = {}
            summaries = load_leaf_summaries_for_cohort(config.cohort_name)
            for summary in summaries:
                summary_data = summary.get("summary") or {}
                orig = summary_data.get("patient_id_original")
                updated = summary_data.get("patient_id_updated")
                if orig and updated and orig != updated:
                    id_manifest[orig] = updated
            
            rename_errors = _rename_patient_folders(config.output_root, pid_strategy, id_manifest, max_workers=config.worker_threads)
            errors.extend(rename_errors)
        except Exception as exc:  # pragma: no cover - defensive logging
            errors.append(f"Failed to rename folders: {exc}")

    duration = time.monotonic() - start_time
    
    return AnonymizeResult(
        total_files=total_files,
        updated_files=updated_files,
        skipped_files=skipped_files,
        duration_seconds=duration,
        audit_rows_written=0,  # No longer tracked separately, DB is authority
        export_path=export_path,
        job_id=job_id,
        errors=errors,
        leaves_skipped=0,  # Calculate from DB if needed
        leaves_completed=updated_files,  # One leaf per updated file in new architecture
        files_reused=0,
        files_output_only=0,
    )


__all__ = ["run_anonymization", "iter_dicom_files"]
