"""Worker logic for parsing DICOM files."""

from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import pydicom

from .batching import BatchSizeController
from .dicom_mappings import (
    CT_SERIES_FIELD_MAP,
    EXTRACT_SPECIFIC_TAGS,
    INSTANCE_FIELD_MAP,
    MRI_SERIES_FIELD_MAP,
    PET_SERIES_FIELD_MAP,
    SERIES_FIELD_MAP,
    STUDY_FIELD_MAP,
    extract_fields,
)
from .profiler import get_global_profiler
from .resume_index import SubjectPathEntry
from .subject_mapping import SubjectResolver


logger = logging.getLogger(__name__)



ALLOWED_SOP_CLASS_UIDS = {
    "1.2.840.10008.5.1.4.1.1.2",      # CT Image Storage
    "1.2.840.10008.5.1.4.1.1.2.1",    # Enhanced CT Image Storage
    "1.2.840.10008.5.1.4.1.1.2.2",    # Legacy Converted Enhanced CT Image Storage
    "1.2.840.10008.5.1.4.1.1.4",      # MR Image Storage
    "1.2.840.10008.5.1.4.1.1.4.1",    # Enhanced MR Image Storage
    "1.2.840.10008.5.1.4.1.1.4.2",    # MR Spectroscopy Storage
    "1.2.840.10008.5.1.4.1.1.4.4",    # Legacy Converted Enhanced MR Image Storage
    "1.2.840.10008.5.1.4.1.1.128",    # PET Image Storage
    "1.2.840.10008.5.1.4.1.1.128.1",  # Legacy Converted Enhanced PET Image Storage
}

ALLOWED_MODALITIES = {"MR", "CT", "PT", "PET"}


def normalize_modality(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().upper()
    return normalized or None


@dataclass
class SeriesPlan:
    series_uid: str
    study_uid: str
    paths: List[Path]


@dataclass
class _SeriesPlanBuilder:
    study_uid: str
    files: List[Tuple[Path, str]]
@dataclass
class InstancePayload:
    subject_key: str
    subject_code: str
    study_uid: str
    series_uid: str
    sop_uid: str
    modality: str
    file_path: str
    study_fields: dict
    series_fields: dict
    instance_fields: dict
    mri_fields: dict
    ct_fields: dict
    pet_fields: dict
    patient_id: str | None
    patient_name: str | None
    subject_resolution_source: str



# Reasonable default for directory scanning parallelism
_SCAN_MAX_WORKERS = 8


def _iter_dicom_files(root: Path, extension_mode: str) -> Iterator[Path]:
    """Yield DICOM-ish files under *root* using a threaded scandir walker."""

    def should_include(entry: os.DirEntry[str]) -> bool:
        if not entry.is_file():
            return False
        return _matches_extension(entry.name, extension_mode)

    def walk(directory: Path) -> tuple[list[Path], list[Path]]:
        files: list[Path] = []
        dirs: list[Path] = []
        with os.scandir(directory) as it:
            for entry in it:
                try:
                    if should_include(entry):
                        files.append(Path(entry.path))
                    elif entry.is_dir():
                        dirs.append(Path(entry.path))
                except FileNotFoundError:
                    # Race: file/dir vanished after scandir listed it.
                    continue
        return files, dirs

    with ThreadPoolExecutor(max_workers=_SCAN_MAX_WORKERS) as executor:
        futures = [executor.submit(walk, root)]
        while futures:
            future = futures.pop()
            files, dirs = future.result()
            for file_path in files:
                yield file_path
            for dir_path in dirs:
                futures.append(executor.submit(walk, dir_path))


def _matches_extension(name: str, mode: str) -> bool:
    if mode == "dcm":
        return name.endswith(".dcm")
    if mode == "DCM":
        return name.endswith(".DCM")
    if mode == "all_dcm":
        return name.lower().endswith(".dcm")
    if mode == "no_ext":
        return not Path(name).suffix
    return name.lower().endswith(".dcm") or not Path(name).suffix


def _relative_within_subject(path: Path, subject: SubjectFolder) -> str:
    try:
        rel = path.relative_to(subject.path)
    except ValueError:
        return path.name
    return rel.as_posix()


def plan_subject_series(
    *,
    subject,
    extension_mode: str,
    resume_tokens: Dict[str, str],
    use_specific_tags: bool = True,
    path_filter: SubjectPathEntry | None = None,
) -> List[SeriesPlan]:
    legacy_token = resume_tokens.get("__legacy__")
    plan: Dict[str, _SeriesPlanBuilder] = {}
    file_iter = _iter_dicom_files(subject.path, extension_mode)
    profiler = get_global_profiler()
    
    for path in file_iter:
        relative_path = _relative_within_subject(path, subject)
        if path_filter and path_filter.contains(relative_path):
            logger.debug("Resume-by-path skipping %s/%s", subject.subject_key, relative_path)
            continue
        start_parse = time.perf_counter()
        if use_specific_tags:
            dataset = pydicom.dcmread(
                path,
                force=True,
                stop_before_pixels=True,
                specific_tags=["StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID", "SOPClassUID"],
            )
        else:
            dataset = pydicom.dcmread(path, force=True, stop_before_pixels=True)
        
        if profiler:
            profiler.record("dicom_parsing_planning", time.perf_counter() - start_parse)
        study_uid = getattr(dataset, "StudyInstanceUID", None)
        series_uid = getattr(dataset, "SeriesInstanceUID", None)
        sop_uid = getattr(dataset, "SOPInstanceUID", None)
        sop_class_uid = getattr(dataset, "SOPClassUID", None) or getattr(
            getattr(dataset, "file_meta", None),
            "MediaStorageSOPClassUID",
            None,
        )
        if not (study_uid and series_uid and sop_uid and sop_class_uid):
            continue
        sop_class_uid_str = str(sop_class_uid)
        if sop_class_uid_str not in ALLOWED_SOP_CLASS_UIDS:
            continue
        token = resume_tokens.get(series_uid, legacy_token)
        if token and sop_uid <= token:
            continue
        entry = plan.get(series_uid)
        if entry is None:
            entry = _SeriesPlanBuilder(study_uid=study_uid, files=[])
            plan[series_uid] = entry
        entry.files.append((path, sop_uid))

    planned: List[SeriesPlan] = []
    for series_uid, builder in plan.items():
        builder.files.sort(key=lambda item: item[1])
        paths = [entry[0] for entry in builder.files]
        planned.append(SeriesPlan(series_uid=series_uid, study_uid=builder.study_uid, paths=paths))
    return planned


def extract_subject_batches(
    *,
    subject,
    extension_mode: str,
    resume_instance: str | None,
    batch_size: int,
    subject_resolver: SubjectResolver | None,
    use_specific_tags: bool = True,
    batch_controller: BatchSizeController | None = None,
    paths: Optional[Sequence[Path]] = None,
    path_filter: SubjectPathEntry | None = None,
) -> Iterable[Tuple[List[InstancePayload], str]]:
    batch: list[InstancePayload] = []
    last_uid: str | None = None
    file_iter: Sequence[Path] | Iterable[Path]
    profiler = get_global_profiler()
    
    if paths is not None:
        file_iter = paths
    else:
        file_iter = _iter_dicom_files(subject.path, extension_mode)
    
    for path in file_iter:
        relative_path = _relative_within_subject(path, subject)
        if path_filter and path_filter.contains(relative_path):
            logger.debug("Resume-by-path skipping %s/%s", subject.subject_key, relative_path)
            continue
        start_parse = time.perf_counter()
        if use_specific_tags:
            dataset = pydicom.dcmread(
                path,
                force=True,
                stop_before_pixels=True,
                specific_tags=EXTRACT_SPECIFIC_TAGS,
            )
        else:
            dataset = pydicom.dcmread(path, force=True, stop_before_pixels=True)
        parse_duration = time.perf_counter() - start_parse
        
        if profiler:
            profiler.record("dicom_parsing", parse_duration)
            profiler.increment_files()
        study_uid = getattr(dataset, "StudyInstanceUID", None)
        series_uid = getattr(dataset, "SeriesInstanceUID", None)
        sop_uid = getattr(dataset, "SOPInstanceUID", None)
        sop_class_uid = getattr(dataset, "SOPClassUID", None)
        if sop_class_uid is None and getattr(dataset, "file_meta", None) is not None:
            sop_class_uid = getattr(dataset.file_meta, "MediaStorageSOPClassUID", None)
        sop_class_uid_str = str(sop_class_uid) if sop_class_uid is not None else None

        if not (study_uid and series_uid and sop_uid and sop_class_uid_str):
            continue
        if sop_class_uid_str not in ALLOWED_SOP_CLASS_UIDS:
            logger.debug("Skipping SOPClassUID %s for file %s", sop_class_uid_str, path)
            continue
        if resume_instance and sop_uid <= resume_instance:
            continue

        modality_raw = getattr(dataset, "Modality", None)
        relative_path = str(path.relative_to(subject.path.parent))
        patient_id_raw = getattr(dataset, "PatientID", None)
        patient_name_raw = getattr(dataset, "PatientName", None)
        patient_id = str(patient_id_raw) if patient_id_raw is not None else None
        patient_name = str(patient_name_raw) if patient_name_raw is not None else None

        if subject_resolver:
            try:
                resolution = subject_resolver.resolve(
                    patient_id=patient_id,
                    patient_name=patient_name,
                    study_uid=study_uid,
                )
            except Exception as exc:
                logger.error("Failed to resolve subject for StudyInstanceUID %s: %s", study_uid, exc)
                raise
            subject_code = resolution.subject_code
            patient_id = resolution.patient_id
            patient_name = resolution.patient_name
            subject_source = resolution.source
        else:
            subject_code = subject.subject_key
            subject_source = "folder"
        study_fields = extract_fields(dataset, STUDY_FIELD_MAP)
        series_fields = extract_fields(dataset, SERIES_FIELD_MAP)
        modality = normalize_modality(modality_raw) or normalize_modality(series_fields.get("modality"))
        if not modality:
            logger.debug("Skipping %s/%s due to missing modality", subject.subject_key, relative_path)
            continue
        if modality not in ALLOWED_MODALITIES:
            logger.debug(
                "Skipping %s/%s because modality %s is not allowed",
                subject.subject_key,
                relative_path,
                modality,
            )
            continue
        series_fields["modality"] = modality
        instance_fields = extract_fields(dataset, INSTANCE_FIELD_MAP)
        mri_fields = extract_fields(dataset, MRI_SERIES_FIELD_MAP) if modality == "MR" else {}
        ct_fields = extract_fields(dataset, CT_SERIES_FIELD_MAP) if modality == "CT" else {}
        pet_fields = extract_fields(dataset, PET_SERIES_FIELD_MAP) if modality in {"PT", "PET"} else {}
        payload = InstancePayload(
            subject_key=subject.subject_key,
            subject_code=subject_code,
            study_uid=study_uid,
            series_uid=series_uid,
            sop_uid=sop_uid,
            modality=modality,
            file_path=relative_path,
            study_fields=study_fields,
            series_fields=series_fields,
            instance_fields=instance_fields,
            mri_fields=mri_fields,
            ct_fields=ct_fields,
            pet_fields=pet_fields,
            patient_id=patient_id,
            patient_name=patient_name,
            subject_resolution_source=subject_source,
        )
        batch.append(payload)
        last_uid = sop_uid
        batch_target = batch_controller.current_size() if batch_controller else batch_size
        if len(batch) >= batch_target:
            start_yield = time.perf_counter()
            yield batch, last_uid
            if profiler:
                profiler.record("batch_assembly", time.perf_counter() - start_yield)
            batch = []
    if batch:
        start_yield = time.perf_counter()
        yield batch, last_uid or ""
        if profiler:
            profiler.record("batch_assembly", time.perf_counter() - start_yield)


