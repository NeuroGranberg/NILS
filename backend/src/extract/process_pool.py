"""Process pool implementation for parallel DICOM extraction.

This module provides true CPU parallelism for DICOM parsing by using
ProcessPoolExecutor, bypassing Python's GIL limitation.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ProcessPoolExecutor, Future, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import pydicom

from .batching import BatchSizeController
from .config import ExtractionConfig
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
from .scanner import SubjectFolder
from .resume_index import SubjectPathEntry
from .subject_mapping import SubjectResolver
from .worker import (
    ALLOWED_MODALITIES,
    ALLOWED_SOP_CLASS_UIDS,
    InstancePayload,
    _iter_dicom_files,
    _matches_extension,
    _relative_within_subject,
    normalize_modality,
)


logger = logging.getLogger(__name__)


# Global worker state (initialized per process)
_WORKER_CONFIG: Optional[ExtractionConfig] = None
_WORKER_RESOLVER: Optional[SubjectResolver] = None
_WORKER_BATCH_SIZE: int = 100


def _worker_init(config: ExtractionConfig, resolver: Optional[SubjectResolver], batch_size: int) -> None:
    """Initialize worker process with shared configuration.
    
    This function is called once per worker process at startup.
    It sets up global state to avoid re-pickling large objects.
    """
    global _WORKER_CONFIG, _WORKER_RESOLVER, _WORKER_BATCH_SIZE
    _WORKER_CONFIG = config
    _WORKER_RESOLVER = resolver
    _WORKER_BATCH_SIZE = batch_size


@dataclass
class SubjectBatchResult:
    """Result from processing a single subject."""
    subject_key: str
    batches: List[Tuple[List[InstancePayload], str]]  # (batch, last_uid)
    files_processed: int
    parse_time: float
    errors: List[str]


def _process_subject_worker(
    subject: SubjectFolder,
    resume_instance: Optional[str],
    path_filter: SubjectPathEntry | None,
) -> SubjectBatchResult:
    """Worker function to process a single subject's DICOM files.
    
    This function runs in a separate process and must be picklable.
    It uses global worker state initialized by _worker_init.
    
    Args:
        subject: Subject folder to process
        resume_instance: Last processed SOP Instance UID (for resume)
    
    Returns:
        SubjectBatchResult with all batches and metadata
    """
    if _WORKER_CONFIG is None:
        raise RuntimeError("Worker not initialized - config not set")
    
    config = _WORKER_CONFIG
    resolver = _WORKER_RESOLVER
    batch_size = _WORKER_BATCH_SIZE
    
    batches: List[Tuple[List[InstancePayload], str]] = []
    batch: List[InstancePayload] = []
    last_uid: str = ""
    files_processed = 0
    errors: List[str] = []
    
    start_time = time.perf_counter()
    
    try:
        file_iter = _iter_dicom_files(subject.path, config.extension_mode.value)
        
        for path in file_iter:
            relative_path = _relative_within_subject(path, subject)
            if path_filter and path_filter.contains(relative_path):
                logger.debug("[worker] Resume-by-path skipping %s/%s", subject.subject_key, relative_path)
                continue
            try:
                # Parse DICOM file
                if config.use_specific_tags:
                    dataset = pydicom.dcmread(
                        path,
                        force=True,
                        stop_before_pixels=True,
                        specific_tags=EXTRACT_SPECIFIC_TAGS,
                    )
                else:
                    dataset = pydicom.dcmread(path, force=True, stop_before_pixels=True)
                
                # Extract required UIDs
                study_uid = getattr(dataset, "StudyInstanceUID", None)
                series_uid = getattr(dataset, "SeriesInstanceUID", None)
                sop_uid = getattr(dataset, "SOPInstanceUID", None)
                sop_class_uid = getattr(dataset, "SOPClassUID", None)
                
                if sop_class_uid is None and getattr(dataset, "file_meta", None) is not None:
                    sop_class_uid = getattr(dataset.file_meta, "MediaStorageSOPClassUID", None)
                
                sop_class_uid_str = str(sop_class_uid) if sop_class_uid is not None else None
                
                # Validate required fields
                if not (study_uid and series_uid and sop_uid and sop_class_uid_str):
                    continue
                
                if sop_class_uid_str not in ALLOWED_SOP_CLASS_UIDS:
                    continue
                
                # Skip if resuming and already processed
                if resume_instance and sop_uid <= resume_instance:
                    continue
                
                # Extract metadata
                modality_raw = getattr(dataset, "Modality", None)
                relative_path = str(path.relative_to(subject.path.parent))
                patient_id_raw = getattr(dataset, "PatientID", None)
                patient_name_raw = getattr(dataset, "PatientName", None)
                patient_id = str(patient_id_raw) if patient_id_raw is not None else None
                patient_name = str(patient_name_raw) if patient_name_raw is not None else None
                
                # Resolve subject code
                if resolver:
                    resolution = resolver.resolve(
                        patient_id=patient_id,
                        patient_name=patient_name,
                        study_uid=study_uid,
                    )
                    subject_code = resolution.subject_code
                    patient_id = resolution.patient_id
                    patient_name = resolution.patient_name
                    subject_source = resolution.source
                else:
                    subject_code = subject.subject_key
                    subject_source = "folder"
                
                # Extract fields
                study_fields = extract_fields(dataset, STUDY_FIELD_MAP)
                series_fields = extract_fields(dataset, SERIES_FIELD_MAP)
                modality = normalize_modality(modality_raw) or normalize_modality(series_fields.get("modality"))
                if not modality:
                    logger.debug("[worker] Skipping %s due to missing modality", relative_path)
                    continue
                if modality not in ALLOWED_MODALITIES:
                    logger.debug("[worker] Skipping %s because modality %s is not allowed", relative_path, modality)
                    continue
                series_fields["modality"] = modality
                instance_fields = extract_fields(dataset, INSTANCE_FIELD_MAP)
                
                mri_fields = extract_fields(dataset, MRI_SERIES_FIELD_MAP) if modality == "MR" else {}
                ct_fields = extract_fields(dataset, CT_SERIES_FIELD_MAP) if modality == "CT" else {}
                pet_fields = extract_fields(dataset, PET_SERIES_FIELD_MAP) if modality in {"PT", "PET"} else {}
                
                # Create payload
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
                files_processed += 1
                
                # Yield batch when size reached
                if len(batch) >= batch_size:
                    batches.append((batch, last_uid))
                    batch = []
            
            except Exception as e:
                errors.append(f"Error processing {path}: {e}")
                continue
        
        # Add final batch if any
        if batch:
            batches.append((batch, last_uid or ""))
    
    except Exception as e:
        errors.append(f"Error processing subject {subject.subject_key}: {e}")
    
    parse_time = time.perf_counter() - start_time
    
    return SubjectBatchResult(
        subject_key=subject.subject_key,
        batches=batches,
        files_processed=files_processed,
        parse_time=parse_time,
        errors=errors,
    )


def extract_subjects_parallel(
    subjects: List[SubjectFolder],
    config: ExtractionConfig,
    resolver: Optional[SubjectResolver],
    max_workers: int = 4,
    resume_tokens: Optional[Dict[str, str]] = None,
    resume_paths: Optional[Dict[str, SubjectPathEntry]] = None,
) -> Iterator[SubjectBatchResult]:
    """Extract DICOM metadata from subjects using parallel processes.
    
    Args:
        subjects: List of subject folders to process
        config: Extraction configuration
        resolver: Subject code resolver (optional)
        max_workers: Number of worker processes
        resume_tokens: Dict of subject_key -> last_processed_sop_uid
    
    Yields:
        SubjectBatchResult for each processed subject
    """
    resume_tokens = resume_tokens or {}
    
    logger.info(f"Starting parallel extraction with {max_workers} worker processes")
    
    with ProcessPoolExecutor(
        max_workers=max_workers,
        initializer=_worker_init,
        initargs=(config, resolver, config.batch_size),
    ) as executor:
        # Submit all subjects for processing
        futures: Dict[Future, SubjectFolder] = {}
        for subject in subjects:
            resume_instance = resume_tokens.get(subject.subject_key)
            subject_filter = resume_paths.get(subject.subject_key) if resume_paths else None
            future = executor.submit(_process_subject_worker, subject, resume_instance, subject_filter)
            futures[future] = subject
        
        # Yield results as they complete
        for future in as_completed(futures):
            subject = futures[future]
            try:
                result = future.result()
                if result.errors:
                    for error in result.errors:
                        logger.warning(error)
                yield result
            except Exception as e:
                logger.error(f"Failed to process subject {subject.subject_key}: {e}")
                # Yield empty result so progress tracking works
                yield SubjectBatchResult(
                    subject_key=subject.subject_key,
                    batches=[],
                    files_processed=0,
                    parse_time=0.0,
                    errors=[str(e)],
                )
