"""Validation tests for Phase 3: concurrent subject processing."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from extract.config import ExtractionConfig
from extract.core import _run_async
from extract.scanner import discover_subjects
from extract.subject_mapping import SubjectResolver
from metadata_db.schema import Instance, Subject, SubjectCohort


def _setup_metadata_db(monkeypatch):
    """Set up in-memory SQLite database for testing."""
    # Patch bootstrap first, before any imports that might call it
    import metadata_db.lifecycle as lifecycle_module

    def mock_bootstrap(auto_restore=None):
        # Schema is already created, so bootstrap is a no-op
        pass

    monkeypatch.setattr(lifecycle_module, "bootstrap", mock_bootstrap, raising=False)
    import extract.writer as writer_module

    monkeypatch.setattr(writer_module, "bootstrap", mock_bootstrap, raising=False)

    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    from metadata_db import schema

    schema.Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)

    import metadata_db.session as session_module

    monkeypatch.setattr(session_module, "SessionLocal", Session, raising=False)
    monkeypatch.setattr(writer_module, "SessionLocal", Session, raising=False)

    return engine, Session


def _create_minimal_dicom(path: Path, uid_suffix: str, patient_id: str = "TEST001") -> None:
    """Create a minimal valid DICOM file."""
    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.4"
    file_meta.MediaStorageSOPInstanceUID = f"1.2.826.0.1.3680043.2.1125.{uid_suffix}"
    file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian

    ds = FileDataset(str(path), {}, file_meta=file_meta, preamble=b"\0" * 128)
    ds.is_little_endian = True
    ds.is_implicit_VR = True

    ds.PatientID = patient_id
    ds.StudyInstanceUID = f"1.2.3.4.5.{uid_suffix}"
    ds.SeriesInstanceUID = f"1.2.3.4.5.6.{uid_suffix}"
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.Modality = "MR"
    ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.4"

    ds.save_as(path)


def _count_subjects() -> int:
    from metadata_db.session import SessionLocal as _SessionLocal

    with _SessionLocal() as session:
        return session.scalar(select(func.count()).select_from(Subject)) or 0


def _count_instances() -> int:
    from metadata_db.session import SessionLocal as _SessionLocal

    with _SessionLocal() as session:
        return session.scalar(select(func.count()).select_from(Instance)) or 0


def _count_subjects_for_cohort(cohort_id: int) -> int:
    from metadata_db.session import SessionLocal as _SessionLocal

    with _SessionLocal() as session:
        stmt = select(func.count()).select_from(SubjectCohort).where(SubjectCohort.cohort_id == cohort_id)
        return session.scalar(stmt) or 0


def test_concurrent_subjects_max_workers_1(tmp_path: Path, monkeypatch):
    """Validate that max_workers=1 processes subjects sequentially."""
    _setup_metadata_db(monkeypatch)

    root = tmp_path / "cohort"
    root.mkdir()

    # Create multiple subject folders
    for i in range(3):
        subject_dir = root / f"subject{i}"
        subject_dir.mkdir()
        _create_minimal_dicom(subject_dir / f"file{i}.dcm", str(i), f"PAT{i}")

    config = ExtractionConfig(
        cohort_id=1,
        cohort_name="TEST_COHORT",
        raw_root=root,
        max_workers=1,
        batch_size=10,
        queue_size=10,
    )

    # Track processing order
    processed_subjects = []

    async def progress_callback(current: int, total: int) -> None:
        processed_subjects.append(current)

    asyncio.run(_run_async(config, progress_callback, None, None))

    # Should process all subjects
    assert processed_subjects[0] == 0
    assert processed_subjects[-1] == 3

    # Verify subjects persisted
    assert _count_subjects() == 3


def test_concurrent_subjects_max_workers_multiple(tmp_path: Path, monkeypatch):
    """Validate that max_workers>1 processes subjects concurrently."""
    _setup_metadata_db(monkeypatch)

    root = tmp_path / "cohort"
    root.mkdir()

    # Create multiple subject folders
    for i in range(5):
        subject_dir = root / f"subject{i}"
        subject_dir.mkdir()
        _create_minimal_dicom(subject_dir / f"file{i}.dcm", str(i), f"PAT{i}")

    config = ExtractionConfig(
        cohort_id=2,
        cohort_name="TEST_COHORT_2",
        raw_root=root,
        max_workers=3,  # Process 3 subjects concurrently
        batch_size=10,
        queue_size=10,
    )

    processed_count = 0

    async def progress_callback(current: int, total: int) -> None:
        nonlocal processed_count
        processed_count = current

    asyncio.run(_run_async(config, progress_callback, None, None))

    # Should process all subjects
    assert processed_count == 5

    # Verify subjects persisted
    assert _count_subjects() == 5


def test_concurrent_subjects_resume_behavior(tmp_path: Path, monkeypatch):
    """Validate that resume works correctly with concurrent processing."""
    _setup_metadata_db(monkeypatch)

    root = tmp_path / "cohort"
    root.mkdir()

    # Create multiple subject folders
    for i in range(3):
        subject_dir = root / f"subject{i}"
        subject_dir.mkdir()
        _create_minimal_dicom(subject_dir / f"file{i}.dcm", str(i), f"PAT{i}")

    config = ExtractionConfig(
        cohort_id=3,
        cohort_name="TEST_COHORT_3",
        raw_root=root,
        max_workers=2,
        batch_size=10,
        queue_size=10,
    )

    # First run - process all subjects
    asyncio.run(_run_async(config, None, None, None))
    initial_instances = _count_instances()
    assert initial_instances == 3

    # Add more files to one subject
    _create_minimal_dicom(root / "subject0" / "file_new.dcm", "10", "PAT0")

    # Second run - should resume subject0 and process others
    asyncio.run(_run_async(config, None, None, None))
    assert _count_instances() == initial_instances + 1


def test_concurrent_subjects_progress_callback_thread_safety(tmp_path: Path, monkeypatch):
    """Validate that progress callback is thread-safe."""
    _setup_metadata_db(monkeypatch)

    root = tmp_path / "cohort"
    root.mkdir()

    # Create many subject folders to stress test concurrency
    for i in range(10):
        subject_dir = root / f"subject{i}"
        subject_dir.mkdir()
        _create_minimal_dicom(subject_dir / f"file{i}.dcm", str(i), f"PAT{i}")

    config = ExtractionConfig(
        cohort_id=4,
        cohort_name="TEST_COHORT_4",
        raw_root=root,
        max_workers=5,
        batch_size=10,
        queue_size=10,
    )

    progress_values = []

    async def progress_callback(current: int, total: int) -> None:
        progress_values.append((current, total))

    asyncio.run(_run_async(config, progress_callback, None, None))
    # Progress should be monotonic
    assert len(progress_values) > 0
    assert progress_values[-1] == (10, 10)

    # Check monotonicity (current should never decrease)
    for i in range(1, len(progress_values)):
        assert progress_values[i][0] >= progress_values[i - 1][0]


def test_concurrent_subjects_equivalent_results(tmp_path: Path, monkeypatch):
    """Validate that max_workers=1 and max_workers>1 produce equivalent results."""
    _setup_metadata_db(monkeypatch)

    root = tmp_path / "cohort"
    root.mkdir()

    # Create test subjects
    for i in range(3):
        subject_dir = root / f"subject{i}"
        subject_dir.mkdir()
        _create_minimal_dicom(subject_dir / f"file{i}.dcm", str(i), f"PAT{i}")

    # Run with max_workers=1
    config1 = ExtractionConfig(
        cohort_id=5,
        cohort_name="TEST_COHORT_5",
        raw_root=root,
        max_workers=1,
        batch_size=10,
        queue_size=10,
    )

    _setup_metadata_db(monkeypatch)
    asyncio.run(_run_async(config1, None, None, None))
    assert _count_subjects() == 3

    # Run with max_workers=3
    config2 = ExtractionConfig(
        cohort_id=6,
        cohort_name="TEST_COHORT_6",
        raw_root=root,
        max_workers=3,
        batch_size=10,
        queue_size=10,
    )

    _setup_metadata_db(monkeypatch)
    asyncio.run(_run_async(config2, None, None, None))
    assert _count_subjects() == 3


def test_extraction_deduplicates_subject_cohort_memberships(tmp_path: Path, monkeypatch):
    _setup_metadata_db(monkeypatch)

    root = tmp_path / "cohort_dedupe"
    root.mkdir()

    subject_dir = root / "subject0"
    subject_dir.mkdir()
    for i in range(12):
        _create_minimal_dicom(subject_dir / f"file{i}.dcm", str(i), "PATIENT1")

    config = ExtractionConfig(
        cohort_id=7,
        cohort_name="TEST_COHORT_DEDUPE",
        raw_root=root,
        max_workers=1,
        batch_size=10,
        queue_size=2,
    )

    asyncio.run(_run_async(config, None, None, None))
    from metadata_db.session import SessionLocal as _SessionLocal

    with _SessionLocal() as session:
        membership_count = session.scalar(select(func.count()).select_from(SubjectCohort)) or 0
    assert membership_count == 1

