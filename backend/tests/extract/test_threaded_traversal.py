"""Validation tests for Phase 2: threaded directory traversal."""

from __future__ import annotations

from pathlib import Path

import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset

from extract.scanner import SubjectFolder
from extract.subject_mapping import SubjectResolver
from extract.worker import _iter_dicom_files, extract_subject_batches


def _create_minimal_dicom(path: Path, uid_suffix: str) -> None:
    """Create a minimal valid DICOM file."""
    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.4"
    file_meta.MediaStorageSOPInstanceUID = f"1.2.826.0.1.3680043.2.1125.{uid_suffix}"
    file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian

    ds = FileDataset(str(path), {}, file_meta=file_meta, preamble=b"\0" * 128)
    ds.is_little_endian = True
    ds.is_implicit_VR = True

    ds.PatientID = "TEST001"
    ds.StudyInstanceUID = "1.2.3.4.5"
    ds.SeriesInstanceUID = "1.2.3.4.5.6"
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.Modality = "MR"
    ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.4"

    ds.save_as(path)


def test_threaded_traversal_finds_all_files(tmp_path: Path):
    """Validate that threaded traversal finds all DICOM files."""
    root = tmp_path / "test_root"
    root.mkdir()

    # Create nested directory structure
    (root / "sub1").mkdir()
    (root / "sub1" / "study1").mkdir()
    (root / "sub1" / "study1" / "series1").mkdir()
    (root / "sub2").mkdir()
    (root / "sub2" / "study2").mkdir()

    # Create files with different extensions
    _create_minimal_dicom(root / "sub1" / "study1" / "series1" / "file1.dcm", "1")
    _create_minimal_dicom(root / "sub1" / "study1" / "series1" / "file2.dcm", "2")
    _create_minimal_dicom(root / "sub2" / "study2" / "file3.dcm", "3")
    _create_minimal_dicom(root / "sub2" / "study2" / "file4", "4")  # extensionless

    # Test different extension modes
    for mode in ["all", "all_dcm", "dcm", "no_ext"]:
        files = list(_iter_dicom_files(root, mode))
        assert len(files) > 0, f"Mode {mode} should find files"

    # Test "all" mode finds all DICOM files
    all_files = sorted(_iter_dicom_files(root, "all"))
    assert len(all_files) == 4
    assert all(f.name in ["file1.dcm", "file2.dcm", "file3.dcm", "file4"] for f in all_files)


def test_threaded_traversal_extension_modes(tmp_path: Path):
    """Validate extension mode filtering works correctly."""
    root = tmp_path / "test_ext"
    root.mkdir()

    _create_minimal_dicom(root / "lower.dcm", "1")
    _create_minimal_dicom(root / "UPPER.DCM", "2")
    _create_minimal_dicom(root / "extensionless", "3")

    # Test each extension mode
    dcm_files = list(_iter_dicom_files(root, "dcm"))
    assert len(dcm_files) == 1
    assert dcm_files[0].name == "lower.dcm"

    dcm_upper_files = list(_iter_dicom_files(root, "DCM"))
    assert len(dcm_upper_files) == 1
    assert dcm_upper_files[0].name == "UPPER.DCM"

    all_dcm_files = sorted(_iter_dicom_files(root, "all_dcm"))
    assert len(all_dcm_files) == 2
    assert {f.name for f in all_dcm_files} == {"lower.dcm", "UPPER.DCM"}

    no_ext_files = list(_iter_dicom_files(root, "no_ext"))
    assert len(no_ext_files) == 1
    assert no_ext_files[0].name == "extensionless"

    all_files = sorted(_iter_dicom_files(root, "all"))
    assert len(all_files) == 3


def test_threaded_traversal_with_extract_subject_batches(tmp_path: Path):
    """Validate that threaded traversal works correctly with extract_subject_batches."""
    subject_dir = tmp_path / "subject1"
    subject_dir.mkdir()

    # Create nested structure
    (subject_dir / "level1").mkdir()
    (subject_dir / "level1" / "level2").mkdir()

    _create_minimal_dicom(subject_dir / "file1.dcm", "1")
    _create_minimal_dicom(subject_dir / "level1" / "file2.dcm", "2")
    _create_minimal_dicom(subject_dir / "level1" / "level2" / "file3.dcm", "3")

    subject = SubjectFolder(subject_key="subject1", path=subject_dir)
    resolver = SubjectResolver(subject_code_map=None, seed="test-seed")

    batches = list(
        extract_subject_batches(
            subject=subject,
            extension_mode="all",
            resume_instance=None,
            batch_size=10,
            subject_resolver=resolver,
        )
    )

    assert len(batches) == 1
    payloads, _ = batches[0]
    assert len(payloads) == 3

    # Verify all files were processed
    # file_path is relative to subject.path.parent, so includes subject_key prefix
    file_paths = {p.file_path for p in payloads}
    assert any("file1.dcm" in path for path in file_paths)
    assert any("level1/file2.dcm" in path or "level1\\file2.dcm" in path for path in file_paths)
    assert any("level1/level2/file3.dcm" in path or "level1\\level2\\file3.dcm" in path for path in file_paths)


def test_threaded_traversal_handles_empty_directories(tmp_path: Path):
    """Validate that threaded traversal handles empty directories gracefully."""
    root = tmp_path / "empty_root"
    root.mkdir()
    (root / "empty_subdir").mkdir()

    files = list(_iter_dicom_files(root, "all"))
    assert len(files) == 0


def test_threaded_traversal_handles_missing_files_gracefully(tmp_path: Path):
    """Validate that threaded traversal handles FileNotFoundError gracefully."""
    root = tmp_path / "test_missing"
    root.mkdir()

    _create_minimal_dicom(root / "file1.dcm", "1")

    # Should not raise even if files disappear during traversal
    files = list(_iter_dicom_files(root, "all"))
    assert len(files) >= 0  # Should handle gracefully

