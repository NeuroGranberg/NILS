"""Validation tests for Phase 1: specific_tags optimization."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pydicom
from pydicom.dataset import FileDataset, FileMetaDataset

from extract.dicom_mappings import EXTRACT_SPECIFIC_TAGS
from extract.scanner import SubjectFolder
from extract.subject_mapping import SubjectResolver
from extract.worker import extract_subject_batches


def _create_test_dicom(path: Path, *, uid_suffix: str = "1") -> None:
    """Create a test DICOM file with various fields."""
    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.4"
    file_meta.MediaStorageSOPInstanceUID = f"1.2.826.0.1.3680043.2.1125.{uid_suffix}"
    file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian

    ds = FileDataset(str(path), {}, file_meta=file_meta, preamble=b"\0" * 128)
    ds.is_little_endian = True
    ds.is_implicit_VR = True

    # Core routing tags
    ds.PatientID = "TEST001"
    ds.PatientName = "Test^Patient^One"
    ds.StudyInstanceUID = "1.2.3.4.5"
    ds.SeriesInstanceUID = "1.2.3.4.5.6"
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.Modality = "MR"
    ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.4"

    # Study fields
    ds.StudyDate = "20240101"
    ds.StudyTime = "120000"
    ds.StudyDescription = "Test Study"
    ds.Manufacturer = "Test Manufacturer"
    ds.ManufacturerModelName = "Test Model"

    # Series fields
    ds.SeriesDate = "20240101"
    ds.SeriesTime = "120100"
    ds.SeriesDescription = "T1 Weighted"
    ds.SequenceName = "T1"
    ds.ProtocolName = "Brain"
    ds.SliceThickness = 1.0
    ds.ImageType = ["ORIGINAL", "PRIMARY"]

    # Instance fields
    ds.InstanceNumber = int(uid_suffix)
    ds.AcquisitionNumber = 1
    ds.Rows = 256
    ds.Columns = 256
    ds.PixelSpacing = [0.5, 0.5]

    # MRI-specific fields
    ds.RepetitionTime = 2000.0
    ds.EchoTime = 30.0
    ds.FlipAngle = 90.0
    ds.MagneticFieldStrength = 3.0

    ds.save_as(path)


def test_specific_tags_extracts_same_fields_as_full_read(tmp_path: Path):
    """Validate that specific_tags extracts identical fields to full read."""
    subject_dir = tmp_path / "subject1"
    subject_dir.mkdir(parents=True)

    _create_test_dicom(subject_dir / "file1.dcm", uid_suffix="1")
    _create_test_dicom(subject_dir / "file2.dcm", uid_suffix="2")

    subject = SubjectFolder(subject_key="subject1", path=subject_dir)
    resolver = SubjectResolver(subject_code_map=None, seed="test-seed")

    # Extract with specific_tags=True (default)
    batches_with_tags = list(
        extract_subject_batches(
            subject=subject,
            extension_mode="all",
            resume_instance=None,
            batch_size=10,
            subject_resolver=resolver,
            use_specific_tags=True,
        )
    )

    # Extract with specific_tags=False
    batches_without_tags = list(
        extract_subject_batches(
            subject=subject,
            extension_mode="all",
            resume_instance=None,
            batch_size=10,
            subject_resolver=resolver,
            use_specific_tags=False,
        )
    )

    assert len(batches_with_tags) == len(batches_without_tags)
    assert len(batches_with_tags) == 1

    payloads_with_tags, _ = batches_with_tags[0]
    payloads_without_tags, _ = batches_without_tags[0]

    assert len(payloads_with_tags) == len(payloads_without_tags)
    assert len(payloads_with_tags) == 2

    # Compare each payload field by field
    for payload_with, payload_without in zip(payloads_with_tags, payloads_without_tags):
        # Compare core fields
        assert payload_with.subject_key == payload_without.subject_key
        assert payload_with.subject_code == payload_without.subject_code
        assert payload_with.study_uid == payload_without.study_uid
        assert payload_with.series_uid == payload_without.series_uid
        assert payload_with.sop_uid == payload_without.sop_uid
        assert payload_with.modality == payload_without.modality
        assert payload_with.patient_id == payload_without.patient_id
        assert payload_with.patient_name == payload_without.patient_name
        assert payload_with.subject_resolution_source == payload_without.subject_resolution_source

        # Compare extracted field dictionaries
        assert payload_with.study_fields == payload_without.study_fields
        assert payload_with.series_fields == payload_without.series_fields
        assert payload_with.instance_fields == payload_without.instance_fields
        assert payload_with.mri_fields == payload_without.mri_fields
        assert payload_with.ct_fields == payload_without.ct_fields
        assert payload_with.pet_fields == payload_without.pet_fields


def test_extract_specific_tags_is_not_empty():
    """Validate that EXTRACT_SPECIFIC_TAGS contains expected tags."""
    assert len(EXTRACT_SPECIFIC_TAGS) > 0
    # Should include core routing tags
    # Check that we have tags (Tag is a function that returns BaseTag instances)
    from pydicom.tag import BaseTag
    assert all(isinstance(tag, BaseTag) for tag in EXTRACT_SPECIFIC_TAGS)


def test_specific_tags_handles_missing_fields_gracefully(tmp_path: Path):
    """Validate that specific_tags handles missing optional fields correctly."""
    subject_dir = tmp_path / "subject1"
    subject_dir.mkdir(parents=True)

    # Create minimal DICOM with only required fields
    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.4"
    file_meta.MediaStorageSOPInstanceUID = "1.2.826.0.1.3680043.2.1125.1"
    file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian

    ds = FileDataset(str(subject_dir / "minimal.dcm"), {}, file_meta=file_meta, preamble=b"\0" * 128)
    ds.is_little_endian = True
    ds.is_implicit_VR = True

    # Only required fields
    ds.PatientID = "MIN001"
    ds.StudyInstanceUID = "1.2.3.4.5"
    ds.SeriesInstanceUID = "1.2.3.4.5.6"
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.Modality = "MR"
    ds.SOPClassUID = "1.2.840.10008.5.1.4.1.1.4"

    ds.save_as(subject_dir / "minimal.dcm")

    subject = SubjectFolder(subject_key="subject1", path=subject_dir)
    resolver = SubjectResolver(subject_code_map=None, seed="test-seed")

    batches = list(
        extract_subject_batches(
            subject=subject,
            extension_mode="all",
            resume_instance=None,
            batch_size=10,
            subject_resolver=resolver,
            use_specific_tags=True,
        )
    )

    assert len(batches) == 1
    payloads, _ = batches[0]
    assert len(payloads) == 1
    payload = payloads[0]

    # Should extract successfully even with minimal fields
    assert payload.patient_id == "MIN001"
    assert payload.modality == "MR"
    # Optional fields should be None or dicts with None values (extract_fields returns None for missing)
    # MRI fields dict will have keys with None values since we extract all MRI fields
    assert payload.mri_fields is not None
    # Check that MRI fields exist but are None (since no MRI-specific data in minimal DICOM)
    assert all(v is None for v in payload.mri_fields.values())

