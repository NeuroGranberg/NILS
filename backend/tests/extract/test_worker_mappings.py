from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from pathlib import Path

import pydicom
from pydicom.dataset import Dataset, FileDataset

from extract.dicom_mappings import CT_SERIES_FIELD_MAP, extract_fields
from extract.subject_mapping import SubjectResolver, subject_code_gen
from extract.worker import extract_subject_batches


def _build_dataset(path: Path) -> None:
    file_meta = Dataset()
    file_meta.TransferSyntaxUID = pydicom.uid.ExplicitVRLittleEndian
    file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.4"
    file_meta.MediaStorageSOPInstanceUID = "2.16.840.1.113662.2.1.1"
    file_meta.ImplementationClassUID = "1.3.6.1.4.1.5962.3.1"
    file_meta.ImplementationVersionName = "IMPL_1.0"

    ds = FileDataset(str(path), {}, file_meta=file_meta, preamble=b"\x00" * 128)
    ds.is_little_endian = True
    ds.is_implicit_VR = False

    ds.StudyInstanceUID = "1.2.3.4.5.6.7.8.9"
    ds.SeriesInstanceUID = "2.16.840.1.113662.2.1"
    ds.SOPInstanceUID = "2.16.840.1.113662.2.1.1"
    ds.Modality = "MR"
    ds.StudyDate = "20240101"
    ds.StudyTime = "010203"
    ds.ModalitiesInStudy = ["MR"]
    ds.Manufacturer = "ACME"
    ds.SeriesDate = "20240102"
    ds.SeriesTime = "030405"
    ds.SeriesDescription = "T1"
    ds.SequenceName = "tfl"
    ds.ProtocolName = "Brain"
    ds.ImageType = ["ORIGINAL", "PRIMARY"]
    ds.ImageOrientationPatient = [1, 0, 0, 0, 1, 0]
    ds.ImagePositionPatient = [0.0, 0.0, 0.0]
    ds.PixelSpacing = [0.5, 0.5]
    ds.InstanceNumber = 1
    ds.AcquisitionNumber = 1
    ds.AcquisitionDate = "20240102"
    ds.AcquisitionTime = "040506"
    ds.ContentDate = "20240102"
    ds.ContentTime = "040507"
    ds.RepetitionTime = 1500.0
    ds.EchoTime = 2.5
    ds.MRAcquisitionType = "3D"
    ds.MagneticFieldStrength = 3.0
    ds.WindowCenter = [30, 80]
    ds.WindowWidth = [400, 800]
    ds.PatientID = "12345"
    ds.PatientName = "Test^Patient"

    ds.save_as(path)


def test_extract_subject_batches_populates_mri_fields(tmp_path):
    dcm_path = tmp_path / "sub-001" / "study" / "series" / "image.dcm"
    dcm_path.parent.mkdir(parents=True)
    _build_dataset(dcm_path)

    subject = SimpleNamespace(subject_key="sub-001", path=dcm_path.parent)
    resolver = SubjectResolver(subject_code_map=None, seed="seed-value")

    batches = list(
        extract_subject_batches(
            subject=subject,
            extension_mode="all_dcm",
            resume_instance=None,
            batch_size=10,
            subject_resolver=resolver,
        )
    )
    assert len(batches) == 1
    payloads, last_uid = batches[0]
    assert last_uid == "2.16.840.1.113662.2.1.1"
    assert len(payloads) == 1
    payload = payloads[0]

    assert payload.subject_code == subject_code_gen("12345", "seed-value")
    assert payload.subject_resolution_source == "hash"
    # Date is now converted from DICOM format (YYYYMMDD) to ISO format (YYYY-MM-DD)
    assert payload.study_fields["study_date"] == "2024-01-01"
    assert payload.series_fields["sequence_name"] == "tfl"
    assert payload.instance_fields["transfer_syntax_uid"] == str(pydicom.uid.ExplicitVRLittleEndian)
    assert payload.instance_fields["window_center"] == "30.0\\80.0"
    assert payload.series_fields["media_storage_sop_instance_uid"] == "2.16.840.1.113662.2.1.1"
    assert payload.series_fields["sop_class_uid"] == "1.2.840.10008.5.1.4.1.1.4"
    assert payload.series_fields["implementation_class_uid"] == "1.3.6.1.4.1.5962.3.1"
    assert payload.series_fields["implementation_version_name"] == "IMPL_1.0"
    assert payload.mri_fields["repetition_time"] == 1500.0
    assert payload.ct_fields == {}
    assert payload.pet_fields == {}


def test_extract_fields_converts_sequences_to_json():
    ds = Dataset()
    seq_item = Dataset()
    seq_item.CodeValue = "123"
    seq_item.CodingSchemeDesignator = "DCM"
    ds.CTDIPhantomTypeCodeSequence = [seq_item]

    result = extract_fields(ds, CT_SERIES_FIELD_MAP)
    value = result["ctdi_phantom_type_code_sequence"]
    assert value is not None
    assert "00080100" in value
    assert "123" in value
