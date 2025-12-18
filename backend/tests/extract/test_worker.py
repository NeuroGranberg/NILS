from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pydicom
from pydicom.dataset import Dataset, FileDataset, FileMetaDataset

from extract.resume_index import ExistingPathIndex
from extract.scanner import SubjectFolder
from extract.subject_mapping import SubjectResolver, subject_code_gen
from extract.worker import extract_subject_batches, plan_subject_series


def _create_dicom(
    path: Path,
    *,
    uid_suffix: str,
    study_uid: str | None = None,
    series_uid: str | None = None,
    sop_uid: str | None = None,
    modality: str = "MR",
) -> None:
    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
    file_meta.MediaStorageSOPInstanceUID = sop_uid or f"1.2.826.0.1.3680043.2.1125.{uid_suffix}"
    file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian

    ds = FileDataset(str(path), {}, file_meta=file_meta, preamble=b"\0" * 128)
    ds.is_little_endian = True
    ds.is_implicit_VR = True

    ds.PatientID = "PATIENT1"
    ds.PatientName = "Test^Patient"
    ds.StudyInstanceUID = study_uid or "1.2.3.4.5"
    ds.SeriesInstanceUID = series_uid or "1.2.3.4.5.6"
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.Modality = modality
    ds.StudyDate = "20240101"
    ds.SeriesDate = "20240101"
    ds.InstanceNumber = int(uid_suffix)
    ds.ContentDate = datetime.now().strftime("%Y%m%d")
    ds.ContentTime = datetime.now().strftime("%H%M%S")

    ds.save_as(path)


def test_extract_subject_batches(tmp_path):
    subject_dir = tmp_path / "dcm-raw" / "subject1"
    subject_dir.mkdir(parents=True)

    _create_dicom(subject_dir / "file1.dcm", uid_suffix="1")
    _create_dicom(subject_dir / "file2.dcm", uid_suffix="2")

    subject = SubjectFolder(subject_key="subject1", path=subject_dir)
    resolver = SubjectResolver(subject_code_map=None, seed="test-seed")
    batches = list(
        extract_subject_batches(
            subject=subject,
            extension_mode="all",
            resume_instance=None,
            batch_size=1,
            subject_resolver=resolver,
        )
    )

    assert len(batches) == 2
    first_batch, first_last_uid = batches[0]
    assert first_batch[0].subject_key == "subject1"
    assert first_batch[0].subject_code == subject_code_gen("PATIENT1", "test-seed")
    assert first_batch[0].subject_resolution_source == "hash"
    assert first_batch[0].modality == "MR"
    all_uids = {str(last_uid) for _, last_uid in batches}
    assert all(uid.startswith("1.2.826.0.1.3680043.2.1125.") for uid in all_uids)

    latest_uid = max(all_uids)

    # Resume should skip processed SOP instances
    resumed = list(
        extract_subject_batches(
            subject=subject,
            extension_mode="all",
            resume_instance=latest_uid,
            batch_size=10,
            subject_resolver=resolver,
        )
    )
    assert len(resumed) == 0


def test_plan_subject_series_respects_resume_tokens(tmp_path):
    subject_dir = tmp_path / "subject2"
    subject_dir.mkdir(parents=True)
    study_uid = "1.2.3.4"
    series_a = "1.2.3.4.5"
    series_b = "1.2.3.4.6"

    _create_dicom(subject_dir / "a1.dcm", uid_suffix="10", study_uid=study_uid, series_uid=series_a, sop_uid="1.2.3.4.5.1")
    _create_dicom(subject_dir / "a2.dcm", uid_suffix="11", study_uid=study_uid, series_uid=series_a, sop_uid="1.2.3.4.5.2")
    _create_dicom(subject_dir / "b1.dcm", uid_suffix="20", study_uid=study_uid, series_uid=series_b, sop_uid="1.2.3.4.6.1")

    subject = SubjectFolder(subject_key="subject2", path=subject_dir)
    resume_tokens = {series_a: "1.2.3.4.5.1"}
    plans = plan_subject_series(
        subject=subject,
        extension_mode="all",
        resume_tokens=resume_tokens,
        use_specific_tags=True,
    )

    assert len(plans) == 2
    plan_map = {plan.series_uid: plan for plan in plans}
    assert plan_map[series_a].paths == [subject_dir / "a2.dcm"]
    assert plan_map[series_b].paths == [subject_dir / "b1.dcm"]


def test_extract_subject_batches_skips_known_paths(tmp_path):
    subject_dir = tmp_path / "subject3"
    subject_dir.mkdir(parents=True)

    _create_dicom(subject_dir / "keep.dcm", uid_suffix="30")
    _create_dicom(subject_dir / "skip.dcm", uid_suffix="31")

    subject = SubjectFolder(subject_key="subject3", path=subject_dir)
    resolver = SubjectResolver(subject_code_map=None, seed="test-seed")

    index = ExistingPathIndex()
    index.add("subject3", "skip.dcm")
    subject_filter = index.entry_for("subject3")

    batches = list(
        extract_subject_batches(
            subject=subject,
            extension_mode="all",
            resume_instance=None,
            batch_size=10,
            subject_resolver=resolver,
            path_filter=subject_filter,
        )
    )

    # Only the "keep" file should survive
    assert len(batches) == 1
    payloads, _ = batches[0]
    assert len(payloads) == 1
    assert payloads[0].file_path.endswith("keep.dcm")


def test_plan_subject_series_skips_known_paths(tmp_path):
    subject_dir = tmp_path / "subject4"
    subject_dir.mkdir(parents=True)
    study_uid = "9.9.9"
    series_uid = "9.9.9.1"

    _create_dicom(subject_dir / "skip_a.dcm", uid_suffix="40", study_uid=study_uid, series_uid=series_uid, sop_uid="9.9.9.1.1")
    _create_dicom(subject_dir / "keep_b.dcm", uid_suffix="41", study_uid=study_uid, series_uid=series_uid, sop_uid="9.9.9.1.2")

    index = ExistingPathIndex()
    index.add("subject4", "skip_a.dcm")
    subject_filter = index.entry_for("subject4")

    subject = SubjectFolder(subject_key="subject4", path=subject_dir)
    plans = plan_subject_series(
        subject=subject,
        extension_mode="all",
        resume_tokens={},
        use_specific_tags=True,
        path_filter=subject_filter,
    )

    assert len(plans) == 1
    assert plans[0].paths == [subject_dir / "keep_b.dcm"]


def test_extract_subject_batches_skips_missing_modality(tmp_path):
    subject_dir = tmp_path / "subject_missing_modality"
    subject_dir.mkdir(parents=True)

    _create_dicom(subject_dir / "bad.dcm", uid_suffix="50", modality="")

    subject = SubjectFolder(subject_key="subject_missing_modality", path=subject_dir)
    resolver = SubjectResolver(subject_code_map=None, seed="seed")

    batches = list(
        extract_subject_batches(
            subject=subject,
            extension_mode="all",
            resume_instance=None,
            batch_size=10,
            subject_resolver=resolver,
        )
    )

    assert batches == []


def test_extract_subject_batches_skips_disallowed_modality(tmp_path):
    subject_dir = tmp_path / "subject_invalid_modality"
    subject_dir.mkdir(parents=True)

    _create_dicom(subject_dir / "xa.dcm", uid_suffix="60", modality="XA")

    subject = SubjectFolder(subject_key="subject_invalid_modality", path=subject_dir)
    resolver = SubjectResolver(subject_code_map=None, seed="seed")

    batches = list(
        extract_subject_batches(
            subject=subject,
            extension_mode="all",
            resume_instance=None,
            batch_size=10,
            subject_resolver=resolver,
        )
    )

    assert batches == []
