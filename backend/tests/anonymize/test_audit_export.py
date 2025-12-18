from pathlib import Path

from anonymize.exporter import process_and_aggregate_audit


def test_process_and_aggregate_audit_creates_reference_columns(tmp_path: Path):
    source_root = tmp_path / "dicoms"
    source_root.mkdir()

    events = [
        {
            "rel_path": "sub-001/session1/file.dcm",
            "study_uid": "1.2.3",
            "tag": "(0010,0020)",
            "tag_name": "Patient ID",
            "action": "replaced",
            "old_value": "PAT001",
            "new_value": "ALS0001",
        },
        {
            "rel_path": "sub-001/session1/file.dcm",
            "study_uid": "1.2.3",
            "tag": "(0008,0020)",
            "tag_name": "Study Date",
            "action": "retained",
            "old_value": "20250101",
            "new_value": "",
        },
        {
            "rel_path": "sub-001/session1/file.dcm",
            "study_uid": "1.2.3",
            "tag": "(0010,0010)",
            "tag_name": "PatientName",
            "action": "removed",
            "old_value": "John^Doe",
            "new_value": "",
        },
        {
            "rel_path": "sub-001/session1/file.dcm",
            "study_uid": "1.2.3",
            "tag": "(0008,0020)",
            "tag_name": "Study Date",
            "action": "added",
            "old_value": "",
            "new_value": "M06",
        },
    ]

    df = process_and_aggregate_audit(events, source_root, "cohort-test")

    assert not df.is_empty()
    columns = df.columns
    assert "Patient_ID_0010_0020_old_value" in columns
    assert "Patient_ID_0010_0020_new_value" in columns
    assert "Study_Date_0008_0020_old_value" in columns
    assert "Study_Date_0008_0020_new_value" in columns
    assert "PatientName_0010_0010" in columns

    row = df.to_dicts()[0]
    assert row["rel_path"] == "sub-001/session1/file.dcm"
    assert row["DataFolder"] == source_root.name
    assert row["ParentFolder"] == "sub-001"
    assert row["SubFolder"] == "session1"

    assert row["Patient_ID_0010_0020_old_value"] == "PAT001"
    assert row["Patient_ID_0010_0020_new_value"] == "ALS0001"
    assert row["Study_Date_0008_0020_old_value"] == "20250101"
    assert row["Study_Date_0008_0020_new_value"] == "M06"
    assert row["PatientName_0010_0010"] == "John^Doe"


def test_process_and_aggregate_audit_handles_late_non_null_values(tmp_path: Path):
    source_root = tmp_path / "dicoms"
    source_root.mkdir()

    events = []
    for idx in range(150):
        pid = f"PAT{idx:04d}"
        rel_path = f"{pid}/study-{idx}/file.dcm"
        events.append(
            {
                "rel_path": rel_path,
                "study_uid": f"1.2.3.{idx}",
                "tag": "(0008,1010)",
                "tag_name": "Station Name",
                "action": "retained",
                "old_value": "Intera" if idx == 149 else "",
                "new_value": "Intera" if idx == 149 else "",
            }
        )

    df = process_and_aggregate_audit(events, source_root, "cohort-test")
    assert not df.is_empty()
    rows = df.to_dicts()
    assert any(row.get("Station_Name_0008_1010") == "Intera" for row in rows)
