from __future__ import annotations

from pathlib import Path

import pytest

from extract.subject_mapping import SubjectResolver, load_subject_code_csv, subject_code_gen


def test_subject_code_gen_consistent() -> None:
    code_one = subject_code_gen("PATIENT123", "seed-value")
    code_two = subject_code_gen("PATIENT123", "seed-value")
    code_other = subject_code_gen("PATIENT123", "different-seed")

    assert code_one == code_two
    assert code_one != code_other
    assert len(code_one) == 16  # 8 bytes digest -> 16 hex chars


def test_load_subject_code_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "mapping.csv"
    csv_path.write_text("patient,subject\nA001,SUBJ-01\nA002,SUBJ-02\n", encoding="utf-8")

    mapping = load_subject_code_csv(csv_path, "patient", "subject")
    assert mapping == {"A001": "SUBJ-01", "A002": "SUBJ-02"}


def test_subject_resolver_prefers_csv(tmp_path: Path) -> None:
    resolver = SubjectResolver(subject_code_map={"KNOWN": "SUBJ-KNOWN"}, seed="seed")

    resolved_known = resolver.resolve(patient_id="KNOWN", patient_name="Name^Known", study_uid="1.2.3")
    assert resolved_known.subject_code == "SUBJ-KNOWN"
    assert resolved_known.source == "csv"

    resolved_unknown = resolver.resolve(patient_id="UNKNOWN", patient_name=None, study_uid="1.2.3")
    assert resolved_unknown.source == "hash"
    assert resolved_unknown.subject_code == subject_code_gen("UNKNOWN", "seed")


def test_load_subject_code_csv_missing_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "mapping.csv"
    csv_path.write_text("patient,subject\nA001,SUBJ-01\n", encoding="utf-8")

    with pytest.raises(ValueError):
        load_subject_code_csv(csv_path, "missing", "subject")
