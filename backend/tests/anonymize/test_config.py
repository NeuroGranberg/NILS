from pathlib import Path

import pytest

from anonymize.config import (
    AnonymizeConfig,
    AuditExportConfig,
    AuditExportFormat,
    DerivativesStatus,
    clean_dcm_raw,
    setup_derivatives_folders,
    PatientIdConfig,
    PatientIdStrategyType,
    CsvMappingConfig,
    CsvMissingMode,
    SequentialIdConfig,
    SequentialDiscoveryMode,
    FolderIdConfig,
    FolderStrategy,
)
from anonymize.core import _build_id_strategy, _rename_patient_folders


def test_valid_config(tmp_path: Path):
    source = tmp_path / "dicom"
    source.mkdir()

    config = AnonymizeConfig(
        source_root=source,
        output_root=source / "out",
        anonymize_categories=["Patient_Information"],
        patient_id=PatientIdConfig(
            enabled=True,
            strategy=PatientIdStrategyType.SEQUENTIAL,
            sequential=SequentialIdConfig(pattern="COHORTXXXX"),
        ),
        audit_export=AuditExportConfig(
            enabled=True,
            format=AuditExportFormat.ENCRYPTED_EXCEL,
            excel_password="secret",
        ),
    )

    assert config.patient_id.sequential is not None
    assert config.audit_resume_per_leaf is True


def test_folder_strategy_falls_back_when_regex_missing(tmp_path: Path):
    source = tmp_path / "dicom"
    subject = source / "SubjectA" / "Series1"
    subject.mkdir(parents=True)
    file_path = subject / "MR.1.dcm"
    file_path.write_text("data")

    config = AnonymizeConfig(
        source_root=source,
        output_root=tmp_path / "out",
        scrub_tags=[(0x0010, 0x0010)],
        anonymize_categories=[],
        patient_id=PatientIdConfig(
            enabled=True,
            strategy=PatientIdStrategyType.FOLDER,
            folder=FolderIdConfig(
                strategy=FolderStrategy.DEPTH,
                depth_after_root=1,
                regex=r"\d{4}",
                fallback_template="COHORTXXXX",
            ),
        ),
        audit_export=AuditExportConfig(enabled=False, format=AuditExportFormat.CSV),
        concurrent_processes=1,
        worker_threads=1,
    )

    strategy = _build_id_strategy(config, [file_path])
    new_id = strategy.map("SubjectA", file_path)
    assert new_id != "SubjectA"
    assert new_id.startswith("COHORT")


def test_folder_strategy_literal_segment(tmp_path: Path):
    source = tmp_path / "dicom"
    subject = source / "SubjectA" / "Series1"
    subject.mkdir(parents=True)
    file_path = subject / "MR.1.dcm"
    file_path.write_text("data")

    config = AnonymizeConfig(
        source_root=source,
        output_root=tmp_path / "out",
        scrub_tags=[(0x0010, 0x0010)],
        anonymize_categories=[],
        patient_id=PatientIdConfig(
            enabled=True,
            strategy=PatientIdStrategyType.FOLDER,
            folder=FolderIdConfig(
                strategy=FolderStrategy.DEPTH,
                depth_after_root=1,
                regex=r"(.+)",
                fallback_template="XXXX",
            ),
        ),
        audit_export=AuditExportConfig(enabled=False, format=AuditExportFormat.CSV),
        concurrent_processes=1,
        worker_threads=1,
    )

    strategy = _build_id_strategy(config, [file_path])
    new_id = strategy.map("SubjectA", file_path)
    assert new_id == "SubjectA"


def test_rename_patient_folders(tmp_path: Path):
    output_root = tmp_path / "dcm-raw"
    nested = output_root / "Patient123" / "visit-Patient123"
    nested.mkdir(parents=True)
    (nested / "study.dcm").write_text("data")

    class DummyStrategy:
        def __init__(self, mapping: dict[str, str]):
            self._mapping = mapping

        def map(self, old_id: str, filepath: Path) -> str:  # pragma: no cover - helper
            return self._mapping.get(old_id, old_id)

        def lookup_replacement(self, old_id: str) -> str | None:  # pragma: no cover - helper
            mapped = self._mapping.get(old_id)
            if mapped and mapped != old_id:
                return mapped
            return None

        def iter_mappings(self):  # pragma: no cover - helper
            return self._mapping.items()

    mapping = {"Patient123": "PATIENT999"}
    errors = _rename_patient_folders(output_root, DummyStrategy(mapping), mapping)
    assert not errors
    assert not (output_root / "Patient123").exists()
    renamed_root = output_root / "PATIENT999"
    assert renamed_root.exists()
    assert (renamed_root / "visit-PATIENT999").exists()


def test_rename_patient_folders_nested_levels(tmp_path: Path):
    output_root = tmp_path / "dcm-raw"
    deep = output_root / "Patient123" / "SEKBF00011733522-Patient123" / "series" / "Patient123-extra"
    deep.mkdir(parents=True)
    untouched = output_root / "OtherSubject"
    untouched.mkdir()

    class DummyStrategy:
        def __init__(self, mapping: dict[str, str]):
            self._mapping = mapping

        def map(self, old_id: str, filepath: Path) -> str:  # pragma: no cover - helper
            return self._mapping.get(old_id, old_id)

        def lookup_replacement(self, old_id: str) -> str | None:  # pragma: no cover - helper
            mapped = self._mapping.get(old_id)
            if mapped and mapped != old_id:
                return mapped
            return None

        def iter_mappings(self):  # pragma: no cover - helper
            return self._mapping.items()

    mapping = {"Patient123": "SUBJ0001"}
    errors = _rename_patient_folders(output_root, DummyStrategy(mapping), {})
    assert not errors

    renamed_root = output_root / "SUBJ0001"
    assert renamed_root.exists()
    assert not (output_root / "Patient123").exists()
    assert (renamed_root / "SEKBF00011733522-SUBJ0001" / "series" / "SUBJ0001-extra").exists()

    # Ensure unrelated directories remain untouched
    assert (output_root / "OtherSubject").exists()

    # Idempotent rerun
    second_errors = _rename_patient_folders(output_root, DummyStrategy(mapping), {})
    assert not second_errors


def test_csv_mapping_defaults(tmp_path: Path):
    source = tmp_path / "dicom"
    source.mkdir()

    csv_path = tmp_path / "mapping.csv"
    csv_path.write_text("pn,size\n")

    csv_config = CsvMappingConfig(
        path=csv_path,
        source_column="old",
        target_column="new",
    )

    mapping = {"ABC": "001"}
    next_counter = csv_config.starting_number_for_missing(mapping)
    assert next_counter == 1

    csv_config = csv_config.model_copy(update={"missing_mode": CsvMissingMode.PER_TOP_FOLDER_SEQ, "missing_pattern": "MISSEDXXXX"})
    mapping["DEF"] = "MISSED0001"
    assert csv_config.starting_number_for_missing(mapping) == 2


def test_csv_hash_mapping_skips_discovery(tmp_path: Path, monkeypatch):
    source = tmp_path / "dicom"
    source.mkdir()
    # Inject a sentinel to detect unintended discovery calls
    def boom(_: Path) -> list[str]:
        raise AssertionError("_discover_pids_by_top_folder should not be called for HASH mode")

    monkeypatch.setattr("anonymize.core._discover_pids_by_top_folder", boom)

    csv_path = tmp_path / "mapping.csv"
    csv_path.write_text("old,new\nA001,B001\n")

    config = AnonymizeConfig(
        source_root=source,
        output_root=tmp_path / "out",
        scrub_tags=[(0x0010, 0x0010)],
        anonymize_categories=[],
        patient_id=PatientIdConfig(
            enabled=True,
            strategy=PatientIdStrategyType.CSV,
            csv_mapping=CsvMappingConfig(
                path=csv_path,
                source_column="old",
                target_column="new",
                missing_mode=CsvMissingMode.HASH,
            ),
        ),
        audit_export=AuditExportConfig(enabled=False, format=AuditExportFormat.CSV),
        concurrent_processes=1,
        worker_threads=1,
    )

    strategy = _build_id_strategy(config, [])
    assert strategy.map("A001", Path("dummy")) == "B001"


def test_sequential_strategy_requires_payload():
    with pytest.raises(ValueError):
        PatientIdConfig(
            enabled=True,
            strategy=PatientIdStrategyType.SEQUENTIAL,
        )

    config = PatientIdConfig(
        enabled=True,
        strategy=PatientIdStrategyType.SEQUENTIAL,
        sequential=SequentialIdConfig(pattern="ALSXXXX", discovery=SequentialDiscoveryMode.ONE_PER_STUDY),
    )
    assert config.sequential is not None


def test_setup_derivatives_initializes_structure(tmp_path: Path):
    selected_root = tmp_path / "cohort"
    selected_root.mkdir()
    (selected_root / "study1").mkdir()
    (selected_root / "study1" / "file.dcm").write_text("data")

    setup = setup_derivatives_folders(selected_root)

    assert setup.status == DerivativesStatus.FRESH
    assert (setup.source_path / "study1" / "file.dcm").exists()
    assert not list(setup.output_path.iterdir())
    assert not (selected_root / "study1").exists()


def test_setup_derivatives_detects_existing_outputs(tmp_path: Path):
    selected_root = tmp_path / "cohort"
    selected_root.mkdir()
    (selected_root / "data").mkdir()
    first_setup = setup_derivatives_folders(selected_root)
    (first_setup.output_path / "previous").mkdir()

    second_setup = setup_derivatives_folders(selected_root)
    assert second_setup.status == DerivativesStatus.RAW_EXISTS_WITH_CONTENT


def test_clean_dcm_raw_only_affects_raw(tmp_path: Path):
    selected_root = tmp_path / "cohort"
    selected_root.mkdir()
    (selected_root / "data").mkdir()
    setup = setup_derivatives_folders(selected_root)
    original_file = setup.source_path / "data" / "nested.txt"
    original_file.parent.mkdir(parents=True, exist_ok=True)
    original_file.write_text("immutable")
    (setup.output_path / "old_run").mkdir()
    (setup.output_path / "old_run" / "file.txt").write_text("remove")

    clean_dcm_raw(setup.output_path)

    assert original_file.exists()
    assert not list(setup.output_path.iterdir())


def test_setup_derivatives_handles_derivatives_root(tmp_path: Path):
    cohort_root = tmp_path / "cohort"
    derivatives_root = cohort_root / "derivatives"
    dcm_original = derivatives_root / "dcm-original"
    dcm_raw = derivatives_root / "dcm-raw"
    dcm_original.mkdir(parents=True)
    (dcm_original / "file.dcm").write_text("data")

    setup = setup_derivatives_folders(derivatives_root)

    assert setup.source_path == dcm_original
    assert setup.output_path == dcm_raw
    assert setup.status == DerivativesStatus.FRESH
    assert not (derivatives_root / "derivatives").exists()


def test_setup_derivatives_handles_dcm_original_root(tmp_path: Path):
    derivatives_root = tmp_path / "cohort" / "derivatives"
    dcm_original = derivatives_root / "dcm-original"
    dcm_raw = derivatives_root / "dcm-raw"
    dcm_original.mkdir(parents=True)
    (dcm_original / "file.dcm").write_text("data")

    setup = setup_derivatives_folders(dcm_original)

    assert setup.source_path == dcm_original
    assert setup.output_path == dcm_raw
    assert setup.status == DerivativesStatus.FRESH
