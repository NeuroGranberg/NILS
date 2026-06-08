"""Unit tests for the unified export-config builder (Part 1.5).

`build_export_config` is the single source of truth for constructing a
`BidsExportConfig` from a frontend config dict. Both export entry points
(cohort `bids` stage and standalone `export` job) feed it the same camelCase
config and differ only in *scope* (`cohort_name` vs `include_stack_ids`).

These tests pin the unification invariants so the two paths can never drift
again (the pre-unification copy-pasted blocks had already diverged on the flat
root names).
"""

from api.services.export_runner import build_export_config, _parse_subject_id_source


def test_subset_scope_sets_stack_ids_only():
    cfg = build_export_config({"outputModes": ["dcm", "nii"]}, include_stack_ids=[3, 1, 2])
    assert cfg.include_stack_ids == [3, 1, 2]
    assert cfg.cohort_name is None


def test_cohort_scope_sets_cohort_name_only():
    cfg = build_export_config({}, cohort_name="StudyA")
    assert cfg.cohort_name == "StudyA"
    assert cfg.include_stack_ids == []


def test_both_scopes_can_coexist():
    # The engine ANDs the two optional WHERE clauses, so both being set is valid.
    cfg = build_export_config({}, cohort_name="StudyA", include_stack_ids=[5])
    assert cfg.cohort_name == "StudyA"
    assert cfg.include_stack_ids == [5]


def test_no_scope_is_neither_filter():
    cfg = build_export_config({})
    assert cfg.cohort_name is None
    assert cfg.include_stack_ids == []


def test_legacy_flat_root_names_normalize_to_canonical():
    # The cohort path historically passed 'dcm-flat'/'nii-flat'; both must
    # canonicalize to 'flat-dcm'/'flat-nifti' (Part 1.5 decision).
    cfg = build_export_config(
        {"flatDcmRootName": "dcm-flat", "flatNiftiRootName": "nii-flat"},
        include_stack_ids=[1],
    )
    assert cfg.flat_dcm_root_name == "flat-dcm"
    assert cfg.flat_nifti_root_name == "flat-nifti"


def test_default_flat_root_names_are_canonical():
    cfg = build_export_config({}, cohort_name="C")
    assert cfg.flat_dcm_root_name == "flat-dcm"
    assert cfg.flat_nifti_root_name == "flat-nifti"


def test_subject_identifier_source_parsing():
    assert _parse_subject_id_source("") == "subject_code"
    assert _parse_subject_id_source(None) == "subject_code"
    assert _parse_subject_id_source("subject_code") == "subject_code"
    assert _parse_subject_id_source("7") == 7
    assert _parse_subject_id_source(7) == 7
    assert _parse_subject_id_source("not-an-int") == "subject_code"


def test_subject_identifier_source_flows_into_config():
    assert build_export_config({"subjectIdentifierSource": "5"}, cohort_name="C").subject_identifier_source == 5
    assert build_export_config({}, cohort_name="C").subject_identifier_source == "subject_code"


def test_legacy_single_output_mode_is_normalized():
    cfg = build_export_config({"outputMode": "nii"}, include_stack_ids=[1])
    assert [m.value for m in cfg.output_modes] == ["nii"]


def test_empty_output_modes_defaults_to_dcm():
    cfg = build_export_config({}, cohort_name="C")
    assert [m.value for m in cfg.output_modes] == ["dcm"]


def test_defaults_are_preserved_from_both_legacy_blocks():
    cfg = build_export_config({}, cohort_name="C")
    assert cfg.copy_workers == 8
    assert cfg.convert_workers == 8
    assert cfg.exclude_provenance == ["ProjectionDerived"]
    assert cfg.include_intents == ["anat", "dwi", "func", "fmap", "perf"]
    assert cfg.include_acceleration_in_name is True
    assert cfg.group_symri is True
    assert cfg.bids_dcm_root_name == "bids-dcm"
    assert cfg.bids_nifti_root_name == "bids-nifti"
