"""DB-free unit tests for the descriptor layer (frozen design §2 / §8).

Covers: YAML+JSON parse, hyphenated-key aliases, forward-compatible unknown-key
warnings (no exception), the one hard schema-version RANGE refuse, missing-name
hard error, the real dcm2niix fixture, and the derived properties.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from analysis_pipeline.descriptor import (
    SUPPORTED_SCHEMA_VERSION_RANGE,
    Descriptor,
    DescriptorError,
    UnsupportedSchemaVersionError,
    parse_descriptor,
    validate_descriptor,
    work_unit_of,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "analysis_pipeline"
DCM2NIIX_FIXTURE = FIXTURES / "dcm2niix.nils.job.yml"


# --- minimal valid descriptors --------------------------------------------

_MIN_YAML = """
name: My Tool
schema-version: "0.5"
command-line: "tool [IN]"
"""

_MIN_JSON = json.dumps(
    {
        "name": "My Tool",
        "schema-version": "0.5",
        "command-line": "tool [IN]",
        "x-nils": {"analysis-level": "subject"},
    }
)


def test_parse_minimal_yaml_string():
    d = parse_descriptor(_MIN_YAML)
    assert isinstance(d, Descriptor)
    assert d.name == "My Tool"
    assert d.slug == "my-tool"
    assert d.command_line == "tool [IN]"


def test_parse_minimal_json_string():
    # JSON is a YAML subset; parser must accept it transparently.
    d = parse_descriptor(_MIN_JSON)
    assert d.name == "My Tool"
    assert d.work_unit == "subject"


def test_hyphenated_aliases_map_to_snake_case():
    yaml_text = """
name: Aliased
command-line: "x [A]"
tool-version: "9.9"
output-files:
  - id: out
    name: Output
    path-template: "[A].nii.gz"
container-image:
  type: docker
  image: "img@sha256:abc"
  container-hash: "sha256:deadbeef"
x-nils:
  analysis-level: subject
"""
    d = parse_descriptor(yaml_text)
    assert d.tool_version == "9.9"
    assert d.output_files[0].path_template == "[A].nii.gz"
    assert d.container_image is not None
    assert d.container_image.container_hash == "sha256:deadbeef"
    assert d.image_digest == "sha256:deadbeef"


def test_populate_by_name_accepts_snake_case_too():
    # populate_by_name=True means the snake_case attribute name also works.
    d = Descriptor.model_validate(
        {"name": "SnakeOk", "schema_version": "0.5", "command_line": "x [A]"}
    )
    assert d.schema_version == "0.5"
    assert d.command_line == "x [A]"


# --- forward-compatible unknown keys --------------------------------------

def test_unknown_top_level_key_warns_but_loads():
    yaml_text = """
name: Future Tool
command-line: "x [A]"
totally-new-top-key: 123
x-nils:
  analysis-level: subject
"""
    d = parse_descriptor(yaml_text)
    # Loaded (no raise) AND the value is carried (extra="allow").
    assert d.name == "Future Tool"
    assert any("totally-new-top-key" in w for w in d.warnings)
    assert getattr(d, "totally-new-top-key", None) == 123 or d.model_dump().get(
        "totally-new-top-key"
    ) == 123


def test_unknown_xnils_needs_key_warns_but_loads():
    yaml_text = """
name: Future Needs
command-line: "x [A]"
x-nils:
  analysis-level: subject
  needs:
    gpu: true
    quantum_accelerator: true
"""
    d = parse_descriptor(yaml_text)
    assert d.x_nils.needs.gpu is True
    assert any("quantum_accelerator" in w for w in d.warnings)


def test_validate_descriptor_returns_warning_list_not_exception():
    warnings = validate_descriptor(
        {"name": "ok", "command-line": "x", "unknown_key": 1}
    )
    assert isinstance(warnings, list)
    assert any("unknown_key" in w for w in warnings)


def test_descriptor_with_nothing_to_run_warns():
    warnings = validate_descriptor({"name": "noop", "x-nils": {}})
    assert any("nothing to run" in w for w in warnings)


# --- hard refuses ----------------------------------------------------------

@pytest.mark.parametrize("bad_version", ["0.4", "0.4.9", "0.6", "0.7", "1.0"])
def test_schema_version_out_of_range_is_rejected(bad_version):
    with pytest.raises(UnsupportedSchemaVersionError):
        parse_descriptor(
            json.dumps({"name": "x", "schema-version": bad_version, "command-line": "y"})
        )


def test_schema_version_in_range_is_accepted():
    lo, _ = SUPPORTED_SCHEMA_VERSION_RANGE
    d = parse_descriptor(
        json.dumps({"name": "x", "schema-version": lo, "command-line": "y"})
    )
    assert d.schema_version == lo
    # a patch within the band is fine
    d2 = parse_descriptor(
        json.dumps({"name": "x", "schema-version": "0.5.3", "command-line": "y"})
    )
    assert d2.schema_version == "0.5.3"


def test_missing_name_is_hard_error():
    with pytest.raises(DescriptorError):
        parse_descriptor(json.dumps({"schema-version": "0.5", "command-line": "y"}))


def test_empty_name_is_hard_error():
    with pytest.raises(DescriptorError):
        parse_descriptor(json.dumps({"name": "   ", "command-line": "y"}))


def test_unsupported_version_is_a_descriptor_error_subclass():
    assert issubclass(UnsupportedSchemaVersionError, DescriptorError)


def test_invalid_yaml_raises_descriptor_error():
    with pytest.raises(DescriptorError):
        parse_descriptor("name: [unterminated\n  : :")


def test_non_mapping_descriptor_raises():
    with pytest.raises(DescriptorError):
        parse_descriptor("- just\n- a\n- list")


# --- groups / inputs round-trip -------------------------------------------

def test_inputs_and_groups_round_trip():
    yaml_text = """
name: Grouped
command-line: "x [A] [B]"
inputs:
  - id: a
    name: A
    type: String
    value-key: "[A]"
    default-value: "foo"
  - id: b
    name: B
    type: Flag
    command-line-flag: "--b"
    value-key: "[B]"
groups:
  - id: g1
    name: Group One
    members: [a, b]
    mutually-exclusive: true
    one-is-required: false
x-nils:
  analysis-level: subject
"""
    d = parse_descriptor(yaml_text)
    assert len(d.inputs) == 2
    assert d.inputs[0].default_value == "foo"
    assert d.inputs[1].command_line_flag == "--b"
    assert len(d.groups) == 1
    assert d.groups[0].members == ["a", "b"]
    assert d.groups[0].mutually_exclusive is True
    assert d.groups[0].one_is_required is False


# --- properties ------------------------------------------------------------

def test_image_digest_from_image_ref_when_no_container_hash():
    d = Descriptor.model_validate(
        {
            "name": "Digest From Ref",
            "command-line": "x",
            "container-image": {"type": "docker", "image": "repo/img@sha256:cafef00d"},
        }
    )
    assert d.image_digest == "sha256:cafef00d"


def test_image_digest_none_without_container_image():
    d = Descriptor.model_validate({"name": "No Image", "command-line": "x"})
    assert d.image_digest is None


def test_work_unit_mapping_helper():
    assert work_unit_of("subject") == "subject"
    assert work_unit_of("dataset") == "group"
    assert work_unit_of("meta") == "group"
    assert work_unit_of("session") == "session"
    # dicom inputs at run/session level → stack
    assert work_unit_of("run", ["dicom"]) == "stack"
    assert work_unit_of("session", ["dicom"]) == "stack"
    # unknown level falls back to subject
    assert work_unit_of("galaxy") == "subject"


# --- the real dcm2niix fixture --------------------------------------------

def test_dcm2niix_fixture_exists():
    assert DCM2NIIX_FIXTURE.is_file(), f"missing fixture: {DCM2NIIX_FIXTURE}"


def test_dcm2niix_fixture_validates_and_parses():
    d = parse_descriptor(DCM2NIIX_FIXTURE)
    assert d.name == "dcm2niix"
    assert d.slug == "dcm2niix"
    # work_unit=stack (analysis-level=run + dicom formats), per the design.
    assert d.analysis_level == "run"
    assert d.work_unit == "stack"
    # container digest pinned
    assert d.image_digest == (
        "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    )
    # no license / gpu / templateflow
    assert d.x_nils.needs.fs_license is False
    assert d.x_nils.needs.gpu is False
    assert d.x_nils.needs.templateflow is False
    # config-form knobs present
    ids = {i.id for i in d.inputs}
    assert {"input_dir", "output_dir", "filename", "compress"} <= ids
    # real command-line present, so no "nothing to run" warning
    assert d.command_line and "dcm2niix" in d.command_line
    assert not any("nothing to run" in w for w in d.warnings)


def test_parse_descriptor_path_object_and_string_path_agree():
    from_path_obj = parse_descriptor(DCM2NIIX_FIXTURE)
    from_str_path = parse_descriptor(str(DCM2NIIX_FIXTURE))
    assert from_path_obj.name == from_str_path.name == "dcm2niix"
