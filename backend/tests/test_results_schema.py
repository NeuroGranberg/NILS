"""DB-free unit tests for the results.json schema + parser (§3 / §8).

Covers: valid results, missing file → empty RunResults, partial counts,
malformed JSON → ResultsError, bare-list tolerance, unknown-key carry, and the
real dcm2niix.results.json fixture.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from analysis_pipeline.results import (
    ResultsError,
    RunResults,
    UnitResult,
    UnitStatus,
    parse_results_json,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "analysis_pipeline"
RESULTS_FIXTURE = FIXTURES / "dcm2niix.results.json"


def _write(tmp_path: Path, payload, name: str = "results.json") -> Path:
    p = tmp_path / name
    if isinstance(payload, str):
        p.write_text(payload, encoding="utf-8")
    else:
        p.write_text(json.dumps(payload), encoding="utf-8")
    return p


# --- valid parsing ---------------------------------------------------------

def test_parse_valid_results_file(tmp_path):
    _write(
        tmp_path,
        {
            "schema_version": "1",
            "units": [
                {
                    "unit_id": "sub-001",
                    "work_unit": "subject",
                    "status": "succeeded",
                    "derivatives": ["sub-001/anat/x.nii.gz"],
                    "metrics": {"snr": 12.3},
                }
            ],
        },
    )
    res = parse_results_json(tmp_path)
    assert isinstance(res, RunResults)
    assert len(res.units) == 1
    u = res.units[0]
    assert u.unit_id == "sub-001"
    assert u.status == UnitStatus.SUCCEEDED
    assert u.derivatives == ["sub-001/anat/x.nii.gz"]
    assert u.metrics["snr"] == 12.3


def test_parse_results_from_file_path_directly(tmp_path):
    p = _write(tmp_path, {"units": [{"unit_id": "a", "work_unit": "stack", "status": "succeeded"}]})
    res = parse_results_json(p)
    assert len(res.units) == 1


def test_parse_results_from_inline_string():
    text = json.dumps({"units": [{"unit_id": "a", "work_unit": "stack", "status": "skipped"}]})
    res = parse_results_json(text)
    assert res.units[0].status == UnitStatus.SKIPPED


# --- missing file → empty --------------------------------------------------

def test_missing_results_file_returns_empty(tmp_path):
    res = parse_results_json(tmp_path)  # empty dir, no results.json
    assert isinstance(res, RunResults)
    assert res.units == []
    assert res.counts == {"succeeded": 0, "failed": 0, "skipped": 0, "total": 0}


def test_nonexistent_path_returns_empty(tmp_path):
    res = parse_results_json(tmp_path / "does-not-exist")
    assert res.units == []


def test_shallow_scan_finds_nested_results(tmp_path):
    sub = tmp_path / "run-output"
    sub.mkdir()
    _write(sub, {"units": [{"unit_id": "z", "work_unit": "group", "status": "succeeded"}]})
    res = parse_results_json(tmp_path)
    assert len(res.units) == 1
    assert res.units[0].unit_id == "z"


# --- partial counts --------------------------------------------------------

def test_partial_counts_succeeded_failed_skipped(tmp_path):
    units = (
        [{"unit_id": f"ok-{i}", "work_unit": "stack", "status": "succeeded"} for i in range(47)]
        + [{"unit_id": f"bad-{i}", "work_unit": "stack", "status": "failed", "error": "boom"} for i in range(2)]
        + [{"unit_id": "skip-0", "work_unit": "stack", "status": "skipped"}]
    )
    _write(tmp_path, {"units": units})
    res = parse_results_json(tmp_path)
    assert res.counts == {"succeeded": 47, "failed": 2, "skipped": 1, "total": 50}
    assert len(res.succeeded) == 47
    assert len(res.failed) == 2
    assert res.failed[0].error == "boom"


# --- malformed / tolerant cases -------------------------------------------

def test_malformed_json_raises_results_error(tmp_path):
    p = tmp_path / "results.json"
    p.write_text("{ this is : not valid json ][", encoding="utf-8")
    with pytest.raises(ResultsError):
        parse_results_json(tmp_path)


def test_structurally_invalid_payload_raises(tmp_path):
    # A bare scalar is neither an object nor a list.
    p = tmp_path / "results.json"
    p.write_text("42", encoding="utf-8")
    with pytest.raises(ResultsError):
        parse_results_json(tmp_path)


def test_missing_required_unit_field_raises(tmp_path):
    # unit missing required 'status' → schema validation error → ResultsError
    _write(tmp_path, {"units": [{"unit_id": "a", "work_unit": "stack"}]})
    with pytest.raises(ResultsError):
        parse_results_json(tmp_path)


def test_bare_list_is_tolerated(tmp_path):
    # results.json that is a bare list of units gets wrapped.
    _write(
        tmp_path,
        [
            {"unit_id": "a", "work_unit": "stack", "status": "succeeded"},
            {"unit_id": "b", "work_unit": "stack", "status": "failed"},
        ],
    )
    res = parse_results_json(tmp_path)
    assert res.counts["total"] == 2
    assert res.counts["succeeded"] == 1


def test_unknown_extra_keys_are_carried(tmp_path):
    _write(
        tmp_path,
        {
            "schema_version": "1",
            "experimental_top": {"k": 1},
            "units": [
                {
                    "unit_id": "a",
                    "work_unit": "stack",
                    "status": "succeeded",
                    "future_field": "carried",
                }
            ],
        },
    )
    res = parse_results_json(tmp_path)
    dumped = res.model_dump()
    assert dumped.get("experimental_top") == {"k": 1}
    assert res.units[0].model_dump().get("future_field") == "carried"


def test_empty_results_object_is_valid(tmp_path):
    _write(tmp_path, {})
    res = parse_results_json(tmp_path)
    assert res.units == []
    assert res.schema_version == "1"


# --- model directly --------------------------------------------------------

def test_unit_result_model_directly():
    u = UnitResult(unit_id="sub-9", work_unit="subject", status="succeeded")
    assert u.status == UnitStatus.SUCCEEDED
    assert u.derivatives == []
    assert u.metrics == {}


# --- the real fixture ------------------------------------------------------

def test_dcm2niix_results_fixture_parses():
    assert RESULTS_FIXTURE.is_file(), f"missing fixture: {RESULTS_FIXTURE}"
    res = parse_results_json(RESULTS_FIXTURE)
    assert res.counts == {"succeeded": 2, "failed": 1, "skipped": 0, "total": 3}
    ok = res.succeeded
    assert {u.unit_id for u in ok} == {"stack-1001", "stack-1002"}
    assert all(u.work_unit == "stack" for u in res.units)
    assert res.failed[0].error and "no valid DICOM" in res.failed[0].error
    # derivative paths preserved on succeeded units
    assert any(d.endswith(".nii.gz") for d in ok[0].derivatives)
