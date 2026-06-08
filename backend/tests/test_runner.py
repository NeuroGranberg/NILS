"""DB-free tests for the analysis-pipeline runner + the post-review fixes.

The runner shipped without a dedicated unit test, which let three real
materialize/slice bugs through review:
  * reusing the run's mirror job for ``run_export_job`` (drives it terminal),
  * swallowing a failed materialization (launches an empty run),
  * scanning the wrong BIDS root (``run_bids_export`` writes under
    ``<root>/bids-nifti/sub-*``, not ``<root>/sub-*``).

These tests pin the fixes. Everything here is DB-free: the pure helpers need no
DB, and ``_materialize`` is exercised with ``job_service`` / ``run_export_job``
monkeypatched and ``_store_input_path`` stubbed.
"""

import json
import types

import pytest

from analysis_pipeline.runner import AnalysisPipelineRunner, _has_subjects
from analysis_pipeline.descriptor import validate_descriptor
from analysis_pipeline.ingest import plan_ingest
from bids.exporter import BidsExportConfig


def _descriptor(formats):
    """Minimal stand-in exposing only what _resolve_bids_root reads."""
    return types.SimpleNamespace(
        x_nils=types.SimpleNamespace(input=types.SimpleNamespace(formats=formats))
    )


def _mksub(root, *names):
    for n in names:
        (root / n / "anat").mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------
# _has_subjects
# --------------------------------------------------------------------------

def test_has_subjects(tmp_path):
    assert not _has_subjects(tmp_path)               # empty
    assert not _has_subjects(tmp_path / "missing")   # absent
    (tmp_path / "sub-01").mkdir()
    assert _has_subjects(tmp_path)
    f = tmp_path / "file"
    f.write_text("x")
    assert not _has_subjects(f)                       # not a dir


# --------------------------------------------------------------------------
# _resolve_bids_root  (HIGH #3 — the exporter nests subjects under bids-nifti)
# --------------------------------------------------------------------------

def test_resolve_bids_root_prefers_nifti(tmp_path):
    cfg = BidsExportConfig()
    _mksub(tmp_path / "bids-nifti", "sub-01")
    root = AnalysisPipelineRunner._resolve_bids_root(tmp_path, cfg, _descriptor(["nifti"]))
    assert root == tmp_path / "bids-nifti"


def test_resolve_bids_root_dicom_format_picks_dcm(tmp_path):
    cfg = BidsExportConfig()
    _mksub(tmp_path / "bids-dcm", "sub-01")
    root = AnalysisPipelineRunner._resolve_bids_root(tmp_path, cfg, _descriptor(["dicom"]))
    assert root == tmp_path / "bids-dcm"


def test_resolve_bids_root_falls_back_to_root(tmp_path):
    cfg = BidsExportConfig()
    _mksub(tmp_path, "sub-01")  # subjects directly under root (e.g. external tree shape)
    root = AnalysisPipelineRunner._resolve_bids_root(tmp_path, cfg, _descriptor(["nifti"]))
    assert root == tmp_path


# --------------------------------------------------------------------------
# _slice_units  (works once given the REAL root)
# --------------------------------------------------------------------------

def test_slice_units_subject(tmp_path):
    runner = AnalysisPipelineRunner()
    _mksub(tmp_path, "sub-01", "sub-02")
    units = runner._slice_units({"work_unit": "subject"}, tmp_path)
    assert [u["unit_id"] for u in units] == ["sub-01", "sub-02"]


def test_slice_units_group_is_single_dataset(tmp_path):
    runner = AnalysisPipelineRunner()
    units = runner._slice_units({"work_unit": "group"}, tmp_path)
    assert units == [{"unit_id": "dataset", "work_unit": "group"}]


def test_slice_units_session(tmp_path):
    runner = AnalysisPipelineRunner()
    (tmp_path / "sub-01" / "ses-01").mkdir(parents=True)
    (tmp_path / "sub-01" / "ses-02").mkdir(parents=True)
    units = runner._slice_units({"work_unit": "session"}, tmp_path)
    assert [u["unit_id"] for u in units] == ["sub-01_ses-01", "sub-01_ses-02"]


def test_slice_units_stack_fallback_when_empty(tmp_path):
    runner = AnalysisPipelineRunner()
    units = runner._slice_units({"work_unit": "stack", "stack_ids": [7, 8]}, tmp_path)
    assert [u["unit_id"] for u in units] == ["stack-7", "stack-8"]


# --------------------------------------------------------------------------
# _materialize  (HIGH #1 separate job, HIGH #2 abort-on-failure, HIGH #3 root)
# --------------------------------------------------------------------------

class _FakeJob:
    def __init__(self, jid, status="completed", last_error=None):
        self.id = jid
        self.status = status
        self.last_error = last_error


class _FakeJobService:
    def __init__(self, final_status="completed"):
        self.final_status = final_status
        self.created = []
        self._counter = 1000

    def create_job(self, *, stage, config, name=None):
        self._counter += 1
        self.created.append({"id": self._counter, "stage": stage, "config": config})
        return _FakeJob(self._counter)

    def mark_running(self, job_id):
        pass

    def get_job(self, job_id):
        return _FakeJob(job_id, status=self.final_status)


def _snap(job_id=999):
    return {
        "input_source": "db_subset",
        "stack_ids": [1, 2],
        "job_id": job_id,
        "descriptor": _descriptor(["nifti"]),
    }


def test_materialize_uses_separate_job_and_resolves_real_root(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_SCRATCH_ROOT", str(tmp_path))
    fake_js = _FakeJobService(final_status="completed")
    monkeypatch.setattr("jobs.service.job_service", fake_js)

    seen = {}

    def fake_run_export_job(job_id, raw_root, derivatives_root, config_model, **kw):
        seen["job_id"] = job_id
        # the exporter writes subjects under <derivatives_root>/bids-nifti/sub-*
        _mksub(__import__("pathlib").Path(derivatives_root) / "bids-nifti", "sub-01")

    monkeypatch.setattr("api.services.export_runner.run_export_job", fake_run_export_job)

    runner = AnalysisPipelineRunner()
    monkeypatch.setattr(runner, "_store_input_path", lambda *a, **k: None)

    result = runner._materialize(5, _snap(job_id=999), {"raw_root": str(tmp_path / "raw")})

    # HIGH #1: a SEPARATE 'export' job was created and used — NOT the run's job 999.
    assert len(fake_js.created) == 1
    assert fake_js.created[0]["stage"] == "export"
    assert seen["job_id"] == fake_js.created[0]["id"]
    assert seen["job_id"] != 999
    # HIGH #3: the resolved root is the nested bids-nifti dir, not the bids dir.
    assert result.name == "bids-nifti"
    assert _has_subjects(result)


def test_materialize_aborts_when_export_job_failed(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_SCRATCH_ROOT", str(tmp_path))
    fake_js = _FakeJobService(final_status="failed")
    monkeypatch.setattr("jobs.service.job_service", fake_js)
    # run_export_job "fails": marks the job failed (simulated by fake) and writes nothing.
    monkeypatch.setattr(
        "api.services.export_runner.run_export_job",
        lambda *a, **k: None,
    )
    runner = AnalysisPipelineRunner()
    monkeypatch.setattr(runner, "_store_input_path", lambda *a, **k: None)

    # HIGH #2: a failed materialization RAISES (so _execute_run marks the run FAILED)
    # instead of silently launching an empty run.
    with pytest.raises(RuntimeError, match="Materialization failed"):
        runner._materialize(6, _snap(), {"raw_root": str(tmp_path / "raw")})


# --------------------------------------------------------------------------
# LOW fixes: analysis-level warning (#6) + ingest status partition (#7)
# --------------------------------------------------------------------------

def test_unknown_analysis_level_warns():
    warns = validate_descriptor(
        {"name": "x", "command-line": "echo", "x-nils": {"analysis-level": "bogus"}}
    )
    assert any("analysis-level" in w for w in warns)

    ok = validate_descriptor(
        {"name": "x", "command-line": "echo", "x-nils": {"analysis-level": "subject"}}
    )
    assert not any("analysis-level" in w and "Unknown" in w for w in ok)


def test_ingest_partition_by_status(tmp_path):
    payload = {
        "units": [
            {"unit_id": "sub-01", "work_unit": "subject", "status": "succeeded", "derivatives": ["a.nii"]},
            {"unit_id": "sub-02", "work_unit": "subject", "status": "failed"},
            {"unit_id": "sub-03", "work_unit": "subject", "status": "skipped"},
        ]
    }
    (tmp_path / "results.json").write_text(json.dumps(payload))
    plan = plan_ingest(1, tmp_path)
    assert [u.unit_id for u in plan.units_to_ingest] == ["sub-01"]
    assert sorted(plan.skipped) == ["sub-02", "sub-03"]
    assert plan.derivative_count == 1
