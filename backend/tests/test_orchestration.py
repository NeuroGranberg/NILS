"""DB-free orchestration-layer tests (frozen design §6, §17.3.1, §17.8).

There is NO running Postgres here. These tests exercise pure logic + in-memory
behavior only:

* ``config`` scratch-path builders + ``PIPELINE_SCRATCH_ROOT`` env override.
* ``needs.apply_needs`` forward-compat (unknown need warns-and-continues; falsey
  skipped; ``gpu`` → ``--nv``; ``work_dir`` provisions a real dir).
* ``StubBridge`` state machine (create_run + results.json, get_state terminal,
  cancel, get_logs cursor).
* ``ingest.plan_ingest`` selects succeeded units + their derivatives, with the
  DB-write gate verified OFF (no DB import/write attempted).
* ``AnalysisPipelineRunner._execute_run`` reaches a launched/terminal state and
  mirrors job status, with materialize (``build_export_config`` /
  ``run_export_job``) and all App-DB helpers monkeypatched in memory.

No live DB / engine is touched.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from analysis_pipeline import config as scratch
from analysis_pipeline.bridge import BridgeRunState, StubBridge
from analysis_pipeline.needs import (
    NEEDS_HANDLERS,
    NeedsContext,
    apply_needs,
)
from analysis_pipeline.ingest import (
    INGEST_DB_WRITE_ENABLED,
    IngestPlan,
    plan_ingest,
)
from analysis_pipeline.descriptor import Descriptor
from analysis_pipeline.models import AnalysisPipelineRunStatus, InputSource
from analysis_pipeline.runner import AnalysisPipelineRunner


# ===========================================================================
# config.py — scratch paths
# ===========================================================================


def test_scratch_root_default(monkeypatch):
    monkeypatch.delenv("PIPELINE_SCRATCH_ROOT", raising=False)
    assert scratch.get_scratch_root() == Path(scratch.DEFAULT_SCRATCH_ROOT)


def test_scratch_root_env_override(monkeypatch):
    monkeypatch.setenv("PIPELINE_SCRATCH_ROOT", "/data/scratch-xyz")
    assert scratch.get_scratch_root() == Path("/data/scratch-xyz")


def test_scratch_root_blank_env_falls_back(monkeypatch):
    monkeypatch.setenv("PIPELINE_SCRATCH_ROOT", "   ")
    assert scratch.get_scratch_root() == Path(scratch.DEFAULT_SCRATCH_ROOT)


def test_run_path_builders_shapes():
    root = Path("/srv/scratch")
    assert scratch.run_scratch_dir(42, root) == root / "run-42"
    assert scratch.run_bids_dir(42, root) == root / "run-42" / "bids"
    assert scratch.run_output_dir(42, root) == root / "run-42" / "output"
    assert scratch.run_work_dir(42, root) == root / "run-42" / "work"


def test_run_path_builders_use_env_root(monkeypatch):
    monkeypatch.setenv("PIPELINE_SCRATCH_ROOT", "/envroot")
    assert scratch.run_bids_dir(3) == Path("/envroot/run-3/bids")
    assert scratch.run_output_dir(3) == Path("/envroot/run-3/output")


# ===========================================================================
# needs.py — apply_needs forward-compat
# ===========================================================================


def test_apply_needs_gpu_appends_nv():
    ctx = apply_needs({"gpu": True})
    assert "--nv" in ctx.flags
    assert ctx.warnings == []


def test_apply_needs_unknown_warns_not_raises():
    ctx = apply_needs({"some_future_need": True})
    assert any("some_future_need" in w for w in ctx.warnings)
    # forward-compatible: never raised.


def test_apply_needs_falsey_skipped():
    ctx = apply_needs({"gpu": False, "fs_license": False})
    assert ctx.flags == []
    # falsey values are skipped silently (no warning, no env intent recorded).
    assert "NILS_NEEDS_FS_LICENSE" not in ctx.env
    assert ctx.warnings == []


def test_apply_needs_work_dir_provisions_real_dir(tmp_path):
    target = tmp_path / "wk"
    ctx = apply_needs({"work_dir": str(target)})
    assert ctx.work_dir == str(target)
    assert target.is_dir()
    assert any(str(target) in b for b in ctx.binds)


def test_apply_needs_fs_license_stub_warns():
    ctx = apply_needs({"fs_license": True})
    assert ctx.env.get("NILS_NEEDS_FS_LICENSE") == "requested"
    assert any("fs_license" in w for w in ctx.warnings)


def test_apply_needs_templateflow_stub_warns():
    ctx = apply_needs({"templateflow": True})
    assert ctx.env.get("NILS_NEEDS_TEMPLATEFLOW") == "requested"
    assert any("templateflow" in w for w in ctx.warnings)


def test_apply_needs_mutates_and_returns_same_ctx():
    ctx = NeedsContext()
    out = apply_needs({"gpu": True}, ctx)
    assert out is ctx


def test_apply_needs_non_dict_warns():
    ctx = apply_needs(["gpu"])  # type: ignore[arg-type]
    assert any("mapping" in w for w in ctx.warnings)


def test_needs_registry_known_keys():
    assert set(NEEDS_HANDLERS) == {"fs_license", "templateflow", "gpu", "work_dir"}


# ===========================================================================
# bridge.py — StubBridge state machine
# ===========================================================================


def _units(*ids):
    return [{"unit_id": i, "work_unit": "subject"} for i in ids]


def test_stub_create_run_returns_id_and_writes_results(tmp_path):
    bridge = StubBridge()
    out = tmp_path / "out"
    rid = bridge.create_run(
        image="img:tag",
        units=_units("sub-01", "sub-02"),
        descriptor={},
        binds=[],
        env={},
        bids_dir=str(tmp_path / "bids"),
        output_dir=str(out),
        work_dir=None,
    )
    assert rid.startswith("stub-")
    results_file = out / "results.json"
    assert results_file.is_file()
    payload = json.loads(results_file.read_text())
    assert payload["schema_version"] == "1"
    assert len(payload["units"]) == 2
    assert all(u["status"] == "succeeded" for u in payload["units"])


def test_stub_immediate_get_state_terminal(tmp_path):
    bridge = StubBridge(immediate=True)
    rid = bridge.create_run(
        image="i", units=_units("sub-01"), descriptor={}, binds=[], env={},
        bids_dir="b", output_dir=str(tmp_path), work_dir=None,
    )
    state = bridge.get_state(rid)
    assert state.state == "completed"
    assert state.units_total == 1
    assert state.units_done == 1
    assert state.units_failed == 0


def test_stub_walking_state_machine(tmp_path):
    bridge = StubBridge(immediate=False)
    rid = bridge.create_run(
        image="i", units=_units("sub-01"), descriptor={}, binds=[], env={},
        bids_dir="b", output_dir=str(tmp_path), work_dir=None,
    )
    # pending -> running -> completed across calls.
    assert bridge.get_state(rid).state == "running"
    assert bridge.get_state(rid).state == "completed"


def test_stub_forced_failure_outcomes(tmp_path):
    bridge = StubBridge(unit_outcomes={"sub-02": "failed"})
    rid = bridge.create_run(
        image="i", units=_units("sub-01", "sub-02"), descriptor={}, binds=[], env={},
        bids_dir="b", output_dir=str(tmp_path), work_dir=None,
    )
    state = bridge.get_state(rid)
    # mixed outcome → still "completed" (partial), with one failed unit.
    assert state.state == "completed"
    assert state.units_done == 1
    assert state.units_failed == 1


def test_stub_all_failed_is_failed_state(tmp_path):
    bridge = StubBridge(unit_outcomes={"sub-01": "failed"})
    rid = bridge.create_run(
        image="i", units=_units("sub-01"), descriptor={}, binds=[], env={},
        bids_dir="b", output_dir=str(tmp_path), work_dir=None,
    )
    assert bridge.get_state(rid).state == "failed"


def test_stub_cancel_flips_state(tmp_path):
    bridge = StubBridge(immediate=False)
    rid = bridge.create_run(
        image="i", units=_units("sub-01"), descriptor={}, binds=[], env={},
        bids_dir="b", output_dir=str(tmp_path), work_dir=None,
    )
    bridge.cancel(rid)
    assert bridge.get_state(rid).state == "canceled"


def test_stub_get_logs_cursor_advances(tmp_path):
    bridge = StubBridge()
    rid = bridge.create_run(
        image="i", units=_units("sub-01"), descriptor={}, binds=[], env={},
        bids_dir="b", output_dir=str(tmp_path), work_dir=None,
    )
    all_logs = bridge.get_logs(rid, since=0)
    assert len(all_logs) > 0
    tail = bridge.get_logs(rid, since=len(all_logs))
    assert tail == []
    # a partial cursor returns the remainder.
    partial = bridge.get_logs(rid, since=1)
    assert partial == all_logs[1:]


def test_stub_unknown_run_id_raises():
    bridge = StubBridge()
    with pytest.raises(KeyError):
        bridge.get_state("stub-nope")


# ===========================================================================
# ingest.py — plan_ingest (pure) + DB-write gate OFF
# ===========================================================================


def _write_results(output_dir: Path, units: list[dict]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "results.json").write_text(
        json.dumps({"schema_version": "1", "units": units})
    )


def test_plan_ingest_selects_succeeded_only(tmp_path):
    _write_results(
        tmp_path,
        [
            {"unit_id": "sub-01", "work_unit": "subject", "status": "succeeded",
             "derivatives": ["sub-01/a.json", "sub-01/b.json"]},
            {"unit_id": "sub-02", "work_unit": "subject", "status": "failed",
             "derivatives": []},
            {"unit_id": "sub-03", "work_unit": "subject", "status": "skipped",
             "derivatives": []},
        ],
    )
    plan = plan_ingest(7, tmp_path)
    assert isinstance(plan, IngestPlan)
    assert [u.unit_id for u in plan.units_to_ingest] == ["sub-01"]
    assert set(plan.skipped) == {"sub-02", "sub-03"}
    assert plan.derivative_count == 2
    assert plan.counts == {"units_to_ingest": 1, "skipped": 2, "derivative_count": 2}


def test_plan_ingest_missing_results_is_empty(tmp_path):
    plan = plan_ingest(9, tmp_path)  # no results.json
    assert plan.units_to_ingest == []
    assert plan.skipped == []
    assert plan.derivative_count == 0


def test_ingest_db_write_gate_is_off():
    # §17.8 — the metadata-DB write adapter is DEFERRED in v1.
    assert INGEST_DB_WRITE_ENABLED is False


def test_run_ingest_job_does_not_touch_db(tmp_path, monkeypatch):
    """The gated-OFF job records a plan + completes-with-warnings, no DB write."""
    import analysis_pipeline.ingest as ingest_mod

    _write_results(
        tmp_path,
        [{"unit_id": "sub-01", "work_unit": "subject", "status": "succeeded",
          "derivatives": ["sub-01/x.json"]}],
    )

    events: list[tuple] = []

    class _FakeJobService:
        def mark_running(self, jid):
            events.append(("running", jid))

        def update_metrics(self, jid, metrics):
            events.append(("metrics", jid, metrics))

        def mark_completed_with_warnings(self, jid, n):
            events.append(("warnings", jid, n))

        def mark_failed(self, jid, err):  # pragma: no cover - must not fire
            events.append(("failed", jid, err))

    fake_jobs = type("M", (), {"job_service": _FakeJobService()})()
    monkeypatch.setitem(
        __import__("sys").modules, "jobs.service", fake_jobs
    )

    # Guard: ensure no metadata-DB session is constructed during ingest.
    import metadata_db.session as md_session

    def _boom(*a, **k):  # pragma: no cover - must not be called
        raise AssertionError("ingest must not open a metadata-DB session in v1")

    monkeypatch.setattr(md_session, "SessionLocal", _boom, raising=False)

    ingest_mod.run_ingest_derivatives_job(99, 7, tmp_path)

    kinds = [e[0] for e in events]
    assert kinds == ["running", "metrics", "warnings"]
    # one succeeded unit → warning count 1
    assert events[-1] == ("warnings", 99, 1)


# ===========================================================================
# runner.py — _execute_run lifecycle with StubBridge + mocked materialize
# ===========================================================================


def _descriptor_dict(work_unit_level="subject", needs=None):
    d = {
        "name": "Demo Pipeline",
        "schema-version": "0.5",
        "command-line": "echo [IN] [OUT]",
        "container-image": {"type": "docker", "image": "ghcr.io/demo@sha256:abcd"},
        "x-nils": {
            "analysis-level": work_unit_level,
            "input": {"formats": ["bids"], "layout": "bids"},
            "needs": needs or {"gpu": True},
        },
    }
    return d


class _RunRow:
    """Minimal in-memory stand-in for an AnalysisPipelineRun row."""

    def __init__(self, run_id, job_id, input_source, stack_ids, external_path):
        self.id = run_id
        self.job_id = job_id
        self.input_source = input_source
        self.stack_ids = stack_ids
        self.external_path = external_path
        self.work_unit = "subject"
        self.status = AnalysisPipelineRunStatus.PENDING.value
        self.provenance = {}
        self.prefect_run_id = None
        self.input_path = None
        self.output_path = None
        self.work_dir = None
        self.last_error = None


def _install_inmemory_runner(monkeypatch, runner, run_row, descriptor, job_events):
    """Replace all App-DB helpers on a runner with in-memory equivalents."""
    snapshot = {
        "run_id": run_row.id,
        "job_id": run_row.job_id,
        "input_source": run_row.input_source,
        "stack_ids": list(run_row.stack_ids or []),
        "external_path": run_row.external_path,
        "work_unit": Descriptor.model_validate(descriptor).work_unit,
        "descriptor": Descriptor.model_validate(descriptor),
    }
    monkeypatch.setattr(runner, "_load_run_snapshot", lambda rid: snapshot)

    def _set_status(rid, status, provenance_extra=None):
        run_row.status = status.value
        if provenance_extra:
            run_row.provenance.update(provenance_extra)
        return run_row.job_id

    monkeypatch.setattr(runner, "_set_status", _set_status)

    def _store_launch(rid, *, prefect_run_id, input_path, output_path, work_dir):
        run_row.prefect_run_id = prefect_run_id
        run_row.input_path = input_path
        run_row.output_path = output_path
        run_row.work_dir = work_dir

    monkeypatch.setattr(runner, "_store_launch", _store_launch)
    monkeypatch.setattr(
        runner, "_store_input_path",
        lambda rid, p: setattr(run_row, "input_path", p),
    )
    monkeypatch.setattr(
        runner, "_record_error",
        lambda rid, e: setattr(run_row, "last_error", e),
    )
    monkeypatch.setattr(runner, "_prefect_run_id", lambda rid: run_row.prefect_run_id)
    # _is_canceled reads run.status from the App-DB; reflect the in-memory row so
    # the lifecycle's cancel re-checks are exercised without a live DB.
    monkeypatch.setattr(
        runner, "_is_canceled",
        lambda rid: run_row.status == AnalysisPipelineRunStatus.CANCELED.value,
    )

    # Minimal job stand-in for the separate materialize child job (distinct from
    # the run's mirror job).
    class _FakeJob:
        def __init__(self, jid):
            self.id = jid
            self.status = "completed"
            self.last_error = None

    # Fake job_service used by _execute_run (imported locally inside the method).
    class _FakeJobService:
        _counter = 5000

        def create_job(self, *, stage, config, name=None):
            type(self)._counter += 1
            job_events.append(("create", stage, type(self)._counter))
            return _FakeJob(type(self)._counter)

        def get_job(self, jid):
            return _FakeJob(jid)

        def mark_running(self, jid):
            job_events.append(("running", jid))

        def mark_completed(self, jid):
            job_events.append(("completed", jid))

        def mark_completed_with_warnings(self, jid, n):
            job_events.append(("warnings", jid, n))

        def mark_failed(self, jid, err):
            job_events.append(("failed", jid, err))

        def cancel_job(self, jid):
            job_events.append(("canceled", jid))

        def update_metrics(self, jid, m):
            job_events.append(("metrics", jid, m))

        def update_progress(self, jid, p):
            job_events.append(("progress", jid, p))

    fake_jobs = type("M", (), {"job_service": _FakeJobService()})()
    monkeypatch.setitem(__import__("sys").modules, "jobs.service", fake_jobs)


def test_execute_run_external_path_reaches_completed(monkeypatch, tmp_path):
    """external_path skips materialization; stub launch → completed; job mirrored."""
    monkeypatch.setenv("PIPELINE_SCRATCH_ROOT", str(tmp_path / "scratch"))

    ext = tmp_path / "external-bids"
    ext.mkdir()
    # one subject so _slice_units yields a real unit.
    (ext / "sub-01").mkdir()

    descriptor = _descriptor_dict()
    run_row = _RunRow(
        run_id=1, job_id=11,
        input_source=InputSource.EXTERNAL_PATH.value,
        stack_ids=None, external_path=str(ext),
    )
    job_events: list[tuple] = []
    runner = AnalysisPipelineRunner(bridge=StubBridge())
    _install_inmemory_runner(monkeypatch, runner, run_row, descriptor, job_events)

    # No export should be called for external_path.
    import api.services.export_runner as er

    def _no_export(*a, **k):  # pragma: no cover - must not fire
        raise AssertionError("external_path must not materialize")

    monkeypatch.setattr(er, "run_export_job", _no_export)

    runner._execute_run(1, raw_cfg=None)

    assert run_row.status == AnalysisPipelineRunStatus.COMPLETED.value
    assert run_row.prefect_run_id and run_row.prefect_run_id.startswith("stub-")
    assert ("completed", 11) in job_events
    assert ("running", 11) in job_events
    # provenance snapshot carries results counts.
    assert run_row.provenance.get("results_counts", {}).get("succeeded", 0) >= 1


def test_execute_run_db_subset_materializes_via_export(monkeypatch, tmp_path):
    """db_subset calls build_export_config + run_export_job (mocked), then launches."""
    monkeypatch.setenv("PIPELINE_SCRATCH_ROOT", str(tmp_path / "scratch"))

    descriptor = _descriptor_dict()
    run_row = _RunRow(
        run_id=2, job_id=22,
        input_source=InputSource.DB_SUBSET.value,
        stack_ids=[101, 102], external_path=None,
    )
    job_events: list[tuple] = []
    runner = AnalysisPipelineRunner(bridge=StubBridge())
    _install_inmemory_runner(monkeypatch, runner, run_row, descriptor, job_events)

    calls = {"build": 0, "run": 0}
    import api.services.export_runner as er

    from bids.exporter import BidsExportConfig

    def _fake_build(cfg, *, cohort_name=None, include_stack_ids=None):
        calls["build"] += 1
        assert include_stack_ids == [101, 102]
        return BidsExportConfig()

    def _fake_run(job_id, raw_root, derivatives_root, config_model, **k):
        calls["run"] += 1
        # the real exporter writes subjects under <root>/bids-nifti/sub-*, NOT
        # directly under <root> — the runner must resolve that nested root.
        (Path(derivatives_root) / "bids-nifti" / "sub-01").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(er, "build_export_config", _fake_build)
    monkeypatch.setattr(er, "run_export_job", _fake_run)

    # raw_root provided explicitly so cohort resolution is bypassed.
    runner._execute_run(2, raw_cfg={"raw_root": str(tmp_path / "dcm-raw")})

    assert calls == {"build": 1, "run": 1}
    assert run_row.status == AnalysisPipelineRunStatus.COMPLETED.value
    # the resolved input path is the REAL nested BIDS root, not the bare bids/ dir.
    assert run_row.input_path and run_row.input_path.endswith("bids-nifti")
    # a SEPARATE 'export' job was created for materialization (not the run mirror).
    assert any(e[0] == "create" and e[1] == "export" for e in job_events)
    assert ("completed", 22) in job_events


def test_execute_run_partial_failure_marks_warnings(monkeypatch, tmp_path):
    """A unit failing in the stub → run completed_with_warnings + job warnings."""
    monkeypatch.setenv("PIPELINE_SCRATCH_ROOT", str(tmp_path / "scratch"))

    ext = tmp_path / "ext"
    ext.mkdir()
    (ext / "sub-01").mkdir()
    (ext / "sub-02").mkdir()

    descriptor = _descriptor_dict()
    run_row = _RunRow(
        run_id=3, job_id=33,
        input_source=InputSource.EXTERNAL_PATH.value,
        stack_ids=None, external_path=str(ext),
    )
    job_events: list[tuple] = []
    # force sub-02 to fail.
    runner = AnalysisPipelineRunner(
        bridge=StubBridge(unit_outcomes={"sub-02": "failed"})
    )
    _install_inmemory_runner(monkeypatch, runner, run_row, descriptor, job_events)

    runner._execute_run(3, raw_cfg=None)

    assert run_row.status == AnalysisPipelineRunStatus.COMPLETED_WITH_WARNINGS.value
    assert any(e[0] == "warnings" for e in job_events)


def test_execute_run_materialize_failure_marks_failed(monkeypatch, tmp_path):
    """An exception during materialize → run failed + job failed."""
    monkeypatch.setenv("PIPELINE_SCRATCH_ROOT", str(tmp_path / "scratch"))

    descriptor = _descriptor_dict()
    run_row = _RunRow(
        run_id=4, job_id=44,
        input_source=InputSource.DB_SUBSET.value,
        stack_ids=[1], external_path=None,
    )
    job_events: list[tuple] = []
    runner = AnalysisPipelineRunner(bridge=StubBridge())
    _install_inmemory_runner(monkeypatch, runner, run_row, descriptor, job_events)

    import api.services.export_runner as er

    def _boom_build(*a, **k):
        raise RuntimeError("export config blew up")

    monkeypatch.setattr(er, "build_export_config", _boom_build)

    runner._execute_run(4, raw_cfg={"raw_root": str(tmp_path / "raw")})

    assert run_row.status == AnalysisPipelineRunStatus.FAILED.value
    assert run_row.last_error and "blew up" in run_row.last_error
    assert any(e[0] == "failed" for e in job_events)


def test_runner_get_state_proxies_bridge(monkeypatch, tmp_path):
    """runner.get_state proxies the bridge once a prefect_run_id exists."""
    bridge = StubBridge()
    rid = bridge.create_run(
        image="i", units=_units("sub-01"), descriptor={}, binds=[], env={},
        bids_dir="b", output_dir=str(tmp_path), work_dir=None,
    )
    runner = AnalysisPipelineRunner(bridge=bridge)
    monkeypatch.setattr(runner, "_prefect_run_id", lambda r: rid)
    state = runner.get_state(123)
    assert isinstance(state, BridgeRunState)
    assert state.state == "completed"


def test_runner_get_state_no_prefect_id_is_pending(monkeypatch):
    runner = AnalysisPipelineRunner(bridge=StubBridge())
    monkeypatch.setattr(runner, "_prefect_run_id", lambda r: None)
    state = runner.get_state(123)
    assert state.state == "pending"
