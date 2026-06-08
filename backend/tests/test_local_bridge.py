"""DB-free tests for LocalBridge (direct apptainer exec) + bridge selection.

No real apptainer is needed: the container runner is injected, so these assert
the argv construction, [KEY]/{key} command rendering, per-unit success/failure
→ results.json synthesis, and the local-first select_bridge() fallback logic.
"""

import json
import types

import pytest

from analysis_pipeline.bridge import StubBridge, select_bridge
from analysis_pipeline import local_bridge as lb
from analysis_pipeline.local_bridge import (
    LocalBridge,
    LocalBridgeUnavailable,
    build_argv,
    build_substitutions,
    render_commands,
    substitute,
)


def _proc(returncode=0, stdout="", stderr=""):
    return types.SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)


def _recording_runner(outcomes=None):
    """Runner that records argv and returns a configurable CompletedProcess.

    outcomes: optional list of returncodes (one per call); default all 0.
    """
    calls = []
    seq = list(outcomes or [])

    def run(argv):
        calls.append(argv)
        rc = seq.pop(0) if seq else 0
        return _proc(returncode=rc, stdout="ok", stderr="boom" if rc else "")

    run.calls = calls
    return run


DCM2NIIX = {
    "name": "dcm2niix",
    "command-line": "dcm2niix -o [OutputLocation] [InputDataset]",
    "inputs": [],
    "x-nils": {"analysis-level": "stack", "needs": {}},
}

MRIQC = {
    "name": "mriqc",
    "x-nils": {
        "analysis-level": "subject",
        "needs": {"gpu": False, "templateflow": True},
        "steps": [
            {"name": "mriqc", "command": "mriqc {bids_dir} {output_dir} participant --participant-label {subject}"}
        ],
    },
}


# ---------------------------------------------------------------------------
# rendering
# ---------------------------------------------------------------------------

def test_substitute_longest_key_first():
    subs = {"[Sub]": "X", "[SubLabel]": "Y"}
    # the longer key must win where they overlap
    assert substitute("[SubLabel]", subs) == "Y"


def test_build_substitutions_strips_bids_prefix_and_maps_paths():
    subs = build_substitutions(MRIQC, {"unit_id": "sub-01"}, bids_dir="/bids", output_dir="/out", work_dir="/work")
    assert subs["{subject}"] == "01"          # label without sub-
    assert subs["[ParticipantLabel]"] == "01"
    assert subs["{bids_dir}"] == "/bids"
    assert subs["{output_dir}"] == "/out"


def test_render_mriqc_step_command():
    cmds = render_commands(MRIQC, {"unit_id": "sub-07"}, bids_dir="/bids", output_dir="/out", work_dir="/work")
    assert cmds == ["mriqc /bids /out participant --participant-label 07"]


def test_render_dcm2niix_command_line_fallback():
    cmds = render_commands(DCM2NIIX, {"unit_id": "stack-3"}, bids_dir="/bids", output_dir="/out", work_dir=None)
    assert cmds == ["dcm2niix -o /out /bids"]


def test_build_argv_apptainer():
    argv = build_argv(
        "apptainer", "img@sha256:x", "tool -i /bids -o /out",
        base_flags=("--cleanenv",), gpu=True, binds=["/bids", "/out"], env={"FS_LICENSE": "/fs"},
    )
    assert argv[:3] == ["apptainer", "exec", "--cleanenv"]
    assert "--nv" in argv
    assert argv.count("--bind") == 2
    assert "--env" in argv and "FS_LICENSE=/fs" in argv
    i = argv.index("img@sha256:x")
    assert argv[i + 1:] == ["tool", "-i", "/bids", "-o", "/out"]


def test_build_argv_docker():
    argv = build_argv(
        "docker", "ghcr.io/x/mriqc", "mriqc /bids /out participant",
        base_flags=("--cleanenv",), gpu=True, binds=["/bids", "host:/c"], env={"K": "V"},
    )
    assert argv[:3] == ["docker", "run", "--rm"]
    assert "--gpus" in argv and "all" in argv
    # bare bind normalizes to PATH:PATH; H:C kept; both via -v
    assert argv.count("-v") == 2
    assert "/bids:/bids" in argv and "host:/c" in argv
    assert "-e" in argv and "K=V" in argv
    i = argv.index("ghcr.io/x/mriqc")
    assert argv[i + 1:] == ["mriqc", "/bids", "/out", "participant"]


# ---------------------------------------------------------------------------
# LocalBridge lifecycle
# ---------------------------------------------------------------------------

def test_unavailable_when_no_runtime(monkeypatch):
    monkeypatch.setattr(lb, "detect_container_runtime", lambda: None)
    with pytest.raises(LocalBridgeUnavailable):
        LocalBridge()


def test_create_run_all_succeed_writes_results(tmp_path):
    runner = _recording_runner()
    bridge = LocalBridge(runtime="apptainer", runner=runner)
    out = tmp_path / "out"
    rid = bridge.create_run(
        image="img@sha256:x",
        units=[{"unit_id": "stack-1", "work_unit": "stack"}, {"unit_id": "stack-2", "work_unit": "stack"}],
        descriptor=DCM2NIIX,
        binds=[], env={}, bids_dir=str(tmp_path / "bids"), output_dir=str(out), work_dir=None,
    )
    st = bridge.get_state(rid)
    assert st.state == "completed"
    assert st.units_total == 2 and st.units_done == 2 and st.units_failed == 0
    assert len(runner.calls) == 2
    # the argv is a real apptainer exec line ending in the dcm2niix tokens
    assert runner.calls[0][:3] == ["apptainer", "exec", "--cleanenv"]
    assert runner.calls[0][-4:] == ["dcm2niix", "-o", str(out), str(tmp_path / "bids")]
    # results.json synthesised
    payload = json.loads((out / "results.json").read_text())
    assert [u["status"] for u in payload["units"]] == ["succeeded", "succeeded"]
    assert bridge.get_logs(rid)  # non-empty logs


def test_create_run_partial_failure(tmp_path):
    runner = _recording_runner(outcomes=[0, 1])  # unit 2 fails
    bridge = LocalBridge(runtime="apptainer", runner=runner)
    out = tmp_path / "out"
    rid = bridge.create_run(
        image="img", units=[{"unit_id": "sub-01", "work_unit": "subject"}, {"unit_id": "sub-02", "work_unit": "subject"}],
        descriptor=MRIQC, binds=[], env={}, bids_dir=str(tmp_path / "bids"), output_dir=str(out), work_dir=str(tmp_path / "work"),
    )
    st = bridge.get_state(rid)
    assert st.state == "completed"  # partial → not a total failure
    assert st.units_done == 1 and st.units_failed == 1
    payload = json.loads((out / "results.json").read_text())
    statuses = {u["unit_id"]: u["status"] for u in payload["units"]}
    assert statuses == {"sub-01": "succeeded", "sub-02": "failed"}


def test_create_run_total_failure_is_failed_state(tmp_path):
    runner = _recording_runner(outcomes=[1])
    bridge = LocalBridge(runtime="apptainer", runner=runner)
    rid = bridge.create_run(
        image="img", units=[{"unit_id": "stack-1", "work_unit": "stack"}],
        descriptor=DCM2NIIX, binds=[], env={}, bids_dir=str(tmp_path / "bids"), output_dir=str(tmp_path / "out"), work_dir=None,
    )
    assert bridge.get_state(rid).state == "failed"


def test_gpu_need_adds_nv(tmp_path):
    runner = _recording_runner()
    bridge = LocalBridge(runtime="apptainer", runner=runner)
    desc = {**DCM2NIIX, "x-nils": {"analysis-level": "stack", "needs": {"gpu": True}}}
    bridge.create_run(
        image="img", units=[{"unit_id": "stack-1", "work_unit": "stack"}],
        descriptor=desc, binds=[], env={}, bids_dir=str(tmp_path / "b"), output_dir=str(tmp_path / "o"), work_dir=None,
    )
    assert "--nv" in runner.calls[0]


def test_no_command_marks_unit_failed(tmp_path):
    runner = _recording_runner()
    bridge = LocalBridge(runtime="apptainer", runner=runner)
    out = tmp_path / "out"
    rid = bridge.create_run(
        image="img", units=[{"unit_id": "sub-01", "work_unit": "subject"}],
        descriptor={"name": "empty", "x-nils": {}}, binds=[], env={},
        bids_dir=str(tmp_path / "b"), output_dir=str(out), work_dir=None,
    )
    assert bridge.get_state(rid).state == "failed"
    assert len(runner.calls) == 0  # nothing executed
    payload = json.loads((out / "results.json").read_text())
    assert payload["units"][0]["status"] == "failed"


def test_derivatives_discovered_from_output_files(tmp_path):
    out = tmp_path / "out"
    (out / "sub-01" / "stats").mkdir(parents=True)
    (out / "sub-01" / "stats" / "aseg.stats").write_text("x")
    desc = {
        "name": "fs", "command-line": "recon-all -s [SubjectLabel]",
        "output-files": [{"id": "aseg", "name": "seg", "path-template": "sub-[SubjectLabel]/stats/aseg.stats"}],
        "x-nils": {"analysis-level": "subject", "needs": {}},
    }
    runner = _recording_runner()
    bridge = LocalBridge(runtime="apptainer", runner=runner)
    rid = bridge.create_run(
        image="img", units=[{"unit_id": "sub-01", "work_unit": "subject"}],
        descriptor=desc, binds=[], env={}, bids_dir=str(tmp_path / "b"), output_dir=str(out), work_dir=None,
    )
    payload = json.loads((out / "results.json").read_text())
    assert payload["units"][0]["derivatives"] == ["sub-01/stats/aseg.stats"]


# ---------------------------------------------------------------------------
# select_bridge (local-first fallback)
# ---------------------------------------------------------------------------

def test_select_bridge_forced_stub(monkeypatch):
    monkeypatch.setenv("NILS_PIPELINE_BRIDGE", "stub")
    assert isinstance(select_bridge(), StubBridge)


def test_select_bridge_local_when_runtime_present(monkeypatch):
    monkeypatch.delenv("NILS_PIPELINE_BRIDGE", raising=False)
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    monkeypatch.setattr(lb, "detect_container_runtime", lambda: "apptainer")
    bridge = select_bridge()
    assert isinstance(bridge, LocalBridge)
    assert bridge.runtime == "apptainer"


def test_select_bridge_falls_back_to_stub_without_runtime(monkeypatch):
    monkeypatch.delenv("NILS_PIPELINE_BRIDGE", raising=False)
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    monkeypatch.setattr(lb, "detect_container_runtime", lambda: None)
    assert isinstance(select_bridge(), StubBridge)


def test_select_bridge_prefect_env_warns_and_uses_local(monkeypatch):
    monkeypatch.delenv("NILS_PIPELINE_BRIDGE", raising=False)
    monkeypatch.setenv("PREFECT_API_URL", "http://prefect:4200/api")
    monkeypatch.setattr(lb, "detect_container_runtime", lambda: "singularity")
    bridge = select_bridge()
    assert isinstance(bridge, LocalBridge)
    assert bridge.runtime == "singularity"


# ---------------------------------------------------------------------------
# REAL container execution (opportunistic — runs an actual hello-world when a
# runtime + tiny image are usable; skips cleanly otherwise).
# ---------------------------------------------------------------------------

def test_real_runtime_hello_world(tmp_path):
    import subprocess
    from analysis_pipeline.local_bridge import detect_container_runtime

    rt = detect_container_runtime()
    if rt is None:
        pytest.skip("no container runtime (apptainer/singularity/docker) on PATH")
    image = "busybox" if rt == "docker" else "docker://busybox"
    probe = ["docker", "run", "--rm", "busybox", "true"] if rt == "docker" else [rt, "exec", image, "true"]
    try:
        if subprocess.run(probe, capture_output=True, timeout=180).returncode != 0:
            pytest.skip(f"{rt} present but cannot run {image} (no image/network)")
    except Exception as exc:  # noqa: BLE001 - environmental
        pytest.skip(f"{rt} probe failed: {exc}")

    out = tmp_path / "out"
    descriptor = {
        "name": "hello-world",
        "command-line": "sh -c \"echo 'hi [SubjectLabel]' > [OutputLocation]/[SubjectLabel].txt\"",
        "output-files": [{"id": "g", "name": "greeting", "path-template": "[SubjectLabel].txt"}],
        "x-nils": {"analysis-level": "subject", "needs": {}},
    }
    bridge = LocalBridge(runtime=rt)  # REAL subprocess runner — actually execs the container
    rid = bridge.create_run(
        image=image,
        units=[{"unit_id": "sub-01", "work_unit": "subject"}],
        descriptor=descriptor, binds=[], env={},
        bids_dir=str(tmp_path / "bids"), output_dir=str(out), work_dir=None,
    )
    st = bridge.get_state(rid)
    assert st.state == "completed", "\n".join(bridge.get_logs(rid))
    assert st.units_done == 1 and st.units_failed == 0
    payload = json.loads((out / "results.json").read_text())
    assert payload["units"][0]["status"] == "succeeded"
    assert payload["units"][0]["derivatives"] == ["01.txt"]
    assert (out / "01.txt").read_text().strip() == "hi 01"
