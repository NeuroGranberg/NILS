"""LocalBridge — run pipelines on the local machine via direct ``apptainer exec``.

This is the **local / standalone execution path**: no Prefect server, no VM, no
prefect-db (spec §14 "local-cpu-pool + direct apptainer exec", §17.8 "best-effort
plain python"). It implements the same :class:`~analysis_pipeline.bridge.PipelineBridge`
Protocol as :class:`StubBridge`, so the runner uses it with zero changes.

Execution model (v1, deliberately simple): ``create_run`` runs **every work-unit
synchronously** (blocking) via ``apptainer exec``, synthesises a conforming
``results.json`` from the per-unit exit codes, sets a terminal state, and returns
— exactly the shape ``StubBridge(immediate=True)`` uses, so the runner's poll
loop sees a finished run on its first poll. Because the run executes inside
``create_run`` (on the shared job executor), a local run occupies one executor
worker for its duration and cancel-mid-run is not supported locally (the run's
``prefect_run_id`` is only stored after it finishes). These are accepted
local-dev trade-offs; the Prefect path is the scaled, cancellable substitute.

The exec wrapper is owned here (spec §7.5), not by any flow: ``--cleanenv``,
explicit ``--bind`` for the scratch roots, ``--nv`` when the descriptor declares
``x-nils.needs.gpu``, and ``--env K=V`` for injected env. NILS never bakes these
into images or flows.

Requires a container runtime (``apptainer`` or ``singularity``) on ``PATH``;
otherwise constructing a LocalBridge raises :class:`LocalBridgeUnavailable` so the
selector can fall back to the stub.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Any, Callable, Optional

from analysis_pipeline.bridge import BridgeRunState, _TERMINAL_STATES

logger = logging.getLogger(__name__)

# Injectable command runner (subprocess.run by default) so tests need no real
# apptainer. Must accept (argv: list[str]) and return an object with
# ``.returncode``, ``.stdout``, ``.stderr``.
CommandRunner = Callable[[list], Any]


class LocalBridgeUnavailable(RuntimeError):
    """No container runtime (apptainer/singularity) is available on this host."""


_APPTAINER_FAMILY = ("apptainer", "singularity")


def detect_container_runtime() -> Optional[str]:
    """First available runtime on PATH: apptainer → singularity → docker.

    Apptainer/singularity are preferred (the spec's SIF-first substrate); docker
    is the common local-dev fallback (neurocontainers are OCI images docker runs
    natively). Override with ``NILS_PIPELINE_RUNTIME`` if needed.
    """
    override = os.getenv("NILS_PIPELINE_RUNTIME", "").strip().lower()
    if override:
        return override if shutil.which(override) else None
    for runtime in (*_APPTAINER_FAMILY, "docker"):
        if shutil.which(runtime):
            return runtime
    return None


# ---------------------------------------------------------------------------
# Command rendering (§17.6 — Boutiques [KEY] + {key} substitution)
# ---------------------------------------------------------------------------


def _x_nils(descriptor: dict) -> dict:
    return descriptor.get("x-nils") or descriptor.get("x_nils") or {}


def build_substitutions(
    descriptor: dict,
    unit: dict,
    *,
    bids_dir: str,
    output_dir: str,
    work_dir: Optional[str],
) -> dict[str, str]:
    """Map every placeholder a command template may use → its value.

    Covers both Boutiques ``[KEY]`` value-keys (from ``inputs[]``) and the
    convenience ``{key}`` form used in the §8 manifest example, plus the reserved
    BIDS-Apps input/output args and the per-unit label.
    """
    subs: dict[str, str] = {}
    # Boutiques inputs: value-key -> default-value (when present).
    for inp in descriptor.get("inputs", []) or []:
        vk = inp.get("value-key") or inp.get("value_key")
        if not vk:
            continue
        dv = inp.get("default-value", inp.get("default_value"))
        subs[vk] = "" if dv is None else str(dv)

    unit_id = str(unit.get("unit_id") or unit.get("id") or "")
    # BIDS participant/session labels are the entity value WITHOUT the prefix.
    label = unit_id.split("-", 1)[1] if "-" in unit_id else unit_id

    reserved = {
        "[SubjectLabel]": label,
        "[ParticipantLabel]": label,
        "[InputDataset]": bids_dir,
        "[OutputLocation]": output_dir,
        "{bids_dir}": bids_dir,
        "{output_dir}": output_dir,
        "{work_dir}": work_dir or "",
        "{subject}": label,
        "{participant}": label,
        "{unit}": unit_id,
    }
    # Reserved keys win for the input/output paths; input defaults fill the rest.
    for k, v in reserved.items():
        subs[k] = v
    return subs


def substitute(template: str, subs: dict[str, str]) -> str:
    """Replace every placeholder in ``template`` (longest keys first)."""
    out = template
    for key in sorted(subs, key=len, reverse=True):
        out = out.replace(key, subs[key])
    return out


def render_commands(
    descriptor: dict,
    unit: dict,
    *,
    bids_dir: str,
    output_dir: str,
    work_dir: Optional[str],
) -> list[str]:
    """Render the command(s) to run for a unit.

    Prefers ``x-nils.steps`` (a reported, error-isolated list); falls back to the
    single ``command-line``. Returns the substituted command strings.
    """
    steps = _x_nils(descriptor).get("steps") or []
    if steps:
        templates = [s.get("command", "") for s in steps if s.get("command")]
    else:
        cl = descriptor.get("command-line") or descriptor.get("command_line")
        templates = [cl] if cl else []
    subs = build_substitutions(
        descriptor, unit, bids_dir=bids_dir, output_dir=output_dir, work_dir=work_dir
    )
    return [substitute(t, subs) for t in templates]


def _normalize_bind(b: str) -> str:
    """A bare ``PATH`` binds to itself (``PATH:PATH``); ``H:C`` is kept as-is."""
    return b if ":" in b else f"{b}:{b}"


def build_argv(
    runtime: str,
    image: str,
    cmd: str,
    *,
    base_flags: tuple[str, ...],
    gpu: bool,
    binds: list[str],
    env: dict[str, str],
) -> list[str]:
    """Assemble one container-exec argv for ``runtime`` (apptainer-family or docker).

    apptainer/singularity → ``<rt> exec [--cleanenv] [--nv] [--bind H:C] [--env K=V] IMAGE cmd``
    docker                → ``docker run --rm [--gpus all] [-v H:C] [-e K=V] IMAGE cmd``
    """
    nb = [_normalize_bind(b) for b in binds]
    tokens = shlex.split(cmd)

    if runtime in _APPTAINER_FAMILY:
        argv: list[str] = [runtime, "exec", *base_flags]
        if gpu:
            argv.append("--nv")
        for b in nb:
            argv += ["--bind", b]
        for k, v in env.items():
            argv += ["--env", f"{k}={v}"]
        argv.append(image)
        return argv + tokens

    if runtime == "docker":
        argv = ["docker", "run", "--rm"]
        if gpu:
            argv += ["--gpus", "all"]
        for b in nb:
            argv += ["-v", b]
        for k, v in env.items():
            argv += ["-e", f"{k}={v}"]
        argv.append(image)
        return argv + tokens

    raise ValueError(f"Unsupported container runtime: {runtime!r}")


# ---------------------------------------------------------------------------
# LocalBridge
# ---------------------------------------------------------------------------


class _LocalRun:
    __slots__ = (
        "prefect_run_id",
        "units",
        "output_dir",
        "logs",
        "state",
        "units_done",
        "units_failed",
    )

    def __init__(self, prefect_run_id: str, units: list[dict], output_dir: str) -> None:
        self.prefect_run_id = prefect_run_id
        self.units = units
        self.output_dir = output_dir
        self.logs: list[str] = []
        self.state = "pending"
        self.units_done = 0
        self.units_failed = 0


class LocalBridge:
    """:class:`PipelineBridge` that runs units locally via ``apptainer exec``."""

    def __init__(
        self,
        *,
        runtime: Optional[str] = None,
        base_flags: tuple[str, ...] = ("--cleanenv",),
        runner: Optional[CommandRunner] = None,
    ) -> None:
        self.runtime = runtime or detect_container_runtime()
        if self.runtime is None:
            raise LocalBridgeUnavailable(
                "No container runtime found on PATH (need 'apptainer', "
                "'singularity', or 'docker'). Install one, or runs use the stub."
            )
        self.base_flags = tuple(base_flags)
        self._run: CommandRunner = runner or self._default_runner
        self._runs: dict[str, _LocalRun] = {}

    @staticmethod
    def _default_runner(argv: list) -> subprocess.CompletedProcess:
        return subprocess.run(argv, capture_output=True, text=True, check=False)

    # -- PipelineBridge ----------------------------------------------------

    def create_run(
        self,
        *,
        image: str,
        units: list[dict],
        descriptor: dict,
        binds: list[str],
        env: dict[str, str],
        bids_dir: str,
        output_dir: str,
        work_dir: str | None,
    ) -> str:
        prefect_run_id = f"local-{uuid.uuid4().hex[:12]}"
        run = _LocalRun(prefect_run_id, list(units or []), output_dir)
        self._runs[prefect_run_id] = run
        run.logs.append(
            f"[local] {self.runtime} run {prefect_run_id} image={image} "
            f"units={len(run.units)}"
        )
        # Bind the scratch roots so the container can see them (the needs binds —
        # e.g. work_dir:/work — are passed through as-is).
        scratch_binds = [bids_dir, output_dir]
        all_binds = scratch_binds + list(binds or [])
        gpu = bool(_x_nils(descriptor).get("needs", {}).get("gpu"))

        self._execute(
            run,
            image=image,
            descriptor=descriptor,
            binds=all_binds,
            env=dict(env or {}),
            gpu=gpu,
            bids_dir=bids_dir,
            output_dir=output_dir,
            work_dir=work_dir,
        )
        logger.info(
            "LocalBridge.create_run -> %s terminal=%s done=%d failed=%d",
            prefect_run_id, run.state, run.units_done, run.units_failed,
        )
        return prefect_run_id

    def get_state(self, prefect_run_id: str) -> BridgeRunState:
        run = self._require(prefect_run_id)
        return BridgeRunState(
            prefect_run_id=prefect_run_id,
            state=run.state,
            units_total=len(run.units),
            units_done=run.units_done,
            units_failed=run.units_failed,
        )

    def get_logs(self, prefect_run_id: str, *, since: int = 0) -> list[str]:
        run = self._require(prefect_run_id)
        return list(run.logs[max(0, since):])

    def cancel(self, prefect_run_id: str) -> None:
        # Local runs execute synchronously inside create_run, so by the time a
        # cancel is reachable the run is already terminal. Best-effort no-op.
        run = self._require(prefect_run_id)
        if run.state not in _TERMINAL_STATES:
            run.state = "canceled"
            run.logs.append(f"[local] run {prefect_run_id} canceled")

    # -- internals ---------------------------------------------------------

    def _require(self, prefect_run_id: str) -> _LocalRun:
        run = self._runs.get(prefect_run_id)
        if run is None:
            raise KeyError(f"Unknown local run id: {prefect_run_id!r}")
        return run

    def _execute(
        self,
        run: _LocalRun,
        *,
        image: str,
        descriptor: dict,
        binds: list[str],
        env: dict[str, str],
        gpu: bool,
        bids_dir: str,
        output_dir: str,
        work_dir: Optional[str],
    ) -> None:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        unit_results: list[dict] = []
        done = 0
        failed = 0

        for unit in run.units:
            unit_id = str(unit.get("unit_id") or unit.get("id") or "unit")
            work_unit = str(unit.get("work_unit") or "subject")
            commands = render_commands(
                descriptor, unit, bids_dir=bids_dir, output_dir=output_dir, work_dir=work_dir
            )
            if not commands:
                failed += 1
                run.logs.append(f"[local] unit {unit_id}: descriptor declares no command")
                unit_results.append(_unit_result(unit_id, work_unit, "failed", [], "no command-line/steps in descriptor"))
                continue

            error: Optional[str] = None
            for cmd in commands:
                argv = build_argv(
                    self.runtime, image, cmd,
                    base_flags=self.base_flags, gpu=gpu, binds=binds, env=env,
                )
                run.logs.append(f"[local] unit {unit_id}: $ {' '.join(argv)}")
                try:
                    proc = self._run(argv)
                except FileNotFoundError as exc:  # runtime vanished mid-run
                    error = f"container runtime not executable: {exc}"
                    run.logs.append(f"[local] unit {unit_id}: {error}")
                    break
                if getattr(proc, "stdout", None):
                    run.logs.append(proc.stdout.rstrip())
                rc = getattr(proc, "returncode", 1)
                if rc != 0:
                    error = (getattr(proc, "stderr", "") or "").rstrip()[-2000:] or f"exit {rc}"
                    run.logs.append(f"[local] unit {unit_id}: FAILED rc={rc}")
                    break

            if error is None:
                done += 1
                derivatives = _discover_derivatives(descriptor, unit, output_dir, bids_dir, work_dir)
                unit_results.append(_unit_result(unit_id, work_unit, "succeeded", derivatives, None))
                run.logs.append(f"[local] unit {unit_id}: OK ({len(derivatives)} derivative(s))")
            else:
                failed += 1
                unit_results.append(_unit_result(unit_id, work_unit, "failed", [], error))

        run.units_done = done
        run.units_failed = failed
        run.state = "failed" if (failed and done == 0 and run.units) else "completed"
        _write_results_json(output_dir, unit_results, run.logs)
        run.logs.append(
            f"[local] run {run.prefect_run_id} terminal={run.state} done={done} failed={failed}"
        )


def _unit_result(unit_id: str, work_unit: str, status: str, derivatives: list[str], error: Optional[str]) -> dict:
    return {
        "unit_id": unit_id,
        "work_unit": work_unit,
        "status": status,
        "derivatives": derivatives,
        "metrics": {},
        "error": error,
    }


def _discover_derivatives(
    descriptor: dict, unit: dict, output_dir: str, bids_dir: str, work_dir: Optional[str]
) -> list[str]:
    """Best-effort: descriptor ``output-files`` path-templates that exist.

    Renders each declared output path-template for the unit and keeps the ones
    present under ``output_dir`` (returned as paths relative to output_dir when
    possible). No declared outputs → empty list (the tool may still have written
    files; v1 does not glob blindly).
    """
    out_root = Path(output_dir)
    subs = build_substitutions(descriptor, unit, bids_dir=bids_dir, output_dir=output_dir, work_dir=work_dir)
    found: list[str] = []
    for of in descriptor.get("output-files", []) or descriptor.get("output_files", []) or []:
        tmpl = of.get("path-template") or of.get("path_template")
        if not tmpl:
            continue
        rel = substitute(tmpl, subs)
        candidate = out_root / rel
        if candidate.exists():
            found.append(rel)
    return found


def _write_results_json(output_dir: str, unit_results: list[dict], logs: list[str]) -> None:
    try:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        payload = {"schema_version": "1", "units": unit_results}
        (out / "results.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except OSError as exc:  # pragma: no cover - fs failure is environmental
        logs.append(f"[local] failed to write results.json: {exc}")
        logger.warning("LocalBridge could not write results.json: %s", exc)


__all__ = [
    "LocalBridge",
    "LocalBridgeUnavailable",
    "detect_container_runtime",
    "render_commands",
    "build_substitutions",
    "substitute",
    "build_argv",
]
