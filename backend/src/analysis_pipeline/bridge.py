"""Execution-substrate seam: :class:`PipelineBridge` + :class:`StubBridge` (§6.1).

The bridge is the ONE swappable boundary between the runner and the real
orchestration substrate (Prefect + apptainer). The runner only ever talks to a
``PipelineBridge``; it never imports Prefect or shells out to apptainer. That
keeps the runner unit-testable now and lets the real ``PrefectBridge`` drop in
later as a same-Protocol implementation with zero runner changes.

Scope boundary (binding): there is NO real Prefect server/worker, NO real
``apptainer exec``, NO prefect-db in this slice. :class:`StubBridge` is an
in-memory, deterministic state machine that:

* records the ``create_run`` request,
* allocates a synthetic ``prefect_run_id`` (``"stub-<uuid>"``),
* writes a conforming ``results.json`` into ``output_dir`` (so the runner's
  success-side mirroring + ``ingest`` have real input to parse),
* advances ``get_state`` pending → running → completed across calls,
* serves buffered synthetic ``get_logs`` lines by a ``since`` cursor,
* flips to ``canceled`` on ``cancel``.

Per-unit outcomes are configurable on the stub (``unit_outcomes``) so a test can
force a partial/failed run without any infrastructure.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Transport DTOs
# ---------------------------------------------------------------------------


class BridgeRunState(BaseModel):
    """A point-in-time view of an orchestrator run (per-unit counts)."""

    model_config = ConfigDict(extra="allow")

    prefect_run_id: str
    state: str  # pending|running|completed|failed|crashed|canceled
    units_total: int = 0
    units_done: int = 0
    units_failed: int = 0


class BridgeRunResult(BaseModel):
    """Terminal outcome of an orchestrator run."""

    model_config = ConfigDict(extra="allow")

    prefect_run_id: str
    final_state: str
    output_dir: str


_TERMINAL_STATES = frozenset({"completed", "failed", "crashed", "canceled"})


@runtime_checkable
class PipelineBridge(Protocol):
    """The execution-substrate seam.

    A real implementation submits a Prefect deployment that runs the units
    through ``apptainer exec``; the stub fakes it deterministically. ``create_run``
    returns the orchestrator run id; the rest key off that id.
    """

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
        """Submit a run; return the orchestrator run id (``prefect_run_id``)."""
        ...

    def get_state(self, prefect_run_id: str) -> BridgeRunState:
        """Current state + per-unit counts for a run."""
        ...

    def get_logs(self, prefect_run_id: str, *, since: int = 0) -> list[str]:
        """Log lines from cursor ``since`` (line index) to the end."""
        ...

    def cancel(self, prefect_run_id: str) -> None:
        """Request cancellation of a run."""
        ...


# ---------------------------------------------------------------------------
# StubBridge
# ---------------------------------------------------------------------------


class _StubRun:
    """In-memory record for a single stubbed run."""

    __slots__ = (
        "prefect_run_id",
        "units",
        "output_dir",
        "logs",
        "state",
        "units_done",
        "units_failed",
        "_poll_count",
    )

    def __init__(self, prefect_run_id: str, units: list[dict], output_dir: str) -> None:
        self.prefect_run_id = prefect_run_id
        self.units = units
        self.output_dir = output_dir
        self.logs: list[str] = []
        self.state = "pending"
        self.units_done = 0
        self.units_failed = 0
        self._poll_count = 0


class StubBridge:
    """Deterministic in-memory :class:`PipelineBridge` (NO real Prefect/apptainer).

    Args:
        immediate: when True (default) ``create_run`` completes the run in one
            shot (every unit succeeds unless overridden) and writes
            ``results.json`` immediately, so a synchronous runner sees a terminal
            state on its first poll. When False, ``get_state`` walks
            pending → running → completed across successive calls (to exercise a
            poller).
        unit_outcomes: optional ``{unit_id: "succeeded"|"failed"|"skipped"}`` map
            to force specific per-unit outcomes (drives partial/failed tests).
    """

    def __init__(
        self,
        *,
        immediate: bool = True,
        unit_outcomes: dict[str, str] | None = None,
    ) -> None:
        self.immediate = immediate
        self.unit_outcomes = dict(unit_outcomes or {})
        self._runs: dict[str, _StubRun] = {}
        # Captured create_run kwargs for test assertions (last call + per id).
        self.created: dict[str, dict[str, Any]] = {}

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
        prefect_run_id = f"stub-{uuid.uuid4().hex[:12]}"
        run = _StubRun(prefect_run_id, list(units or []), output_dir)
        run.logs.append(
            f"[stub] created run {prefect_run_id} image={image} "
            f"units={len(run.units)} binds={len(binds)} flags_env={len(env)}"
        )
        self._runs[prefect_run_id] = run
        self.created[prefect_run_id] = {
            "image": image,
            "units": list(units or []),
            "descriptor": descriptor,
            "binds": list(binds),
            "env": dict(env),
            "bids_dir": bids_dir,
            "output_dir": output_dir,
            "work_dir": work_dir,
        }
        if self.immediate:
            self._complete(run)
        else:
            run.state = "pending"
        logger.info(
            "StubBridge.create_run -> %s (%d units, immediate=%s)",
            prefect_run_id,
            len(run.units),
            self.immediate,
        )
        return prefect_run_id

    def get_state(self, prefect_run_id: str) -> BridgeRunState:
        run = self._require(prefect_run_id)
        if not self.immediate and run.state not in _TERMINAL_STATES:
            # Walk pending -> running -> completed across calls.
            run._poll_count += 1
            if run._poll_count == 1:
                run.state = "running"
                run.logs.append(f"[stub] run {prefect_run_id} now running")
            else:
                self._complete(run)
        return BridgeRunState(
            prefect_run_id=prefect_run_id,
            state=run.state,
            units_total=len(run.units),
            units_done=run.units_done,
            units_failed=run.units_failed,
        )

    def get_logs(self, prefect_run_id: str, *, since: int = 0) -> list[str]:
        run = self._require(prefect_run_id)
        if since < 0:
            since = 0
        return list(run.logs[since:])

    def cancel(self, prefect_run_id: str) -> None:
        run = self._require(prefect_run_id)
        if run.state in _TERMINAL_STATES:
            return
        run.state = "canceled"
        run.logs.append(f"[stub] run {prefect_run_id} canceled")

    # -- internals ---------------------------------------------------------

    def _require(self, prefect_run_id: str) -> _StubRun:
        run = self._runs.get(prefect_run_id)
        if run is None:
            raise KeyError(f"Unknown stub run id: {prefect_run_id!r}")
        return run

    def _outcome_for(self, unit: dict) -> str:
        unit_id = str(unit.get("unit_id") or unit.get("id") or "")
        return self.unit_outcomes.get(unit_id, "succeeded")

    def _complete(self, run: _StubRun) -> None:
        """Resolve every unit, write results.json, set terminal state."""
        unit_results: list[dict] = []
        done = 0
        failed = 0
        for unit in run.units:
            status = self._outcome_for(unit)
            unit_id = str(unit.get("unit_id") or unit.get("id") or "unit")
            work_unit = str(unit.get("work_unit") or "subject")
            if status == "failed":
                failed += 1
                unit_results.append(
                    {
                        "unit_id": unit_id,
                        "work_unit": work_unit,
                        "status": "failed",
                        "derivatives": [],
                        "metrics": {},
                        "error": "stub-forced failure",
                    }
                )
            elif status == "skipped":
                unit_results.append(
                    {
                        "unit_id": unit_id,
                        "work_unit": work_unit,
                        "status": "skipped",
                        "derivatives": [],
                        "metrics": {},
                        "error": None,
                    }
                )
            else:
                done += 1
                unit_results.append(
                    {
                        "unit_id": unit_id,
                        "work_unit": work_unit,
                        "status": "succeeded",
                        # A plausible derivative path under output_dir.
                        "derivatives": [f"{unit_id}/{unit_id}_stub.json"],
                        "metrics": {"stub": True},
                        "error": None,
                    }
                )
            run.logs.append(f"[stub] unit {unit_id} -> {status}")

        run.units_done = done
        run.units_failed = failed
        run.state = "failed" if (failed and done == 0 and run.units) else "completed"

        self._write_results(run, unit_results)
        run.logs.append(
            f"[stub] run {run.prefect_run_id} terminal={run.state} "
            f"done={done} failed={failed}"
        )

    def _write_results(self, run: _StubRun, unit_results: list[dict]) -> None:
        try:
            out = Path(run.output_dir)
            out.mkdir(parents=True, exist_ok=True)
            payload = {"schema_version": "1", "units": unit_results}
            (out / "results.json").write_text(
                json.dumps(payload, indent=2), encoding="utf-8"
            )
        except OSError as exc:  # pragma: no cover - fs failure is environmental
            run.logs.append(f"[stub] failed to write results.json: {exc}")
            logger.warning("StubBridge could not write results.json: %s", exc)


# ---------------------------------------------------------------------------
# Bridge selection (local-first; Prefect opt-in; stub fallback)
# ---------------------------------------------------------------------------


def select_bridge() -> PipelineBridge:
    """Pick the execution substrate. **Local-first, no VM required.**

    Order of precedence:

    * ``NILS_PIPELINE_BRIDGE=stub|local`` forces that bridge (tests / overrides).
    * ``PREFECT_API_URL`` set → the Prefect path (not implemented yet — warns and
      falls through to local; it becomes the scaled, cancellable substitute).
    * else → :class:`LocalBridge` when a container runtime (apptainer/singularity)
      is on PATH — real local execution, no Prefect/VM.
    * else → :class:`StubBridge` (runs are SIMULATED), with a loud warning.
    """
    forced = os.getenv("NILS_PIPELINE_BRIDGE", "").strip().lower()
    if forced == "stub":
        return StubBridge()

    def _local_or_stub() -> PipelineBridge:
        from analysis_pipeline.local_bridge import LocalBridge, LocalBridgeUnavailable

        try:
            bridge = LocalBridge()
            logger.info(
                "Analysis-pipeline execution: LocalBridge (%s exec, no Prefect).",
                bridge.runtime,
            )
            return bridge
        except LocalBridgeUnavailable as exc:
            logger.warning(
                "%s Runs will be SIMULATED (StubBridge) — install apptainer/"
                "singularity for real local execution.",
                exc,
            )
            return StubBridge()

    if forced in ("local", "apptainer", "singularity"):
        return _local_or_stub()

    if os.getenv("PREFECT_API_URL"):
        logger.warning(
            "PREFECT_API_URL is set but PrefectBridge is not implemented yet; "
            "using local execution."
        )
    return _local_or_stub()


__all__ = [
    "BridgeRunState",
    "BridgeRunResult",
    "PipelineBridge",
    "StubBridge",
    "select_bridge",
]
