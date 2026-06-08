"""Analysis-pipeline routes (frozen design §7).

Thin HTTP surface over the analysis-pipeline domain layers:

* **Registry** (:mod:`analysis_pipeline.registry`) — register / list / refresh /
  remove repos and list / show pipelines. A registered repo is fetched, its
  commit sha pinned, and one :class:`AnalysisPipeline` per discovered descriptor
  is persisted (identity ``(repo, sha, slug)``).
* **Runner** (:mod:`analysis_pipeline.runner`) — create / list / show / cancel
  runs and proxy bridge state + logs. ``create_run`` is 202-style: it pins the
  immutable refs, writes the run + a mirrored Job, and hands the lifecycle to the
  shared background executor.
* **Ingest** (:mod:`analysis_pipeline.ingest`) — plan (dry-run) or start the
  ``ingest_derivatives`` job (DB-write gated OFF, §17.8).

Handlers stay THIN — all domain logic lives in the registry / runner / ingest
layers. Style mirrors :mod:`api.routes.export`: lazy ``_job_executor`` import for
the background ingest job, ``serialize_job`` for the mirrored job payload, and a
``202`` + serialized job for launches.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, Body, HTTPException, Query, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from api.utils.serializers import serialize_job
from jobs.service import job_service

from analysis_pipeline.ingest import plan_ingest, run_ingest_derivatives_job
from analysis_pipeline.results import parse_results_json
from analysis_pipeline.models import (
    AnalysisPipelineDTO,
    AnalysisPipelineRepoDTO,
    AnalysisPipelineRunDTO,
    InputSource,
)
from analysis_pipeline.registry import (
    RegistryError,
    analysis_pipeline_registry,
)
from analysis_pipeline.bridge import select_bridge
from analysis_pipeline.runner import AnalysisPipelineRunner

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/analysis-pipelines", tags=["analysis-pipelines"])

# Execution substrate is chosen at startup (local-first, no VM required):
# LocalBridge (direct apptainer exec) when a container runtime is present, else
# StubBridge (simulated); PrefectBridge is opt-in via PREFECT_API_URL later.
# See analysis_pipeline.bridge.select_bridge.
_runner = AnalysisPipelineRunner(bridge=select_bridge())


# ---------------------------------------------------------------------------
# Request bodies (validated by FastAPI/pydantic — bad bodies → 422)
# ---------------------------------------------------------------------------


class RegisterRepoBody(BaseModel):
    """Body for ``POST /repos`` — register a repo by URL."""

    url: str = Field(..., min_length=1, description="Repository URL (or local fixture path).")
    ref: Optional[str] = Field(None, description="Optional branch/tag/ref to pin.")


class RefreshRepoBody(BaseModel):
    """Body for ``POST /repos/{id}/refresh`` — re-pin the latest sha."""

    ref: Optional[str] = Field(None, description="Optional branch/tag/ref to re-resolve.")


class CreateRunBody(BaseModel):
    """Body for ``POST /{pipeline_id}/runs`` — create + launch a run."""

    input_source: str = Field(
        ...,
        description="'db_subset' (stack_ids) or 'external_path' (external_path).",
    )
    stack_ids: Optional[list[int]] = Field(
        None, description="Metadata-DB stack ids (required for db_subset)."
    )
    external_path: Optional[str] = Field(
        None, description="Existing BIDS root (required for external_path)."
    )
    params: Optional[dict[str, Any]] = Field(
        None, description="Resolved config-form values for the descriptor inputs."
    )
    output_path: Optional[str] = Field(
        None, description="Optional explicit derivatives output root."
    )
    retain_input: bool = Field(
        True, description="Keep the materialized BIDS input after the run (GC policy)."
    )
    config: Optional[dict[str, Any]] = Field(
        None,
        description="Raw export/materialization config (passed to build_export_config).",
    )
    name: Optional[str] = Field(None, description="Optional human-friendly run name.")


# ---------------------------------------------------------------------------
# Serialization helpers (DTO → JSON-safe dict)
# ---------------------------------------------------------------------------


def _serialize_repo(repo: Any) -> dict:
    return AnalysisPipelineRepoDTO.model_validate(repo).model_dump(mode="json")


def _serialize_pipeline(pipeline: Any) -> dict:
    return AnalysisPipelineDTO.model_validate(pipeline).model_dump(mode="json")


def _serialize_run(run: Any) -> dict:
    return AnalysisPipelineRunDTO.model_validate(run).model_dump(mode="json")


# ---------------------------------------------------------------------------
# Repos
# ---------------------------------------------------------------------------


@router.post("/repos", status_code=201)
def register_repo(body: RegisterRepoBody = Body(...)):
    """Register a repo by URL → repo row + one pipeline per detected descriptor."""
    try:
        repo, pipelines = analysis_pipeline_registry.register_repo(body.url, ref=body.ref)
    except RegistryError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # descriptor parse/validate or fetch failure
        logger.exception("register_repo failed for url=%s", body.url)
        raise HTTPException(status_code=400, detail=f"Failed to register repo: {exc}")

    return JSONResponse(
        status_code=201,
        content={
            "repo": _serialize_repo(repo),
            "pipelines": [_serialize_pipeline(p) for p in pipelines],
        },
    )


@router.get("/repos")
def list_repos():
    """List all registered repo versions (newest first)."""
    repos = analysis_pipeline_registry.list_repos()
    return {"repos": [_serialize_repo(r) for r in repos]}


@router.post("/repos/{repo_id}/refresh")
def refresh_repo(repo_id: int, body: RefreshRepoBody = Body(default=RefreshRepoBody())):
    """Re-pin the latest sha → new immutable version (no-op if sha unchanged)."""
    try:
        repo, pipelines = analysis_pipeline_registry.refresh_repo(repo_id, ref=body.ref)
    except RegistryError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        logger.exception("refresh_repo failed for repo_id=%s", repo_id)
        raise HTTPException(status_code=400, detail=f"Failed to refresh repo: {exc}")

    return {
        "repo": _serialize_repo(repo),
        "pipelines": [_serialize_pipeline(p) for p in pipelines],
    }


@router.delete("/repos/{repo_id}", status_code=204)
def remove_repo(repo_id: int) -> Response:
    """Unregister a repo version (refused if any pipeline has runs — §15.1)."""
    try:
        analysis_pipeline_registry.remove_repo(repo_id)
    except RegistryError as exc:
        # No-such-repo vs has-runs are both RegistryError; surface as 409 so the
        # caller can distinguish "cannot remove" from a generic 404.
        msg = str(exc)
        status = 404 if "No analysis-pipeline repo" in msg else 409
        raise HTTPException(status_code=status, detail=msg)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Pipelines (inventory)
# ---------------------------------------------------------------------------


@router.get("")
def list_pipelines(repo_id: Optional[int] = Query(None, description="Scope to a repo version.")):
    """List pipelines (inventory; optionally scoped to one repo version)."""
    pipelines = analysis_pipeline_registry.list_pipelines(repo_id=repo_id)
    return {"pipelines": [_serialize_pipeline(p) for p in pipelines]}


@router.get("/{pipeline_id}")
def get_pipeline(pipeline_id: int):
    """Pipeline detail (descriptor, work_unit, needs, pinned refs, version)."""
    pipeline = analysis_pipeline_registry.get_pipeline(pipeline_id)
    if pipeline is None:
        raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")
    return {"pipeline": _serialize_pipeline(pipeline)}


@router.get("/{pipeline_id}/runs")
def list_pipeline_runs(pipeline_id: int):
    """All runs for a pipeline, newest first (persists across UI reloads)."""
    runs = _runner.list_runs(pipeline_id)
    return {"runs": [_serialize_run(r) for r in runs]}


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


@router.post("/{pipeline_id}/runs", status_code=202)
def create_run(pipeline_id: int, body: CreateRunBody = Body(...)):
    """Create + launch a run (202). Returns the run + its mirrored job.

    Synchronous and fast: the runner pins immutable refs, writes the run +
    a mirrored Job, and submits the lifecycle to the shared background executor.
    """
    try:
        input_source = InputSource(body.input_source)
    except ValueError:
        valid = ", ".join(s.value for s in InputSource)
        raise HTTPException(
            status_code=400,
            detail=f"Invalid input_source {body.input_source!r}; expected one of: {valid}",
        )

    try:
        run_id = _runner.create_run(
            pipeline_id=pipeline_id,
            input_source=input_source,
            stack_ids=body.stack_ids,
            external_path=body.external_path,
            params=body.params,
            output_path=body.output_path,
            retain_input=body.retain_input,
            raw_cfg=body.config,
            name=body.name,
        )
    except ValueError as exc:
        # Missing-stack_ids / missing-external_path / pipeline-not-found.
        msg = str(exc)
        status = 404 if "not found" in msg else 400
        raise HTTPException(status_code=status, detail=msg)
    except Exception as exc:
        logger.exception("create_run failed for pipeline_id=%s", pipeline_id)
        raise HTTPException(status_code=400, detail=f"Failed to create run: {exc}")

    run = _runner.get_run(run_id)
    if run is None:  # pragma: no cover - run just created
        raise HTTPException(status_code=500, detail="Run vanished after creation")

    job_payload = None
    if run.job_id is not None:
        job_payload = serialize_job(job_service.get_job(run.job_id))

    return JSONResponse(
        status_code=202,
        content={"run": _serialize_run(run), "job": job_payload},
    )


@router.get("/runs/{run_id}")
def get_run(run_id: int):
    """Run detail (status + provenance + mirrored job)."""
    run = _runner.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    job_payload = None
    if run.job_id is not None:
        job_payload = serialize_job(job_service.get_job(run.job_id))

    return {"run": _serialize_run(run), "job": job_payload}


@router.get("/runs/{run_id}/state")
def get_run_state(run_id: int):
    """Proxied bridge state for a run (per-unit counts)."""
    run = _runner.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    state = _runner.get_state(run_id)
    return {"state": state.model_dump(mode="json")}


@router.get("/runs/{run_id}/logs")
def get_run_logs(run_id: int, since: int = Query(0, ge=0, description="Line-index cursor.")):
    """Per-unit logs from cursor ``since`` (plain JSON; SSE seam is net-new)."""
    run = _runner.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    lines = _runner.get_logs(run_id, since=since)
    return {"since": since, "next_cursor": since + len(lines), "lines": lines}


@router.get("/runs/{run_id}/results")
def get_run_results(run_id: int):
    """Per-unit outcomes from the run's ``results.json`` (drives the results table).

    Empty (no units) until the run has produced an ``output_dir`` with results.
    """
    run = _runner.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    if not run.output_path:
        return {"results": {"schema_version": "1", "units": []}}
    results = parse_results_json(run.output_path)
    return {"results": results.model_dump(mode="json")}


@router.post("/runs/{run_id}/cancel", status_code=202)
def cancel_run(run_id: int):
    """Cancel a run (bridge cancel transport → run.status=canceled → job flip)."""
    run = _runner.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    try:
        _runner.cancel_run(run_id)
    except Exception as exc:
        logger.exception("cancel_run failed for run_id=%s", run_id)
        raise HTTPException(status_code=400, detail=f"Failed to cancel run: {exc}")

    refreshed = _runner.get_run(run_id)
    return JSONResponse(
        status_code=202,
        content={"run": _serialize_run(refreshed) if refreshed is not None else None},
    )


@router.post("/runs/{run_id}/ingest", status_code=202)
def ingest_run(
    run_id: int,
    dry_run: bool = Query(False, description="Return the IngestPlan without starting a job."),
):
    """Plan (dry-run) or start the ``ingest_derivatives`` job (DB-write gated OFF)."""
    run = _runner.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    output_dir = run.output_path
    if not output_dir:
        raise HTTPException(
            status_code=409,
            detail="Run has no output_path yet; nothing to ingest.",
        )

    if dry_run:
        plan = plan_ingest(run_id, output_dir)
        return {"plan": plan.model_dump(mode="json")}

    job = job_service.create_job(
        stage="ingest_derivatives",
        config={"run_id": run_id, "output_dir": output_dir},
        name=f"Ingest derivatives: run {run_id}",
    )

    # Run the (gated) ingest job on the shared background executor (lazy import
    # of _job_executor to dodge the api.server circular import — same pattern as
    # cohorts.py / export.py).
    from api.server import _job_executor

    _job_executor.submit(run_ingest_derivatives_job, job.id, run_id, output_dir)
    logger.info("ingest_derivatives job %s submitted for run %s", job.id, run_id)

    return JSONResponse(
        status_code=202,
        content={"job": serialize_job(job_service.get_job(job.id))},
    )


__all__ = ["router"]
