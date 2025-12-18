"""Job management API routes."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from jobs.service import job_service
from jobs.models import JobStatus
from cohorts.service import cohort_service
from api.utils.serializers import serialize_job as _serialize_job, serialize_jobs as _serialize_jobs
from api.models.jobs import UpdateExtractPerformancePayload

router = APIRouter(prefix="/api/jobs", tags=["jobs"])

# Field mapping for extraction performance config
_EXTRACT_PERFORMANCE_FIELD_MAP = {
    "maxWorkers": "max_workers",
    "batchSize": "batch_size",
    "queueSize": "queue_size",
    "seriesWorkersPerSubject": "series_workers_per_subject",
    "adaptiveBatchingEnabled": "adaptive_batching_enabled",
    "adaptiveTargetTxMs": "target_tx_ms",
    "adaptiveMinBatchSize": "min_batch_size",
    "adaptiveMaxBatchSize": "max_batch_size",
    "useProcessPool": "use_process_pool",
    "processPoolWorkers": "process_pool_workers",
    "dbWriterPoolSize": "db_writer_pool_size",
}


def _apply_extract_performance_patch(
    job_config: dict,
    payload: UpdateExtractPerformancePayload,
) -> tuple[dict, dict]:
    """Apply extraction performance updates to job config."""
    updates = payload.model_dump(exclude_none=True)
    if not updates:
        raise ValueError("No fields provided for update")

    new_job_config = dict(job_config)
    stage_updates: dict[str, Any] = {}

    for camel_key, value in updates.items():
        mapped_key = _EXTRACT_PERFORMANCE_FIELD_MAP.get(camel_key)
        if not mapped_key:
            continue
        stage_updates[camel_key] = value
        new_job_config[mapped_key] = value

    if not stage_updates:
        raise ValueError("No supported fields provided for update")

    # Ensure adaptive batch bounds remain consistent if only one side provided
    if (
        "min_batch_size" in new_job_config
        and "max_batch_size" in new_job_config
        and new_job_config["min_batch_size"] > new_job_config["max_batch_size"]
    ):
        raise ValueError("adaptiveMinBatchSize cannot exceed adaptiveMaxBatchSize")

    return new_job_config, stage_updates


def _update_extract_stage_config(cohort_id: int, updates: dict[str, Any]) -> None:
    """Update extraction stage config via pipeline service."""
    if not updates:
        return

    from nils_dataset_pipeline import nils_pipeline_service
    
    # Get current config from pipeline
    current_config = nils_pipeline_service.get_step_config(cohort_id, 'extract', 'extract')
    if current_config is None:
        return
    
    # Merge updates
    new_config = {**current_config, **updates}
    nils_pipeline_service.update_step_config(cohort_id, 'extract', 'extract', new_config)


def _sync_stage_with_job(job):
    """Sync stage status with job status (no-op, pipeline service handles this)."""
    pass  # Pipeline service handles job/stage sync automatically


@router.get("")
def list_jobs():
    """List all jobs."""
    from metadata_db.metrics import get_cohort_metrics
    jobs = job_service.list_jobs()
    return JSONResponse(_serialize_jobs(jobs, get_cohort_metrics))


@router.post("/{job_id}/{action}")
def job_action(job_id: int, action: str):
    """Perform an action on a job (pause/resume/cancel/retry)."""
    if action not in ["pause", "resume", "cancel", "retry"]:
        raise HTTPException(status_code=400, detail=f"Invalid action: {action}")

    job = job_service.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if action == "retry":
        raise HTTPException(status_code=400, detail="Retry is not supported via this endpoint")
    elif action == "pause":
        job_service.pause_job(job_id)
    elif action == "resume":
        job_service.resume_job(job_id)
    elif action == "cancel":
        job_service.cancel_job(job_id)

    updated_job = job_service.get_job(job_id)
    _sync_stage_with_job(updated_job)
    
    from metadata_db.metrics import get_cohort_metrics
    return JSONResponse(_serialize_job(updated_job, get_cohort_metrics))


@router.delete("/{job_id}")
def delete_job(job_id: int):
    """Delete a job record.
    
    Only completed, failed, or canceled jobs can be deleted.
    """
    job = job_service.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Only allow deleting terminal jobs
    if job.status not in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELED]:
        raise HTTPException(
            status_code=400, 
            detail=f"Cannot delete job in '{job.status.value}' status. Cancel it first."
        )
    
    job_service.delete_job(job_id)
    return JSONResponse({"success": True, "deleted_job_id": job_id})


@router.patch("/{job_id}/config")
def update_job_config(job_id: int, payload: UpdateExtractPerformancePayload):
    """Update extraction performance configuration for a paused job."""
    job = job_service.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.stage != "extract":
        raise HTTPException(status_code=400, detail="Only extraction jobs support performance tuning")
    if job.status != JobStatus.PAUSED:
        raise HTTPException(status_code=400, detail="Job must be paused before editing performance settings")
    if not payload.has_updates():
        raise HTTPException(status_code=400, detail="No fields provided for update")

    try:
        updated_config, stage_updates = _apply_extract_performance_patch(job.config, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    updated_job = job_service.update_config(job_id, updated_config)
    cohort_identifier = updated_config.get("cohort_id")
    try:
        cohort_id = int(cohort_identifier)
    except (TypeError, ValueError):
        cohort_id = None
    
    if cohort_id is not None and stage_updates:
        _update_extract_stage_config(cohort_id, stage_updates)

    from metadata_db.metrics import get_cohort_metrics
    return JSONResponse(_serialize_job(updated_job, get_cohort_metrics))
