"""Serialization utilities for API responses."""
from __future__ import annotations

import datetime as dt
from jobs.models import JobDTO


def isoformat(value: dt.datetime | None) -> str | None:
    """Convert datetime to ISO format string.
    
    Args:
        value: Datetime to convert
        
    Returns:
        ISO format string or None
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc).isoformat()
    return value.isoformat()


def job_time_stats(job: JobDTO) -> dict[str, int | None]:
    """Calculate elapsed time, ETA, and total time for a job.
    
    Args:
        job: Job DTO
        
    Returns:
        Dict with elapsedMs, etaMs, totalMs
    """
    start = job.started_at
    if start is None:
        return {"elapsedMs": None, "etaMs": None, "totalMs": None}

    if start.tzinfo is None:
        start = start.replace(tzinfo=dt.timezone.utc)
    else:
        start = start.astimezone(dt.timezone.utc)

    end_reference = job.finished_at
    if end_reference is None:
        end_reference = dt.datetime.now(dt.timezone.utc)
    elif end_reference.tzinfo is None:
        end_reference = end_reference.replace(tzinfo=dt.timezone.utc)
    else:
        end_reference = end_reference.astimezone(dt.timezone.utc)

    elapsed_seconds = max((end_reference - start).total_seconds(), 0.0)
    elapsed_ms = int(elapsed_seconds * 1000)

    eta_ms: int | None = None
    progress = job.progress
    if 0 < progress < 100 and elapsed_ms > 0:
        estimated_total = int(round(elapsed_ms * 100 / progress))
        eta_ms = max(estimated_total - elapsed_ms, 0)

    if job.finished_at is not None:
        total_ms: int | None = elapsed_ms
    elif eta_ms is not None:
        total_ms = elapsed_ms + eta_ms
    else:
        total_ms = None

    return {
        "elapsedMs": elapsed_ms,
        "etaMs": eta_ms,
        "totalMs": total_ms,
    }


def serialize_job(job: JobDTO | None, get_cohort_metrics_fn=None) -> dict | None:
    """Serialize job DTO to API response format.
    
    Args:
        job: Job DTO to serialize
        get_cohort_metrics_fn: Optional function to get cohort metrics
        
    Returns:
        Serialized job dict or None
    """
    if job is None:
        return None

    config = job.config or {}
    payload = {
        "id": job.id,
        "stageId": job.stage,
        "stepId": config.get("step_id"),
        "status": job.status.value,
        "progress": job.progress,
        "cohortId": config.get("cohort_id"),
        "cohortName": config.get("cohort_name"),
        "submittedAt": isoformat(job.created_at),
        "startedAt": isoformat(job.started_at),
        "finishedAt": isoformat(job.finished_at),
        "errorMessage": job.last_error,
        "config": config,
    }

    payload.update(job_time_stats(job))

    metrics_payload = job.metrics
    if metrics_payload is None and job.stage == "extract" and get_cohort_metrics_fn:
        cohort_identifier = config.get("cohort_id")
        try:
            cohort_id_int = int(cohort_identifier)
        except (TypeError, ValueError):
            cohort_id_int = None
        if cohort_id_int is not None:
            metrics_payload = get_cohort_metrics_fn(cohort_id_int)

    if metrics_payload:
        payload["metrics"] = metrics_payload

    return payload


def serialize_jobs(jobs: list[JobDTO], get_cohort_metrics_fn=None) -> list[dict]:
    """Serialize list of job DTOs.
    
    Args:
        jobs: List of job DTOs
        get_cohort_metrics_fn: Optional function to get cohort metrics
        
    Returns:
        List of serialized job dicts
    """
    return [
        payload
        for payload in (serialize_job(job, get_cohort_metrics_fn) for job in jobs)
        if payload is not None
    ]
