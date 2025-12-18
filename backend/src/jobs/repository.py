"""Data access helpers for job persistence."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Optional

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from .models import Job, JobDTO, JobRun, JobRunDTO, JobStatus


def create_job(session: Session, *, stage: str, config: dict, name: Optional[str] = None) -> Job:
    job = Job(stage=stage, config=config, name=name)
    session.add(job)
    session.flush()
    run = JobRun(job_id=job.id, status=JobStatus.QUEUED)
    session.add(run)
    session.flush()
    return job


def update_job_status(session: Session, job_id: int, status: JobStatus, *, message: Optional[str] = None) -> None:
    job = session.get(Job, job_id)
    if not job:
        raise ValueError(f"Job {job_id} not found")
    job.status = status
    if status == JobStatus.RUNNING:
        job.started_at = datetime.now(timezone.utc)
    elif status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELED}:
        job.finished_at = datetime.now(timezone.utc)
    if message:
        job.last_error = message

    # update latest run
    run = session.scalar(
        select(JobRun).where(JobRun.job_id == job_id).order_by(JobRun.started_at.desc()).limit(1)
    )
    if run:
        run.status = status
        if status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELED}:
            run.finished_at = datetime.now(timezone.utc)
            run.message = message


def update_job_progress(session: Session, job_id: int, progress: int) -> None:
    session.execute(update(Job).where(Job.id == job_id).values(progress=progress))
    run = session.scalar(
        select(JobRun).where(JobRun.job_id == job_id).order_by(JobRun.started_at.desc()).limit(1)
    )
    if run:
        run.progress = progress


def update_job_metrics(session: Session, job_id: int, metrics: dict) -> None:
    job = session.get(Job, job_id)
    if not job:
        raise ValueError(f"Job {job_id} not found")
    merged = dict(job.metrics or {})
    merged.update(metrics)
    job.metrics = merged
    run = session.scalar(
        select(JobRun).where(JobRun.job_id == job_id).order_by(JobRun.started_at.desc()).limit(1)
    )
    if run:
        current = dict(run.metrics or {})
        current.update(metrics)
        run.metrics = current


def list_jobs(session: Session) -> Iterable[JobDTO]:
    result = session.scalars(select(Job).order_by(Job.created_at.desc())).all()
    return [JobDTO.model_validate(job) for job in result]


def get_job(session: Session, job_id: int) -> Optional[JobDTO]:
    job = session.get(Job, job_id)
    if not job:
        return None
    return JobDTO.model_validate(job)


def get_job_runs(session: Session, job_id: int) -> list[JobRunDTO]:
    runs = session.scalars(select(JobRun).where(JobRun.job_id == job_id).order_by(JobRun.started_at)).all()
    return [JobRunDTO.model_validate(run) for run in runs]


def update_job_config(session: Session, job_id: int, config: dict) -> None:
    job = session.get(Job, job_id)
    if not job:
        raise ValueError(f"Job {job_id} not found")
    job.config = config


def delete_job(session: Session, job_id: int) -> None:
    """Delete a job and its associated runs."""
    job = session.get(Job, job_id)
    if not job:
        raise ValueError(f"Job {job_id} not found")
    # Runs will be deleted via cascade
    session.delete(job)
    session.flush()
