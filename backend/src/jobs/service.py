"""High-level job service orchestrating persistence and runner calls."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from sqlalchemy.exc import SQLAlchemyError

from db.session import engine, session_scope

from .models import Base, JobDTO, JobStatus
from . import repository
from .control import JobControl


logger = logging.getLogger(__name__)

_PROGRESS_LOG_STEP = 5


def _context_from_config(config: Optional[dict]) -> dict[str, Any]:
    if not isinstance(config, dict):
        return {}
    context: dict[str, Any] = {}
    for key in ("cohort_id", "cohort_name", "resume", "resume_by_path"):
        if key in config:
            context[key] = config.get(key)
    return context


def _log_job_event(
    event: str,
    job_id: int,
    *,
    stage: Optional[str] = None,
    status: Optional[str] = None,
    progress: Optional[int] = None,
    message: Optional[str] = None,
    config: Optional[dict] = None,
    extra: Optional[dict[str, Any]] = None,
    level: int = logging.INFO,
) -> None:
    if not logger.isEnabledFor(level):
        return
    parts = [f"event={event}", f"job_id={job_id}"]
    if stage:
        parts.append(f"stage={stage}")
    if status:
        parts.append(f"status={status}")
    if progress is not None:
        parts.append(f"progress={progress}")
    context = {}
    context.update(_context_from_config(config))
    if extra:
        context.update(extra)
    for key, value in context.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    if message:
        parts.append(f"message={message}")
    logger.log(level, " ".join(parts))


class JobService:
    def __init__(self) -> None:
        self._initialized = False
        self._active_controls: Dict[int, JobControl] = {}
        self._progress_log: Dict[int, int] = {}

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        try:
            Base.metadata.create_all(engine)
        except Exception as exc:  # pragma: no cover - initialization failure
            raise RuntimeError("Failed to initialize job tables") from exc
        self._initialized = True

    def register_control(self, job_id: int, control: JobControl) -> None:
        self._active_controls[job_id] = control

    def unregister_control(self, job_id: int) -> None:
        if job_id in self._active_controls:
            del self._active_controls[job_id]

    def _reset_progress_log(self, job_id: int) -> None:
        self._progress_log.pop(job_id, None)

    def _should_log_progress(self, job_id: int, progress: int) -> bool:
        last = self._progress_log.get(job_id)
        if last is None or progress in {0, 100} or abs(progress - last) >= _PROGRESS_LOG_STEP:
            self._progress_log[job_id] = progress
            return True
        return False

    def create_job(self, *, stage: str, config: dict, name: Optional[str] = None) -> JobDTO:
        self._ensure_initialized()
        with session_scope() as session:
            job = repository.create_job(session, stage=stage, config=config, name=name)
            session.refresh(job)
            dto = JobDTO.model_validate(job)
        _log_job_event(
            "created",
            dto.id,
            stage=dto.stage,
            status=dto.status.value,
            config=dto.config,
            extra={"name": dto.name},
        )
        return dto

    def mark_running(self, job_id: int) -> None:
        self._ensure_initialized()
        with session_scope() as session:
            repository.update_job_status(session, job_id, JobStatus.RUNNING)
            job = repository.get_job(session, job_id)
        _log_job_event(
            "running",
            job_id,
            stage=job.stage if job else None,
            status=JobStatus.RUNNING.value,
            config=job.config if job else None,
        )

    def mark_completed(self, job_id: int) -> None:
        self._ensure_initialized()
        with session_scope() as session:
            repository.update_job_status(session, job_id, JobStatus.COMPLETED)
            repository.update_job_progress(session, job_id, 100)
            job = repository.get_job(session, job_id)
        self._reset_progress_log(job_id)
        _log_job_event(
            "completed",
            job_id,
            stage=job.stage if job else None,
            status=JobStatus.COMPLETED.value,
            progress=100,
            config=job.config if job else None,
        )

    def mark_failed(self, job_id: int, error: str) -> None:
        self._ensure_initialized()
        with session_scope() as session:
            repository.update_job_status(session, job_id, JobStatus.FAILED, message=error)
            job = repository.get_job(session, job_id)
        self._reset_progress_log(job_id)
        _log_job_event(
            "failed",
            job_id,
            stage=job.stage if job else None,
            status=JobStatus.FAILED.value,
            message=error,
            config=job.config if job else None,
            level=logging.ERROR,
        )

    def pause_job(self, job_id: int) -> None:
        self._ensure_initialized()
        if job_id in self._active_controls:
            self._active_controls[job_id].pause()
        
        with session_scope() as session:
            repository.update_job_status(session, job_id, JobStatus.PAUSED)
            job = repository.get_job(session, job_id)
        _log_job_event(
            "paused",
            job_id,
            stage=job.stage if job else None,
            status=JobStatus.PAUSED.value,
            progress=job.progress if job else None,
            config=job.config if job else None,
        )

    def resume_job(self, job_id: int) -> None:
        self._ensure_initialized()
        if job_id in self._active_controls:
            self._active_controls[job_id].resume()
            
        with session_scope() as session:
            repository.update_job_status(session, job_id, JobStatus.RUNNING)
            job = repository.get_job(session, job_id)
        _log_job_event(
            "resumed",
            job_id,
            stage=job.stage if job else None,
            status=JobStatus.RUNNING.value,
            progress=job.progress if job else None,
            config=job.config if job else None,
        )

    def cancel_job(self, job_id: int) -> None:
        self._ensure_initialized()
        if job_id in self._active_controls:
            self._active_controls[job_id].cancel()
            
        with session_scope() as session:
            repository.update_job_status(session, job_id, JobStatus.CANCELED)
            job = repository.get_job(session, job_id)
        self._reset_progress_log(job_id)
        _log_job_event(
            "canceled",
            job_id,
            stage=job.stage if job else None,
            status=JobStatus.CANCELED.value,
            progress=job.progress if job else None,
            config=job.config if job else None,
        )

    def delete_job(self, job_id: int) -> None:
        """Delete a job record from the database."""
        self._ensure_initialized()
        with session_scope() as session:
            job = repository.get_job(session, job_id)
            if job is None:
                raise ValueError(f"Job {job_id} not found")
            repository.delete_job(session, job_id)
        self._reset_progress_log(job_id)
        _log_job_event(
            "deleted",
            job_id,
            stage=job.stage,
            status=job.status.value,
            config=job.config,
        )

    def update_config(self, job_id: int, config: dict) -> JobDTO:
        self._ensure_initialized()
        with session_scope() as session:
            repository.update_job_config(session, job_id, config)
            updated = repository.get_job(session, job_id)
            if updated is None:
                raise ValueError(f"Job {job_id} not found")
        _log_job_event(
            "config_updated",
            job_id,
            stage=updated.stage,
            status=updated.status.value,
            config=updated.config,
        )
        return updated

    def update_progress(self, job_id: int, progress: int) -> None:
        self._ensure_initialized()
        try:
            with session_scope() as session:
                repository.update_job_progress(session, job_id, progress)
                should_log = self._should_log_progress(job_id, progress)
                job = repository.get_job(session, job_id) if should_log else None
        except SQLAlchemyError as exc:  # pragma: no cover - best-effort update
            # Progress updates are best-effort; log and continue without raising.
            # This prevents issues like database OOM from killing long-running jobs.
            logger.warning(
                "Failed to update job %s progress to %s: %s",
                job_id,
                progress,
                exc,
            )
            return

        if should_log:
            _log_job_event(
                "progress",
                job_id,
                stage=job.stage if job else None,
                status=job.status.value if job else None,
                progress=progress,
                config=job.config if job else None,
            )

    def update_metrics(self, job_id: int, metrics: dict) -> None:
        self._ensure_initialized()
        try:
            with session_scope() as session:
                repository.update_job_metrics(session, job_id, metrics)
                job = repository.get_job(session, job_id)
        except SQLAlchemyError as exc:  # pragma: no cover - best-effort update
            logger.warning(
                "Failed to update job %s metrics: %s",
                job_id,
                exc,
            )
            return

        metric_keys = ",".join(sorted(str(key) for key in metrics.keys())) or "none"
        _log_job_event(
            "metrics",
            job_id,
            stage=job.stage if job else None,
            status=job.status.value if job else None,
            progress=job.progress if job else None,
            config=job.config if job else None,
            extra={"metric_keys": metric_keys},
        )

    def list_jobs(self, *, cohort_id: Optional[int] = None, stage: Optional[str] = None) -> list[JobDTO]:
        self._ensure_initialized()
        with session_scope() as session:
            jobs = list(repository.list_jobs(session))

        if cohort_id is not None:
            jobs = [job for job in jobs if job.config.get("cohort_id") == cohort_id]
        if stage is not None:
            jobs = [job for job in jobs if job.stage == stage]
        return jobs

    def list_jobs_for_stage(self, cohort_id: int, stage: str, limit: int = 10) -> list[JobDTO]:
        jobs = self.list_jobs(cohort_id=cohort_id, stage=stage)
        return jobs[:limit]

    def get_job(self, job_id: int) -> Optional[JobDTO]:
        self._ensure_initialized()
        with session_scope() as session:
            return repository.get_job(session, job_id)


job_service = JobService()
