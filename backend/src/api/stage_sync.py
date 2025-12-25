"""Helpers for reconciling cohort stage metadata with job records."""

from __future__ import annotations

import logging
from typing import Optional

from cohorts.service import cohort_service
from jobs.models import JobStatus
from jobs.service import job_service

logger = logging.getLogger(__name__)


JOB_TO_STAGE_STATUS = {
    JobStatus.QUEUED: "pending",
    JobStatus.RUNNING: "running",
    JobStatus.PAUSED: "paused",
    JobStatus.COMPLETED: "completed",
    JobStatus.FAILED: "failed",
    JobStatus.CANCELED: "pending",
}


# =============================================================================
# DUAL-WRITE HELPERS (for migration period)
# =============================================================================


def sync_step_to_pipeline(
    cohort_id: int,
    stage_id: str,
    status: str,
    progress: int = 0,
    job_id: Optional[int] = None,
    step_id: Optional[str] = None,
    metrics: Optional[dict] = None,
    handover: Optional[dict] = None,
) -> None:
    """Sync a stage/step status change to the new pipeline_steps table.
    
    This should be called alongside cohort_service.update_stages() during
    the migration period to keep both systems in sync.
    
    Args:
        cohort_id: The cohort ID.
        stage_id: The stage ID (e.g., 'anonymize', 'extract', 'sort').
        status: The new status ('pending', 'running', 'completed', 'failed', 'blocked').
        progress: Progress percentage (0-100).
        job_id: Optional job ID if a job is running this step.
        step_id: Optional step ID for multi-step stages (sorting).
        metrics: Optional metrics to save.
        handover: Optional handover data to save.
    """
    try:
        from nils_dataset_pipeline import nils_pipeline_service
        from nils_dataset_pipeline.repository import get_step, update_step_status, set_step_job
        from db.session import SessionLocal
        
        with SessionLocal() as session:
            step = get_step(session, cohort_id, stage_id, step_id)
            if not step:
                logger.debug(
                    "Pipeline step not found for sync: cohort=%d stage=%s step=%s",
                    cohort_id, stage_id, step_id
                )
                return
            
            # Update status and progress
            step.status = status
            step.progress = progress
            
            # Update job reference
            if job_id is not None:
                step.current_job_id = job_id
            elif status in ("completed", "failed", "pending"):
                # Clear job_id when step is no longer running
                step.current_job_id = None
            
            # Update metrics if provided
            if metrics is not None:
                step.metrics = metrics
            
            # Update handover if provided
            if handover is not None:
                step.handover_data = handover
            
            session.commit()
            
    except Exception as e:
        # Log but don't fail - pipeline sync is secondary during migration
        logger.warning(
            "Failed to sync to pipeline: cohort=%d stage=%s step=%s error=%s",
            cohort_id, stage_id, step_id, e
        )


def complete_pipeline_step(
    cohort_id: int,
    stage_id: str,
    step_id: Optional[str] = None,
    metrics: Optional[dict] = None,
    handover: Optional[dict] = None,
) -> None:
    """Mark a pipeline step as completed and unlock the next step.
    
    This is a convenience wrapper that handles the completion flow.
    """
    try:
        from nils_dataset_pipeline import nils_pipeline_service
        nils_pipeline_service.complete_step(
            cohort_id=cohort_id,
            stage_id=stage_id,
            step_id=step_id,
            metrics=metrics,
            handover=handover,
        )
    except Exception as e:
        logger.warning(
            "Failed to complete pipeline step: cohort=%d stage=%s step=%s error=%s",
            cohort_id, stage_id, step_id, e
        )


def start_pipeline_step(
    cohort_id: int,
    stage_id: str,
    job_id: int,
    step_id: Optional[str] = None,
) -> None:
    """Mark a pipeline step as running with a job.
    
    This is a convenience wrapper that handles the start flow.
    """
    try:
        from nils_dataset_pipeline import nils_pipeline_service
        nils_pipeline_service.start_step(
            cohort_id=cohort_id,
            stage_id=stage_id,
            job_id=job_id,
            step_id=step_id,
        )
    except Exception as e:
        logger.warning(
            "Failed to start pipeline step: cohort=%d stage=%s step=%s job=%d error=%s",
            cohort_id, stage_id, step_id, job_id, e
        )


def fail_pipeline_step(
    cohort_id: int,
    stage_id: str,
    step_id: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    """Mark a pipeline step as failed.
    
    This is a convenience wrapper that handles the failure flow.
    """
    try:
        from nils_dataset_pipeline import nils_pipeline_service
        nils_pipeline_service.fail_step(
            cohort_id=cohort_id,
            stage_id=stage_id,
            step_id=step_id,
            error=error,
        )
    except Exception as e:
        logger.warning(
            "Failed to mark pipeline step as failed: cohort=%d stage=%s step=%s error=%s",
            cohort_id, stage_id, step_id, e
        )


def reconcile_stage_jobs() -> None:
    """Reconcile job states on startup - mark orphaned running jobs as interrupted.
    
    When the backend restarts (e.g., due to crash, OOM, or manual restart), any jobs
    that were marked as RUNNING are now orphaned - their actual process is gone.
    
    This function:
    1. Finds all jobs with status=RUNNING
    2. Marks them as FAILED with a clear message about the interruption
    3. The user can then restart the job (with resume=True to continue from where it left off)
    
    This is critical for long-running extraction jobs that may take days to complete.
    """
    from db.session import SessionLocal
    from jobs.models import Job, JobStatus
    from datetime import datetime, timezone
    
    try:
        with SessionLocal() as session:
            # Find all "running" jobs - these are orphaned since we just started
            orphaned_jobs = session.query(Job).filter(
                Job.status == JobStatus.RUNNING
            ).all()
            
            if not orphaned_jobs:
                logger.info("No orphaned running jobs found on startup")
                return
            
            for job in orphaned_jobs:
                old_progress = job.progress
                old_metrics = job.metrics or {}
                
                # Mark as failed with a clear message
                job.status = JobStatus.FAILED
                job.finished_at = datetime.now(timezone.utc)
                job.last_error = (
                    f"Job interrupted by backend restart. "
                    f"Progress was {old_progress}% with {old_metrics.get('instances', 0):,} instances. "
                    f"Restart the job to resume from this point."
                )
                
                logger.warning(
                    "Marked orphaned job as failed: job_id=%d stage=%s progress=%d%% instances=%s",
                    job.id,
                    job.stage,
                    old_progress,
                    old_metrics.get('instances', 'N/A'),
                )
                
                # Also update the pipeline step status
                cohort_id = job.config.get('cohort_id') if job.config else None
                if cohort_id:
                    sync_step_to_pipeline(
                        cohort_id=cohort_id,
                        stage_id=job.stage,
                        status="failed",
                        progress=old_progress,
                        job_id=None,  # Clear job reference
                    )
            
            session.commit()
            logger.info(
                "Reconciled %d orphaned running job(s) on startup",
                len(orphaned_jobs),
            )
            
    except Exception as e:
        logger.exception("Failed to reconcile orphaned jobs on startup: %s", e)
