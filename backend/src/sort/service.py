"""Sorting pipeline service - orchestrates step execution with SSE streaming."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncGenerator

from metadata_db.session import engine as metadata_engine
from db.session import SessionLocal as AppSessionLocal
from cohorts.repository import get_cohort

from .models import (
    ProgressEvent,
    SortingConfig,
    SortingStepId,
    SORTING_STEPS,
    StepProgress,
    StepStatus,
)
from .queries import get_cohort_info
from .steps.base import StepContext, StepResult
from .steps.step1_checkup import Step1Checkup
# Use Polars-optimized version for better performance
from .steps.step2_stack_fingerprint_polars import Step2StackFingerprint
from .steps.step3_classification import Step3Classification
from .steps.step4_completion import Step4Completion
from nils_dataset_pipeline import nils_pipeline_service
from nils_dataset_pipeline.ordering import get_step_ids_for_stage

logger = logging.getLogger(__name__)


# =============================================================================
# Helper Functions for Step-Wise Execution
# =============================================================================


def get_previous_step(step_id: str) -> str | None:
    """Get the previous step ID in the pipeline."""
    step_order = get_step_ids_for_stage('sort')
    try:
        idx = step_order.index(step_id)
        return step_order[idx - 1] if idx > 0 else None
    except ValueError:
        return None


def reconstruct_handover(step_id: str, handover_data: dict[str, Any], conn) -> Any:
    """Reconstruct handover object from database JSON.
    
    Args:
        step_id: The step that produced this handover
        handover_data: The raw handover dict from database
        conn: Database connection for querying additional data if needed
        
    Returns:
        Handover object appropriate for the step
    """
    from .models import Step1Handover, Step2Handover, SeriesForProcessing
    from .queries import get_cohort_info
    
    handover_type = handover_data.get('type')
    
    if handover_type == 'step1' or step_id == 'checkup':
        # Reconstruct Step1Handover
        series_ids = handover_data.get('series_ids', [])
        
        # Query series details for series_to_process
        if series_ids:
            from sqlalchemy import text
            result = conn.execute(text("""
                SELECT 
                    s.series_id,
                    s.series_instance_uid,
                    s.modality,
                    s.study_id,
                    st.subject_id,
                    st.study_instance_uid,
                    st.study_date,
                    su.subject_code
                FROM series s
                JOIN study st ON s.study_id = st.study_id
                JOIN subject su ON st.subject_id = su.subject_id
                WHERE s.series_id = ANY(:series_ids)
            """), {"series_ids": series_ids})
            
            series_to_process = [
                SeriesForProcessing(
                    series_id=row.series_id,
                    series_instance_uid=row.series_instance_uid,
                    modality=row.modality,
                    study_id=row.study_id,
                    subject_id=row.subject_id,
                    study_instance_uid=row.study_instance_uid,
                    study_date=row.study_date,
                    subject_code=row.subject_code,
                )
                for row in result
            ]
        else:
            series_to_process = []
        
        return Step1Handover(
            series_to_process=series_to_process,
            series_ids=set(series_ids),
            series_uids={s.series_instance_uid for s in series_to_process},
            study_ids={s.study_id for s in series_to_process},
            subject_ids={s.subject_id for s in series_to_process},
            cohort_id=handover_data.get('cohort_id'),
            cohort_name=handover_data.get('cohort_name', ''),
            processing_mode=handover_data.get('processing_mode', 'incremental'),
            metrics=handover_data.get('metrics', {}),
        )
    
    elif handover_type == 'step2' or step_id == 'stack_fingerprint':
        # Reconstruct Step2Handover
        from .models import Step2Metrics
        
        metrics = Step2Metrics(
            total_fingerprints_created=handover_data.get('fingerprints_created', 0),
            stacks_processed=handover_data.get('stacks_processed', 0),
            series_with_multiple_stacks=handover_data.get('series_with_multiple_stacks', 0),
            breakdown_by_modality=handover_data.get('breakdown_by_modality', {}),
        )
        
        return Step2Handover(
            fingerprint_ids=handover_data.get('fingerprint_ids', []),
            series_stack_ids=handover_data.get('series_stack_ids', []),
            cohort_id=handover_data.get('cohort_id'),
            cohort_name=handover_data.get('cohort_name', ''),
            processing_mode=handover_data.get('processing_mode', 'incremental'),
            fingerprints_created=handover_data.get('fingerprints_created', 0),
            stacks_processed=handover_data.get('stacks_processed', 0),
            series_with_multiple_stacks=handover_data.get('series_with_multiple_stacks', 0),
            breakdown_by_modality=handover_data.get('breakdown_by_modality', {}),
            metrics=metrics,
        )
    
    elif handover_type == 'step3' or step_id == 'classification':
        # Reconstruct Step3Handover
        from .models import Step3Handover, Step3Metrics
        
        metrics = Step3Metrics(
            total_classified=handover_data.get('total_classified', 0),
            review_required_count=handover_data.get('review_required_count', 0),
        )
        
        return Step3Handover(
            classified_stack_ids=handover_data.get('classified_stack_ids', []),
            stacks_requiring_review=handover_data.get('stacks_requiring_review', []),
            cohort_id=handover_data.get('cohort_id'),
            cohort_name=handover_data.get('cohort_name', ''),
            processing_mode=handover_data.get('processing_mode', 'incremental'),
            total_classified=handover_data.get('total_classified', 0),
            review_required_count=handover_data.get('review_required_count', 0),
            metrics=metrics,
        )
    
    elif handover_type == 'step4' or step_id == 'completion':
        # Reconstruct Step4Handover
        from .models import Step4Handover, Step4Metrics
        
        metrics = Step4Metrics(
            total_processed=handover_data.get('total_completed', 0),
            base_filled_count=handover_data.get('gaps_filled', 0) // 2,  # Approximate
            technique_filled_count=handover_data.get('gaps_filled', 0) // 2,
            misc_resolved_count=handover_data.get('misc_resolved', 0),
        )
        
        return Step4Handover(
            completed_stack_ids=handover_data.get('completed_stack_ids', []),
            stacks_requiring_review=handover_data.get('stacks_requiring_review', []),
            cohort_id=handover_data.get('cohort_id'),
            cohort_name=handover_data.get('cohort_name', ''),
            processing_mode=handover_data.get('processing_mode', 'incremental'),
            total_completed=handover_data.get('total_completed', 0),
            gaps_filled=handover_data.get('gaps_filled', 0),
            misc_resolved=handover_data.get('misc_resolved', 0),
            review_required_count=handover_data.get('review_required_count', 0),
            metrics=metrics,
        )
    
    else:
        logger.warning(f"Unknown handover type: {handover_type} for step: {step_id}")
        return None


def get_step_instance(step_id: str, progress_callback) -> Any:
    """Factory for step instances.
    
    Args:
        step_id: The step ID to instantiate
        progress_callback: Async callback for progress updates
        
    Returns:
        BaseStep instance for the requested step
    """
    if step_id == 'checkup':
        return Step1Checkup(progress_callback)
    elif step_id == 'stack_fingerprint':
        return Step2StackFingerprint(progress_callback)
    elif step_id == 'classification':
        return Step3Classification(progress_callback)
    elif step_id == 'completion':
        return Step4Completion(progress_callback)
    else:
        raise ValueError(f"Unknown step_id: {step_id}")


class SortingService:
    """Service for running the sorting pipeline with progress streaming."""

    def __init__(self):
        """Initialize the sorting service."""
        self._active_jobs: dict[int, asyncio.Event] = {}  # job_id -> cancel_event

    def _get_engine(self):
        """Get the metadata database engine."""
        return metadata_engine

    def _update_cohort_stage_steps(
        self,
        session,
        cohort_id: int,
        completed_step_id: str,
    ) -> None:
        """Mark a sorting step as completed via pipeline service.
        
        Note: The session parameter is kept for backward compatibility but is
        no longer used - pipeline service manages its own sessions.
        """
        # Pipeline service handles step completion
        nils_pipeline_service.complete_step(
            cohort_id=cohort_id,
            stage_id='sort',
            step_id=completed_step_id,
        )

    async def run_pipeline(
        self,
        cohort_id: int,
        job_id: int,
        config: SortingConfig,
    ) -> AsyncGenerator[ProgressEvent, None]:
        """Run the sorting pipeline and yield progress events.

        This is an async generator that yields SSE events as the pipeline progresses.

        Args:
            cohort_id: The cohort to process
            job_id: The job ID for tracking
            config: Sorting configuration

        Yields:
            ProgressEvent objects for SSE streaming
        """
        # Create cancel event for this job
        cancel_event = asyncio.Event()
        self._active_jobs[job_id] = cancel_event

        try:
            engine = self._get_engine()

            with engine.connect() as conn:
                # Get cohort info
                cohort_info = get_cohort_info(conn, cohort_id)
                if not cohort_info:
                    yield ProgressEvent(
                        type="pipeline_error",
                        error=f"Cohort {cohort_id} not found",
                    )
                    return

                cohort_name = cohort_info["name"]

                # Create event queue for progress updates
                progress_queue: asyncio.Queue[StepProgress | None] = asyncio.Queue()

                async def progress_callback(update: StepProgress) -> None:
                    """Callback that puts progress updates on the queue."""
                    await progress_queue.put(update)

                # Build step context
                context = StepContext(
                    cohort_id=cohort_id,
                    cohort_name=cohort_name,
                    config=config,
                    conn=conn,
                    job_id=job_id,
                )

                # ═══════════════════════════════════════════════════════════
                # STEP 1: Checkup - Verify cohort scope and data integrity
                # ═══════════════════════════════════════════════════════════
                step1 = Step1Checkup(progress_callback)

                # Yield step_start event
                yield ProgressEvent(
                    type="step_start",
                    step_id=step1.step_id,
                    step_title=step1.step_title,
                )

                # Run step in background task so we can yield progress
                step_task = asyncio.create_task(step1.execute(context))

                # Yield progress events as they come
                while not step_task.done():
                    # Check for cancellation
                    if cancel_event.is_set():
                        step_task.cancel()
                        yield ProgressEvent(
                            type="pipeline_cancelled",
                            step_id=step1.step_id,
                        )
                        return

                    try:
                        # Wait for progress update with timeout
                        update = await asyncio.wait_for(
                            progress_queue.get(),
                            timeout=0.1,
                        )
                        if update is not None:
                            yield ProgressEvent(
                                type="step_progress",
                                step_id=update.step_id,
                                progress=update.progress,
                                message=update.message,
                                metrics=update.metrics,
                                current_action=update.current_action,
                                logs=update.logs,
                            )
                    except asyncio.TimeoutError:
                        # No update available, check task again
                        continue

                # Get step result
                result: StepResult = await step_task

                # Drain any remaining progress updates
                while not progress_queue.empty():
                    update = progress_queue.get_nowait()
                    if update is not None:
                        yield ProgressEvent(
                            type="step_progress",
                            step_id=update.step_id,
                            progress=update.progress,
                            message=update.message,
                            metrics=update.metrics,
                            logs=update.logs,
                        )

                if result.success:
                    yield ProgressEvent(
                        type="step_complete",
                        step_id=step1.step_id,
                        metrics=result.metrics,
                    )

                    # Commit the transaction (for date imputation updates)
                    conn.commit()

                    # Store handover for next step
                    context.previous_handover = result.handover

                    # ═══════════════════════════════════════════════════════
                    # PERSIST STEP DATA TO APPLICATION DATABASE
                    # ═══════════════════════════════════════════════════════
                    handover = result.handover
                    if handover:
                        try:
                            # Save via pipeline service
                            nils_pipeline_service.save_handover(
                                cohort_id=cohort_id,
                                stage_id='sort',
                                step_id=step1.step_id,
                                handover_data=handover.to_dict() if hasattr(handover, 'to_dict') else handover,
                            )
                            nils_pipeline_service.save_metrics(
                                cohort_id=cohort_id,
                                stage_id='sort',
                                step_id=step1.step_id,
                                metrics=result.metrics,
                            )
                            # Complete the step
                            nils_pipeline_service.complete_step(
                                cohort_id=cohort_id,
                                stage_id='sort',
                                step_id=step1.step_id,
                            )
                            logger.info(
                                "Persisted step '%s' data: %d series, metrics saved",
                                step1.step_id,
                                len(handover.series_ids),
                            )
                        except Exception as e:
                            logger.error("Failed to persist step data: %s", e)
                            # Don't fail the pipeline, just log the error

                    # Log summary
                    logger.info(
                        "Step 1 complete: %d series to process (mode=%s)",
                        len(handover.series_to_process) if handover else 0,
                        handover.processing_mode if handover else "unknown",
                    )
                else:
                    yield ProgressEvent(
                        type="step_error",
                        step_id=step1.step_id,
                        error=result.error,
                        metrics=result.metrics,
                    )
                    # Pipeline stops on error
                    yield ProgressEvent(
                        type="pipeline_error",
                        error=f"Step 1 failed: {result.error}",
                    )
                    return

                # ═══════════════════════════════════════════════════════════
                # STEP 2: Stack Fingerprint - Build classification features
                # ═══════════════════════════════════════════════════════════
                step2 = Step2StackFingerprint(progress_callback)

                # Yield step_start event
                yield ProgressEvent(
                    type="step_start",
                    step_id=step2.step_id,
                    step_title=step2.step_title,
                )

                # Run step in background task so we can yield progress
                step_task = asyncio.create_task(step2.execute(context))

                # Yield progress events as they come
                while not step_task.done():
                    # Check for cancellation
                    if cancel_event.is_set():
                        step_task.cancel()
                        yield ProgressEvent(
                            type="pipeline_cancelled",
                            step_id=step2.step_id,
                        )
                        return

                    try:
                        # Wait for progress update with timeout
                        update = await asyncio.wait_for(
                            progress_queue.get(),
                            timeout=0.1,
                        )
                        if update is not None:
                            yield ProgressEvent(
                                type="step_progress",
                                step_id=update.step_id,
                                progress=update.progress,
                                message=update.message,
                                metrics=update.metrics,
                                current_action=update.current_action,
                                logs=update.logs,
                            )
                    except asyncio.TimeoutError:
                        # No update available, check task again
                        continue

                # Get step result
                result2: StepResult = await step_task

                # Drain any remaining progress updates
                while not progress_queue.empty():
                    update = progress_queue.get_nowait()
                    if update is not None:
                        yield ProgressEvent(
                            type="step_progress",
                            step_id=update.step_id,
                            progress=update.progress,
                            message=update.message,
                            metrics=update.metrics,
                            logs=update.logs,
                        )

                if result2.success:
                    yield ProgressEvent(
                        type="step_complete",
                        step_id=step2.step_id,
                        metrics=result2.metrics,
                    )

                    # Commit the transaction
                    conn.commit()

                    # Store handover for next step
                    context.previous_handover = result2.handover

                    # ═══════════════════════════════════════════════════════
                    # PERSIST STEP DATA TO APPLICATION DATABASE
                    # ═══════════════════════════════════════════════════════
                    handover2 = result2.handover
                    if handover2:
                        try:
                            # Save via pipeline service
                            nils_pipeline_service.save_handover(
                                cohort_id=cohort_id,
                                stage_id='sort',
                                step_id=step2.step_id,
                                handover_data=handover2.to_dict() if hasattr(handover2, 'to_dict') else handover2,
                            )
                            nils_pipeline_service.save_metrics(
                                cohort_id=cohort_id,
                                stage_id='sort',
                                step_id=step2.step_id,
                                metrics=result2.metrics,
                            )
                            # Complete the step
                            nils_pipeline_service.complete_step(
                                cohort_id=cohort_id,
                                stage_id='sort',
                                step_id=step2.step_id,
                            )
                            logger.info(
                                "Persisted step '%s' data: %d stacks, metrics saved",
                                step2.step_id,
                                len(handover2.series_stack_ids),
                            )
                        except Exception as e:
                            logger.error("Failed to persist step data: %s", e)
                            # Don't fail the pipeline, just log the error

                    # Log summary
                    logger.info(
                        "Step 2 complete: %d stacks discovered (%d multi-stack series)",
                        handover2.stacks_discovered if handover2 else 0,
                        handover2.series_with_multiple_stacks if handover2 else 0,
                    )
                else:
                    yield ProgressEvent(
                        type="step_error",
                        step_id=step2.step_id,
                        error=result2.error,
                        metrics=result2.metrics,
                    )
                    # Pipeline stops on error
                    yield ProgressEvent(
                        type="pipeline_error",
                        error=f"Step 2 failed: {result2.error}",
                    )
                    return

                # ═══════════════════════════════════════════════════════════
                # STEP 3: Classification - Classify each stack
                # ═══════════════════════════════════════════════════════════
                step3 = Step3Classification(progress_callback)

                # Yield step_start event
                yield ProgressEvent(
                    type="step_start",
                    step_id=step3.step_id,
                    step_title=step3.step_title,
                )

                # Run step in background task so we can yield progress
                step_task = asyncio.create_task(step3.execute(context))

                # Yield progress events as they come
                while not step_task.done():
                    # Check for cancellation
                    if cancel_event.is_set():
                        step_task.cancel()
                        yield ProgressEvent(
                            type="pipeline_cancelled",
                            step_id=step3.step_id,
                        )
                        return

                    try:
                        # Wait for progress update with timeout
                        update = await asyncio.wait_for(
                            progress_queue.get(),
                            timeout=0.1,
                        )
                        if update is not None:
                            yield ProgressEvent(
                                type="step_progress",
                                step_id=update.step_id,
                                progress=update.progress,
                                message=update.message,
                                metrics=update.metrics,
                                current_action=update.current_action,
                                logs=update.logs,
                            )
                    except asyncio.TimeoutError:
                        # No update available, check task again
                        continue

                # Get step result
                result3: StepResult = await step_task

                # Drain any remaining progress updates
                while not progress_queue.empty():
                    update = progress_queue.get_nowait()
                    if update is not None:
                        yield ProgressEvent(
                            type="step_progress",
                            step_id=update.step_id,
                            progress=update.progress,
                            message=update.message,
                            metrics=update.metrics,
                            logs=update.logs,
                        )

                if result3.success:
                    yield ProgressEvent(
                        type="step_complete",
                        step_id=step3.step_id,
                        metrics=result3.metrics,
                    )

                    # Commit the transaction
                    conn.commit()

                    # Store handover for next step
                    context.previous_handover = result3.handover

                    # ═══════════════════════════════════════════════════════
                    # PERSIST STEP DATA TO APPLICATION DATABASE
                    # ═══════════════════════════════════════════════════════
                    handover3 = result3.handover
                    if handover3:
                        try:
                            # Save via pipeline service
                            nils_pipeline_service.save_handover(
                                cohort_id=cohort_id,
                                stage_id='sort',
                                step_id=step3.step_id,
                                handover_data=handover3.to_dict() if hasattr(handover3, 'to_dict') else handover3,
                            )
                            nils_pipeline_service.save_metrics(
                                cohort_id=cohort_id,
                                stage_id='sort',
                                step_id=step3.step_id,
                                metrics=result3.metrics,
                            )
                            # Complete the step
                            nils_pipeline_service.complete_step(
                                cohort_id=cohort_id,
                                stage_id='sort',
                                step_id=step3.step_id,
                            )
                            logger.info(
                                "Persisted step '%s' data: %d classified, %d requiring review",
                                step3.step_id,
                                handover3.total_classified,
                                handover3.review_required_count,
                            )
                        except Exception as e:
                            logger.error("Failed to persist step data: %s", e)
                            # Don't fail the pipeline, just log the error

                    # Log summary
                    logger.info(
                        "Step 3 complete: %d classified (%d requiring review)",
                        handover3.total_classified if handover3 else 0,
                        handover3.review_required_count if handover3 else 0,
                    )
                else:
                    yield ProgressEvent(
                        type="step_error",
                        step_id=step3.step_id,
                        error=result3.error,
                        metrics=result3.metrics,
                    )
                    # Pipeline stops on error
                    yield ProgressEvent(
                        type="pipeline_error",
                        error=f"Step 3 failed: {result3.error}",
                    )
                    return

                # ═══════════════════════════════════════════════════════════
                # STEP 4: Completion - Fill gaps and flag for review
                # ═══════════════════════════════════════════════════════════
                step4 = Step4Completion(progress_callback)

                # Yield step_start event
                yield ProgressEvent(
                    type="step_start",
                    step_id=step4.step_id,
                    step_title=step4.step_title,
                )

                # Run step in background task so we can yield progress
                step_task = asyncio.create_task(step4.execute(context))

                # Yield progress events as they come
                while not step_task.done():
                    # Check for cancellation
                    if cancel_event.is_set():
                        step_task.cancel()
                        yield ProgressEvent(
                            type="pipeline_cancelled",
                            step_id=step4.step_id,
                        )
                        return

                    try:
                        # Wait for progress update with timeout
                        update = await asyncio.wait_for(
                            progress_queue.get(),
                            timeout=0.1,
                        )
                        if update is not None:
                            yield ProgressEvent(
                                type="step_progress",
                                step_id=update.step_id,
                                progress=update.progress,
                                message=update.message,
                                metrics=update.metrics,
                                current_action=update.current_action,
                                logs=update.logs,
                            )
                    except asyncio.TimeoutError:
                        # No update available, check task again
                        continue

                # Get step result
                result4: StepResult = await step_task

                # Drain any remaining progress updates
                while not progress_queue.empty():
                    update = progress_queue.get_nowait()
                    if update is not None:
                        yield ProgressEvent(
                            type="step_progress",
                            step_id=update.step_id,
                            progress=update.progress,
                            message=update.message,
                            metrics=update.metrics,
                            logs=update.logs,
                        )

                if result4.success:
                    yield ProgressEvent(
                        type="step_complete",
                        step_id=step4.step_id,
                        metrics=result4.metrics,
                    )

                    # Commit the transaction
                    conn.commit()

                    # ═══════════════════════════════════════════════════════
                    # PERSIST STEP DATA TO APPLICATION DATABASE
                    # ═══════════════════════════════════════════════════════
                    handover4 = result4.handover
                    if handover4:
                        try:
                            # Save via pipeline service
                            nils_pipeline_service.save_handover(
                                cohort_id=cohort_id,
                                stage_id='sort',
                                step_id=step4.step_id,
                                handover_data=handover4.to_dict() if hasattr(handover4, 'to_dict') else handover4,
                            )
                            nils_pipeline_service.save_metrics(
                                cohort_id=cohort_id,
                                stage_id='sort',
                                step_id=step4.step_id,
                                metrics=result4.metrics,
                            )
                            # Complete the step
                            nils_pipeline_service.complete_step(
                                cohort_id=cohort_id,
                                stage_id='sort',
                                step_id=step4.step_id,
                            )
                            logger.info(
                                "Persisted step '%s' data: %d completed, %d gaps filled, %d requiring review",
                                step4.step_id,
                                handover4.total_completed,
                                handover4.gaps_filled,
                                handover4.review_required_count,
                            )
                        except Exception as e:
                            logger.error("Failed to persist step data: %s", e)
                            # Don't fail the pipeline, just log the error

                    # Log summary
                    logger.info(
                        "Step 4 complete: %d completed (%d gaps filled, %d requiring review)",
                        handover4.total_completed if handover4 else 0,
                        handover4.gaps_filled if handover4 else 0,
                        handover4.review_required_count if handover4 else 0,
                    )
                else:
                    yield ProgressEvent(
                        type="step_error",
                        step_id=step4.step_id,
                        error=result4.error,
                        metrics=result4.metrics,
                    )
                    # Pipeline stops on error
                    yield ProgressEvent(
                        type="pipeline_error",
                        error=f"Step 4 failed: {result4.error}",
                    )
                    return

                # ═══════════════════════════════════════════════════════════
                # PIPELINE COMPLETE
                # ═══════════════════════════════════════════════════════════

                # Yield pipeline completion
                summary = {
                    "steps_completed": 4,
                    "total_steps": 4,  # Steps 1-4 implemented
                    "series_to_process": result.metrics.get("series_to_process_count", 0),
                    "stacks_discovered": result2.metrics.get("total_fingerprints_created", 0) if result2.success else 0,
                    "stacks_classified": result3.metrics.get("total_classified", 0) if result3.success else 0,
                    "gaps_filled": result4.metrics.get("base_filled_count", 0) + result4.metrics.get("technique_filled_count", 0) if result4.success else 0,
                    "review_required": result4.metrics.get("stacks_newly_flagged", 0) + result3.metrics.get("review_required_count", 0) if result4.success else 0,
                    "processing_mode": result.handover.processing_mode if result.handover else "unknown",
                }

                yield ProgressEvent(
                    type="pipeline_complete",
                    summary=summary,
                )

        except Exception as e:
            logger.exception("Pipeline failed with exception")
            yield ProgressEvent(
                type="pipeline_error",
                error=str(e),
            )
        finally:
            # Clean up
            self._active_jobs.pop(job_id, None)

    async def run_single_step(
        self,
        cohort_id: int,
        job_id: int,
        step_id: str,
        config: SortingConfig,
        preview_mode: bool = False,
    ) -> AsyncGenerator[ProgressEvent, None]:
        """Run a single step independently (step-wise execution).

        This method enables the new step-wise execution pattern where:
        - User controls each step individually  
        - Can review results before proceeding
        - Can change config and re-run from any step
        
        Args:
            cohort_id: The cohort to process
            job_id: The job ID for tracking
            step_id: The step to run
            config: Sorting configuration
            preview_mode: If True, generate preview without DB insert (Step 2 only)

        Yields:
            ProgressEvent objects for SSE streaming
        """
        # Register job for cancellation
        cancel_event = asyncio.Event()
        self._active_jobs[job_id] = cancel_event

        try:
            # ═════════════════════════════════════════════════════════════════
            # LOAD PREVIOUS HANDOVER (if not first step)
            # ═════════════════════════════════════════════════════════════════
            previous_step_id = get_previous_step(step_id)
            previous_handover = None
            
            if previous_step_id:
                # Load handover from previous step via pipeline service
                handover_data = nils_pipeline_service.get_handover(
                    cohort_id=cohort_id,
                    stage_id='sort',
                    step_id=previous_step_id,
                )
                
                if not handover_data:
                    # Previous step hasn't been run yet
                    yield ProgressEvent(
                        type="step_error",
                        step_id=step_id,
                        error=f"Cannot run step '{step_id}': previous step '{previous_step_id}' has not been completed yet",
                    )
                    return
                
                # Reconstruct handover object
                engine = self._get_engine()
                with engine.begin() as conn:
                    previous_handover = reconstruct_handover(
                        previous_step_id,
                        handover_data,
                        conn,
                    )
                
                if not previous_handover:
                    yield ProgressEvent(
                        type="step_error",
                        step_id=step_id,
                        error=f"Failed to reconstruct handover from step '{previous_step_id}'",
                    )
                    return

            # ═════════════════════════════════════════════════════════════════
            # INITIALIZE STEP
            # ═════════════════════════════════════════════════════════════════
            
            async def progress_callback(
                percentage: float,
                message: str,
                details: dict | None = None,
            ):
                if cancel_event.is_set():
                    raise asyncio.CancelledError("Job cancelled by user")
                yield ProgressEvent(
                    type="step_progress",
                    step_id=step_id,
                    percentage=percentage,
                    message=message,
                    details=details or {},
                )

            # Create progress queue for step communication
            progress_queue: asyncio.Queue[StepProgress | None] = asyncio.Queue()
            
            async def progress_callback(update: StepProgress) -> None:
                """Callback that puts progress updates on the queue."""
                await progress_queue.put(update)
            
            # Get step instance
            try:
                step = get_step_instance(step_id, progress_callback)
            except ValueError as e:
                yield ProgressEvent(
                    type="step_error",
                    step_id=step_id,
                    error=str(e),
                )
                return

            # ═════════════════════════════════════════════════════════════════
            # EXECUTE STEP
            # ═════════════════════════════════════════════════════════════════
            
            engine = self._get_engine()
            with engine.begin() as conn:
                # Get cohort info
                cohort_info = get_cohort_info(conn, cohort_id)
                
                # Build context
                context = StepContext(
                    conn=conn,
                    cohort_id=cohort_id,
                    cohort_name=cohort_info['name'],
                    config=config,
                    previous_handover=previous_handover,
                    preview_mode=preview_mode,
                )

                # Announce step start
                yield ProgressEvent(
                    type="step_start",
                    step_id=step_id,
                    step_title=step.step_title,
                )

                # Execute step as background task
                step_task = asyncio.create_task(step.execute(context))
                
                # Yield progress events as they come
                while not step_task.done():
                    # Check cancellation
                    if cancel_event.is_set():
                        step_task.cancel()
                        yield ProgressEvent(
                            type="step_cancelled",
                            step_id=step_id,
                            message="Step cancelled by user",
                        )
                        return
                    
                    try:
                        # Wait for progress update with timeout
                        update = await asyncio.wait_for(
                            progress_queue.get(),
                            timeout=0.1,
                        )
                        if update is not None:
                            yield ProgressEvent(
                                type="step_progress",
                                step_id=update.step_id,
                                progress=update.progress,
                                message=update.message,
                                metrics=update.metrics,
                                current_action=update.current_action,
                                logs=update.logs,
                            )
                    except asyncio.TimeoutError:
                        # No update available, check task again
                        continue

                # Get step result
                try:
                    result: StepResult = await step_task
                    
                    # Drain any remaining progress updates
                    while not progress_queue.empty():
                        update = progress_queue.get_nowait()
                        if update is not None:
                            yield ProgressEvent(
                                type="step_progress",
                                step_id=update.step_id,
                                progress=update.progress,
                                message=update.message,
                                metrics=update.metrics,
                                logs=update.logs,
                            )
                    
                    if result.success:
                        # ═════════════════════════════════════════════════════
                        # PERSIST STEP DATA TO APPLICATION DATABASE
                        # ═════════════════════════════════════════════════════
                        handover = result.handover
                        
                        try:
                            # Save handover for next step (if exists)
                            if handover:
                                nils_pipeline_service.save_handover(
                                    cohort_id=cohort_id,
                                    stage_id='sort',
                                    step_id=step_id,
                                    handover_data=handover.to_dict() if hasattr(handover, 'to_dict') else handover,
                                )
                                # Mark step as completed
                                nils_pipeline_service.complete_step(
                                    cohort_id=cohort_id,
                                    stage_id='sort',
                                    step_id=step_id,
                                )
                            
                            # Save metrics for UI display (always save, even in preview mode)
                            nils_pipeline_service.save_metrics(
                                cohort_id=cohort_id,
                                stage_id='sort',
                                step_id=step_id,
                                metrics=result.metrics,
                            )
                            logger.info(
                                "Persisted step '%s' data for cohort %d (handover: %s)",
                                step_id,
                                cohort_id,
                                "yes" if handover else "no (preview mode)",
                            )
                        except Exception as e:
                            logger.error("Failed to persist step data: %s", e)
                            # Don't fail the pipeline, just log the error

                        # Announce completion
                        yield ProgressEvent(
                            type="step_complete",
                            step_id=step_id,
                            metrics=result.metrics,
                        )
                    else:
                        # Step failed
                        yield ProgressEvent(
                            type="step_error",
                            step_id=step_id,
                            error=result.error or "Step failed",
                        )

                except asyncio.CancelledError:
                    yield ProgressEvent(
                        type="step_cancelled",
                        step_id=step_id,
                        message="Step cancelled by user",
                    )
                    return
                except Exception as e:
                    logger.error("Step '%s' failed with exception: %s", step_id, e, exc_info=True)
                    yield ProgressEvent(
                        type="step_error",
                        step_id=step_id,
                        error=f"Step failed: {str(e)}",
                    )
                    return

        finally:
            # Clean up
            self._active_jobs.pop(job_id, None)

    async def run_step(
        self,
        cohort_id: int,
        job_id: int,
        step_id: str,
        config: SortingConfig,
    ) -> AsyncGenerator[ProgressEvent, None]:
        """Run a specific step (for re-running a completed step).

        Args:
            cohort_id: The cohort to process
            job_id: The job ID for tracking
            step_id: The specific step to run
            config: Sorting configuration

        Yields:
            ProgressEvent objects for SSE streaming
        """
        # For now, only support re-running Step 1
        if step_id != SortingStepId.CHECKUP.value:
            yield ProgressEvent(
                type="step_error",
                step_id=step_id,
                error=f"Re-running step '{step_id}' is not yet supported",
            )
            return

        # Run the full pipeline starting from Step 1
        async for event in self.run_pipeline(cohort_id, job_id, config):
            yield event

    def cancel_job(self, job_id: int) -> bool:
        """Cancel a running sorting job.

        Args:
            job_id: The job ID to cancel

        Returns:
            True if job was found and cancelled, False otherwise
        """
        cancel_event = self._active_jobs.get(job_id)
        if cancel_event:
            cancel_event.set()
            return True
        return False

    def get_step_metadata(self) -> list[dict[str, Any]]:
        """Get metadata for all sorting steps.

        Returns:
            List of step metadata dicts for UI display
        """
        return [
            {
                "id": step["id"].value,
                "title": step["title"],
                "description": step["description"],
            }
            for step in SORTING_STEPS
        ]


# Singleton instance
sorting_service = SortingService()
