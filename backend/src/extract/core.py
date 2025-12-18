"""Extraction orchestrator."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional


logger = logging.getLogger(__name__)


def _job_tag(job_id: Optional[int]) -> str:
    return f"job_id={job_id}" if job_id is not None else "job_id=adhoc"


def _metric_summary(metrics: dict) -> str:
    keys = ["subjects", "studies", "series", "instances"]
    parts = []
    for key in keys:
        value = metrics.get(key)
        if value is not None:
            parts.append(f"{key}={value}")
    return ", ".join(parts) if parts else f"metrics_keys={','.join(sorted(str(k) for k in metrics.keys()))}"

from .batching import BatchSizeController, BatchSizeSettings
from .config import ExtractionConfig
from .limits import calculate_safe_instance_batch_rows
from .process_pool import extract_subjects_parallel, SubjectBatchResult
from .resume_index import ExistingPathIndex, SubjectPathEntry, build_existing_path_index
from .profiler import ExtractionProfiler, set_global_profiler
from .scanner import SubjectFolder, discover_subjects
from .subject_mapping import SubjectResolver
from .worker import extract_subject_batches, plan_subject_series
from .writer import Writer
from .writer_pool import WriterPool
from jobs.control import JobControl
from jobs.errors import JobCancelledError


ProgressCallback = Callable[[int, int], Awaitable[None] | None]


@dataclass
class ExtractionResult:
    total_subjects: int
    baseline_completed: int
    completed_total: int
    metrics: dict[str, int]


_QUEUE_TIMEOUT_SECONDS = 0.5


async def _control_checkpoint(control: Optional[JobControl], job_id: Optional[int]) -> None:
    if control is None:
        return
    await control.checkpoint(job_id)


async def _queue_put(
    queue: asyncio.Queue,
    item,
    control: Optional[JobControl],
    job_id: Optional[int],
) -> None:
    from .profiler import get_global_profiler
    profiler = get_global_profiler()
    
    while True:
        if control:
            await control.checkpoint(job_id)
        queue_start = time.perf_counter()
        try:
            await asyncio.wait_for(queue.put(item), timeout=_QUEUE_TIMEOUT_SECONDS)
            if profiler:
                profiler.record("queue_put", time.perf_counter() - queue_start)
            return
        except asyncio.TimeoutError:
            continue


async def _run_async_process_pool(
    config: ExtractionConfig,
    progress: Optional[ProgressCallback],
    job_id: Optional[int],
    control: Optional[JobControl],
) -> ExtractionResult:
    """Run extraction using ProcessPoolExecutor for true CPU parallelism.
    
    This implementation uses process-based workers for DICOM parsing,
    bypassing Python's GIL to achieve true parallel processing.
    """
    job_tag = _job_tag(job_id)
    logger.info(
        "Extraction start %s cohort_id=%s cohort=%s raw_root=%s resume=%s resume_by_path=%s",
        job_tag,
        config.cohort_id,
        config.cohort_name,
        config.raw_root,
        config.resume,
        config.resume_by_path,
    )
    # Initialize profiler
    profiler = ExtractionProfiler()
    set_global_profiler(profiler)
    profiler.start()
    
    # Phase 1: Subject Discovery
    discovery_start = time.perf_counter()
    subjects = list(discover_subjects(config.raw_root))
    profiler.record("subject_discovery", time.perf_counter() - discovery_start)
    
    total_subjects = len(subjects)
    logger.info("Extraction discovery %s subjects=%d", job_tag, total_subjects)
    processed_subjects = 0
    resume_index: ExistingPathIndex | None = None
    resume_subject_filters: dict[str, SubjectPathEntry] | None = None

    if config.resume and config.resume_by_path and subjects:
        subject_keys = [subject.subject_key for subject in subjects]
        resume_index = build_existing_path_index(config.cohort_id, subject_keys)
        resume_subject_filters = {
            key: entry
            for key in subject_keys
            if (entry := resume_index.entry_for(key)) is not None
        }
        top_counts = sorted(
            ((key, len(entry)) for key, entry in resume_subject_filters.items()),
            key=lambda item: item[1],
            reverse=True,
        )
        summary = ", ".join(f"{key}={count}" for key, count in top_counts[:5])
        logger.info(
            "%s resume_paths files=%d subjects=%d%s",
            job_tag,
            resume_index.total_paths,
            len(resume_subject_filters),
            f" (top: {summary})" if summary else "",
        )

    if progress:
        await _maybe_await(progress(processed_subjects, total_subjects))

    try:
        subject_resolver = SubjectResolver(
            subject_code_map=config.subject_code_map,
            seed=config.resolved_subject_code_seed(),
        )
    except ValueError as exc:
        raise RuntimeError(f"Invalid subject mapping configuration: {exc}") from exc

    safe_batch_rows = calculate_safe_instance_batch_rows()
    min_size = max(1, min(config.min_batch_size, safe_batch_rows))
    max_size = max(min_size, min(config.max_batch_size, safe_batch_rows))
    initial_size = max(min_size, min(config.batch_size, max_size))
    batch_settings = BatchSizeSettings(
        initial=initial_size,
        minimum=min_size,
        maximum=max_size,
        target_ms=config.target_tx_ms,
        enabled=config.adaptive_batching_enabled,
    )
    batch_controller = BatchSizeController(batch_settings)

    # Determine number of worker processes and DB writers
    num_workers = config.process_pool_workers or config.max_workers
    num_db_writers = config.db_writer_pool_size
    logger.info(
        "Extraction execution %s workers=%d writer_mode=%s db_writers=%d queue=%d",
        job_tag,
        num_workers,
        "pool" if num_db_writers > 1 else "single",
        num_db_writers,
        config.queue_size,
    )
    
    # Use writer pool if multiple writers configured, otherwise single writer
    use_writer_pool = num_db_writers > 1
    
    if use_writer_pool:
        logger.info("%s writer_pool concurrency=%d", job_tag, num_db_writers)
        writer_pool = WriterPool(
            config=config,
            num_writers=num_db_writers,
            job_id=job_id,
            progress_cb=progress,
            batch_controller=batch_controller,
            control=control,
            path_index=resume_index,
        )
        writer = None
        queue = None
        writer_task = None
    else:
        logger.info("%s writer_pool single-writer", job_tag)
        queue = asyncio.Queue(maxsize=config.queue_size)
        writer = Writer(
            config=config,
            queue=queue,
            job_id=job_id,
            progress_cb=progress,
            batch_controller=batch_controller,
            control=control,
            path_index=resume_index,
        )
        writer_pool = None
    
    # Context manager for writer or writer_pool
    writer_context = writer_pool if use_writer_pool else writer
    
    async with writer_context:
        if use_writer_pool:
            await writer_pool.start_writers(total_subjects)
        else:
            writer_task = asyncio.create_task(writer.consume(total_subjects))
        
        try:
            # Run ProcessPoolExecutor directly without blocking
            # Results stream as workers complete, allowing parallel DB writes
            loop = asyncio.get_event_loop()
            
            async def process_with_streaming_pool():
                """Process subjects using ProcessPoolExecutor with true result streaming.
                
                Results are sent to the queue as each worker completes, allowing
                DB writes to happen in parallel with DICOM parsing.
                """
                import queue as queue_module
                from threading import Thread
                
                # Build resume tokens (empty for now, will be implemented properly)
                resume_tokens: dict[str, str] = {}
                
                # Create a thread-safe queue for streaming results from process pool
                result_queue: queue_module.Queue = queue_module.Queue()
                
                def run_pool_and_stream():
                    """Run ProcessPoolExecutor and stream results to queue."""
                    try:
                        # Extract subjects in parallel, results yield as they complete
                        for result in extract_subjects_parallel(
                            subjects=subjects,
                            config=config,
                            resolver=subject_resolver,
                            max_workers=num_workers,
                            resume_tokens=resume_tokens,
                            resume_paths=resume_subject_filters,
                        ):
                            result_queue.put(("result", result))
                    except Exception as e:
                        result_queue.put(("error", e))
                    finally:
                        result_queue.put(("done", None))
                
                # Start process pool in background thread
                logger.info("Extraction streaming %s starting process pool", job_tag)
                pool_thread = Thread(target=run_pool_and_stream, daemon=True)
                pool_thread.start()
                
                # Stream results to async queue as they arrive
                while True:
                    # Get result from thread-safe queue (non-blocking via run_in_executor)
                    msg_type, data = await loop.run_in_executor(None, result_queue.get)
                    
                    if msg_type == "done":
                        break
                    elif msg_type == "error":
                        raise data
                    elif msg_type == "result":
                        result = data
                        await _control_checkpoint(control, job_id)
                        
                        # Send each batch to the writer pool or single writer
                        for batch, last_uid in result.batches:
                            if writer_pool:
                                await writer_pool.put_batch(
                                    result.subject_key,
                                    None,
                                    batch,
                                    last_uid,
                                    False,
                                )
                            else:
                                await _queue_put(
                                    queue,
                                    (result.subject_key, None, batch, last_uid, False),
                                    control,
                                    job_id,
                                )
                        
                        # Mark subject complete
                        if writer_pool:
                            await writer_pool.put_batch(
                                result.subject_key,
                                None,
                                None,
                                None,
                                True,
                            )
                        else:
                            await _queue_put(
                                queue,
                                (result.subject_key, None, None, None, True),
                                control,
                                job_id,
                            )
                        
                        # Update progress
                        nonlocal processed_subjects
                        processed_subjects += 1
                        if progress:
                            await _maybe_await(progress(processed_subjects, total_subjects))
                
                # Wait for thread to finish
                await loop.run_in_executor(None, pool_thread.join)
                
                # Signal writer(s) we're done
                if writer_pool:
                    await writer_pool.signal_completion()
                else:
                    await _queue_put(queue, None, control, job_id)
            
            await process_with_streaming_pool()
            
            # Wait for writers to complete
            if writer_pool:
                await writer_pool.wait_for_completion()
            else:
                await writer_task
            
        except JobCancelledError:
            if writer_pool:
                for task in writer_pool.writer_tasks:
                    task.cancel()
                await asyncio.gather(*writer_pool.writer_tasks, return_exceptions=True)
            else:
                writer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, JobCancelledError):
                    await writer_task
            raise
        except Exception as exc:
            if writer_pool:
                for task in writer_pool.writer_tasks:
                    task.cancel()
                await asyncio.gather(*writer_pool.writer_tasks, return_exceptions=True)
            else:
                writer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, JobCancelledError):
                    await writer_task
            raise

    # Aggregate metrics from writer(s)
    if writer_pool:
        metrics = writer_pool.aggregate_metrics()
    else:
        metrics = writer.snapshot_metrics()
    
    # Stop profiler and log results
    profiler.stop()
    profiler.log_summary()
    
    # Add profiler metrics to result
    perf_summary = profiler.get_summary()
    metrics["performance"] = perf_summary
    
    # Clean up global profiler
    set_global_profiler(None)
    
    result = ExtractionResult(
        total_subjects=total_subjects,
        baseline_completed=0,
        completed_total=processed_subjects,
        metrics=metrics,
    )
    logger.info("Extraction complete %s processed_subjects=%d %s", job_tag, processed_subjects, _metric_summary(metrics))
    return result


async def _run_async(
    config: ExtractionConfig,
    progress: Optional[ProgressCallback],
    job_id: Optional[int],
    control: Optional[JobControl],
) -> ExtractionResult:
    job_tag = _job_tag(job_id)
    logger.info(
        "Extraction start %s cohort_id=%s cohort=%s raw_root=%s resume=%s resume_by_path=%s",
        job_tag,
        config.cohort_id,
        config.cohort_name,
        config.raw_root,
        config.resume,
        config.resume_by_path,
    )
    # Initialize profiler
    profiler = ExtractionProfiler()
    set_global_profiler(profiler)
    profiler.start()
    
    # Phase 1: Subject Discovery
    discovery_start = time.perf_counter()
    subjects = list(discover_subjects(config.raw_root))
    profiler.record("subject_discovery", time.perf_counter() - discovery_start)
    
    total_subjects = len(subjects)
    logger.info("Extraction discovery %s subjects=%d", job_tag, total_subjects)
    processed_subjects = 0
    baseline_completed = 0
    resume_index: ExistingPathIndex | None = None

    if config.resume and config.resume_by_path and subjects:
        subject_keys = [subject.subject_key for subject in subjects]
        resume_index = build_existing_path_index(config.cohort_id, subject_keys)
        entries = {
            key: resume_index.entry_for(key)
            for key in subject_keys
            if resume_index.entry_for(key) is not None
        }
        top_counts = sorted(
            ((key, len(entry)) for key, entry in entries.items()),
            key=lambda item: item[1],
            reverse=True,
        )
        summary = ", ".join(f"{key}={count}" for key, count in top_counts[:5])
        logger.info(
            "%s resume_paths files=%d subjects=%d%s",
            job_tag,
            resume_index.total_paths,
            len(entries),
            f" (top: {summary})" if summary else "",
        )

    if progress:
        await _maybe_await(progress(processed_subjects, total_subjects))

    try:
        subject_resolver = SubjectResolver(
            subject_code_map=config.subject_code_map,
            seed=config.resolved_subject_code_seed(),
        )
    except ValueError as exc:
        raise RuntimeError(f"Invalid subject mapping configuration: {exc}") from exc

    safe_batch_rows = calculate_safe_instance_batch_rows()
    min_size = max(1, min(config.min_batch_size, safe_batch_rows))
    max_size = max(min_size, min(config.max_batch_size, safe_batch_rows))
    initial_size = max(min_size, min(config.batch_size, max_size))
    batch_settings = BatchSizeSettings(
        initial=initial_size,
        minimum=min_size,
        maximum=max_size,
        target_ms=config.target_tx_ms,
        enabled=config.adaptive_batching_enabled,
    )
    batch_controller = BatchSizeController(batch_settings)

    queue: asyncio.Queue = asyncio.Queue(maxsize=config.queue_size)
    writer = Writer(
        config=config,
        queue=queue,
        job_id=job_id,
        progress_cb=progress,
        batch_controller=batch_controller,
        control=control,
        path_index=resume_index,
    )

    async with writer:
        writer_task = asyncio.create_task(writer.consume(total_subjects))
        processed = processed_subjects
        progress_lock = asyncio.Lock()
        sem = asyncio.Semaphore(config.max_workers)

        async def handle_subject(subject: SubjectFolder) -> None:
            nonlocal processed
            await _control_checkpoint(control, job_id)
            # Normal processing
            subject_filter = resume_index.entry_for(subject.subject_key) if resume_index else None
            await _process_subject(
                subject,
                config,
                queue,
                subject_resolver,
                batch_controller,
                control,
                job_id,
                subject_filter,
            )

            if progress:
                async with progress_lock:
                    processed += 1
                    await _maybe_await(progress(processed, total_subjects))

        async def subject_worker(subject: SubjectFolder) -> None:
            async with sem:
                await handle_subject(subject)

        tasks: list[asyncio.Task] = []

        async def _shutdown_tasks(exc: Exception | None = None) -> None:
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            writer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, JobCancelledError):
                await writer_task
            if exc:
                raise exc

        try:
            for subject in subjects:
                await _control_checkpoint(control, job_id)
                tasks.append(asyncio.create_task(subject_worker(subject)))

            await asyncio.gather(*tasks)
            await _queue_put(queue, None, control, job_id)
            await writer_task
        except JobCancelledError as exc:
            await _shutdown_tasks(exc)
        except Exception as exc:
            await _shutdown_tasks(exc)

    metrics = writer.snapshot_metrics()
    
    # Stop profiler and log results
    profiler.stop()
    profiler.log_summary()
    
    # Add profiler metrics to result
    perf_summary = profiler.get_summary()
    metrics["performance"] = perf_summary
    
    # Clean up global profiler
    set_global_profiler(None)
    
    result = ExtractionResult(
        total_subjects=total_subjects,
        baseline_completed=baseline_completed,
        completed_total=processed,
        metrics=metrics,
    )
    logger.info("Extraction complete %s processed_subjects=%d %s", job_tag, processed, _metric_summary(metrics))
    return result


async def _process_subject(
    subject: SubjectFolder,
    config: ExtractionConfig,
    queue: asyncio.Queue,
    subject_resolver: SubjectResolver,
    batch_controller: BatchSizeController,
    control: Optional[JobControl],
    job_id: Optional[int],
    path_filter: SubjectPathEntry | None,
) -> None:
    if config.series_workers_per_subject <= 1:
        resume_token = None
        last_instance = None
        for batch, last_instance in extract_subject_batches(
            subject=subject,
            extension_mode=config.extension_mode.value,
            resume_instance=resume_token,
            batch_size=config.batch_size,
            subject_resolver=subject_resolver,
            use_specific_tags=config.use_specific_tags,
            batch_controller=batch_controller,
            path_filter=path_filter,
        ):
            await _control_checkpoint(control, job_id)
            await _queue_put(queue, (subject.subject_key, None, batch, last_instance, False), control, job_id)
        await _queue_put(queue, (subject.subject_key, None, None, last_instance, True), control, job_id)
        return

    series_plans = plan_subject_series(
        subject=subject,
        extension_mode=config.extension_mode.value,
        resume_tokens={},
        use_specific_tags=config.use_specific_tags,
        path_filter=path_filter,
    )
    if not series_plans:
        await queue.put((subject.subject_key, None, None, None, True))
        return

    sem = asyncio.Semaphore(config.series_workers_per_subject)

    async def process_plan(plan) -> None:
        async with sem:
            await _control_checkpoint(control, job_id)
            last_instance = None
            for batch, last_instance in extract_subject_batches(
                subject=subject,
                extension_mode=config.extension_mode.value,
                resume_instance=None,
                batch_size=config.batch_size,
                subject_resolver=subject_resolver,
                use_specific_tags=config.use_specific_tags,
                batch_controller=batch_controller,
                paths=plan.paths,
                path_filter=path_filter,
            ):
                await _queue_put(queue, (subject.subject_key, plan.series_uid, batch, last_instance, False), control, job_id)

    await asyncio.gather(*(process_plan(plan) for plan in series_plans))
    await _queue_put(queue, (subject.subject_key, None, None, None, True), control, job_id)


async def _maybe_await(result: Awaitable[None] | None) -> None:
    if result is None:
        return
    await result


def run_extraction(
    config: ExtractionConfig,
    progress: Optional[ProgressCallback] = None,
    job_id: Optional[int] = None,
    control: Optional[JobControl] = None,
) -> ExtractionResult:
    """Run DICOM metadata extraction.
    
    Automatically selects the optimal implementation based on configuration:
    - If use_process_pool=True (default): Use ProcessPoolExecutor for true CPU parallelism
    - If use_process_pool=False: Use legacy asyncio implementation
    """
    job_tag = _job_tag(job_id)
    try:
        if config.use_process_pool:
            return asyncio.run(_run_async_process_pool(config, progress, job_id, control))
        return asyncio.run(_run_async(config, progress, job_id, control))
    except Exception:
        logger.exception("Extraction failure %s", job_tag)
        raise
