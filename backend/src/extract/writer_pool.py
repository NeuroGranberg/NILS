"""Writer pool for parallel database writes.

This module provides multiple concurrent database writers to eliminate
the single-writer bottleneck identified in performance profiling.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional

from .batching import BatchSizeController
from .config import ExtractionConfig
from .resume_index import ExistingPathIndex
from .writer import Writer, ProgressCallback
from jobs.control import JobControl


logger = logging.getLogger(__name__)


class WriterPool:
    """Manages multiple concurrent database writers for parallel writes.
    
    Distributes batches across writers using subject-based hashing to
    maintain cache efficiency (same subject always goes to same writer).
    """
    
    def __init__(
        self,
        *,
        config: ExtractionConfig,
        num_writers: int,
        job_id: Optional[int],
        progress_cb: Optional[ProgressCallback],
        batch_controller: BatchSizeController,
        control: Optional[JobControl] = None,
        path_index: Optional[ExistingPathIndex] = None,
    ):
        self.config = config
        self.num_writers = num_writers
        self.job_id = job_id
        self.progress_cb = progress_cb
        self.batch_controller = batch_controller
        self.control = control
        self.path_index = path_index
        
        # Create queues and writers
        self.queues: List[asyncio.Queue] = []
        self.writers: List[Writer] = []
        self.writer_tasks: List[asyncio.Task] = []
        
        # Routing: subject_key -> writer_index
        self._subject_routing: Dict[str, int] = {}
        self._next_writer_idx = 0
        
    async def __aenter__(self) -> "WriterPool":
        """Initialize all writers and start their tasks."""
        logger.info(f"Initializing writer pool with {self.num_writers} concurrent writers")
        
        # Create queues and writers
        for i in range(self.num_writers):
            queue = asyncio.Queue(maxsize=self.config.queue_size)
            self.queues.append(queue)
            
            writer = Writer(
                config=self.config,
                queue=queue,
                job_id=self.job_id,
                progress_cb=self.progress_cb if i == 0 else None,  # Only first writer reports progress
                batch_controller=self.batch_controller,
                control=self.control,
                path_index=self.path_index,
            )
            self.writers.append(writer)
            
            # Enter writer context
            await writer.__aenter__()
        
        return self
    
    async def __aexit__(self, exc_type, exc, tb):
        """Cleanup all writers."""
        # Stop all writer tasks
        for task in self.writer_tasks:
            if not task.done():
                task.cancel()
        
        if self.writer_tasks:
            await asyncio.gather(*self.writer_tasks, return_exceptions=True)
        
        # Exit all writer contexts
        for writer in self.writers:
            await writer.__aexit__(exc_type, exc, tb)
    
    async def start_writers(self, total_subjects: int):
        """Start all writer consume tasks."""
        for i, writer in enumerate(self.writers):
            task = asyncio.create_task(
                writer.consume(total_subjects),
                name=f"writer-{i}"
            )
            self.writer_tasks.append(task)
    
    def route_subject(self, subject_key: str) -> int:
        """Determine which writer should handle batches for this subject.
        
        Uses consistent hashing to ensure the same subject always goes to
        the same writer, maintaining cache efficiency.
        """
        if subject_key not in self._subject_routing:
            # Hash-based routing for cache efficiency
            writer_idx = hash(subject_key) % self.num_writers
            self._subject_routing[subject_key] = writer_idx
            logger.debug(f"Routing subject {subject_key} to writer {writer_idx}")
        
        return self._subject_routing[subject_key]
    
    async def put_batch(
        self,
        subject_key: str,
        series_uid: Optional[str],
        batch: Optional[list],
        last_instance: Optional[str],
        completed: bool,
    ):
        """Put a batch item into the appropriate writer's queue.
        
        Routes based on subject_key to maintain cache efficiency.
        """
        writer_idx = self.route_subject(subject_key)
        queue = self.queues[writer_idx]
        
        item = (subject_key, series_uid, batch, last_instance, completed)
        await queue.put(item)
    
    async def signal_completion(self):
        """Signal all writers that processing is complete."""
        logger.info("Signaling completion to all writers")
        for i, queue in enumerate(self.queues):
            await queue.put(None)
            logger.debug(f"Sent completion signal to writer {i}")
    
    async def wait_for_completion(self):
        """Wait for all writer tasks to complete."""
        if self.writer_tasks:
            await asyncio.gather(*self.writer_tasks, return_exceptions=True)
    
    def aggregate_metrics(self) -> dict:
        """Aggregate metrics from all writers."""
        aggregated = {
            "subjects": 0,
            "studies": 0,
            "series": 0,
            "instances": 0,
        }
        safe_batch_limit: int | None = None
        
        for writer in self.writers:
            metrics = writer.snapshot_metrics()
            for key in aggregated:
                if key in metrics:
                    aggregated[key] += metrics[key]
            safe_value = metrics.get("safe_batch_rows")
            if safe_value is not None:
                safe_batch_limit = safe_value if safe_batch_limit is None else min(safe_batch_limit, safe_value)

        if safe_batch_limit is not None:
            aggregated["safe_batch_rows"] = safe_batch_limit
        
        logger.info(
            f"Aggregated metrics: {aggregated['subjects']} subjects, "
            f"{aggregated['studies']} studies, {aggregated['series']} series, "
            f"{aggregated['instances']} instances"
        )
        
        return aggregated


async def create_writer_pool(
    *,
    config: ExtractionConfig,
    num_writers: int,
    job_id: Optional[int],
    progress_cb: Optional[ProgressCallback],
    batch_controller: BatchSizeController,
    control: Optional[JobControl] = None,
) -> WriterPool:
    """Create and initialize a writer pool.
    
    Args:
        config: Extraction configuration
        num_writers: Number of concurrent writers (2-8 recommended)
        job_id: Optional job ID for tracking
        progress_cb: Optional progress callback
        batch_controller: Batch size controller
        control: Optional job control for pause/cancel
    
    Returns:
        Initialized WriterPool ready to accept batches
    """
    pool = WriterPool(
        config=config,
        num_writers=num_writers,
        job_id=job_id,
        progress_cb=progress_cb,
        batch_controller=batch_controller,
        control=control,
    )
    
    return pool
