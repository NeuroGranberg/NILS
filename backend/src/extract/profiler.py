"""Performance profiling and instrumentation for extraction pipeline."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, List, Optional


logger = logging.getLogger(__name__)


@dataclass
class OperationStats:
    """Statistics for a single operation type."""
    count: int = 0
    total_duration: float = 0.0
    min_duration: float = float('inf')
    max_duration: float = 0.0
    
    def record(self, duration: float) -> None:
        self.count += 1
        self.total_duration += duration
        self.min_duration = min(self.min_duration, duration)
        self.max_duration = max(self.max_duration, duration)
    
    @property
    def avg_duration(self) -> float:
        return self.total_duration / self.count if self.count > 0 else 0.0
    
    def to_dict(self) -> dict:
        return {
            "count": self.count,
            "total_seconds": round(self.total_duration, 3),
            "avg_seconds": round(self.avg_duration, 6),
            "min_seconds": round(self.min_duration, 6) if self.min_duration != float('inf') else 0.0,
            "max_seconds": round(self.max_duration, 6),
        }


@dataclass
class ExtractionProfiler:
    """Thread-safe performance profiler for extraction operations.
    
    Tracks timing for key operations:
    - subject_discovery: Filesystem scanning for subject folders
    - dicom_parsing: Individual pydicom.dcmread() calls
    - batch_assembly: Grouping instances into batches
    - db_write_batch: Database batch write operations
    - db_commit: Database commit operations
    - queue_put: Time waiting to put items in queue
    - queue_get: Time waiting to get items from queue
    """
    
    _stats: Dict[str, OperationStats] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _start_time: Optional[float] = None
    _end_time: Optional[float] = None
    _files_processed: int = 0
    
    def start(self) -> None:
        """Mark the start of the extraction process."""
        self._start_time = time.perf_counter()
    
    def stop(self) -> None:
        """Mark the end of the extraction process."""
        self._end_time = time.perf_counter()
    
    def record(self, operation: str, duration: float) -> None:
        """Record a timing measurement for an operation."""
        with self._lock:
            if operation not in self._stats:
                self._stats[operation] = OperationStats()
            self._stats[operation].record(duration)
    
    def increment_files(self, count: int = 1) -> None:
        """Increment the count of files processed."""
        with self._lock:
            self._files_processed += count
    
    @contextmanager
    def timing(self, operation: str):
        """Context manager for timing a code block.
        
        Usage:
            with profiler.timing("dicom_parsing"):
                dataset = pydicom.dcmread(path)
        """
        start = time.perf_counter()
        try:
            yield
        finally:
            duration = time.perf_counter() - start
            self.record(operation, duration)
    
    def get_stats(self) -> Dict[str, OperationStats]:
        """Get a copy of current statistics."""
        with self._lock:
            return dict(self._stats)
    
    def get_summary(self) -> dict:
        """Get a summary of all collected metrics."""
        with self._lock:
            stats = dict(self._stats)
            start = self._start_time
            end = self._end_time or time.perf_counter()
            files = self._files_processed
        
        total_duration = (end - start) if start else 0.0
        
        summary = {
            "total_duration_seconds": round(total_duration, 3),
            "files_processed": files,
            "throughput_files_per_second": round(files / total_duration, 2) if total_duration > 0 else 0.0,
            "operations": {}
        }
        
        # Add operation stats
        for operation, op_stats in stats.items():
            summary["operations"][operation] = op_stats.to_dict()
        
        # Calculate time breakdown percentages
        if total_duration > 0:
            breakdown = {}
            for operation, op_stats in stats.items():
                percentage = (op_stats.total_duration / total_duration) * 100
                breakdown[operation] = round(percentage, 1)
            summary["time_breakdown_percent"] = breakdown
        
        return summary
    
    def log_summary(self, level: int = logging.INFO) -> None:
        """Log a formatted summary of performance metrics."""
        summary = self.get_summary()
        
        logger.log(level, "=" * 60)
        logger.log(level, "EXTRACTION PERFORMANCE REPORT")
        logger.log(level, "=" * 60)
        logger.log(level, f"Total Duration: {summary['total_duration_seconds']:.2f} seconds")
        logger.log(level, f"Files Processed: {summary['files_processed']:,}")
        logger.log(level, f"Throughput: {summary['throughput_files_per_second']:.2f} files/sec")
        logger.log(level, "")
        
        if "time_breakdown_percent" in summary:
            logger.log(level, "Time Breakdown:")
            breakdown = summary["time_breakdown_percent"]
            ops = summary["operations"]
            
            # Sort by percentage descending
            sorted_ops = sorted(breakdown.items(), key=lambda x: x[1], reverse=True)
            
            for operation, percentage in sorted_ops:
                op_stats = ops[operation]
                logger.log(
                    level,
                    f"  {operation:20s}: {percentage:5.1f}% "
                    f"({op_stats['total_seconds']:8.2f}s, "
                    f"{op_stats['count']:,} calls, "
                    f"avg={op_stats['avg_seconds']*1000:.2f}ms)"
                )
            
            logger.log(level, "")
            
            # Identify bottleneck
            if sorted_ops:
                bottleneck = sorted_ops[0][0]
                bottleneck_pct = sorted_ops[0][1]
                logger.log(level, f"PRIMARY BOTTLENECK: {bottleneck} ({bottleneck_pct:.1f}% of time)")
        
        logger.log(level, "=" * 60)
    
    def save_json(self, file_path: str) -> None:
        """Save metrics to a JSON file for analysis."""
        summary = self.get_summary()
        with open(file_path, 'w') as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Performance metrics saved to {file_path}")


# Global profiler instance (optional, can also be passed explicitly)
_global_profiler: Optional[ExtractionProfiler] = None


def get_global_profiler() -> Optional[ExtractionProfiler]:
    """Get the global profiler instance if it exists."""
    return _global_profiler


def set_global_profiler(profiler: Optional[ExtractionProfiler]) -> None:
    """Set the global profiler instance."""
    global _global_profiler
    _global_profiler = profiler


def create_profiler() -> ExtractionProfiler:
    """Create a new profiler instance."""
    return ExtractionProfiler()
