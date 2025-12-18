"""System routes for health checks and resource monitoring."""

from __future__ import annotations

import asyncio
import time

import psutil
from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import text

from extract.limits import calculate_safe_instance_batch_rows
from metadata_db.session import SessionLocal as MetadataSessionLocal


router = APIRouter(prefix="/api", tags=["system"])


class SystemResourcesResponse(BaseModel):
    cpu_count: int
    memory_total: int
    memory_available: int
    disk_read_bytes_per_sec: float
    disk_write_bytes_per_sec: float
    recommended_processes: int
    recommended_workers: int
    recommended_queue_depth: int
    recommended_batch_size: int
    recommended_adaptive_min_batch: int
    recommended_adaptive_max_batch: int
    recommended_series_workers_per_subject: int
    recommended_db_writer_pool: int
    safe_instance_batch_rows: int
    max_workers_cap: int
    max_batch_cap: int
    max_queue_cap: int
    max_adaptive_batch_cap: int
    max_db_writer_pool_cap: int


class HealthResponse(BaseModel):
    status: str


class ReadinessResponse(BaseModel):
    status: str
    database: str


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Basic health check endpoint."""
    return {"status": "healthy"}


@router.get("/ready", response_model=ReadinessResponse)
async def readiness_check():
    """Readiness check that verifies database connectivity."""
    try:
        with MetadataSessionLocal() as session:
            session.execute(text("SELECT 1"))
        return {"status": "ready", "database": "connected"}
    except Exception:
        return {"status": "not_ready", "database": "disconnected"}


@router.get("/system/resources", response_model=SystemResourcesResponse)
async def get_system_resources():
    """Get system resource information and recommendations."""
    cpu_count = psutil.cpu_count(logical=True) or 1
    mem = psutil.virtual_memory()
    total_gb = mem.total / (1024 ** 3)
    available_gb = mem.available / (1024 ** 3)

    io_before = psutil.disk_io_counters()
    start = time.perf_counter()
    await asyncio.sleep(0.1)  # Non-blocking sleep
    elapsed = max(time.perf_counter() - start, 1e-3)
    io_after = psutil.disk_io_counters()

    read_bytes_per_sec = 0.0
    write_bytes_per_sec = 0.0
    if io_before and io_after:
        read_bytes_per_sec = max(0.0, (io_after.read_bytes - io_before.read_bytes) / elapsed)
        write_bytes_per_sec = max(0.0, (io_after.write_bytes - io_before.write_bytes) / elapsed)

    max_workers_cap = max(4, min(cpu_count, 128))
    max_queue_cap = 500
    max_batch_cap = 5000
    max_adaptive_batch_cap = 20000
    max_db_writer_pool_cap = 16
    safe_instance_batch_rows = calculate_safe_instance_batch_rows()

    def clamp(value: float | int, lower: int, upper: int) -> int:
        return max(lower, min(int(value), upper)) if upper >= lower else int(value)

    if available_gb >= 256:
        worker_ratio = 0.85
    elif available_gb >= 128:
        worker_ratio = 0.75
    elif available_gb >= 64:
        worker_ratio = 0.6
    elif available_gb >= 32:
        worker_ratio = 0.5
    else:
        worker_ratio = 0.35

    recommended_workers = clamp(max(1, round(cpu_count * worker_ratio)), 1, max_workers_cap)
    recommended_processes_seed = max(1, round(cpu_count * 0.5))
    recommended_processes = clamp(min(recommended_workers, recommended_processes_seed), 1, max_workers_cap)
    recommended_queue_depth = clamp(max(10, recommended_workers * 2), 10, max_queue_cap)

    if available_gb >= 256:
        recommended_batch_size = 2000
    elif available_gb >= 128:
        recommended_batch_size = 1500
    elif available_gb >= 64:
        recommended_batch_size = 1000
    elif available_gb >= 32:
        recommended_batch_size = 750
    else:
        recommended_batch_size = 250
    recommended_batch_size = min(recommended_batch_size, max_batch_cap, safe_instance_batch_rows)

    recommended_adaptive_min_batch = max(
        50,
        min(recommended_batch_size // 2, recommended_batch_size, safe_instance_batch_rows),
    )
    recommended_adaptive_max_batch = max(
        recommended_batch_size,
        min(recommended_batch_size * 4, max_adaptive_batch_cap, safe_instance_batch_rows),
    )

    if recommended_workers >= 64:
        recommended_db_writer_pool = 6
    elif recommended_workers >= 24:
        recommended_db_writer_pool = 4
    else:
        recommended_db_writer_pool = 3
    recommended_db_writer_pool = min(max_db_writer_pool_cap, recommended_db_writer_pool)

    recommended_series_workers_per_subject = clamp(max(1, cpu_count // 24), 1, 8)

    return SystemResourcesResponse(
        cpu_count=cpu_count,
        memory_total=int(mem.total),
        memory_available=int(mem.available),
        disk_read_bytes_per_sec=read_bytes_per_sec,
        disk_write_bytes_per_sec=write_bytes_per_sec,
        recommended_processes=recommended_processes,
        recommended_workers=recommended_workers,
        recommended_queue_depth=recommended_queue_depth,
        recommended_batch_size=recommended_batch_size,
        recommended_adaptive_min_batch=recommended_adaptive_min_batch,
        recommended_adaptive_max_batch=recommended_adaptive_max_batch,
        recommended_series_workers_per_subject=recommended_series_workers_per_subject,
        recommended_db_writer_pool=recommended_db_writer_pool,
        safe_instance_batch_rows=safe_instance_batch_rows,
        max_workers_cap=max_workers_cap,
        max_batch_cap=max_batch_cap,
        max_queue_cap=max_queue_cap,
        max_adaptive_batch_cap=max_adaptive_batch_cap,
        max_db_writer_pool_cap=max_db_writer_pool_cap,
    )
