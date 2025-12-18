"""Inline job runner that executes the anonymization pipeline (V2) and optional compression."""

from __future__ import annotations

import time
from typing import Callable

from anonymize import run_anonymization
from anonymize.config import AnonymizeConfig

from compress.config import CompressionConfig
from compress.engine import run_compression

from .service import job_service


def run_anonymize_job(job_id: int, config: AnonymizeConfig) -> None:
    last_percent = -1
    last_update_at = 0.0
    MIN_INTERVAL = 5.0

    def progress_callback(done: int, total: int, subjects_seen: int | None = None) -> None:
        nonlocal last_percent, last_update_at
        now = time.monotonic()
        if now - last_update_at < MIN_INTERVAL:
            return
        total_subjects = getattr(config, "total_subjects", None)

        if subjects_seen is not None and total_subjects and total_subjects > 0:
            percent = int(subjects_seen * 100 / total_subjects)
        elif total > 0:
            # Fallback: file-based progress if subject info unavailable
            percent = int(done * 100 / total)
        else:
            return

        percent = max(0, min(99, percent))
        if percent <= last_percent:
            return
        job_service.update_progress(job_id, percent)
        last_percent = percent
        last_update_at = now

    job_service.mark_running(job_id)
    try:
        run_anonymization(config, progress=progress_callback, job_id=job_id)
    except Exception as exc:  # pragma: no cover - defensive
        job_service.mark_failed(job_id, str(exc))
        raise
    else:
        job_service.mark_completed(job_id)


def run_compress_job(job_id: int, config: CompressionConfig) -> None:
    def progress_callback(done: int, total: int) -> None:
        percent = int((done / total) * 100) if total else 0
        job_service.update_progress(job_id, percent)

    job_service.mark_running(job_id)
    try:
        run_compression(config, progress=progress_callback)
    except Exception as exc:  # pragma: no cover - defensive
        job_service.mark_failed(job_id, str(exc))
        raise
    else:
        job_service.mark_completed(job_id)
