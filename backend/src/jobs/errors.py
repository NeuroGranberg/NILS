"""Custom job-related exceptions."""

from __future__ import annotations


class JobCancelledError(RuntimeError):
    """Raised when a running job is canceled via control signal."""

    def __init__(self, job_id: int | None = None, message: str | None = None) -> None:
        base = message or "Job canceled by user request"
        if job_id is not None:
            base = f"Job {job_id} canceled by user request"
        super().__init__(base)
