"""Helpers for reporting extraction progress with resume awareness."""

from __future__ import annotations

from typing import Callable, Optional


class ExtractionProgressTracker:
    """Convert subject-level progress callbacks into percentage updates.

    The tracker records the baseline number of completed subjects on the first
    callback invocation, so resumed runs start from the correct percentage.
    """

    def __init__(self, send: Callable[[int], None]) -> None:
        self._send = send
        self._baseline: Optional[int] = None
        self._total: Optional[int] = None
        self._last_percent: Optional[int] = None

    @property
    def baseline_completed(self) -> int:
        return self._baseline or 0

    @property
    def total_subjects(self) -> int:
        return self._total or 0

    def update(self, processed: int, total: int) -> None:
        if self._baseline is None:
            self._baseline = processed

        if self._total is None or total > self._total:
            self._total = total

        total_subjects = self._total or 0
        if total_subjects <= 0:
            percent = 100
        else:
            baseline = self._baseline or 0
            delta = max(processed - baseline, 0)
            completed = baseline + delta
            percent_raw = int((completed * 100) / total_subjects)
            if completed < total_subjects:
                percent = min(max(percent_raw, 0), 99)
            else:
                percent = 100

        if percent != self._last_percent:
            self._send(percent)
            self._last_percent = percent

    def finalize(self) -> None:
        if self._last_percent != 100:
            self._send(100)
            self._last_percent = 100
