"""Adaptive batch sizing utilities for metadata extraction."""

from __future__ import annotations

import threading
from dataclasses import dataclass


@dataclass
class BatchSizeSettings:
    initial: int
    minimum: int
    maximum: int
    target_ms: int
    enabled: bool


class BatchSizeController:
    def __init__(self, settings: BatchSizeSettings) -> None:
        self._settings = settings
        self._lock = threading.Lock()
        self._current = max(settings.minimum, min(settings.initial, settings.maximum))

    def current_size(self) -> int:
        with self._lock:
            return self._current

    def record(self, instances_written: int, duration_seconds: float) -> None:
        if instances_written <= 0 or not self._settings.enabled:
            return
        duration_ms = duration_seconds * 1000.0
        with self._lock:
            current = self._current
            target = self._settings.target_ms
            updated = current
            if duration_ms < target / 2 and current < self._settings.maximum:
                updated = min(self._settings.maximum, max(current + 1, int(current * 1.25)))
            elif duration_ms > target * 2 and current > self._settings.minimum:
                updated = max(self._settings.minimum, max(1, int(current / 1.25)))
            if updated != current:
                self._current = updated
