from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Final

from .errors import JobCancelledError


logger = logging.getLogger(__name__)

_DEFAULT_SLEEP_SECONDS: Final[float] = 0.2


@dataclass
class JobControl:
    """Lightweight cooperative control signals for long-running jobs."""

    _stopped: bool = False
    _paused: bool = False
    _sleep_seconds: float = field(default=_DEFAULT_SLEEP_SECONDS, init=False, repr=False)

    def cancel(self) -> None:
        if self._stopped:
            return
        logger.info("Signal: Cancel requested")
        self._stopped = True

    def pause(self) -> None:
        if self._paused or self._stopped:
            return
        logger.info("Signal: Pause requested")
        self._paused = True

    def resume(self) -> None:
        if not self._paused:
            return
        logger.info("Signal: Resume requested")
        self._paused = False

    def reset(self) -> None:
        logger.info("Signal: Control reset")
        self._stopped = False
        self._paused = False

    async def wait_if_paused(self) -> None:
        """Suspend progress while paused without blocking the loop."""

        while self._paused and not self._stopped:
            await asyncio.sleep(self._sleep_seconds)

    def wait_if_paused_blocking(self) -> None:
        """Blocking variant for non-async contexts (CLI, synchronous loops)."""

        import time

        while self._paused and not self._stopped:
            time.sleep(self._sleep_seconds)

    @property
    def should_stop(self) -> bool:
        return self._stopped

    @property
    def is_paused(self) -> bool:
        return self._paused

    async def checkpoint(self, job_id: int | None = None) -> None:
        """Raise if canceled and wait out pauses before proceeding."""

        if self._stopped:
            raise JobCancelledError(job_id)
        await self.wait_if_paused()
        if self._stopped:
            raise JobCancelledError(job_id)

    def checkpoint_blocking(self, job_id: int | None = None) -> None:
        if self._stopped:
            raise JobCancelledError(job_id)
        self.wait_if_paused_blocking()
        if self._stopped:
            raise JobCancelledError(job_id)
