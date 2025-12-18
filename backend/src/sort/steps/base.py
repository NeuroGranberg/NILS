"""Base class for sorting pipeline steps."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, TypeVar

from sqlalchemy.engine import Connection

from ..models import SortingConfig, StepProgress, StepStatus


@dataclass
class StepContext:
    """Context passed to each step during execution."""

    cohort_id: int
    cohort_name: str
    config: SortingConfig
    conn: Connection
    job_id: int | None = None

    # Carried forward from previous steps
    previous_handover: Any = None
    
    # Preview mode: generate preview without DB insert
    preview_mode: bool = False


@dataclass
class StepResult:
    """Result of a step execution."""

    success: bool
    handover: Any = None  # Data passed to next step
    error: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)


# Type alias for progress callback
ProgressCallback = Callable[[StepProgress], Awaitable[None]]

T = TypeVar("T")


class BaseStep(ABC):
    """Abstract base class for sorting pipeline steps.

    Each step:
    1. Receives a context with cohort info, config, and database connection
    2. Emits progress updates via the progress_callback
    3. Returns a StepResult with success/failure and data for the next step
    """

    # Subclasses must define these
    step_id: str
    step_title: str
    
    # Maximum log lines to keep in buffer (rolling window)
    MAX_LOG_LINES = 100

    def __init__(self, progress_callback: ProgressCallback | None = None):
        """Initialize the step with an optional progress callback.

        Args:
            progress_callback: Async function to call with progress updates.
                              If None, progress updates are silently ignored.
        """
        self._progress_callback = progress_callback
        self._log_buffer: list[str] = []

    def log(self, message: str) -> None:
        """Add a log message to the buffer.
        
        Log messages are included in progress updates and streamed to the frontend.
        The buffer is a rolling window of MAX_LOG_LINES entries.
        
        Args:
            message: Log message to add
        """
        timestamp = datetime.now().strftime("%H:%M:%S")
        self._log_buffer.append(f"[{timestamp}] {message}")
        
        # Keep buffer size bounded
        if len(self._log_buffer) > self.MAX_LOG_LINES:
            self._log_buffer.pop(0)

    def clear_logs(self) -> None:
        """Clear the log buffer."""
        self._log_buffer.clear()

    def get_logs(self) -> list[str]:
        """Get a copy of the current log buffer."""
        return self._log_buffer.copy()

    async def emit_progress(
        self,
        progress: int,
        message: str,
        metrics: dict[str, Any] | None = None,
        current_action: str | None = None,
    ) -> None:
        """Send a progress update to the frontend.

        Args:
            progress: Percentage complete (0-100)
            message: Human-readable status message
            metrics: Optional metrics dict for UI display
            current_action: Optional current action label
        """
        if self._progress_callback is None:
            return

        update = StepProgress(
            step_id=self.step_id,
            status=StepStatus.RUNNING,
            progress=progress,
            message=message,
            metrics=metrics or {},
            current_action=current_action,
            logs=self.get_logs(),  # Include recent log lines
        )
        await self._progress_callback(update)

    async def emit_error(self, error: str, metrics: dict[str, Any] | None = None) -> None:
        """Send an error update to the frontend.

        Args:
            error: Error message
            metrics: Optional metrics dict for UI display
        """
        if self._progress_callback is None:
            return

        update = StepProgress(
            step_id=self.step_id,
            status=StepStatus.ERROR,
            progress=0,
            message=f"Error: {error}",
            metrics=metrics or {},
            error=error,
        )
        await self._progress_callback(update)

    async def emit_complete(self, metrics: dict[str, Any] | None = None) -> None:
        """Send a completion update to the frontend.

        Args:
            metrics: Final metrics dict for UI display
        """
        if self._progress_callback is None:
            return

        update = StepProgress(
            step_id=self.step_id,
            status=StepStatus.COMPLETE,
            progress=100,
            message="Complete",
            metrics=metrics or {},
        )
        await self._progress_callback(update)

    async def emit_warning(self, metrics: dict[str, Any] | None = None) -> None:
        """Emit step warning status (completed with data quality issues).

        Args:
            metrics: Final metrics dict for UI display
        """
        if self._progress_callback is None:
            return

        update = StepProgress(
            step_id=self.step_id,
            status=StepStatus.WARNING,
            progress=100,
            message="Complete with warnings",
            metrics=metrics or {},
        )
        await self._progress_callback(update)

    @abstractmethod
    async def execute(self, context: StepContext) -> StepResult:
        """Execute the step.

        Args:
            context: Step execution context

        Returns:
            StepResult with success status and handover data for next step
        """
        pass
