"""ORM model for pipeline step tracking.

This module defines the NilsDatasetPipelineStep model which stores the state
of each stage/step in a cohort's processing pipeline.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional, TYPE_CHECKING

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

# Import Base from cohorts.models since we have a relationship to Cohort
# Note: The jobs.models.Job FK works because both use the same database
from cohorts.models import Base

if TYPE_CHECKING:
    from cohorts.models import Cohort


def _utc_now() -> datetime:
    """Get current UTC timestamp."""
    return datetime.now(timezone.utc)


class NilsDatasetPipelineStep(Base):
    """Tracks state for each stage/step in a cohort's processing pipeline.
    
    This table replaces:
    - cohorts.stages JSON column
    - sorting_step_handover table
    - sorting_step_metrics table
    
    For simple stages (anonymize, extract, bids), step_id is NULL.
    For multi-step stages (sort), each step is a separate row.
    
    Example rows for cohort_id=1:
        | sort_order | stage_id  | step_id         | status    | progress |
        |------------|-----------|-----------------|-----------|----------|
        | 0          | anonymize | NULL            | completed | 100      |
        | 1          | extract   | NULL            | completed | 100      |
        | 2          | sort      | checkup         | completed | 100      |
        | 3          | sort      | stack_discovery | running   | 45       |
        | 4          | sort      | classify        | blocked   | 0        |
        | 5          | bids      | NULL            | blocked   | 0        |
    """
    
    __tablename__ = "nils_dataset_pipeline_steps"
    __table_args__ = (
        UniqueConstraint(
            "cohort_id", "stage_id", "step_id",
            name="uq_nils_pipeline_step",
        ),
        Index("ix_nils_pipeline_steps_cohort", "cohort_id"),
        Index(
            "ix_nils_pipeline_steps_running",
            "status",
            postgresql_where="status = 'running'",
        ),
    )

    # Primary key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign key to cohort (cascade delete when cohort is deleted)
    cohort_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("cohorts.id", ondelete="CASCADE"),
        nullable=False,
    )
    
    # Stage identification
    stage_id: Mapped[str] = mapped_column(String(20), nullable=False)
    step_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    
    # Display metadata
    title: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # State
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="blocked",
    )
    progress: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    
    # Configuration (stage-specific settings)
    config: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    
    # Current job (real FK with referential integrity!)
    # Note: use_alter=True defers FK creation to avoid SQLAlchemy Base metadata
    # resolution issues (Job uses a different Base class than Cohort)
    current_job_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("jobs.id", ondelete="SET NULL", use_alter=True),
        nullable=True,
    )
    
    # Handover data (step-to-step data passing for sorting steps)
    handover_data: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    
    # Metrics (completion metrics for UI display)
    metrics: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    
    # Ordering within the pipeline
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utc_now,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=_utc_now,
        onupdate=_utc_now,
    )
    
    # Relationships
    cohort: Mapped["Cohort"] = relationship(
        "Cohort",
        back_populates="pipeline_steps",
    )
    # Note: We don't create a relationship to Job because Job is in a different
    # SQLAlchemy Base (jobs.models.Base vs cohorts.models.Base).
    # The FK constraint is still enforced at the database level.
    # To access the job, use: job_service.get_job(step.current_job_id)
    
    def __repr__(self) -> str:
        step_part = f"/{self.step_id}" if self.step_id else ""
        return (
            f"<NilsDatasetPipelineStep("
            f"cohort_id={self.cohort_id}, "
            f"stage={self.stage_id}{step_part}, "
            f"status={self.status}, "
            f"progress={self.progress}%"
            f")>"
        )
    
    @property
    def is_simple_stage(self) -> bool:
        """Check if this is a simple stage (no sub-steps)."""
        return self.step_id is None
    
    @property
    def is_completed(self) -> bool:
        """Check if this step is completed."""
        return self.status == "completed"
    
    @property
    def is_running(self) -> bool:
        """Check if this step is currently running."""
        return self.status == "running"
    
    @property
    def is_blocked(self) -> bool:
        """Check if this step is blocked."""
        return self.status == "blocked"
    
    @property
    def can_run(self) -> bool:
        """Check if this step can be run (pending or failed)."""
        return self.status in ("pending", "failed")
