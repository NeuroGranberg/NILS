"""SQLAlchemy models and DTOs for cohort tracking."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING

from pydantic import BaseModel, ConfigDict
from sqlalchemy import JSON, DateTime, Integer, String, Text, Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

if TYPE_CHECKING:
    from nils_dataset_pipeline.models import NilsDatasetPipelineStep


class Base(DeclarativeBase):
    pass


class Cohort(Base):
    __tablename__ = "cohorts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False, unique=True)
    source_path: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tags: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    anonymization_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)
    status: Mapped[str] = mapped_column(String(50), default='idle', nullable=False)
    total_subjects: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    total_sessions: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    total_series: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    completion_percentage: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Pipeline state is stored in nils_dataset_pipeline_steps table
    pipeline_steps: Mapped[list["NilsDatasetPipelineStep"]] = relationship(
        "NilsDatasetPipelineStep",
        back_populates="cohort",
        cascade="all, delete-orphan",
        order_by="NilsDatasetPipelineStep.sort_order",
    )


class CohortDTO(BaseModel):
    """Data transfer object for cohort API responses."""
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    name: str
    source_path: str
    description: Optional[str] = None
    tags: list[str] = []
    anonymization_enabled: bool = False
    created_at: datetime
    updated_at: datetime
    status: str = 'idle'
    total_subjects: int = 0
    total_sessions: int = 0
    total_series: int = 0
    completion_percentage: int = 0
    # stages is populated from pipeline_steps by the service layer
    stages: list[dict] = []


class CreateCohortPayload(BaseModel):
    """Payload for creating a new cohort."""
    name: str
    source_path: str
    description: Optional[str] = None
    tags: list[str] = []
    anonymization_enabled: bool = False
    anonymize_config: Optional[dict] = None
