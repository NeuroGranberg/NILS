"""SQLAlchemy models and DTOs for job tracking."""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from sqlalchemy import JSON, Column, DateTime, Enum, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class JobStatus(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    stage: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), default=JobStatus.QUEUED, nullable=False)
    config: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    progress: Mapped[int] = mapped_column(Integer, default=0)
    metrics: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    runs: Mapped[list[JobRun]] = relationship("JobRun", back_populates="job", cascade="all, delete-orphan")


class JobRun(Base):
    __tablename__ = "job_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"))
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), default=JobStatus.RUNNING)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    progress: Mapped[int] = mapped_column(Integer, default=0)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    metrics: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    job: Mapped[Job] = relationship("Job", back_populates="runs")


class JobDTO(BaseModel):
    id: int
    name: Optional[str]
    stage: str
    status: JobStatus
    progress: int
    created_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    last_error: Optional[str]
    config: dict
    metrics: Optional[dict] = None

    model_config = {
        "from_attributes": True,
    }


class JobRunDTO(BaseModel):
    id: int
    job_id: int
    status: JobStatus
    progress: int
    started_at: datetime
    finished_at: Optional[datetime]
    message: Optional[str]
    metrics: Optional[dict] = None

    model_config = {
        "from_attributes": True,
    }
