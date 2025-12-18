"""SQLAlchemy models and DTOs for QC Pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator
from sqlalchemy import (
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    Index,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from cohorts.models import Base


# =============================================================================
# SQLAlchemy Models (Application DB)
# =============================================================================


class QCSession(Base):
    """QC session for a cohort - tracks overall QC progress."""

    __tablename__ = "qc_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cohort_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    status: Mapped[str] = mapped_column(
        String(20), default="pending", nullable=False
    )  # pending, in_progress, completed, abandoned
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    total_items: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    reviewed_items: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    confirmed_items: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Relationships
    items: Mapped[list["QCItem"]] = relationship(
        "QCItem",
        back_populates="session",
        cascade="all, delete-orphan",
        order_by="QCItem.priority.desc(), QCItem.id",
    )


class QCItem(Base):
    """Individual QC item - a stack requiring review."""

    __tablename__ = "qc_items"
    __table_args__ = (
        Index("ix_qc_items_session_category", "session_id", "category"),
        Index("ix_qc_items_series_uid", "series_instance_uid"),
        UniqueConstraint(
            "session_id",
            "series_instance_uid",
            "stack_index",
            "category",
            name="uq_qc_items_session_series_stack_category",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    session_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("qc_sessions.id", ondelete="CASCADE"), nullable=False
    )
    category: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # base, provenance, technique, body_part, contrast
    series_instance_uid: Mapped[str] = mapped_column(String(128), nullable=False)
    study_instance_uid: Mapped[str] = mapped_column(String(128), nullable=False)
    stack_index: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), default="pending", nullable=False
    )  # pending, reviewed, confirmed, skipped
    priority: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False
    )  # Higher = more urgent
    review_reasons_csv: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    confirmed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    session: Mapped["QCSession"] = relationship("QCSession", back_populates="items")
    draft_changes: Mapped[list["QCDraftChange"]] = relationship(
        "QCDraftChange",
        back_populates="item",
        cascade="all, delete-orphan",
        order_by="QCDraftChange.field_name",
    )


class QCDraftChange(Base):
    """Draft change for a QC item - stored until user confirms."""

    __tablename__ = "qc_draft_changes"
    __table_args__ = (
        UniqueConstraint("item_id", "field_name", name="uq_draft_change_item_field"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    item_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("qc_items.id", ondelete="CASCADE"), nullable=False
    )
    field_name: Mapped[str] = mapped_column(
        String(50), nullable=False
    )  # base, technique, provenance, etc.
    original_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    new_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    change_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # Relationships
    item: Mapped["QCItem"] = relationship("QCItem", back_populates="draft_changes")


# =============================================================================
# Pydantic DTOs (API Models)
# =============================================================================


class QCDraftChangeDTO(BaseModel):
    """DTO for draft change."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    item_id: int
    field_name: str
    original_value: Optional[str] = None
    new_value: Optional[str] = None
    change_reason: Optional[str] = None


class QCClassificationDTO(BaseModel):
    """Classification data from metadata DB."""

    series_stack_id: int
    series_instance_uid: str
    study_instance_uid: str
    stack_index: int = 0

    # Subject/Study context
    subject_id: Optional[str] = None
    study_date: Optional[str] = None
    series_time: Optional[str] = None  # For sorting by acquisition time

    # Classification fields
    directory_type: Optional[str] = None
    base: Optional[str] = None
    technique: Optional[str] = None
    modifier_csv: Optional[str] = None
    construct_csv: Optional[str] = None
    provenance: Optional[str] = None
    acceleration_csv: Optional[str] = None
    post_contrast: Optional[int] = None
    localizer: Optional[int] = None
    spinal_cord: Optional[int] = None

    # Review flags
    manual_review_required: Optional[int] = None
    manual_review_reasons_csv: Optional[str] = None

    # Geometry (for body part QC)
    aspect_ratio: Optional[float] = None
    fov_x_mm: Optional[float] = None
    fov_y_mm: Optional[float] = None
    slices_count: Optional[int] = None

    # Orientation (from stack_fingerprint)
    orientation: Optional[str] = None  # Axial/Coronal/Sagittal

    # Series info
    series_description: Optional[str] = None
    modality: Optional[str] = None


class QCRuleViolationDTO(BaseModel):
    """Rule violation detected during QC."""

    rule_id: str
    category: str
    severity: str  # error, warning, info
    message: str


class QCItemDTO(BaseModel):
    """DTO for QC item with enriched data."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    session_id: int
    category: str
    series_instance_uid: str
    study_instance_uid: str
    stack_index: int = 0
    status: str
    priority: int = 0
    review_reasons: list[str] = []
    created_at: datetime
    updated_at: datetime
    reviewed_at: Optional[datetime] = None
    confirmed_at: Optional[datetime] = None

    # Enriched data (from metadata DB)
    classification: Optional[QCClassificationDTO] = None
    draft_changes: list[QCDraftChangeDTO] = []
    rule_violations: list[QCRuleViolationDTO] = []

    @field_validator("review_reasons", mode="before")
    @classmethod
    def parse_review_reasons(cls, v):
        if v is None:
            return []
        if isinstance(v, str):
            return [r.strip() for r in v.split(",") if r.strip()]
        return v


class QCSessionDTO(BaseModel):
    """DTO for QC session."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    cohort_id: int
    status: str
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_items: int = 0
    reviewed_items: int = 0
    confirmed_items: int = 0

    # Computed fields
    category_counts: dict[str, int] = {}


# =============================================================================
# Request Payloads
# =============================================================================


class CreateQCSessionPayload(BaseModel):
    """Payload for creating a new QC session."""

    cohort_id: int
    categories: list[str] = [
        "base",
        "provenance",
        "technique",
        "body_part",
        "contrast",
    ]
    include_flagged_only: bool = True


class UpdateQCItemPayload(BaseModel):
    """Payload for updating a QC item (saving draft changes)."""

    changes: dict[str, Optional[str]]  # field_name -> new_value
    change_reason: Optional[str] = None


class ConfirmQCChangesPayload(BaseModel):
    """Payload for confirming QC changes."""

    item_ids: list[int]
