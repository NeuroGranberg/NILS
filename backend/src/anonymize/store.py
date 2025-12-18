"""Database persistence for anonymization audit records."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import JSON, DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from db.session import engine, session_scope
from jobs.models import Base


class AnonymizeStudyAudit(Base):
    __tablename__ = "anonymize_study_audit"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    study_instance_uid: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    cohort_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    leaf_rel_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class AnonymizeLeafSummary(Base):
    __tablename__ = "anonymize_leaf_summary"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    study_instance_uid: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    cohort_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    leaf_rel_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    files_total: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    files_written: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    files_reused: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    files_with_errors: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    summary: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


_tables_initialized = False


def _ensure_tables() -> None:
    """Initialize anonymization audit tables if not already done."""
    global _tables_initialized
    if _tables_initialized:
        return
    Base.metadata.create_all(engine)
    _tables_initialized = True


def study_audit_exists(study_uid: Optional[str]) -> bool:
    _ensure_tables()
    if not study_uid:
        return False
    with session_scope() as session:
        return (
            session.query(AnonymizeStudyAudit)
            .filter(AnonymizeStudyAudit.study_instance_uid == study_uid)
            .limit(1)
            .count()
            > 0
        )


def mark_study_audit_complete(
    study_uid: Optional[str], *, leaf_rel_path: Optional[str] = None, cohort_name: Optional[str] = None
) -> None:
    if not study_uid:
        return
    _ensure_tables()
    with session_scope() as session:
        existing = (
            session.query(AnonymizeStudyAudit)
            .filter(AnonymizeStudyAudit.study_instance_uid == study_uid)
            .one_or_none()
        )
        if existing:
            return
        session.add(
            AnonymizeStudyAudit(
                study_instance_uid=study_uid,
                cohort_name=cohort_name,
                leaf_rel_path=leaf_rel_path,
            )
        )


def record_leaf_audit_summary(
    study_uid: Optional[str],
    *,
    cohort_name: Optional[str],
    leaf_rel_path: Optional[str],
    files_total: int,
    files_written: int,
    files_reused: int,
    files_with_errors: int,
    patient_id_original: Optional[str],
    patient_id_updated: Optional[str],
    errors: list[str],
    audit_payload: Optional[dict] = None,
) -> None:
    if not study_uid:
        return

    summary_payload = {
        "patient_id_original": patient_id_original,
        "patient_id_updated": patient_id_updated,
        "errors": errors,
    }
    if audit_payload:
        summary_payload["audit"] = audit_payload

    _ensure_tables()
    with session_scope() as session:
        existing = (
            session.query(AnonymizeLeafSummary)
            .filter(AnonymizeLeafSummary.study_instance_uid == study_uid)
            .one_or_none()
        )

        if existing:
            existing.cohort_name = cohort_name
            existing.leaf_rel_path = leaf_rel_path
            existing.files_total = files_total
            existing.files_written = files_written
            existing.files_reused = files_reused
            existing.files_with_errors = files_with_errors
            existing.summary = summary_payload
            return

        session.add(
            AnonymizeLeafSummary(
                study_instance_uid=study_uid,
                cohort_name=cohort_name,
                leaf_rel_path=leaf_rel_path,
                files_total=files_total,
                files_written=files_written,
                files_reused=files_reused,
                files_with_errors=files_with_errors,
                summary=summary_payload,
            )
        )


def load_leaf_summaries_for_cohort(cohort_name: Optional[str]) -> list[dict]:
    _ensure_tables()
    with session_scope() as session:
        query = session.query(AnonymizeLeafSummary)
        if cohort_name is None:
            query = query.filter(AnonymizeLeafSummary.cohort_name.is_(None))
        else:
            query = query.filter(AnonymizeLeafSummary.cohort_name == cohort_name)
        rows = query.order_by(AnonymizeLeafSummary.study_instance_uid).all()

    return [
        {
            "study_uid": row.study_instance_uid,
            "leaf_rel_path": row.leaf_rel_path,
            "summary": row.summary or {},
        }
        for row in rows
    ]
