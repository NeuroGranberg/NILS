"""Utility helpers for metadata metric aggregation."""

from __future__ import annotations

from typing import Optional

from sqlalchemy import func, select

from .schema import Instance, Series, Study, Subject, SubjectCohort
from .session import SessionLocal


def get_cohort_metrics(cohort_id: int) -> Optional[dict[str, int]]:
    """Return aggregate counts for a cohort.

    The counts include total subjects, studies, series, and instances that have
    been ingested for the specified cohort. A ``None`` return indicates the
    metrics could not be generated (e.g., database unavailable).
    """

    try:
        with SessionLocal() as session:
            subject_count = session.scalar(
                select(func.count(func.distinct(SubjectCohort.subject_id)))
                .select_from(SubjectCohort)
                .where(SubjectCohort.cohort_id == cohort_id)
            )

            study_count = session.scalar(
                select(func.count(func.distinct(Study.study_id)))
                .select_from(Study)
                .join(Subject, Study.subject_id == Subject.subject_id)
                .join(SubjectCohort, Subject.subject_id == SubjectCohort.subject_id)
                .where(SubjectCohort.cohort_id == cohort_id)
            )

            series_count = session.scalar(
                select(func.count(func.distinct(Series.series_id)))
                .select_from(Series)
                .join(Subject, Series.subject_id == Subject.subject_id)
                .join(SubjectCohort, Subject.subject_id == SubjectCohort.subject_id)
                .where(SubjectCohort.cohort_id == cohort_id)
            )

            instance_count = session.scalar(
                select(func.count())
                .select_from(Instance)
                .join(Series, Instance.series_id == Series.series_id)
                .join(Subject, Series.subject_id == Subject.subject_id)
                .join(SubjectCohort, Subject.subject_id == SubjectCohort.subject_id)
                .where(SubjectCohort.cohort_id == cohort_id)
            )

    except Exception:  # pragma: no cover - defensive guard
        return None

    return {
        "subjects": int(subject_count or 0),
        "studies": int(study_count or 0),
        "series": int(series_count or 0),
        "instances": int(instance_count or 0),
    }
