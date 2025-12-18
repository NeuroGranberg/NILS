"""Functions to compute cohort statistics from the metadata database."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine


def get_cohort_stats(cohort_name: str, *, engine: Engine) -> dict[str, int]:
    """
    Get subject and session counts for a cohort from the metadata database.

    Args:
        cohort_name: The normalized cohort name to look up
        engine: SQLAlchemy engine for the metadata database

    Returns:
        Dictionary with 'total_subjects' and 'total_sessions' counts
    """
    with engine.connect() as conn:
        # Get cohort_id from metadata database
        cohort_result = conn.execute(
            text("SELECT cohort_id FROM cohort WHERE LOWER(name) = LOWER(:name)"),
            {"name": cohort_name},
        ).fetchone()

        if not cohort_result:
            return {"total_subjects": 0, "total_sessions": 0}

        cohort_id = cohort_result[0]

        # Count subjects in this cohort
        subjects_result = conn.execute(
            text("""
                SELECT COUNT(DISTINCT subject_id) 
                FROM subject_cohorts 
                WHERE cohort_id = :cohort_id
            """),
            {"cohort_id": cohort_id},
        ).fetchone()

        total_subjects = subjects_result[0] if subjects_result else 0

        # Count sessions (studies) for subjects in this cohort
        sessions_result = conn.execute(
            text("""
                SELECT COUNT(DISTINCT s.study_id)
                FROM study s
                INNER JOIN subject_cohorts sc ON s.subject_id = sc.subject_id
                WHERE sc.cohort_id = :cohort_id
            """),
            {"cohort_id": cohort_id},
        ).fetchone()

        total_sessions = sessions_result[0] if sessions_result else 0

        # Count stacks (series_stack) for subjects in this cohort
        stacks_result = conn.execute(
            text("""
                SELECT COUNT(DISTINCT ss.series_stack_id)
                FROM series_stack ss
                INNER JOIN series s ON ss.series_id = s.series_id
                INNER JOIN study st ON s.study_id = st.study_id
                INNER JOIN subject_cohorts sc ON st.subject_id = sc.subject_id
                WHERE sc.cohort_id = :cohort_id
            """),
            {"cohort_id": cohort_id},
        ).fetchone()

        total_stacks = stacks_result[0] if stacks_result else 0

        return {
            "total_subjects": total_subjects,
            "total_sessions": total_sessions,
            "total_series": total_stacks,  # Mapping stacks count to total_series field
        }


def get_all_cohort_stats(*, engine: Engine) -> dict[str, dict[str, int]]:
    """
    Get subject, session, and stack counts for all cohorts.

    Performance optimized: Uses scalar subqueries instead of multiple LEFT JOINs
    to avoid Cartesian product explosion. Each subquery filters by cohort_id
    and aggregates independently, preventing row multiplication.

    Args:
        engine: SQLAlchemy engine for the metadata database

    Returns:
        Dictionary mapping cohort names (lowercase) to stats dicts
    """
    with engine.connect() as conn:
        # Optimized query using scalar subqueries instead of JOIN + COUNT(DISTINCT)
        # This avoids Cartesian products when cohorts have many series/stacks
        results = conn.execute(
            text("""
                SELECT
                    c.name,
                    (SELECT COUNT(*)
                     FROM subject_cohorts sc
                     WHERE sc.cohort_id = c.cohort_id) as subject_count,
                    (SELECT COUNT(DISTINCT st.study_id)
                     FROM study st
                     JOIN subject_cohorts sc2 ON st.subject_id = sc2.subject_id
                     WHERE sc2.cohort_id = c.cohort_id) as session_count,
                    (SELECT COUNT(*)
                     FROM series_stack ss
                     JOIN series s ON ss.series_id = s.series_id
                     JOIN study st2 ON s.study_id = st2.study_id
                     JOIN subject_cohorts sc3 ON st2.subject_id = sc3.subject_id
                     WHERE sc3.cohort_id = c.cohort_id) as stack_count
                FROM cohort c
            """)
        ).fetchall()

        stats = {}
        for row in results:
            cohort_name = row[0].lower() if row[0] else ""
            stats[cohort_name] = {
                "total_subjects": row[1] or 0,
                "total_sessions": row[2] or 0,
                "total_series": row[3] or 0,
            }

        return stats
