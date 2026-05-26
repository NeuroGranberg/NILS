"""Resolve cohort identity between application DB and metadata DB.

The application DB (cohorts.id) and metadata DB (cohort.cohort_id) use
independent auto-increment sequences. This module provides helpers to
translate an application-side cohort ID into the correct metadata-side
cohort ID by matching on the normalized cohort name.
"""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)


def resolve_metadata_cohort_id(app_cohort_id: int) -> Optional[int]:
    """Translate an application DB cohort ID to the metadata DB cohort ID.

    Looks up the cohort name from the application DB, then queries the
    metadata DB for the matching cohort_id.

    Returns None if the cohort is not found in either database.
    """
    cohort_name = get_cohort_name_from_app_db(app_cohort_id)
    if not cohort_name:
        return None
    return resolve_metadata_cohort_id_by_name(cohort_name)


def resolve_metadata_cohort_id_by_name(cohort_name: str) -> Optional[int]:
    """Resolve a metadata DB cohort_id from a cohort name."""
    from metadata_db.session import SessionLocal as MetadataSessionLocal

    with MetadataSessionLocal() as meta_db:
        row = meta_db.execute(
            text("SELECT cohort_id FROM cohort WHERE LOWER(name) = LOWER(:name)"),
            {"name": cohort_name},
        ).fetchone()
        if not row:
            logger.warning(
                "Cohort '%s' not found in metadata DB", cohort_name
            )
            return None
        return row.cohort_id


def get_cohort_name_from_app_db(app_cohort_id: int) -> Optional[str]:
    """Look up the normalized cohort name from the application DB.

    Uses raw SQL to avoid ORM relationship resolution issues.
    """
    from db.session import engine

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT name FROM cohorts WHERE id = :id"),
            {"id": app_cohort_id},
        ).fetchone()
        if not row:
            logger.warning("Cohort id=%d not found in application DB", app_cohort_id)
            return None
        return row.name
