"""SQL queries for the sorting pipeline.

All queries are parameterized and use SQLAlchemy text() for safety.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Connection


# =============================================================================
# Modality SOP Class UID Mappings
# =============================================================================

MODALITY_SOP_CLASS_UIDS = {
    "MR": {
        "1.2.840.10008.5.1.4.1.1.4",      # MR Image Storage
        "1.2.840.10008.5.1.4.1.1.4.1",    # Enhanced MR Image Storage
        "1.2.840.10008.5.1.4.1.1.4.2",    # MR Spectroscopy Storage
        "1.2.840.10008.5.1.4.1.1.4.4",    # Legacy Converted Enhanced MR Image Storage
    },
    "CT": {
        "1.2.840.10008.5.1.4.1.1.2",      # CT Image Storage
        "1.2.840.10008.5.1.4.1.1.2.1",    # Enhanced CT Image Storage
        "1.2.840.10008.5.1.4.1.1.2.2",    # Legacy Converted Enhanced CT Image Storage
    },
    "PT": {  # Note: PET modality is "PT" in DICOM
        "1.2.840.10008.5.1.4.1.1.128",    # PET Image Storage
        "1.2.840.10008.5.1.4.1.1.128.1",  # Legacy Converted Enhanced PET Image Storage
    },
}


# =============================================================================
# Step 1.1: Cohort Subject Resolution
# =============================================================================

QUERY_COHORT_SUBJECTS = """
SELECT s.subject_id, s.subject_code
FROM subject s
INNER JOIN subject_cohorts sc ON s.subject_id = sc.subject_id
WHERE sc.cohort_id = :cohort_id
  AND s.is_active = 1
ORDER BY s.subject_code
"""


def get_cohort_subjects(conn: Connection, cohort_id: int) -> list[dict[str, Any]]:
    """Get all active subjects belonging to a cohort.

    Args:
        conn: Database connection
        cohort_id: The cohort ID to query

    Returns:
        List of dicts with subject_id and subject_code
    """
    result = conn.execute(text(QUERY_COHORT_SUBJECTS), {"cohort_id": cohort_id})
    return [{"subject_id": row.subject_id, "subject_code": row.subject_code} for row in result]


# =============================================================================
# Step 1.2: Study Discovery
# =============================================================================

QUERY_STUDIES_FOR_SUBJECTS = """
SELECT study_id, study_instance_uid, study_date, subject_id
FROM study
WHERE subject_id = ANY(:subject_ids)
  AND EXISTS (
      SELECT 1 FROM series ser WHERE ser.study_id = study.study_id
  )
ORDER BY subject_id, study_date
"""


def get_studies_for_subjects(conn: Connection, subject_ids: Sequence[int]) -> list[dict[str, Any]]:
    """Get all studies for a list of subjects, excluding empty branches.

    Empty branches are studies with no series (PACS duplicates with no instances).

    Args:
        conn: Database connection
        subject_ids: List of subject IDs

    Returns:
        List of study dicts with study_id, study_instance_uid, study_date, subject_id.
        Excludes studies that have no series.
    """
    if not subject_ids:
        return []

    # Use PostgreSQL ANY() for safe parameter binding
    result = conn.execute(
        text(QUERY_STUDIES_FOR_SUBJECTS),
        {"subject_ids": list(subject_ids)},
    )
    return [
        {
            "study_id": row.study_id,
            "study_instance_uid": row.study_instance_uid,
            "study_date": row.study_date,
            "subject_id": row.subject_id,
        }
        for row in result
    ]


# =============================================================================
# Step 1.3: Study Date Validation & Repair
# =============================================================================

QUERY_SERIES_DATE_FOR_STUDY = """
SELECT series_date
FROM series
WHERE study_id = :study_id AND series_date IS NOT NULL
LIMIT 1
"""

QUERY_INSTANCE_ACQUISITION_DATE_FOR_STUDY = """
SELECT i.acquisition_date
FROM instance i
INNER JOIN series s ON i.series_id = s.series_id
WHERE s.study_id = :study_id AND i.acquisition_date IS NOT NULL
LIMIT 1
"""

QUERY_INSTANCE_CONTENT_DATE_FOR_STUDY = """
SELECT i.content_date
FROM instance i
INNER JOIN series s ON i.series_id = s.series_id
WHERE s.study_id = :study_id AND i.content_date IS NOT NULL
LIMIT 1
"""

UPDATE_STUDY_DATE = """
UPDATE study
SET study_date = :study_date
WHERE study_id = :study_id
"""


def get_series_date_for_study(conn: Connection, study_id: int) -> date | None:
    """Try to get a series_date from any series in the study."""
    result = conn.execute(text(QUERY_SERIES_DATE_FOR_STUDY), {"study_id": study_id})
    row = result.fetchone()
    return row.series_date if row else None


def get_acquisition_date_for_study(conn: Connection, study_id: int) -> date | None:
    """Try to get acquisition_date from any instance in the study."""
    result = conn.execute(text(QUERY_INSTANCE_ACQUISITION_DATE_FOR_STUDY), {"study_id": study_id})
    row = result.fetchone()
    return row.acquisition_date if row else None


def get_content_date_for_study(conn: Connection, study_id: int) -> date | None:
    """Try to get content_date from any instance in the study."""
    result = conn.execute(text(QUERY_INSTANCE_CONTENT_DATE_FOR_STUDY), {"study_id": study_id})
    row = result.fetchone()
    return row.content_date if row else None


def update_study_date(conn: Connection, study_id: int, study_date: date) -> None:
    """Update the study_date for a study (persist imputed date)."""
    conn.execute(text(UPDATE_STUDY_DATE), {"study_id": study_id, "study_date": study_date})


# =============================================================================
# Step 1.4: Series Collection
# =============================================================================

QUERY_SERIES_FOR_STUDIES = """
SELECT
    s.series_id,
    s.series_instance_uid,
    s.modality,
    s.sop_class_uid,
    s.study_id,
    s.subject_id,
    st.study_instance_uid,
    st.study_date,
    sub.subject_code
FROM series s
INNER JOIN study st ON s.study_id = st.study_id
INNER JOIN subject sub ON s.subject_id = sub.subject_id
WHERE s.study_id = ANY(:study_ids)
  AND EXISTS (
      SELECT 1 FROM instance i WHERE i.series_id = s.series_id
  )
ORDER BY sub.subject_code, st.study_date, s.series_instance_uid
"""


def get_series_for_studies(conn: Connection, study_ids: Sequence[int]) -> list[dict[str, Any]]:
    """Get all series for a list of studies, with denormalized subject/study info.

    Excludes empty branches: series with no instances (PACS duplicates).

    Args:
        conn: Database connection
        study_ids: List of study IDs

    Returns:
        List of series dicts with all fields needed for SeriesForProcessing.
        Excludes series that have no instances.
    """
    if not study_ids:
        return []

    # Use PostgreSQL ANY() for safe parameter binding
    result = conn.execute(
        text(QUERY_SERIES_FOR_STUDIES),
        {"study_ids": list(study_ids)},
    )
    return [
        {
            "series_id": row.series_id,
            "series_instance_uid": row.series_instance_uid,
            "modality": row.modality,
            "sop_class_uid": row.sop_class_uid,
            "study_id": row.study_id,
            "subject_id": row.subject_id,
            "study_instance_uid": row.study_instance_uid,
            "study_date": row.study_date,
            "subject_code": row.subject_code,
        }
        for row in result
    ]


def filter_series_by_modality(
    series_rows: list[dict[str, Any]],
    selected_modalities: list[str],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Filter series by selected modalities using modality field OR sop_class_uid.
    
    A series is included if:
    - Its modality field matches any selected modality, OR
    - Its sop_class_uid matches any SOP class UID for selected modalities
    
    Args:
        series_rows: List of series dicts (must include 'modality' and 'sop_class_uid')
        selected_modalities: List of selected modality codes (e.g., ["MR", "CT", "PT"])
    
    Returns:
        Tuple of (filtered_series, modality_counts) where modality_counts is a dict
        showing the count of series for each modality in the filtered result
    """
    if not selected_modalities:
        # If no modalities selected, return empty
        return [], {}
    
    # Build set of allowed SOP class UIDs for selected modalities
    allowed_sop_class_uids = set()
    for modality in selected_modalities:
        if modality in MODALITY_SOP_CLASS_UIDS:
            allowed_sop_class_uids.update(MODALITY_SOP_CLASS_UIDS[modality])
    
    # Filter series using OR logic
    filtered = []
    for series in series_rows:
        series_modality = series.get("modality")
        series_sop_class_uid = series.get("sop_class_uid")
        
        # Include if modality matches
        if series_modality in selected_modalities:
            filtered.append(series)
            continue
        
        # Include if SOP class UID matches
        if series_sop_class_uid and series_sop_class_uid in allowed_sop_class_uids:
            filtered.append(series)
            continue
    
    # Calculate modality breakdown for filtered results
    modality_counts_raw = Counter(series.get("modality") for series in filtered if series.get("modality"))
    
    # Ensure all selected modalities are in the result (with 0 if not found)
    modality_counts = {}
    for modality in selected_modalities:
        modality_counts[modality] = modality_counts_raw.get(modality, 0)
    
    return filtered, modality_counts


# =============================================================================
# Step 1.5: Existing Classification Check
# =============================================================================

QUERY_CLASSIFIED_SERIES = """
SELECT s.series_id, s.series_instance_uid
FROM series s
INNER JOIN series_classification_cache scc ON s.series_id = scc.series_id
WHERE s.series_id = ANY(:series_ids)
  AND scc.classification_string IS NOT NULL
"""


def get_classified_series_ids(conn: Connection, series_ids: Sequence[int]) -> set[int]:
    """Get the set of series IDs that already have classifications.

    Args:
        conn: Database connection
        series_ids: List of series IDs to check

    Returns:
        Set of series_ids that are already classified
    """
    if not series_ids:
        return set()

    # Use PostgreSQL ANY() for safe parameter binding
    result = conn.execute(
        text(QUERY_CLASSIFIED_SERIES),
        {"series_ids": list(series_ids)},
    )
    return {row.series_id for row in result}


# =============================================================================
# Cohort Info Query
# =============================================================================

QUERY_COHORT_BY_ID = """
SELECT cohort_id, name, path, description
FROM cohort
WHERE cohort_id = :cohort_id
"""


def get_cohort_info(conn: Connection, cohort_id: int) -> dict[str, Any] | None:
    """Get cohort info by ID.

    Args:
        conn: Database connection
        cohort_id: The cohort ID

    Returns:
        Dict with cohort info or None if not found
    """
    result = conn.execute(text(QUERY_COHORT_BY_ID), {"cohort_id": cohort_id})
    row = result.fetchone()
    if not row:
        return None
    return {
        "cohort_id": row.cohort_id,
        "name": row.name,
        "path": row.path,
        "description": row.description,
    }


# =============================================================================
# Step 2: Stack Finalization Queries
# =============================================================================

UPDATE_STACK_INSTANCE_COUNTS = """
UPDATE series_stack ss
SET stack_n_instances = sub.cnt
FROM (
    SELECT series_stack_id, COUNT(*) as cnt
    FROM instance
    WHERE series_stack_id IS NOT NULL
    GROUP BY series_stack_id
) sub
WHERE ss.series_stack_id = sub.series_stack_id
  AND ss.series_id = ANY(:series_ids)
"""

QUERY_STACKS_FOR_FINALIZATION = """
SELECT 
    series_stack_id,
    series_id,
    stack_index,
    stack_modality,
    stack_n_instances,
    stack_orientation_confidence,
    stack_echo_time,
    stack_inversion_time,
    stack_echo_numbers,
    stack_echo_train_length,
    stack_repetition_time,
    stack_flip_angle,
    stack_receive_coil_name,
    stack_image_orientation,
    stack_image_type,
    stack_xray_exposure,
    stack_kvp,
    stack_tube_current,
    stack_pet_bed_index,
    stack_pet_frame_type
FROM series_stack
WHERE series_id = ANY(:series_ids)
ORDER BY series_id, stack_index
"""

UPDATE_STACK_KEY = """
UPDATE series_stack
SET stack_key = :stack_key
WHERE series_stack_id = :series_stack_id
"""


def update_stack_instance_counts(conn: Connection, series_ids: list[int]) -> int:
    """Update stack_n_instances for all stacks in the given series.
    
    Args:
        conn: Database connection
        series_ids: List of series IDs
        
    Returns:
        Number of stacks updated
    """
    if not series_ids:
        return 0
    
    result = conn.execute(text(UPDATE_STACK_INSTANCE_COUNTS), {"series_ids": series_ids})
    return result.rowcount


@dataclass
class StackForFinalization:
    """Stack record with all data needed for finalization."""
    series_stack_id: int
    series_id: int
    stack_index: int
    stack_modality: str
    stack_n_instances: int | None
    stack_orientation_confidence: float | None
    stack_echo_time: float | None
    stack_inversion_time: float | None
    stack_echo_numbers: str | None
    stack_echo_train_length: int | None
    stack_repetition_time: float | None
    stack_flip_angle: float | None
    stack_receive_coil_name: str | None
    stack_image_orientation: str | None
    stack_image_type: str | None
    stack_xray_exposure: float | None
    stack_kvp: float | None
    stack_tube_current: float | None
    stack_pet_bed_index: int | None
    stack_pet_frame_type: str | None


def query_stacks_for_finalization(conn: Connection, series_ids: list[int]) -> list[StackForFinalization]:
    """Query all stacks with data needed for finalization.
    
    Args:
        conn: Database connection
        series_ids: List of series IDs
        
    Returns:
        List of StackForFinalization records
    """
    if not series_ids:
        return []
    
    result = conn.execute(text(QUERY_STACKS_FOR_FINALIZATION), {"series_ids": series_ids})
    
    stacks = []
    for row in result:
        stacks.append(StackForFinalization(
            series_stack_id=row.series_stack_id,
            series_id=row.series_id,
            stack_index=row.stack_index,
            stack_modality=row.stack_modality,
            stack_n_instances=row.stack_n_instances,
            stack_orientation_confidence=row.stack_orientation_confidence,
            stack_echo_time=row.stack_echo_time,
            stack_inversion_time=row.stack_inversion_time,
            stack_echo_numbers=row.stack_echo_numbers,
            stack_echo_train_length=row.stack_echo_train_length,
            stack_repetition_time=row.stack_repetition_time,
            stack_flip_angle=row.stack_flip_angle,
            stack_receive_coil_name=row.stack_receive_coil_name,
            stack_image_orientation=row.stack_image_orientation,
            stack_image_type=row.stack_image_type,
            stack_xray_exposure=row.stack_xray_exposure,
            stack_kvp=row.stack_kvp,
            stack_tube_current=row.stack_tube_current,
            stack_pet_bed_index=row.stack_pet_bed_index,
            stack_pet_frame_type=row.stack_pet_frame_type,
        ))
    
    return stacks


def update_stack_key(conn: Connection, series_stack_id: int, stack_key: str | None) -> None:
    """Update stack_key for a single stack.
    
    Args:
        conn: Database connection
        series_stack_id: Stack ID to update
        stack_key: The stack key value
    """
    conn.execute(text(UPDATE_STACK_KEY), {
        "series_stack_id": series_stack_id,
        "stack_key": stack_key,
    })
