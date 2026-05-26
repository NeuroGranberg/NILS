"""Candidate stack enumeration for Body Part QC.

The seeder needs a pool of slice candidates drawn from the cohort. We
re-use the same eligibility filter the cohort applies at Apply time so
the user labels and trains on the same population they predict on:

    provenance ∈ {RawRecon}
    directory_type = 'anat'
    localizer = 0
    provenance ∉ {SyMRI, ProjectionDerived, SubtractionDerived}

For each eligible stack we pick **one central slice** (slice_index =
floor(num_slices / 2)). That keeps the seeding pool small (1 row per
stack) and means user-approved samples already correspond to a cached
embedding key.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text

from metadata_db.session import SessionLocal as MetadataSessionLocal

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CandidateStack:
    """One slice from one eligible stack, ready to feed the seeder."""
    stack_id: int
    slice_index: int            # central slice
    series_instance_uid: str
    study_id: int
    subject_id: int             # session key: (subject_id, session_date)
    subject_code: str
    session_date: Optional[str]
    technique: Optional[str]
    orientation: Optional[str]
    body_part: Optional[str]    # prior keyword-based label, may be None
    num_slices: int


# ---------------------------------------------------------------------------
# Category ↔ keyword body_part mapping (defaults)
# ---------------------------------------------------------------------------

DEFAULT_CATEGORY_KEYWORD_MAP: dict[str, list[str]] = {
    "Brain": ["brain"],
    "Spine": ["spine"],
    "Brain-Neck": ["brain-neck", "neck"],
    # Chest has no keyword-based labels in the current detector.
}


def partition_by_category(
    candidates: list[CandidateStack],
    category: str,
    category_keyword_map: Optional[dict[str, list[str]]] = None,
) -> tuple[list[CandidateStack], list[CandidateStack]]:
    """Split candidates into (prior_pool, null_pool) for seeding.

    Args:
        candidates: All eligible candidates.
        category: The QC category being seeded (e.g. "Brain").
        category_keyword_map: Mapping of category → keyword body_part
            values that match it. Defaults to
            :data:`DEFAULT_CATEGORY_KEYWORD_MAP`.

    Returns:
        A 2-tuple:
        - ``prior_pool``: candidates whose ``body_part`` matches the
          category (these are pre-approved keyword-based suggestions).
        - ``null_pool``: candidates with ``body_part=None``
          (to be scored by zero-shot).

        Candidates with a *different* non-None body_part (e.g. spine
        when seeding Brain) are excluded from both pools.
    """
    mapping = category_keyword_map or DEFAULT_CATEGORY_KEYWORD_MAP
    match_values = set(v.lower() for v in mapping.get(category, []))

    prior_pool: list[CandidateStack] = []
    null_pool: list[CandidateStack] = []

    for c in candidates:
        if c.body_part is None:
            null_pool.append(c)
        elif c.body_part.lower() in match_values:
            prior_pool.append(c)
        # else: different label → excluded from this category's seeding

    return prior_pool, null_pool


def enumerate_candidates(
    cohort_name: str,
    *,
    sample_per_stack: int = 1,
) -> list[CandidateStack]:
    """Enumerate one central-slice candidate per eligible stack.

    Args:
        cohort_name: Cohort name as recorded in
            ``series_classification_cache.dicom_origin_cohort``.
        sample_per_stack: Currently fixed to 1 (central slice). Reserved
            for a future "3 central slices" variant.

    Returns:
        One :class:`CandidateStack` per eligible stack.
    """
    if sample_per_stack != 1:
        raise NotImplementedError("Only sample_per_stack=1 is supported in V1.")

    sql = """
        SELECT
            scc.series_stack_id,
            s.series_instance_uid,
            s.study_id,
            s.subject_id,
            subj.subject_code,
            st.study_date::text AS session_date,
            scc.technique,
            scc.orientation_patient AS orientation,
            scc.body_part,
            scc.slices_count
        FROM series_classification_cache scc
        JOIN series s ON s.series_instance_uid = scc.series_instance_uid
        JOIN study st ON s.study_id = st.study_id
        LEFT JOIN subject subj ON s.subject_id = subj.subject_id
        WHERE LOWER(scc.dicom_origin_cohort) = LOWER(:cohort_name)
          AND scc.directory_type = 'anat'
          AND COALESCE(scc.localizer, 0) = 0
          AND COALESCE(scc.provenance, '') NOT IN
              ('SyMRI', 'ProjectionDerived', 'SubtractionDerived')
    """

    out: list[CandidateStack] = []
    with MetadataSessionLocal() as db:
        rows = db.execute(text(sql), {"cohort_name": cohort_name}).fetchall()

    for r in rows:
        n_slices = int(r.slices_count or 0)
        if n_slices <= 0:
            # Stack has no readable slice data; can't seed from it.
            continue
        out.append(CandidateStack(
            stack_id=int(r.series_stack_id),
            slice_index=n_slices // 2,
            series_instance_uid=r.series_instance_uid,
            study_id=int(r.study_id),
            subject_id=int(r.subject_id) if r.subject_id else 0,
            subject_code=r.subject_code or "",
            session_date=r.session_date,
            technique=r.technique,
            orientation=r.orientation,
            body_part=r.body_part,
            num_slices=n_slices,
        ))
    return out
