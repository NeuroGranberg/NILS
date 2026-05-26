"""Service layer for per-cohort classification keyword overrides.

Responsibilities:
- Load & persist delta rows in ``cohort_classification_overrides``.
- Validate bucket paths against the catalog published by
  ``classification.overrides.EDITABLE_BUCKETS``.
- Build the merged view (defaults + deltas + effective) served to the API.

This module is intentionally thin — the merge math lives in
``classification.overrides`` so it can be reused by the sort pipeline
without pulling in DB code.
"""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from db.session import session_scope

from classification.overrides import (
    EDITABLE_BUCKETS,
    apply_bucket_override,
    build_catalog,
    is_valid_bucket,
)

from .models import (
    AxisViewDTO,
    BucketOverrideDTO,
    BucketViewDTO,
    CohortClassificationOverride,
    CohortKeywordConfigDTO,
)
from .repository import get_cohort

logger = logging.getLogger(__name__)


_MAX_KEYWORD_LEN = 128


# =============================================================================
# Low-level loaders (usable by sort pipeline)
# =============================================================================


def load_override_map(cohort_id: int) -> dict[tuple[str, str], dict[str, list[str]]]:
    """Return override deltas for a cohort as ``{(axis, bucket_path): {added, removed}}``.

    Used directly by the classification pipeline at sort-time.
    Returns an empty dict when there are no overrides (cohort behaves like defaults).
    """
    with session_scope() as session:
        rows = session.scalars(
            select(CohortClassificationOverride).where(
                CohortClassificationOverride.cohort_id == cohort_id
            )
        ).all()
        result: dict[tuple[str, str], dict[str, list[str]]] = {}
        for row in rows:
            key = (row.axis, row.bucket_path)
            result[key] = {
                "added": list(row.added or []),
                "removed": list(row.removed or []),
            }
        return result


def _fetch_override_rows(
    session: Session, cohort_id: int
) -> dict[tuple[str, str], CohortClassificationOverride]:
    rows = session.scalars(
        select(CohortClassificationOverride).where(
            CohortClassificationOverride.cohort_id == cohort_id
        )
    ).all()
    return {(row.axis, row.bucket_path): row for row in rows}


# =============================================================================
# Validation helpers
# =============================================================================


def _normalize_keyword_list(items: list[str], field: str) -> list[str]:
    """Validate & dedup a keyword list while preserving the user's exact form.

    We intentionally do NOT strip whitespace: some global defaults include
    leading/trailing spaces as a word-boundary trick (e.g. ``" -c"``). A user
    who wants to remove such a default must be able to round-trip the exact
    string. Empty / whitespace-only entries are still dropped.
    """
    out: list[str] = []
    seen: set[str] = set()
    for item in items or []:
        if not isinstance(item, str):
            raise ValueError(f"{field}: entries must be strings")
        if not item.strip():
            continue
        if len(item) > _MAX_KEYWORD_LEN:
            raise ValueError(
                f"{field}: keyword '{item[:20]}...' exceeds "
                f"{_MAX_KEYWORD_LEN} characters"
            )
        key = item.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _validate_bucket(axis: str, bucket_path: str) -> None:
    if axis not in EDITABLE_BUCKETS:
        raise ValueError(f"Unknown axis: {axis!r}")
    if not is_valid_bucket(axis, bucket_path):
        raise ValueError(
            f"Unknown bucket path {bucket_path!r} for axis {axis!r}"
        )


# =============================================================================
# Public API
# =============================================================================


def get_config(cohort_id: int) -> CohortKeywordConfigDTO:
    """Return the full keyword configuration for a cohort (defaults + deltas)."""
    # Ensure the cohort exists.
    with session_scope() as session:
        cohort = get_cohort(session, cohort_id)
        if cohort is None:
            raise LookupError(f"Cohort {cohort_id} not found")
        override_rows = _fetch_override_rows(session, cohort_id)

    catalog = build_catalog()  # [{axis, label, description, buckets: [...]}]
    axes: list[AxisViewDTO] = []
    for axis_entry in catalog:
        buckets: list[BucketViewDTO] = []
        for bucket in axis_entry["buckets"]:
            key = (axis_entry["axis"], bucket["bucket_path"])
            row = override_rows.get(key)
            added = list(row.added) if row else []
            removed = list(row.removed) if row else []
            effective = apply_bucket_override(bucket["defaults"], added, removed)
            buckets.append(BucketViewDTO(
                axis=axis_entry["axis"],
                bucket_path=bucket["bucket_path"],
                display_name=bucket["display_name"],
                group_label=bucket.get("group_label"),
                description=bucket.get("description"),
                defaults=bucket["defaults"],
                added=added,
                removed=removed,
                effective=effective,
            ))
        axes.append(AxisViewDTO(
            axis=axis_entry["axis"],
            label=axis_entry["label"],
            description=axis_entry.get("description"),
            buckets=buckets,
        ))
    return CohortKeywordConfigDTO(cohort_id=cohort_id, axes=axes)


def get_global_catalog() -> list[AxisViewDTO]:
    """Return the read-only global catalog (no cohort context, defaults only)."""
    catalog = build_catalog()
    axes: list[AxisViewDTO] = []
    for axis_entry in catalog:
        buckets = [
            BucketViewDTO(
                axis=axis_entry["axis"],
                bucket_path=b["bucket_path"],
                display_name=b["display_name"],
                group_label=b.get("group_label"),
                description=b.get("description"),
                defaults=b["defaults"],
                added=[],
                removed=[],
                effective=list(b["defaults"]),
            )
            for b in axis_entry["buckets"]
        ]
        axes.append(AxisViewDTO(
            axis=axis_entry["axis"],
            label=axis_entry["label"],
            description=axis_entry.get("description"),
            buckets=buckets,
        ))
    return axes


def update_bucket(
    cohort_id: int,
    axis: str,
    bucket_path: str,
    added: list[str],
    removed: list[str],
) -> BucketViewDTO:
    """Upsert a single bucket's deltas and return its merged view."""
    _validate_bucket(axis, bucket_path)

    normalized_added = _normalize_keyword_list(added, "added")
    normalized_removed = _normalize_keyword_list(removed, "removed")

    # added ∩ removed must be empty (case-insensitive)
    added_lower = {kw.lower() for kw in normalized_added}
    overlap = [kw for kw in normalized_removed if kw.lower() in added_lower]
    if overlap:
        raise ValueError(
            f"Keyword(s) cannot be both added and removed: {overlap[:3]}"
        )

    with session_scope() as session:
        cohort = get_cohort(session, cohort_id)
        if cohort is None:
            raise LookupError(f"Cohort {cohort_id} not found")

        row = session.scalar(
            select(CohortClassificationOverride).where(
                CohortClassificationOverride.cohort_id == cohort_id,
                CohortClassificationOverride.axis == axis,
                CohortClassificationOverride.bucket_path == bucket_path,
            )
        )

        # If the delta is empty, treat this as a reset (no row needed).
        if not normalized_added and not normalized_removed:
            if row is not None:
                session.delete(row)
            session.flush()
        elif row is None:
            row = CohortClassificationOverride(
                cohort_id=cohort_id,
                axis=axis,
                bucket_path=bucket_path,
                added=normalized_added,
                removed=normalized_removed,
            )
            session.add(row)
            session.flush()
        else:
            row.added = normalized_added
            row.removed = normalized_removed
            session.flush()

    # Rebuild and return this bucket's view.
    return _single_bucket_view(cohort_id, axis, bucket_path)


def reset_bucket(cohort_id: int, axis: str, bucket_path: str) -> BucketViewDTO:
    """Delete the override row for a bucket, reverting it to pure defaults."""
    _validate_bucket(axis, bucket_path)
    with session_scope() as session:
        cohort = get_cohort(session, cohort_id)
        if cohort is None:
            raise LookupError(f"Cohort {cohort_id} not found")
        session.execute(
            delete(CohortClassificationOverride).where(
                CohortClassificationOverride.cohort_id == cohort_id,
                CohortClassificationOverride.axis == axis,
                CohortClassificationOverride.bucket_path == bucket_path,
            )
        )
    return _single_bucket_view(cohort_id, axis, bucket_path)


def reset_all(cohort_id: int) -> None:
    """Delete every override row for a cohort."""
    with session_scope() as session:
        cohort = get_cohort(session, cohort_id)
        if cohort is None:
            raise LookupError(f"Cohort {cohort_id} not found")
        session.execute(
            delete(CohortClassificationOverride).where(
                CohortClassificationOverride.cohort_id == cohort_id
            )
        )


# =============================================================================
# Internal helpers
# =============================================================================


def _single_bucket_view(
    cohort_id: int, axis: str, bucket_path: str
) -> BucketViewDTO:
    """Build one bucket's view; raises if the bucket is unknown."""
    config = get_config(cohort_id)
    for axis_view in config.axes:
        if axis_view.axis != axis:
            continue
        for bucket in axis_view.buckets:
            if bucket.bucket_path == bucket_path:
                return bucket
    # Should never happen because validation ran; be defensive anyway.
    raise LookupError(f"Bucket not found: {axis}:{bucket_path}")
