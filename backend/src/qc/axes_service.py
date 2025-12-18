"""Service for Axes Prediction QC - streamlined QC for classification axes.

This service uses the draft pattern: changes are saved to application_db first,
and only pushed to metadata_db when the user explicitly confirms.

Draft flow:
1. User selects an axis value → save_axis_draft() → app_db.qc_draft_changes
2. User continues making changes (all stored as drafts)
3. User clicks "Submit" → confirm_axes_changes() → metadata_db.series_classification_cache
4. Or user clicks "Discard" → discard_axes_changes() → deletes drafts
"""

from __future__ import annotations

import logging
from typing import Any
from functools import lru_cache
from typing import Any
from typing import Optional

from sqlalchemy import text, Integer, String
from sqlalchemy.sql.expression import bindparam

from db.session import session_scope
from metadata_db.session import SessionLocal as MetadataSessionLocal

from . import repository
from .models import QCSession

logger = logging.getLogger(__name__)


# Axes that can be QC'd in this module
AXES_CATEGORIES = ["base", "technique", "modifier", "provenance", "construct"]

# Map axis name to DB column name
AXIS_TO_COLUMN = {
    "base": "base",
    "technique": "technique",
    "modifier": "modifier_csv",
    "provenance": "provenance",
    "construct": "construct_csv",
}

# Map review reason prefixes to axes
AXES_REASON_PREFIXES = {
    "base": ["base:"],
    "technique": ["technique:"],
    "modifier": ["modifier:"],
    "provenance": ["provenance:"],
    "construct": ["construct:"],
}


@lru_cache(maxsize=1)
def get_axis_options_from_yaml() -> dict[str, Any]:
    """
    Load axis options dynamically from detection YAML files via detectors.

    Returns:
        Dict mapping axis name to list of valid options.
        Cached after first call for performance.
    """
    from classification.detectors import (
        BaseContrastDetector,
        TechniqueDetector,
        ModifierDetector,
        ProvenanceDetector,
        ConstructDetector,
    )

    base_detector = BaseContrastDetector()
    technique_detector = TechniqueDetector()
    modifier_detector = ModifierDetector()
    provenance_detector = ProvenanceDetector()
    construct_detector = ConstructDetector()

    return {
        "base": base_detector.get_all_bases(),
        "technique": technique_detector.get_all_techniques(),
        "technique_metadata": technique_detector.get_technique_metadata_map(),
        "modifier": modifier_detector.get_all_modifiers(),
        "provenance": [None] + provenance_detector.get_all_provenances(),
        "construct": construct_detector.get_all_constructs(),
    }


def _determine_axis_flags(review_reasons: str | None) -> dict[str, str | None]:
    """
    Determine which axes have flags and what type of flag.

    Returns dict like: {"base": "missing", "technique": "conflict", ...}
    """
    flags = {}
    if not review_reasons:
        return flags

    # Parse each reason individually to avoid cross-axis contamination
    reasons = [r.strip().lower() for r in review_reasons.split(",") if r.strip()]

    for axis, prefixes in AXES_REASON_PREFIXES.items():
        for reason in reasons:
            # Check if this specific reason belongs to this axis
            if any(reason.startswith(p.lower()) for p in prefixes):
                # Extract flag type from THIS reason only
                if "missing" in reason:
                    flags[axis] = "missing"
                elif "conflict" in reason:
                    flags[axis] = "conflict"
                elif "low_confidence" in reason:
                    flags[axis] = "low_confidence"
                elif "ambiguous" in reason:
                    flags[axis] = "ambiguous"
                else:
                    flags[axis] = "review"
                break  # Found flag for this axis, move to next axis

    return flags


class AxesQCService:
    """Service for Axes Prediction QC operations with draft pattern."""

    def __init__(self):
        self._initialized = False

    def _ensure_initialized(self) -> None:
        """Ensure QC tables exist in application DB."""
        if self._initialized:
            return

        from cohorts.models import Base
        from db.session import engine
        from .models import QCSession, QCItem, QCDraftChange

        Base.metadata.create_all(engine, checkfirst=True)
        self._initialized = True

    # =========================================================================
    # Session Management
    # =========================================================================

    def get_or_create_session(self, cohort_id: int) -> dict:
        """
        Get existing axes QC session or create a new one.

        Returns session info with draft change counts.
        """
        self._ensure_initialized()

        with session_scope() as db:
            # Look for existing non-completed session for this cohort
            session = repository.get_session_for_cohort(db, cohort_id)
            if session and session.status not in ("completed", "abandoned"):
                return self._session_to_dict(db, session)

            # Create new session
            session = repository.create_session(db, cohort_id, status="in_progress")
            db.flush()

            return self._session_to_dict(db, session)

    def _session_to_dict(self, db, session: QCSession) -> dict:
        """Convert session to dict with counts."""
        # Count items with draft changes
        draft_count = 0
        items_with_changes = repository.get_items_with_draft_changes(db, session.id, category="axes")
        draft_count = len(items_with_changes)

        # Count total draft changes
        total_changes = sum(len(item.draft_changes) for item in items_with_changes)

        return {
            "id": session.id,
            "cohort_id": session.cohort_id,
            "status": session.status,
            "draft_item_count": draft_count,
            "draft_change_count": total_changes,
            "created_at": session.created_at.isoformat() if session.created_at else None,
            "updated_at": session.updated_at.isoformat() if session.updated_at else None,
        }

    def _build_axis_filter(self, axis: Optional[str], flag_type: Optional[str]) -> str:
        """
        Build SQL filter clause for axis and flag_type filters.

        Args:
            axis: Optional axis filter (base, technique, modifier, provenance, construct)
            flag_type: Optional flag type filter (missing, conflict, low_confidence, ambiguous, review)

        Returns:
            SQL WHERE clause fragment
        """
        valid_axes = ["base", "technique", "modifier", "provenance", "construct"]
        valid_flag_types = ["missing", "conflict", "low_confidence", "ambiguous", "review"]

        # If filtering by specific axis and flag_type
        if axis and axis in valid_axes and flag_type and flag_type in valid_flag_types:
            # Match pattern like "base:missing" or "base:low_confidence"
            return f"scc.manual_review_reasons_csv LIKE '%{axis}:{flag_type}%'"

        # If filtering by specific axis only (any flag type)
        if axis and axis in valid_axes:
            return f"scc.manual_review_reasons_csv LIKE '%{axis}:%'"

        # If filtering by flag type only (any axis)
        if flag_type and flag_type in valid_flag_types:
            # Match flag type after any axis prefix
            conditions = [f"scc.manual_review_reasons_csv LIKE '%{ax}:{flag_type}%'" for ax in valid_axes]
            return f"({' OR '.join(conditions)})"

        # Default: show all axes QC items
        return """
            scc.manual_review_reasons_csv LIKE '%base:%'
            OR scc.manual_review_reasons_csv LIKE '%technique:%'
            OR scc.manual_review_reasons_csv LIKE '%modifier:%'
            OR scc.manual_review_reasons_csv LIKE '%provenance:%'
            OR scc.manual_review_reasons_csv LIKE '%construct:%'
        """

    def _build_localizer_filter(self) -> str:
        """
        Build SQL filter to exclude localizers with only low_confidence flags.

        Localizers are low-value acquisitions where low_confidence isn't worth reviewing.
        Only show localizers if they have missing or conflict flags.

        Returns:
            SQL WHERE clause fragment
        """
        # For localizers (directory_type = 'localizer'), only show if they have
        # missing or conflict flags on any axis
        high_priority_flags = []
        for axis in ["base", "technique", "modifier", "provenance", "construct"]:
            high_priority_flags.append(f"scc.manual_review_reasons_csv LIKE '%{axis}:missing%'")
            high_priority_flags.append(f"scc.manual_review_reasons_csv LIKE '%{axis}:conflict%'")

        return f"""
            (
                scc.directory_type != 'localizer'
                OR ({' OR '.join(high_priority_flags)})
            )
        """

    # =========================================================================
    # Item Retrieval
    # =========================================================================

    def get_axes_qc_items(
        self,
        cohort_id: int,
        offset: int = 0,
        limit: int = 100,
        axis: Optional[str] = None,
        flag_type: Optional[str] = None,
    ) -> tuple[list[dict], int]:
        """
        Get stacks needing axes QC, sorted for navigation.

        Also enriches items with any pending draft changes from app_db.

        Sort order: subject_code ASC, study_date ASC, field_strength DESC,
                   manufacturer ASC, model ASC, stack_id ASC

        Optional filters:
        - axis: Filter to items with flags on this axis (base, technique, etc.)
        - flag_type: Filter to items with this flag type (missing, conflict, etc.)

        Returns: (items, total_count)
        """
        self._ensure_initialized()

        # First get session to look up draft changes
        session_info = self.get_or_create_session(cohort_id)
        session_id = session_info["id"]

        # Get draft changes for this session
        draft_changes_by_uid = {}
        with session_scope() as db:
            items_with_changes = repository.get_items_with_draft_changes(db, session_id, category="axes")
            for item in items_with_changes:
                draft_changes_by_uid[item.series_instance_uid] = {
                    dc.field_name: dc.new_value for dc in item.draft_changes
                }

        with MetadataSessionLocal() as meta_db:
            # Build filter clauses
            axis_filter = self._build_axis_filter(axis, flag_type)
            localizer_filter = self._build_localizer_filter()
            query_params = {"cohort_id": cohort_id}

            # First get total count
            count_query = f"""
                SELECT COUNT(DISTINCT scc.series_stack_id)
                FROM series_classification_cache scc
                JOIN series s ON scc.series_instance_uid = s.series_instance_uid
                LEFT JOIN subject_cohorts sc ON s.subject_id = sc.subject_id
                LEFT JOIN cohort c ON sc.cohort_id = c.cohort_id
                WHERE (
                    scc.dicom_origin_cohort = (SELECT name FROM cohort WHERE cohort_id = :cohort_id)
                    OR c.cohort_id = :cohort_id
                )
                AND scc.manual_review_required = 1
                AND ({axis_filter})
                AND {localizer_filter}
            """
            total = meta_db.execute(text(count_query), query_params).scalar() or 0

            # Main query with all needed data
            query = f"""
                SELECT
                    scc.series_stack_id,
                    scc.series_instance_uid,
                    ss.stack_index,
                    scc.manual_review_reasons_csv,

                    -- Subject/Session info
                    subj.subject_code,
                    subj.subject_id,
                    st.study_date,

                    -- Scanner info (magnetic_field_strength is in mri_series_details)
                    msd.magnetic_field_strength,
                    sf.manufacturer,
                    sf.manufacturer_model,

                    -- Acquisition parameters (only non-null shown)
                    sf.modality as stack_modality,
                    ss.stack_key,
                    ss.stack_echo_time,
                    ss.stack_repetition_time,
                    ss.stack_inversion_time,
                    ss.stack_flip_angle,
                    sf.mr_acquisition_type,
                    sf.fov_x,
                    sf.fov_y,

                    -- Sequence tags
                    ss.stack_image_type,
                    sf.scanning_sequence,
                    sf.sequence_variant,
                    sf.scan_options,

                    -- Contrast search blob fields
                    s.sequence_name,
                    s.protocol_name,
                    s.series_description,
                    s.series_comments,

                    -- Current classification values
                    scc.base,
                    scc.technique,
                    scc.modifier_csv,
                    scc.provenance,
                    scc.construct_csv,
                    scc.acceleration_csv,

                    -- Intent fields
                    scc.directory_type,
                    scc.spinal_cord,
                    scc.post_contrast

                FROM series_classification_cache scc
                JOIN series s ON scc.series_instance_uid = s.series_instance_uid
                JOIN study st ON s.study_id = st.study_id
                LEFT JOIN subject subj ON s.subject_id = subj.subject_id
                LEFT JOIN series_stack ss ON scc.series_stack_id = ss.series_stack_id
                LEFT JOIN stack_fingerprint sf ON ss.series_stack_id = sf.series_stack_id
                LEFT JOIN mri_series_details msd ON s.series_id = msd.series_id
                LEFT JOIN subject_cohorts sc ON s.subject_id = sc.subject_id
                LEFT JOIN cohort c ON sc.cohort_id = c.cohort_id
                WHERE (
                    scc.dicom_origin_cohort = (SELECT name FROM cohort WHERE cohort_id = :cohort_id)
                    OR c.cohort_id = :cohort_id
                )
                AND scc.manual_review_required = 1
                AND ({axis_filter})
                AND {localizer_filter}
                ORDER BY
                    subj.subject_code ASC NULLS LAST,
                    st.study_date ASC NULLS LAST,
                    msd.magnetic_field_strength DESC NULLS LAST,
                    sf.manufacturer ASC NULLS LAST,
                    sf.manufacturer_model ASC NULLS LAST,
                    scc.series_stack_id ASC
                LIMIT :limit OFFSET :offset
            """

            rows = meta_db.execute(
                text(query),
                {**query_params, "limit": limit, "offset": offset}
            ).fetchall()

            items = []
            for row in rows:
                item = self._row_to_item_dict(row)

                # Enrich with draft changes
                series_uid = row.series_instance_uid
                if series_uid in draft_changes_by_uid:
                    item["draft_changes"] = draft_changes_by_uid[series_uid]
                    item["has_draft"] = True
                else:
                    item["draft_changes"] = {}
                    item["has_draft"] = False

                items.append(item)

            return items, total

    def get_axes_qc_item(self, stack_id: int, cohort_id: Optional[int] = None) -> Optional[dict]:
        """Get a single stack with full details for QC, including draft changes."""
        self._ensure_initialized()

        with MetadataSessionLocal() as meta_db:
            query = """
                SELECT
                    scc.series_stack_id,
                    scc.series_instance_uid,
                    ss.stack_index,
                    scc.manual_review_reasons_csv,

                    -- Subject/Session info
                    subj.subject_code,
                    subj.subject_id,
                    st.study_date,

                    -- Scanner info (magnetic_field_strength is in mri_series_details)
                    msd.magnetic_field_strength,
                    sf.manufacturer,
                    sf.manufacturer_model,

                    -- Acquisition parameters
                    sf.modality as stack_modality,
                    ss.stack_key,
                    ss.stack_echo_time,
                    ss.stack_repetition_time,
                    ss.stack_inversion_time,
                    ss.stack_flip_angle,
                    sf.mr_acquisition_type,
                    sf.fov_x,
                    sf.fov_y,

                    -- Sequence tags
                    ss.stack_image_type,
                    sf.scanning_sequence,
                    sf.sequence_variant,
                    sf.scan_options,

                    -- Contrast search blob fields
                    s.sequence_name,
                    s.protocol_name,
                    s.series_description,
                    s.series_comments,

                    -- Current classification values
                    scc.base,
                    scc.technique,
                    scc.modifier_csv,
                    scc.provenance,
                    scc.construct_csv,
                    scc.acceleration_csv,

                    -- Intent fields
                    scc.directory_type,
                    scc.spinal_cord,
                    scc.post_contrast

                FROM series_classification_cache scc
                JOIN series s ON scc.series_instance_uid = s.series_instance_uid
                JOIN study st ON s.study_id = st.study_id
                LEFT JOIN subject subj ON s.subject_id = subj.subject_id
                LEFT JOIN series_stack ss ON scc.series_stack_id = ss.series_stack_id
                LEFT JOIN stack_fingerprint sf ON ss.series_stack_id = sf.series_stack_id
                LEFT JOIN mri_series_details msd ON s.series_id = msd.series_id
                WHERE scc.series_stack_id = :stack_id
            """

            row = meta_db.execute(text(query), {"stack_id": stack_id}).fetchone()

            if not row:
                return None

            item = self._row_to_item_dict(row)

            # Get draft changes if we have a cohort_id
            if cohort_id:
                session_info = self.get_or_create_session(cohort_id)
                session_id = session_info["id"]

                with session_scope() as db:
                    # Find QC item for this series
                    qc_item = repository.get_item_by_uid(
                        db, session_id, row.series_instance_uid, row.stack_index or 0
                    )
                    if qc_item:
                        qc_item = repository.get_item_with_changes(db, qc_item.id)
                        item["draft_changes"] = {
                            dc.field_name: dc.new_value for dc in qc_item.draft_changes
                        }
                        item["has_draft"] = len(qc_item.draft_changes) > 0
                    else:
                        item["draft_changes"] = {}
                        item["has_draft"] = False
            else:
                item["draft_changes"] = {}
                item["has_draft"] = False

            return item

    def _row_to_item_dict(self, row) -> dict:
        """Convert a database row to item dictionary."""
        flags = _determine_axis_flags(row.manual_review_reasons_csv)

        # Build compact params dict (only non-null values)
        params = {}
        if row.stack_modality:
            params["modality"] = row.stack_modality
        if row.stack_key:
            params["key"] = row.stack_key
        if row.stack_echo_time is not None:
            params["te"] = round(row.stack_echo_time, 1)
        if row.stack_repetition_time is not None:
            params["tr"] = round(row.stack_repetition_time, 1)
        if row.stack_inversion_time is not None:
            params["ti"] = round(row.stack_inversion_time, 1)
        if row.stack_flip_angle is not None:
            params["fa"] = round(row.stack_flip_angle, 1)
        if row.mr_acquisition_type:
            params["acq"] = row.mr_acquisition_type
        if row.fov_x and row.fov_y:
            params["fov"] = f"{int(row.fov_x)}x{int(row.fov_y)}"

        # Build tags dict (only non-null values)
        tags = {}
        if row.stack_image_type:
            tags["image_type"] = row.stack_image_type
        if row.scanning_sequence:
            tags["scanning_seq"] = row.scanning_sequence
        if row.sequence_variant:
            tags["seq_variant"] = row.sequence_variant
        if row.scan_options:
            tags["scan_options"] = row.scan_options
        if row.sequence_name:
            tags["seq_name"] = row.sequence_name
        if row.protocol_name:
            tags["protocol"] = row.protocol_name
        if row.series_description:
            tags["description"] = row.series_description
        if row.series_comments:
            tags["comments"] = row.series_comments

        # Scanner info
        scanner = {}
        if row.magnetic_field_strength:
            scanner["field_strength"] = row.magnetic_field_strength
        if row.manufacturer:
            scanner["manufacturer"] = row.manufacturer
        if row.manufacturer_model:
            scanner["model"] = row.manufacturer_model

        # Current classification values
        current = {
            "base": row.base,
            "technique": row.technique,
            "modifier": row.modifier_csv,
            "provenance": row.provenance,
            "construct": row.construct_csv,
            "acceleration": row.acceleration_csv,
        }

        # Intent fields
        intent = {
            "directory_type": row.directory_type,
            "spinal_cord": row.spinal_cord,
            "post_contrast": row.post_contrast,
        }

        return {
            "stack_id": row.series_stack_id,
            "series_uid": row.series_instance_uid,
            "stack_index": row.stack_index or 0,
            "subject_code": row.subject_code,
            "subject_id": row.subject_id,
            "study_date": str(row.study_date) if row.study_date else None,
            "scanner": scanner,
            "params": params,
            "tags": tags,
            "flags": flags,
            "current": current,
            "intent": intent,
        }

    # =========================================================================
    # Draft Operations
    # =========================================================================

    def save_axis_draft(
        self,
        cohort_id: int,
        stack_id: int,
        axis: str,
        value: str | None
    ) -> dict:
        """
        Save an axis value change as a draft in application_db.

        Does NOT write to metadata_db. Use confirm_axes_changes() to persist.
        """
        self._ensure_initialized()

        if axis not in AXES_CATEGORIES:
            raise ValueError(f"Invalid axis: {axis}. Must be one of {AXES_CATEGORIES}")

        column = AXIS_TO_COLUMN[axis]

        # Get session
        session_info = self.get_or_create_session(cohort_id)
        session_id = session_info["id"]

        # Get series info from metadata_db
        with MetadataSessionLocal() as meta_db:
            query = """
                SELECT scc.series_instance_uid, ss.stack_index, st.study_instance_uid,
                       scc.base, scc.technique, scc.modifier_csv, scc.provenance, scc.construct_csv
                FROM series_classification_cache scc
                JOIN series s ON scc.series_instance_uid = s.series_instance_uid
                JOIN study st ON s.study_id = st.study_id
                LEFT JOIN series_stack ss ON scc.series_stack_id = ss.series_stack_id
                WHERE scc.series_stack_id = :stack_id
            """
            row = meta_db.execute(text(query), {"stack_id": stack_id}).fetchone()
            if not row:
                raise ValueError(f"Stack {stack_id} not found")

            series_uid = row.series_instance_uid
            stack_index = row.stack_index or 0
            study_uid = row.study_instance_uid

            # Get original value for the axis
            original_value = getattr(row, column, None)
            if original_value is not None:
                original_value = str(original_value)

        # Save draft change in app_db
        with session_scope() as db:
            # Get or create QC item for this stack
            qc_item = repository.get_item_by_uid(db, session_id, series_uid, stack_index)
            if not qc_item:
                # Create new QC item
                qc_item = repository.create_item(
                    db,
                    session_id=session_id,
                    category="axes",
                    series_instance_uid=series_uid,
                    study_instance_uid=study_uid,
                    stack_index=stack_index,
                    priority=0,
                    review_reasons_csv=None,
                )

            # Upsert draft change
            repository.upsert_draft_change(
                db,
                item_id=qc_item.id,
                field_name=column,
                original_value=original_value,
                new_value=value,
                change_reason="User QC correction",
            )

            # Update item status
            if qc_item.status == "pending":
                repository.update_item_status(db, qc_item.id, "reviewed")

        return {
            "success": True,
            "stack_id": stack_id,
            "axis": axis,
            "value": value,
            "draft": True,  # Indicates this is a draft, not persisted yet
        }

    def confirm_axes_changes(self, cohort_id: int) -> dict:
        """
        Confirm and push all draft changes to metadata_db.

        Returns count of confirmed items and changes.
        """
        self._ensure_initialized()

        session_info = self.get_or_create_session(cohort_id)
        session_id = session_info["id"]

        confirmed_items = 0
        confirmed_changes = 0

        with session_scope() as app_db:
            # Get all items with draft changes
            items_with_changes = repository.get_items_with_draft_changes(app_db, session_id, category="axes")

            for item in items_with_changes:
                if not item.draft_changes:
                    continue

                # Push changes to metadata_db
                success = self._push_item_to_metadata_db(item)
                if success:
                    confirmed_changes += len(item.draft_changes)
                    confirmed_items += 1

                    # Mark item as confirmed and delete draft changes
                    repository.update_item_status(app_db, item.id, "confirmed")
                    repository.delete_draft_changes_for_item(app_db, item.id)

        return {
            "success": True,
            "confirmed_items": confirmed_items,
            "confirmed_changes": confirmed_changes,
        }

    def discard_axes_changes(self, cohort_id: int) -> dict:
        """
        Discard all draft changes for a cohort's axes QC session.

        Returns count of discarded items and changes.
        """
        self._ensure_initialized()

        session_info = self.get_or_create_session(cohort_id)
        session_id = session_info["id"]

        discarded_items = 0
        discarded_changes = 0

        with session_scope() as db:
            # Get all items with draft changes
            items_with_changes = repository.get_items_with_draft_changes(db, session_id, category="axes")

            for item in items_with_changes:
                discarded_changes += len(item.draft_changes)
                discarded_items += 1

                # Delete draft changes
                repository.delete_draft_changes_for_item(db, item.id)

                # Reset status to pending
                if item.status == "reviewed":
                    repository.update_item_status(db, item.id, "pending")

        return {
            "success": True,
            "discarded_items": discarded_items,
            "discarded_changes": discarded_changes,
        }

    def _push_item_to_metadata_db(self, item) -> bool:
        """Push draft changes for a single item to metadata_db."""
        if not item.draft_changes:
            return True

        # Build update values from draft changes
        update_values = {}
        for change in item.draft_changes:
            field = change.field_name
            if field in AXIS_TO_COLUMN.values():
                update_values[field] = change.new_value

        if not update_values:
            return True

        with MetadataSessionLocal() as meta_db:
            # Get current review reasons to update them
            reasons_query = """
                SELECT manual_review_reasons_csv
                FROM series_classification_cache
                WHERE series_instance_uid = :series_uid
            """
            row = meta_db.execute(
                text(reasons_query), {"series_uid": item.series_instance_uid}
            ).fetchone()

            # Remove review reasons for axes we're updating
            new_reasons = None
            if row and row.manual_review_reasons_csv:
                reasons = row.manual_review_reasons_csv.split(",")
                filtered_reasons = []

                for reason in reasons:
                    reason_lower = reason.lower().strip()
                    keep = True

                    # Check each axis we're updating
                    for axis, column in AXIS_TO_COLUMN.items():
                        if column in update_values:
                            prefixes = AXES_REASON_PREFIXES.get(axis, [])
                            if any(reason_lower.startswith(p.lower()) for p in prefixes):
                                keep = False
                                break

                    if keep:
                        filtered_reasons.append(reason)

                new_reasons = ",".join(filtered_reasons) if filtered_reasons else None

            # Build and execute update query
            set_clauses = [f"{k} = :{k}" for k in update_values.keys()]
            set_clauses.append("manual_review_reasons_csv = :new_reasons")

            # Clear manual_review_required if no reasons left
            set_clauses.append(
                "manual_review_required = CASE WHEN COALESCE(:new_reasons, '') = '' THEN 0 ELSE manual_review_required END"
            )

            query = f"""
                UPDATE series_classification_cache
                SET {', '.join(set_clauses)}
                WHERE series_instance_uid = :series_uid
            """

            params = {
                **update_values,
                "new_reasons": new_reasons,
                "series_uid": item.series_instance_uid,
            }

            meta_db.execute(
                text(query).bindparams(bindparam("new_reasons", type_=String)),
                params
            )
            meta_db.commit()

        return True

    # =========================================================================
    # Legacy method (now uses draft pattern)
    # =========================================================================

    def update_axis_value(
        self,
        cohort_id: int,
        stack_id: int,
        axis: str,
        value: str | None
    ) -> dict:
        """
        Update a single axis value for a stack.

        Now saves as draft instead of direct write to metadata_db.
        """
        return self.save_axis_draft(cohort_id, stack_id, axis, value)

    # =========================================================================
    # Filter Options
    # =========================================================================

    def get_available_filters(self, cohort_id: int) -> dict:
        """
        Get available axes and flag types that have QC items.

        Returns dict with available_axes and available_flags lists,
        containing only values that have at least one QC item.
        """
        self._ensure_initialized()

        with MetadataSessionLocal() as meta_db:
            # Query to get all review reasons for this cohort
            query = """
                SELECT DISTINCT scc.manual_review_reasons_csv
                FROM series_classification_cache scc
                JOIN series s ON scc.series_instance_uid = s.series_instance_uid
                LEFT JOIN subject_cohorts sc ON s.subject_id = sc.subject_id
                LEFT JOIN cohort c ON sc.cohort_id = c.cohort_id
                WHERE (
                    scc.dicom_origin_cohort = (SELECT name FROM cohort WHERE cohort_id = :cohort_id)
                    OR c.cohort_id = :cohort_id
                )
                AND scc.manual_review_required = 1
                AND (
                    scc.manual_review_reasons_csv LIKE '%base:%'
                    OR scc.manual_review_reasons_csv LIKE '%technique:%'
                    OR scc.manual_review_reasons_csv LIKE '%modifier:%'
                    OR scc.manual_review_reasons_csv LIKE '%provenance:%'
                    OR scc.manual_review_reasons_csv LIKE '%construct:%'
                )
            """

            rows = meta_db.execute(text(query), {"cohort_id": cohort_id}).fetchall()

            # Parse all review reasons to find available axes and flags
            available_axes = set()
            available_flags = set()

            valid_axes = ["base", "technique", "modifier", "provenance", "construct"]
            valid_flags = ["missing", "conflict", "low_confidence", "ambiguous", "review"]

            for row in rows:
                if not row.manual_review_reasons_csv:
                    continue

                reasons = [r.strip().lower() for r in row.manual_review_reasons_csv.split(",") if r.strip()]

                for reason in reasons:
                    # Check each axis
                    for axis in valid_axes:
                        if reason.startswith(f"{axis}:"):
                            available_axes.add(axis)
                            # Extract flag type
                            for flag in valid_flags:
                                if flag in reason:
                                    available_flags.add(flag)
                                    break
                            else:
                                # Default to "review" if no specific flag found
                                available_flags.add("review")

            return {
                "available_axes": sorted(list(available_axes)),
                "available_flags": sorted(list(available_flags)),
            }

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def get_image_comments_for_stack(self, stack_id: int) -> str | None:
        """Get image_comments from a representative instance of the stack."""
        with MetadataSessionLocal() as meta_db:
            query = """
                SELECT i.image_comments
                FROM instance i
                WHERE i.series_stack_id = :stack_id
                AND i.image_comments IS NOT NULL
                AND i.image_comments != ''
                LIMIT 1
            """
            row = meta_db.execute(text(query), {"stack_id": stack_id}).fetchone()
            return row.image_comments if row else None


# Global service instance
axes_qc_service = AxesQCService()
