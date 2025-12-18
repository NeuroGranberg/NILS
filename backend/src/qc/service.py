"""Service layer for QC Pipeline - business logic orchestration."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text, update

logger = logging.getLogger(__name__)

from db.session import session_scope, SessionLocal
from metadata_db.session import SessionLocal as MetadataSessionLocal
from metadata_db.schema import SeriesClassificationCache, Series, Study, SeriesStack

from . import repository
from .models import (
    QCSession,
    QCItem,
    QCSessionDTO,
    QCItemDTO,
    QCClassificationDTO,
    QCDraftChangeDTO,
    QCRuleViolationDTO,
    CreateQCSessionPayload,
    UpdateQCItemPayload,
    ConfirmQCChangesPayload,
)
from .rules_engine import rules_engine, RuleContext


# =============================================================================
# Category Configuration
# =============================================================================

# Map review reason prefixes to QC categories
# Note: The classification pipeline uses various prefixes, we match any of these
CATEGORY_REASON_PREFIXES = {
    "base": ["base:"],
    "provenance": ["provenance:"],
    "technique": ["technique:"],
    "body_part": ["body_part:", "bodypart:", "spine", "heuristic:"],
    "contrast": ["contrast:"],
    "modifier": ["modifier:"],
    "construct": ["construct:"],
    # Combined axes category for AxesQC - matches any classification axis
    "axes": ["base:", "technique:", "modifier:", "provenance:", "construct:"],
}

# Fields that can be edited in each category
CATEGORY_EDITABLE_FIELDS = {
    "base": ["base", "directory_type"],
    "provenance": ["provenance"],
    "technique": ["technique"],
    "body_part": ["spinal_cord", "directory_type"],
    "contrast": ["post_contrast"],
    "modifier": ["modifier_csv", "directory_type"],
    "construct": ["construct_csv", "directory_type"],
    # Combined axes category for AxesQC - all 5 classification axes
    "axes": ["base", "technique", "modifier_csv", "provenance", "construct_csv"],
}

# All editable classification fields
ALL_EDITABLE_FIELDS = [
    "base",
    "technique",
    "provenance",
    "modifier_csv",
    "construct_csv",
    "acceleration_csv",
    "directory_type",
    "post_contrast",
    "localizer",
    "spinal_cord",
]


class QCService:
    """Service for QC Pipeline operations."""

    def __init__(self):
        self._initialized = False

    def _ensure_initialized(self) -> None:
        """Ensure QC tables exist in application DB."""
        if self._initialized:
            return

        from cohorts.models import Base
        from db.session import engine

        # Import models to register them with Base
        from .models import QCSession, QCItem, QCDraftChange

        Base.metadata.create_all(engine, checkfirst=True)
        self._initialized = True

    # =========================================================================
    # Session Management
    # =========================================================================

    def create_session(self, payload: CreateQCSessionPayload) -> QCSessionDTO:
        """Create a new QC session and populate items from classification cache."""
        self._ensure_initialized()

        with session_scope() as db:
            # Create session
            session = repository.create_session(db, payload.cohort_id)

            # Populate items from metadata DB
            items_created = self._populate_session_items(
                db,
                session.id,
                payload.cohort_id,
                payload.categories,
                payload.include_flagged_only,
            )

            # Update counts
            session.total_items = items_created
            db.flush()

            return self._session_to_dto(db, session)

    def get_session(self, session_id: int) -> Optional[QCSessionDTO]:
        """Get a QC session by ID."""
        self._ensure_initialized()

        with session_scope() as db:
            session = repository.get_session(db, session_id)
            if not session:
                return None
            return self._session_to_dto(db, session)

    def get_or_create_session(self, cohort_id: int) -> QCSessionDTO:
        """Get existing active session or create a new one."""
        self._ensure_initialized()

        with session_scope() as db:
            # Look for existing non-completed session
            session = repository.get_session_for_cohort(db, cohort_id)
            if session and session.status not in ("completed", "abandoned"):
                return self._session_to_dto(db, session)

            # Create new session
            session = repository.create_session(db, cohort_id)

            # Populate with all categories
            items_created = self._populate_session_items(
                db,
                session.id,
                cohort_id,
                categories=["base", "provenance", "technique", "body_part", "contrast"],
                include_flagged_only=True,
            )

            session.total_items = items_created
            db.flush()

            return self._session_to_dto(db, session)

    def get_session_summary(self, session_id: int) -> dict:
        """Get session summary with counts by category and status."""
        self._ensure_initialized()

        with session_scope() as db:
            session = repository.get_session(db, session_id)
            if not session:
                return {}

            category_counts = repository.count_items_by_category(db, session_id)
            status_counts = repository.count_items_by_status(db, session_id)

            return {
                "session_id": session_id,
                "cohort_id": session.cohort_id,
                "status": session.status,
                "total_items": session.total_items,
                "reviewed_items": session.reviewed_items,
                "confirmed_items": session.confirmed_items,
                "category_counts": category_counts,
                "status_counts": status_counts,
            }

    def refresh_session(self, session_id: int) -> Optional[QCSessionDTO]:
        """Refresh session items from metadata DB."""
        self._ensure_initialized()

        with session_scope() as db:
            session = repository.get_session(db, session_id)
            if not session:
                return None

            # Get cohort_id to repopulate
            cohort_id = session.cohort_id

            # Delete existing pending items (keep reviewed/confirmed)
            # For now, we'll just update counts
            repository.update_session_counts(db, session_id)

            return self._session_to_dto(db, session)

    # =========================================================================
    # Data Viewer (Subject -> Session -> Stack)
    # =========================================================================

    def get_subjects_for_cohort(
        self,
        cohort_id: int,
        offset: int = 0,
        limit: int = 50,
        search: Optional[str] = None,
        sort_by: Optional[str] = "code",
    ) -> tuple[list[dict], int]:
        """Get paginated subjects for a cohort with search across multiple fields."""
        self._ensure_initialized()

        with MetadataSessionLocal() as meta_db:
            # Base filtering conditions
            where_clause = "WHERE c.cohort_id = :cohort_id"
            params = {"cohort_id": cohort_id, "sort_by": sort_by or "code"}

            if search:
                where_clause += """
                    AND (
                        s.subject_code ILIKE :search
                        OR CAST(s.subject_id AS TEXT) ILIKE :search
                        OR EXISTS (
                            SELECT 1 FROM subject_other_identifiers soi_search 
                            WHERE soi_search.subject_id = s.subject_id 
                            AND soi_search.other_identifier ILIKE :search
                        )
                    )
                """
                params["search"] = f"%{search}%"

            # Count total
            count_query = f"""
                SELECT count(DISTINCT s.subject_id)
                FROM subject s
                JOIN subject_cohorts sc ON s.subject_id = sc.subject_id
                JOIN cohort c ON sc.cohort_id = c.cohort_id
                {where_clause}
            """
            total = meta_db.execute(text(count_query), params).scalar()

            # Get subjects with aggregated other identifiers
            select_query = f"""
                SELECT 
                    s.subject_id, 
                    s.subject_code, 
                    s.patient_sex, 
                    s.patient_birth_date,
                    s.created_at,
                    COALESCE(
                        json_agg(
                            json_build_object(
                                'type', it.id_type_name, 
                                'value', soi.other_identifier
                            ) 
                        ) FILTER (WHERE soi.other_identifier IS NOT NULL), 
                        '[]'
                    ) as other_ids
                FROM subject s
                JOIN subject_cohorts sc ON s.subject_id = sc.subject_id
                JOIN cohort c ON sc.cohort_id = c.cohort_id
                LEFT JOIN subject_other_identifiers soi ON s.subject_id = soi.subject_id
                LEFT JOIN id_types it ON soi.id_type_id = it.id_type_id
                {where_clause}
                GROUP BY s.subject_id, s.subject_code, s.patient_sex, s.patient_birth_date, s.created_at
                ORDER BY 
                    CASE WHEN :sort_by = 'code' THEN s.subject_code END ASC,
                    CASE WHEN :sort_by != 'code' THEN 
                        (
                            SELECT soi_sort.other_identifier 
                            FROM subject_other_identifiers soi_sort 
                            JOIN id_types it_sort ON soi_sort.id_type_id = it_sort.id_type_id 
                            WHERE soi_sort.subject_id = s.subject_id 
                            AND it_sort.id_type_name = :sort_by 
                            LIMIT 1
                        )
                    END ASC NULLS LAST,
                    s.subject_code ASC
                LIMIT :limit OFFSET :offset
            """
            params["limit"] = limit
            params["offset"] = offset

            rows = meta_db.execute(text(select_query), params).fetchall()

            subjects = []
            for row in rows:
                subjects.append(
                    {
                        "id": row.subject_id,
                        "code": row.subject_code,
                        "sex": row.patient_sex,
                        "birth_date": str(row.patient_birth_date)
                        if row.patient_birth_date
                        else None,
                        "created_at": row.created_at,
                        "other_ids": row.other_ids if row.other_ids else [],
                    }
                )

            return subjects, total

    def get_sessions_for_subject(self, subject_id: int) -> list[dict]:
        """
        Get sessions for a subject.
        A 'session' is defined as a unique study_date.
        """
        self._ensure_initialized()

        with MetadataSessionLocal() as meta_db:
            # Group studies by date to form "sessions"
            query = """
                SELECT 
                    study_date,
                    COUNT(study_id) as study_count,
                    string_agg(DISTINCT modality, ',') as modalities
                FROM study
                WHERE subject_id = :subject_id
                GROUP BY study_date
                ORDER BY study_date DESC NULLS LAST
            """

            rows = meta_db.execute(text(query), {"subject_id": subject_id}).fetchall()

            sessions = []
            for row in rows:
                date_str = str(row.study_date) if row.study_date else "Unknown Date"
                sessions.append(
                    {
                        "date": date_str,
                        "study_count": row.study_count,
                        "modalities": row.modalities.split(",")
                        if row.modalities
                        else [],
                    }
                )

            return sessions

    def get_stacks_for_session(
        self, subject_id: int, date_str: str
    ) -> list[QCClassificationDTO]:
        """Get all stacks for a subject on a specific date (session)."""
        self._ensure_initialized()

        with MetadataSessionLocal() as meta_db:
            # Query similar to _query_classification but for a list
            query = """
                SELECT
                    scc.series_stack_id,
                    scc.series_instance_uid,
                    st.study_instance_uid,
                    COALESCE(ss.stack_index, 0) as stack_index,
                    subj.subject_code as subject_id,
                    st.study_date,
                    scc.directory_type,
                    scc.base,
                    scc.technique,
                    scc.modifier_csv,
                    scc.construct_csv,
                    scc.provenance,
                    scc.acceleration_csv,
                    scc.post_contrast,
                    scc.localizer,
                    scc.spinal_cord,
                    scc.manual_review_required,
                    scc.manual_review_reasons_csv,
                    scc.aspect_ratio,
                    scc.fov_x_mm,
                    scc.fov_y_mm,
                    scc.slices_count,
                    s.series_description,
                    s.modality,
                    s.series_time,
                    sf.stack_orientation
                FROM series_classification_cache scc
                JOIN series s ON scc.series_instance_uid = s.series_instance_uid
                JOIN study st ON s.study_id = st.study_id
                LEFT JOIN series_stack ss ON scc.series_stack_id = ss.series_stack_id
                LEFT JOIN subject subj ON s.subject_id = subj.subject_id
                LEFT JOIN stack_fingerprint sf ON scc.series_stack_id = sf.series_stack_id
                WHERE s.subject_id = :subject_id
            """

            params = {"subject_id": subject_id}

            if date_str == "Unknown Date":
                query += " AND st.study_date IS NULL"
            else:
                query += " AND st.study_date = :date"
                params["date"] = date_str

            query += " ORDER BY s.series_id, ss.stack_index"

            result = meta_db.execute(text(query), params)
            rows = result.fetchall()

            dtos = []
            for row in rows:
                study_date_str = None
                if row.study_date:
                    try:
                        study_date_str = str(row.study_date)
                    except Exception:
                        study_date_str = None

                # Convert series_time to string for JSON serialization
                series_time_str = None
                if row.series_time:
                    try:
                        series_time_str = str(row.series_time)
                    except Exception:
                        series_time_str = None

                dtos.append(
                    QCClassificationDTO(
                        series_stack_id=row.series_stack_id,
                        series_instance_uid=row.series_instance_uid,
                        study_instance_uid=row.study_instance_uid,
                        stack_index=row.stack_index or 0,
                        subject_id=str(row.subject_id),  # Ensure string for display
                        study_date=study_date_str,
                        series_time=series_time_str,
                        directory_type=row.directory_type,
                        base=row.base,
                        technique=row.technique,
                        modifier_csv=row.modifier_csv,
                        construct_csv=row.construct_csv,
                        provenance=row.provenance,
                        acceleration_csv=row.acceleration_csv,
                        post_contrast=row.post_contrast,
                        localizer=row.localizer,
                        spinal_cord=row.spinal_cord,
                        manual_review_required=row.manual_review_required,
                        manual_review_reasons_csv=row.manual_review_reasons_csv,
                        aspect_ratio=row.aspect_ratio,
                        fov_x_mm=row.fov_x_mm,
                        fov_y_mm=row.fov_y_mm,
                        slices_count=row.slices_count,
                        orientation=row.stack_orientation,
                        series_description=row.series_description,
                        modality=row.modality,
                    )
                )

            return dtos

    # =========================================================================
    # Item Operations
    # =========================================================================

    def get_items_for_category(
        self,
        session_id: int,
        category: str,
        offset: int = 0,
        limit: int = 50,
        status: Optional[str] = None,
    ) -> tuple[list[QCItemDTO], int]:
        """Get paginated items for a category with classification data."""
        self._ensure_initialized()

        with session_scope() as db:
            logger.debug(
                f"get_items_for_category: session_id={session_id}, category={category}, status={status}"
            )
            items, total = repository.list_items_for_session(
                db,
                session_id,
                category=category,
                status=status,
                offset=offset,
                limit=limit,
            )
            logger.debug(f"Found {len(items)} items, total={total}")

            # Enrich with classification data from metadata DB
            dtos = [self._item_to_dto(item) for item in items]
            self._enrich_items_with_classification(dtos)

            return dtos, total

    def get_item_detail(self, item_id: int) -> Optional[QCItemDTO]:
        """Get full item details including classification data."""
        self._ensure_initialized()

        with session_scope() as db:
            item = repository.get_item_with_changes(db, item_id)
            if not item:
                return None

            dto = self._item_to_dto(item)
            self._enrich_items_with_classification([dto])

            return dto

    def update_item(
        self, item_id: int, payload: UpdateQCItemPayload
    ) -> Optional[QCItemDTO]:
        """Save draft changes for an item."""
        self._ensure_initialized()

        with session_scope() as db:
            item = repository.get_item_with_changes(db, item_id)
            if not item:
                return None

            # Get current classification to store original values
            classification = self._get_classification_for_item(item)

            # Upsert draft changes
            for field_name, new_value in payload.changes.items():
                if field_name not in ALL_EDITABLE_FIELDS:
                    continue

                original_value = None
                if classification:
                    original_value = getattr(classification, field_name, None)
                    if original_value is not None:
                        original_value = str(original_value)

                repository.upsert_draft_change(
                    db,
                    item_id,
                    field_name,
                    original_value,
                    new_value,
                    payload.change_reason,
                )

            # Update item status to reviewed
            if item.status == "pending":
                repository.update_item_status(db, item_id, "reviewed")

            # Update session counts
            repository.update_session_counts(db, item.session_id)

            # Refresh item
            item = repository.get_item_with_changes(db, item_id)
            dto = self._item_to_dto(item)
            self._enrich_items_with_classification([dto])

            return dto

    def discard_changes(self, item_id: int) -> Optional[QCItemDTO]:
        """Discard all draft changes for an item."""
        self._ensure_initialized()

        with session_scope() as db:
            item = repository.get_item(db, item_id)
            if not item:
                return None

            # Delete draft changes
            repository.delete_draft_changes_for_item(db, item_id)

            # Reset status to pending if it was reviewed
            if item.status == "reviewed":
                repository.update_item_status(db, item_id, "pending")
                repository.update_session_counts(db, item.session_id)

            # Refresh item
            item = repository.get_item_with_changes(db, item_id)
            dto = self._item_to_dto(item)
            self._enrich_items_with_classification([dto])

            return dto

    def skip_item(self, item_id: int) -> Optional[QCItemDTO]:
        """Mark item as skipped (no changes needed)."""
        self._ensure_initialized()

        with session_scope() as db:
            item = repository.get_item(db, item_id)
            if not item:
                return None

            # Delete any draft changes
            repository.delete_draft_changes_for_item(db, item_id)

            # Update status
            repository.update_item_status(db, item_id, "skipped")
            repository.update_session_counts(db, item.session_id)

            # Refresh item
            item = repository.get_item_with_changes(db, item_id)
            dto = self._item_to_dto(item)
            self._enrich_items_with_classification([dto])

            return dto

    # =========================================================================
    # Confirmation
    # =========================================================================

    def confirm_items(self, session_id: int, payload: ConfirmQCChangesPayload) -> int:
        """Confirm and push draft changes to metadata DB."""
        self._ensure_initialized()

        confirmed_count = 0

        with session_scope() as app_db:
            # Get items with draft changes
            for item_id in payload.item_ids:
                item = repository.get_item_with_changes(app_db, item_id)
                if not item or item.session_id != session_id:
                    continue

                if not item.draft_changes:
                    # No changes to push, just mark as confirmed
                    repository.update_item_status(app_db, item_id, "confirmed")
                    confirmed_count += 1
                    continue

                # Push changes to metadata DB
                success = self._push_changes_to_metadata_db(item)
                if success:
                    repository.update_item_status(app_db, item_id, "confirmed")
                    confirmed_count += 1

            # Update session counts
            repository.update_session_counts(app_db, session_id)

        return confirmed_count

    def confirm_all_reviewed(self, session_id: int) -> int:
        """Confirm all reviewed items in a session."""
        self._ensure_initialized()

        with session_scope() as db:
            # Get all reviewed items
            items, _ = repository.list_items_for_session(
                db, session_id, status="reviewed", offset=0, limit=10000
            )

            item_ids = [item.id for item in items]

        if not item_ids:
            return 0

        return self.confirm_items(
            session_id, ConfirmQCChangesPayload(item_ids=item_ids)
        )

    # =========================================================================
    # Private Helpers
    # =========================================================================

    def _populate_session_items(
        self,
        db,
        session_id: int,
        cohort_id: int,
        categories: list[str],
        include_flagged_only: bool,
    ) -> int:
        """Populate QC items from metadata DB classification cache."""
        items_to_create = []

        with MetadataSessionLocal() as meta_db:
            # Get cohort info
            from cohorts.repository import get_cohort

            cohort = get_cohort(db, cohort_id)
            if not cohort:
                return 0

            # Query classification cache for items needing review
            # Cohort matching strategy (in order of preference):
            # 1. Direct match on dicom_origin_cohort (new data with properly populated cohort)
            # 2. Fallback to subject->cohort relationship via series.subject_id (legacy data)
            # 3. Include items with empty/NULL cohort for legacy data migration
            query = """
                SELECT DISTINCT
                    scc.series_stack_id,
                    scc.series_instance_uid,
                    scc.manual_review_required,
                    scc.manual_review_reasons_csv,
                    s.series_id,
                    st.study_instance_uid,
                    ss.stack_index
                FROM series_classification_cache scc
                JOIN series s ON scc.series_instance_uid = s.series_instance_uid
                JOIN study st ON s.study_id = st.study_id
                LEFT JOIN series_stack ss ON scc.series_stack_id = ss.series_stack_id
                LEFT JOIN subject_cohorts sc ON s.subject_id = sc.subject_id
                LEFT JOIN cohort c ON sc.cohort_id = c.cohort_id
                WHERE (
                    -- Direct cohort name match (preferred)
                    scc.dicom_origin_cohort = :cohort_name
                    -- OR subject is linked to this cohort (fallback for legacy data)
                    OR c.name = :cohort_name
                    -- OR cohort is empty/None/empty string (legacy data without cohort info)
                    OR (scc.dicom_origin_cohort IS NULL OR scc.dicom_origin_cohort = 'None' OR scc.dicom_origin_cohort = '')
                )
            """

            if include_flagged_only:
                query += " AND scc.manual_review_required = 1"

            # Limit to reasonable number of items for initial load
            # Can be refreshed later to get more
            query += " LIMIT 10000"

            result = meta_db.execute(text(query), {"cohort_name": cohort.name})
            rows = result.fetchall()
            logger.info(f"Found {len(rows)} series to review from metadata DB")

            # Group items by category based on review reasons
            category_counts = {cat: 0 for cat in categories}
            skipped_no_category = 0

            for row in rows:
                series_instance_uid = row.series_instance_uid
                study_instance_uid = row.study_instance_uid
                stack_index = row.stack_index or 0
                review_reasons = row.manual_review_reasons_csv or ""

                # Determine which categories this item belongs to
                item_categories = self._categorize_item(review_reasons, categories)

                if not item_categories:
                    skipped_no_category += 1
                    if skipped_no_category <= 5:  # Log first 5
                        logger.debug(
                            f"Item skipped (no category match): series={series_instance_uid[:20]}..., reasons='{review_reasons}'"
                        )

                for category in item_categories:
                    category_counts[category] = category_counts.get(category, 0) + 1
                    # Calculate priority (higher for errors)
                    priority = self._calculate_priority(review_reasons, category)

                    items_to_create.append(
                        {
                            "session_id": session_id,
                            "category": category,
                            "series_instance_uid": series_instance_uid,
                            "study_instance_uid": study_instance_uid,
                            "stack_index": stack_index,
                            "priority": priority,
                            "review_reasons_csv": review_reasons,
                        }
                    )

        # Log category distribution
        logger.info(f"Category distribution: {category_counts}")
        logger.debug(f"Skipped (no category): {skipped_no_category}")
        logger.info(f"Total items to create: {len(items_to_create)}")

        # Bulk create items
        if items_to_create:
            created = repository.bulk_create_items(db, items_to_create)
            logger.info(f"Created {created} items in database")
            return created

        return 0

    def _categorize_item(
        self, review_reasons: str, allowed_categories: list[str]
    ) -> list[str]:
        """Determine which QC categories an item belongs to based on review reasons."""
        if not review_reasons:
            return []

        reasons_lower = review_reasons.lower()
        categories = []

        for category, prefixes in CATEGORY_REASON_PREFIXES.items():
            if category not in allowed_categories:
                continue

            for prefix in prefixes:
                if prefix.lower() in reasons_lower:
                    categories.append(category)
                    break

        # Log unrecognized review reasons for debugging (no fallback to base)
        if not categories:
            logger.warning(
                f"Unrecognized review reasons (no category match): {review_reasons}"
            )

        return categories

    def _calculate_priority(self, review_reasons: str, category: str) -> int:
        """Calculate item priority. Higher = more urgent."""
        priority = 0

        if "low_confidence" in review_reasons:
            priority += 1
        if "missing" in review_reasons:
            priority += 2
        if "conflict" in review_reasons:
            priority += 3
        if "ambiguous" in review_reasons:
            priority += 2

        return priority

    def _session_to_dto(self, db, session: QCSession) -> QCSessionDTO:
        """Convert session to DTO with category counts."""
        category_counts = repository.count_items_by_category(db, session.id)
        logger.debug(
            f"_session_to_dto: session_id={session.id}, category_counts={category_counts}"
        )

        return QCSessionDTO(
            id=session.id,
            cohort_id=session.cohort_id,
            status=session.status,
            created_at=session.created_at,
            updated_at=session.updated_at,
            started_at=session.started_at,
            completed_at=session.completed_at,
            total_items=session.total_items,
            reviewed_items=session.reviewed_items,
            confirmed_items=session.confirmed_items,
            category_counts=category_counts,
        )

    def _item_to_dto(self, item: QCItem) -> QCItemDTO:
        """Convert item to DTO."""
        return QCItemDTO(
            id=item.id,
            session_id=item.session_id,
            category=item.category,
            series_instance_uid=item.series_instance_uid,
            study_instance_uid=item.study_instance_uid,
            stack_index=item.stack_index,
            status=item.status,
            priority=item.priority,
            review_reasons=item.review_reasons_csv,
            created_at=item.created_at,
            updated_at=item.updated_at,
            reviewed_at=item.reviewed_at,
            confirmed_at=item.confirmed_at,
            draft_changes=[
                QCDraftChangeDTO.model_validate(dc) for dc in item.draft_changes
            ],
        )

    def _enrich_items_with_classification(self, items: list[QCItemDTO]) -> None:
        """Enrich items with classification data and rule violations from metadata DB."""
        if not items:
            return

        with MetadataSessionLocal() as meta_db:
            # Query classification cache and evaluate rules
            for item in items:
                classification = self._query_classification(
                    meta_db, item.series_instance_uid, item.stack_index
                )
                if classification:
                    item.classification = classification

                    # Evaluate rules for this item's category
                    violations = self._evaluate_rules(classification, item.category)
                    item.rule_violations = violations

    def _query_classification(
        self, meta_db, series_instance_uid: str, stack_index: int
    ) -> Optional[QCClassificationDTO]:
        """Query classification data for a series/stack."""
        query = """
            SELECT
                scc.series_stack_id,
                scc.series_instance_uid,
                st.study_instance_uid,
                COALESCE(ss.stack_index, 0) as stack_index,
                -- Subject/Study context
                subj.subject_code as subject_id,
                st.study_date,
                -- Classification fields
                scc.directory_type,
                scc.base,
                scc.technique,
                scc.modifier_csv,
                scc.construct_csv,
                scc.provenance,
                scc.acceleration_csv,
                scc.post_contrast,
                scc.localizer,
                scc.spinal_cord,
                scc.manual_review_required,
                scc.manual_review_reasons_csv,
                scc.aspect_ratio,
                scc.fov_x_mm,
                scc.fov_y_mm,
                scc.slices_count,
                s.series_description,
                s.modality
            FROM series_classification_cache scc
            JOIN series s ON scc.series_instance_uid = s.series_instance_uid
            JOIN study st ON scc.study_id = st.study_id
            LEFT JOIN series_stack ss ON scc.series_stack_id = ss.series_stack_id
            LEFT JOIN subject subj ON s.subject_id = subj.subject_id
            WHERE scc.series_instance_uid = :series_uid
        """

        result = meta_db.execute(text(query), {"series_uid": series_instance_uid})
        row = result.fetchone()

        if not row:
            return None

        # Format study_date if available
        study_date_str = None
        if row.study_date:
            try:
                study_date_str = str(row.study_date)
            except Exception:
                study_date_str = None

        return QCClassificationDTO(
            series_stack_id=row.series_stack_id,
            series_instance_uid=row.series_instance_uid,
            study_instance_uid=row.study_instance_uid,
            stack_index=row.stack_index or 0,
            subject_id=row.subject_id,
            study_date=study_date_str,
            series_number=None,  # Not available in series table
            directory_type=row.directory_type,
            base=row.base,
            technique=row.technique,
            modifier_csv=row.modifier_csv,
            construct_csv=row.construct_csv,
            provenance=row.provenance,
            acceleration_csv=row.acceleration_csv,
            post_contrast=row.post_contrast,
            localizer=row.localizer,
            spinal_cord=row.spinal_cord,
            manual_review_required=row.manual_review_required,
            manual_review_reasons_csv=row.manual_review_reasons_csv,
            aspect_ratio=row.aspect_ratio,
            fov_x_mm=row.fov_x_mm,
            fov_y_mm=row.fov_y_mm,
            slices_count=row.slices_count,
            series_description=row.series_description,
            modality=row.modality,
        )

    def _get_classification_for_item(
        self, item: QCItem
    ) -> Optional[QCClassificationDTO]:
        """Get classification data for a single item."""
        with MetadataSessionLocal() as meta_db:
            return self._query_classification(
                meta_db, item.series_instance_uid, item.stack_index
            )

    def _evaluate_rules(
        self, classification: QCClassificationDTO, category: str
    ) -> list[QCRuleViolationDTO]:
        """Evaluate QC rules for a classification."""
        # Build context from classification DTO
        ctx = RuleContext(
            base=classification.base,
            technique=classification.technique,
            provenance=classification.provenance,
            modifier_csv=classification.modifier_csv,
            construct_csv=classification.construct_csv,
            directory_type=classification.directory_type,
            post_contrast=classification.post_contrast,
            localizer=classification.localizer,
            spinal_cord=classification.spinal_cord,
            aspect_ratio=classification.aspect_ratio,
            fov_x_mm=classification.fov_x_mm,
            fov_y_mm=classification.fov_y_mm,
            slices_count=classification.slices_count,
            manual_review_required=classification.manual_review_required,
            manual_review_reasons_csv=classification.manual_review_reasons_csv,
            series_description=classification.series_description,
            modality=classification.modality,
        )

        # Evaluate rules for the category
        violations = rules_engine.evaluate(ctx, category)

        # Convert to DTOs
        return [
            QCRuleViolationDTO(
                rule_id=v.rule_id,
                category=v.category,
                severity=v.severity,
                message=v.message,
                details=v.details,
            )
            for v in violations
        ]

    def _push_changes_to_metadata_db(self, item: QCItem) -> bool:
        """Push draft changes to metadata DB's series_classification_cache."""
        if not item.draft_changes:
            return True

        # Build update values
        update_values = {}
        for change in item.draft_changes:
            field = change.field_name
            if field not in ALL_EDITABLE_FIELDS:
                continue

            value = change.new_value
            # Convert string values to appropriate types
            if field in ("post_contrast", "localizer", "spinal_cord"):
                value = int(value) if value else None

            update_values[field] = value

        if not update_values:
            return True

        # Clear manual review flag since we're addressing the issue
        update_values["manual_review_required"] = 0

        with MetadataSessionLocal() as meta_db:
            # Update by series_instance_uid
            query = """
                UPDATE series_classification_cache
                SET {}
                WHERE series_instance_uid = :series_uid
            """.format(", ".join(f"{k} = :{k}" for k in update_values.keys()))

            params = {"series_uid": item.series_instance_uid, **update_values}
            meta_db.execute(text(query), params)
            meta_db.commit()

        return True


# Global service instance
qc_service = QCService()
