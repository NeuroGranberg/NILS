"""Repository layer for QC Pipeline - data access functions."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func, select, delete, update
from sqlalchemy.orm import Session, selectinload

from .models import QCSession, QCItem, QCDraftChange


# =============================================================================
# QC Session Repository
# =============================================================================


def create_session(
    db: Session,
    cohort_id: int,
    status: str = "pending",
) -> QCSession:
    """Create a new QC session."""
    session = QCSession(
        cohort_id=cohort_id,
        status=status,
    )
    db.add(session)
    db.flush()
    return session


def get_session(db: Session, session_id: int) -> Optional[QCSession]:
    """Get a QC session by ID."""
    return db.get(QCSession, session_id)


def get_session_with_items(db: Session, session_id: int) -> Optional[QCSession]:
    """Get a QC session with items eagerly loaded."""
    stmt = (
        select(QCSession)
        .where(QCSession.id == session_id)
        .options(selectinload(QCSession.items))
    )
    return db.execute(stmt).scalar_one_or_none()


def get_session_for_cohort(db: Session, cohort_id: int) -> Optional[QCSession]:
    """Get the most recent QC session for a cohort."""
    stmt = (
        select(QCSession)
        .where(QCSession.cohort_id == cohort_id)
        .order_by(QCSession.created_at.desc())
        .limit(1)
    )
    return db.execute(stmt).scalar_one_or_none()


def list_sessions_for_cohort(db: Session, cohort_id: int) -> list[QCSession]:
    """List all QC sessions for a cohort."""
    stmt = (
        select(QCSession)
        .where(QCSession.cohort_id == cohort_id)
        .order_by(QCSession.created_at.desc())
    )
    return list(db.execute(stmt).scalars().all())


def update_session(
    db: Session,
    session_id: int,
    **kwargs,
) -> Optional[QCSession]:
    """Update a QC session."""
    session = get_session(db, session_id)
    if session:
        for key, value in kwargs.items():
            if hasattr(session, key):
                setattr(session, key, value)
        db.flush()
    return session


def update_session_counts(db: Session, session_id: int) -> Optional[QCSession]:
    """Recalculate and update session counts from items."""
    session = get_session(db, session_id)
    if not session:
        return None

    # Count total items
    total_stmt = select(func.count(QCItem.id)).where(QCItem.session_id == session_id)
    session.total_items = db.execute(total_stmt).scalar_one()

    # Count reviewed items (reviewed + confirmed + skipped)
    reviewed_stmt = select(func.count(QCItem.id)).where(
        QCItem.session_id == session_id,
        QCItem.status.in_(["reviewed", "confirmed", "skipped"]),
    )
    session.reviewed_items = db.execute(reviewed_stmt).scalar_one()

    # Count confirmed items
    confirmed_stmt = select(func.count(QCItem.id)).where(
        QCItem.session_id == session_id,
        QCItem.status == "confirmed",
    )
    session.confirmed_items = db.execute(confirmed_stmt).scalar_one()

    db.flush()
    return session


def delete_session(db: Session, session_id: int) -> bool:
    """Delete a QC session and all its items."""
    session = get_session(db, session_id)
    if session:
        db.delete(session)
        db.flush()
        return True
    return False


# =============================================================================
# QC Item Repository
# =============================================================================


def create_item(
    db: Session,
    session_id: int,
    category: str,
    series_instance_uid: str,
    study_instance_uid: str,
    stack_index: int = 0,
    priority: int = 0,
    review_reasons_csv: Optional[str] = None,
) -> QCItem:
    """Create a new QC item."""
    item = QCItem(
        session_id=session_id,
        category=category,
        series_instance_uid=series_instance_uid,
        study_instance_uid=study_instance_uid,
        stack_index=stack_index,
        priority=priority,
        review_reasons_csv=review_reasons_csv,
    )
    db.add(item)
    db.flush()
    return item


def bulk_create_items(db: Session, items: list[dict]) -> int:
    """Bulk create QC items. Returns count of created items."""
    if not items:
        return 0

    db_items = [QCItem(**item_data) for item_data in items]
    db.add_all(db_items)
    db.flush()
    return len(db_items)


def get_item(db: Session, item_id: int) -> Optional[QCItem]:
    """Get a QC item by ID."""
    return db.get(QCItem, item_id)


def get_item_with_changes(db: Session, item_id: int) -> Optional[QCItem]:
    """Get a QC item with draft changes eagerly loaded."""
    stmt = (
        select(QCItem)
        .where(QCItem.id == item_id)
        .options(selectinload(QCItem.draft_changes))
    )
    return db.execute(stmt).scalar_one_or_none()


def get_item_by_uid(
    db: Session,
    session_id: int,
    series_instance_uid: str,
    stack_index: int = 0,
) -> Optional[QCItem]:
    """Get a QC item by UID within a session."""
    stmt = select(QCItem).where(
        QCItem.session_id == session_id,
        QCItem.series_instance_uid == series_instance_uid,
        QCItem.stack_index == stack_index,
    )
    return db.execute(stmt).scalar_one_or_none()


def list_items_for_session(
    db: Session,
    session_id: int,
    category: Optional[str] = None,
    status: Optional[str] = None,
    offset: int = 0,
    limit: int = 50,
) -> tuple[list[QCItem], int]:
    """
    List QC items for a session with optional filtering.
    Returns (items, total_count).
    """
    # Base query
    base_conditions = [QCItem.session_id == session_id]
    if category:
        base_conditions.append(QCItem.category == category)
    if status:
        base_conditions.append(QCItem.status == status)

    # Count query
    count_stmt = select(func.count(QCItem.id)).where(*base_conditions)
    total = db.execute(count_stmt).scalar_one()

    # Items query with eager loading of draft changes
    items_stmt = (
        select(QCItem)
        .where(*base_conditions)
        .options(selectinload(QCItem.draft_changes))
        .order_by(QCItem.priority.desc(), QCItem.id)
        .offset(offset)
        .limit(limit)
    )
    items = list(db.execute(items_stmt).scalars().all())

    return items, total


def count_items_by_category(db: Session, session_id: int) -> dict[str, int]:
    """Get item counts grouped by category."""
    stmt = (
        select(QCItem.category, func.count(QCItem.id))
        .where(QCItem.session_id == session_id)
        .group_by(QCItem.category)
    )
    results = db.execute(stmt).all()
    return {category: count for category, count in results}


def count_items_by_status(
    db: Session,
    session_id: int,
    category: Optional[str] = None,
) -> dict[str, int]:
    """Get item counts grouped by status."""
    conditions = [QCItem.session_id == session_id]
    if category:
        conditions.append(QCItem.category == category)

    stmt = (
        select(QCItem.status, func.count(QCItem.id))
        .where(*conditions)
        .group_by(QCItem.status)
    )
    results = db.execute(stmt).all()
    return {status: count for status, count in results}


def update_item(db: Session, item_id: int, **kwargs) -> Optional[QCItem]:
    """Update a QC item."""
    item = get_item(db, item_id)
    if item:
        for key, value in kwargs.items():
            if hasattr(item, key):
                setattr(item, key, value)
        db.flush()
    return item


def update_item_status(
    db: Session,
    item_id: int,
    status: str,
) -> Optional[QCItem]:
    """Update item status with timestamp."""
    item = get_item(db, item_id)
    if item:
        item.status = status
        now = datetime.now(timezone.utc)
        if status == "reviewed":
            item.reviewed_at = now
        elif status == "confirmed":
            item.confirmed_at = now
        db.flush()
    return item


def bulk_update_item_status(
    db: Session,
    item_ids: list[int],
    status: str,
) -> int:
    """Bulk update item statuses. Returns count of updated items."""
    if not item_ids:
        return 0

    now = datetime.now(timezone.utc)
    values = {"status": status, "updated_at": now}

    if status == "reviewed":
        values["reviewed_at"] = now
    elif status == "confirmed":
        values["confirmed_at"] = now

    stmt = update(QCItem).where(QCItem.id.in_(item_ids)).values(**values)
    result = db.execute(stmt)
    db.flush()
    return result.rowcount


def delete_item(db: Session, item_id: int) -> bool:
    """Delete a QC item."""
    item = get_item(db, item_id)
    if item:
        db.delete(item)
        db.flush()
        return True
    return False


# =============================================================================
# QC Draft Change Repository
# =============================================================================


def create_draft_change(
    db: Session,
    item_id: int,
    field_name: str,
    original_value: Optional[str],
    new_value: Optional[str],
    change_reason: Optional[str] = None,
) -> QCDraftChange:
    """Create a new draft change."""
    change = QCDraftChange(
        item_id=item_id,
        field_name=field_name,
        original_value=original_value,
        new_value=new_value,
        change_reason=change_reason,
    )
    db.add(change)
    db.flush()
    return change


def upsert_draft_change(
    db: Session,
    item_id: int,
    field_name: str,
    original_value: Optional[str],
    new_value: Optional[str],
    change_reason: Optional[str] = None,
) -> QCDraftChange:
    """Create or update a draft change."""
    # Check if change exists
    stmt = select(QCDraftChange).where(
        QCDraftChange.item_id == item_id,
        QCDraftChange.field_name == field_name,
    )
    existing = db.execute(stmt).scalar_one_or_none()

    if existing:
        existing.new_value = new_value
        existing.change_reason = change_reason
        db.flush()
        return existing
    else:
        return create_draft_change(
            db, item_id, field_name, original_value, new_value, change_reason
        )


def get_draft_changes_for_item(db: Session, item_id: int) -> list[QCDraftChange]:
    """Get all draft changes for an item."""
    stmt = (
        select(QCDraftChange)
        .where(QCDraftChange.item_id == item_id)
        .order_by(QCDraftChange.field_name)
    )
    return list(db.execute(stmt).scalars().all())


def delete_draft_changes_for_item(db: Session, item_id: int) -> int:
    """Delete all draft changes for an item. Returns count deleted."""
    stmt = delete(QCDraftChange).where(QCDraftChange.item_id == item_id)
    result = db.execute(stmt)
    db.flush()
    return result.rowcount


def delete_draft_change(db: Session, change_id: int) -> bool:
    """Delete a specific draft change."""
    change = db.get(QCDraftChange, change_id)
    if change:
        db.delete(change)
        db.flush()
        return True
    return False


def get_items_with_draft_changes(
    db: Session,
    session_id: int,
    category: Optional[str] = None,
) -> list[QCItem]:
    """Get all items that have draft changes."""
    conditions = [QCItem.session_id == session_id]
    if category:
        conditions.append(QCItem.category == category)

    # Get item IDs that have draft changes
    subquery = select(QCDraftChange.item_id).distinct()

    stmt = (
        select(QCItem)
        .where(*conditions, QCItem.id.in_(subquery))
        .options(selectinload(QCItem.draft_changes))
        .order_by(QCItem.priority.desc(), QCItem.id)
    )
    return list(db.execute(stmt).scalars().all())
