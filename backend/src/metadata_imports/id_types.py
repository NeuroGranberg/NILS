from __future__ import annotations

from typing import Sequence

from pydantic import BaseModel
from sqlalchemy import delete, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from metadata_db import schema


class IdTypeError(Exception):
    """Base exception for identifier type operations."""


class IdTypeAlreadyExistsError(IdTypeError):
    """Raised when attempting to create or rename an identifier type to a duplicate name."""


class IdTypeNotFoundError(IdTypeError):
    """Raised when an identifier type cannot be located."""


class IdTypeInfo(BaseModel):
    id: int
    name: str
    description: str | None = None
    identifiersCount: int = 0


class IdTypeListResponse(BaseModel):
    items: list[IdTypeInfo]


class DeleteIdTypeResult(BaseModel):
    id: int
    name: str
    identifiersDeleted: int


def _normalize_name(name: str) -> str:
    cleaned = (name or "").strip()
    if not cleaned:
        raise ValueError("Identifier type name must be provided")
    return cleaned


def _normalize_description(description: str | None) -> str | None:
    if description is None:
        return None
    cleaned = description.strip()
    return cleaned or None


def _ensure_unique_name(session: Session, *, name: str, exclude_id: int | None = None) -> None:
    lower_name = name.lower()
    stmt = select(schema.IdType).where(func.lower(schema.IdType.id_type_name) == lower_name)
    if exclude_id is not None:
        stmt = stmt.where(schema.IdType.id_type_id != exclude_id)
    existing = session.execute(stmt).scalar_one_or_none()
    if existing is not None:
        raise IdTypeAlreadyExistsError(name)


def list_id_types(*, engine: Engine) -> IdTypeListResponse:
    with Session(engine) as session:
        rows = (
            session.execute(
                select(
                    schema.IdType.id_type_id.label("id"),
                    schema.IdType.id_type_name.label("name"),
                    schema.IdType.description.label("description"),
                    func.count(schema.SubjectOtherIdentifier.subject_other_identifier_id).label("identifiers_count"),
                )
                .outerjoin(
                    schema.SubjectOtherIdentifier,
                    schema.SubjectOtherIdentifier.id_type_id == schema.IdType.id_type_id,
                )
                .group_by(
                    schema.IdType.id_type_id,
                    schema.IdType.id_type_name,
                    schema.IdType.description,
                )
                .order_by(func.lower(schema.IdType.id_type_name))
            )
            .mappings()
            .all()
        )

    items = [
        IdTypeInfo(
            id=record["id"],
            name=record["name"],
            description=record["description"],
            identifiersCount=record["identifiers_count"] or 0,
        )
        for record in rows
    ]
    return IdTypeListResponse(items=items)


def create_id_type(*, engine: Engine, name: str, description: str | None) -> IdTypeInfo:
    normalized_name = _normalize_name(name)
    normalized_description = _normalize_description(description)

    with Session(engine) as session:
        _ensure_unique_name(session, name=normalized_name)

        record = schema.IdType(id_type_name=normalized_name, description=normalized_description)
        session.add(record)
        try:
            session.commit()
        except Exception as exc:  # pragma: no cover - defensive rollback
            session.rollback()
            try:
                _ensure_unique_name(session, name=normalized_name)
            except IdTypeAlreadyExistsError as duplicate_error:
                raise duplicate_error from exc
            raise
        session.refresh(record)

    return IdTypeInfo(id=record.id_type_id, name=record.id_type_name, description=record.description, identifiersCount=0)


def update_id_type(*, engine: Engine, id_type_id: int, name: str, description: str | None) -> IdTypeInfo:
    normalized_name = _normalize_name(name)
    normalized_description = _normalize_description(description)

    with Session(engine) as session:
        record = session.get(schema.IdType, id_type_id)
        if record is None:
            raise IdTypeNotFoundError(id_type_id)

        if (
            record.id_type_name.strip() != normalized_name
            or (record.description or "") != (normalized_description or "")
        ):
            _ensure_unique_name(session, name=normalized_name, exclude_id=id_type_id)
            record.id_type_name = normalized_name
            record.description = normalized_description
            try:
                session.commit()
            except Exception as exc:  # pragma: no cover - defensive rollback
                session.rollback()
                try:
                    _ensure_unique_name(session, name=normalized_name, exclude_id=id_type_id)
                except IdTypeAlreadyExistsError as duplicate_error:
                    raise duplicate_error from exc
                raise
        else:
            session.expire(record)

        identifiers_count = session.execute(
            select(func.count(schema.SubjectOtherIdentifier.subject_other_identifier_id)).where(
                schema.SubjectOtherIdentifier.id_type_id == id_type_id
            )
        ).scalar_one()

    return IdTypeInfo(
        id=id_type_id,
        name=normalized_name,
        description=normalized_description,
        identifiersCount=identifiers_count or 0,
    )


def delete_id_type(*, engine: Engine, id_type_id: int) -> DeleteIdTypeResult:
    with Session(engine) as session:
        record = session.get(schema.IdType, id_type_id)
        if record is None:
            raise IdTypeNotFoundError(id_type_id)

        name = record.id_type_name
        identifier_count = session.execute(
            select(func.count(schema.SubjectOtherIdentifier.subject_other_identifier_id)).where(
                schema.SubjectOtherIdentifier.id_type_id == id_type_id
            )
        ).scalar_one()

        session.execute(
            delete(schema.SubjectOtherIdentifier).where(
                schema.SubjectOtherIdentifier.id_type_id == id_type_id
            )
        )

        session.delete(record)
        try:
            session.commit()
        except Exception:  # pragma: no cover - defensive rollback
            session.rollback()
            raise

    return DeleteIdTypeResult(
        id=id_type_id,
        name=name,
        identifiersDeleted=identifier_count or 0,
    )


def get_id_type_models(*, engine: Engine) -> Sequence[schema.IdType]:
    with Session(engine) as session:
        rows = (
            session.execute(select(schema.IdType).order_by(func.lower(schema.IdType.id_type_name)))
            .scalars()
            .all()
        )
    return rows
