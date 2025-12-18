"""Subject ID type management API routes."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from metadata_imports.id_types import (
    list_id_types as list_id_types_service,
    create_id_type,
    update_id_type,
    delete_id_type,
    get_id_type_models,
    IdTypeAlreadyExistsError,
    IdTypeNotFoundError,
    IdTypeListResponse,
    IdTypeInfo,
    DeleteIdTypeResult,
)
from metadata_imports.subjects import build_fields_response
from api.models.metadata import CreateIdTypePayload, UpdateIdTypePayload
from metadata_db.session import engine as metadata_engine

router = APIRouter(prefix="/api/metadata/id-types", tags=["id-types"])


@router.get("", response_model=IdTypeListResponse)
def list_id_types_endpoint():
    """List all subject ID types."""
    return list_id_types_service(engine=metadata_engine)


@router.post("", response_model=IdTypeInfo, status_code=status.HTTP_201_CREATED)
def create_id_type_endpoint(payload: CreateIdTypePayload):
    """Create a new subject ID type."""
    try:
        return create_id_type(
            engine=metadata_engine,
            name=payload.name,
            description=payload.description
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except IdTypeAlreadyExistsError as exc:
        raise HTTPException(status_code=409, detail="Identifier type already exists") from exc


@router.put("/{id_type_id}", response_model=IdTypeInfo)
def update_id_type_endpoint(id_type_id: int, payload: UpdateIdTypePayload):
    """Update an existing subject ID type."""
    try:
        return update_id_type(
            engine=metadata_engine,
            id_type_id=id_type_id,
            name=payload.name,
            description=payload.description,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except IdTypeNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Identifier type not found") from exc
    except IdTypeAlreadyExistsError as exc:
        raise HTTPException(status_code=409, detail="Identifier type already exists") from exc


@router.delete("/{id_type_id}", response_model=DeleteIdTypeResult)
def delete_id_type_endpoint(id_type_id: int):
    """Delete a subject ID type."""
    try:
        return delete_id_type(engine=metadata_engine, id_type_id=id_type_id)
    except IdTypeNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Identifier type not found") from exc
