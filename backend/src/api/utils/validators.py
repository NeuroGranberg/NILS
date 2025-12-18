"""Validation utilities for API request payloads."""
from __future__ import annotations

from fastapi import HTTPException

from metadata_db.session import SessionLocal
from metadata_db import schema


def validate_mapping_columns(
    subject_code: str | None,
    columns: list[str],
    mapping: dict[str, dict[str, str]],
    required_fields: list[str],
) -> None:
    """Validate that required fields are mapped to valid columns.
    
    Args:
        subject_code: Optional subject code context
        columns: Available CSV columns
        mapping: Field to column mapping
        required_fields: Required field names
        
    Raises:
        HTTPException: If validation fails
    """
    for field in required_fields:
        if field not in mapping:
            detail = f"Missing required field: {field}"
            if subject_code:
                detail = f"{detail} for subjectCode={subject_code}"
            raise HTTPException(status_code=400, detail=detail)

        field_mapping = mapping[field]
        if not isinstance(field_mapping, dict):
            raise HTTPException(
                status_code=400,  
                detail=f"Field '{field}' must be an object with 'column' key",
            )

        column_name = field_mapping.get("column", "").strip()
        if not column_name:
            raise HTTPException(status_code=400, detail=f"Missing column for field '{field}'")

        if column_name not in columns:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{column_name}' not found in CSV (field={field})",
            )


def validate_subject_identifier_columns(
    columns: list[str],
    fields: dict[str, dict[str, str]],
) -> None:
    """Validate subject identifier import columns."""
    validate_mapping_columns(
        None,
        columns,
        fields,
        required_fields=["subject_code", "id_value"],
    )


def validate_cohort_mapping_columns(
    columns: list[str],
    fields: dict[str, dict[str, str]],
) -> None:
    """Validate cohort import mapping columns."""
    validate_mapping_columns(
        None,
        columns,
        fields,
        required_fields=["name"],
    )


def validate_subject_cohort_mapping_columns(
    columns: list[str],
    fields: dict[str, dict[str, str]],
) -> None:
    """Validate subject-cohort mapping import columns."""
    validate_mapping_columns(
        None,
        columns,
        fields,
        required_fields=["subject_code", "cohort_name"],
    )


def validate_subject_id_type_id(subject_id_type_id: int | None) -> int:
    """Validate subject ID type exists and return it.
    
    Args:
        subject_id_type_id: ID type to validate
        
    Returns:
        Validated ID type ID
        
    Raises:
        HTTPException: If ID type not found
    """
    if subject_id_type_id is None:
        raise HTTPException(status_code=400, detail="subjectIdTypeId is required")
    with SessionLocal() as session:
        id_type = session.get(schema.SubjectIDType, subject_id_type_id)
        if not id_type:
            raise HTTPException(status_code=404, detail=f"SubjectIDType {subject_id_type_id} not found")
    return subject_id_type_id
