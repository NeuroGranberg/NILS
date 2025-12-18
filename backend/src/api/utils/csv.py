"""CSV file handling utilities for the API."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import HTTPException
import polars as pl

# Constants
_ALLOWED_TOKEN_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
)

# CSV Upload directory
CSV_UPLOAD_DIR = Path(os.getenv("CSV_UPLOAD_DIR", "resource/uploads/csv")).resolve()
CSV_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def sanitize_csv_token(token: str) -> str:
    """Validate and return a sanitized CSV token.
    
    Args:
        token: Token to sanitize
        
    Returns:
        Sanitized token
        
    Raises:
        HTTPException: If token is invalid
    """
    if not token:
        raise HTTPException(status_code=400, detail="Missing CSV token")
    if any(ch not in _ALLOWED_TOKEN_CHARS for ch in token):
        raise HTTPException(status_code=400, detail="Invalid CSV token")
    return token


def csv_file_path(token: str) -> Path:
    """Get file path for CSV token."""
    return CSV_UPLOAD_DIR / f"{token}.csv"


def csv_metadata_path(token: str) -> Path:
    """Get metadata path for CSV token."""
    return CSV_UPLOAD_DIR / f"{token}.json"


def extract_csv_columns(token: str) -> list[str]:
    """Extract column names from CSV file.
    
    Args:
        token: CSV token
        
    Returns:
        List of column names
    """
    csv_path = csv_file_path(token)
    df = pl.read_csv(csv_path, n_rows=0)
    return df.columns


def write_csv_metadata(token: str, metadata: dict[str, Any]) -> None:
    """Write metadata for CSV file."""
    import json
    csv_metadata_path(token).write_text(json.dumps(metadata))


def read_csv_metadata(token: str) -> dict[str, Any] | None:
    """Read metadata for CSV file."""
    import json
    meta_path = csv_metadata_path(token)
    if not meta_path.exists():
        return None
    try:
        return json.loads(meta_path.read_text())
    except Exception:
        return None


def resolve_import_csv(file_path: str | None, csv_token: str | None) -> tuple[Path, dict[str, Any]]:
    """Resolve CSV file path and metadata from either filePath or csvToken.
    
    Args:
        file_path: Direct file path (optional)
        csv_token: CSV upload token (optional)
        
    Returns:
        Tuple of (resolved_path, metadata_dict)
        
    Raises:
        HTTPException: If neither or both are provided, or if token not found
    """
    if file_path and csv_token:
        raise HTTPException(status_code=400, detail="Provide either filePath or csvToken, not both")
    if not file_path and not csv_token:
        raise HTTPException(status_code=400, detail="Provide either filePath or csvToken")

    if file_path:
        resolved = Path(file_path).expanduser().resolve()
        return resolved, {}

    # csv_token path
    token = sanitize_csv_token(csv_token)  # type: ignore
    csv_path = csv_file_path(token)
    if not csv_path.exists():
        raise HTTPException(status_code=404, detail=f"CSV file not found for token={token}")
    metadata = read_csv_metadata(token) or {}
    return csv_path, metadata
