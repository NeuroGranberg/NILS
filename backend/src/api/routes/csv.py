"""CSV upload and preview API routes."""
from __future__ import annotations

import csv
import json
import secrets
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from api.utils.csv import (
    CSV_UPLOAD_DIR,
    csv_file_path,
    csv_metadata_path,
    sanitize_csv_token,
    read_csv_metadata,
    write_csv_metadata,
)

router = APIRouter(prefix="/api/uploads/csv", tags=["csv"])


def _extract_csv_columns(path: Path) -> list[str]:
    """Extract column headers from CSV file."""
    if not path.exists():
        raise HTTPException(status_code=404, detail="CSV file not found")
    with path.open("r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return []
    return [column.strip() for column in header if column is not None]


@router.post("")
async def upload_csv(file: UploadFile = File(...)):
    """Upload a CSV file and return a token for later use.
    
    Returns:
        JSON with token, filename, and columns
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="CSV file must include a filename")

    token = secrets.token_urlsafe(16)
    destination = csv_file_path(token)
    
    try:
        with destination.open("wb") as target:
            while True:
                chunk = await file.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break
                target.write(chunk)
    finally:
        await file.close()

    columns = _extract_csv_columns(destination)
    write_csv_metadata(token, {"filename": file.filename, "columns": columns})
    
    return JSONResponse({"token": token, "filename": file.filename, "columns": columns})


@router.get("/{token}/columns")
def get_csv_columns(token: str):
    """Get column names from a previously uploaded CSV.
    
    Args:
        token: CSV upload token
        
    Returns:
        JSON with filename and columns
    """
    token = sanitize_csv_token(token)
    path = csv_file_path(token)
    
    if not path.exists():
        raise HTTPException(status_code=404, detail="Uploaded CSV not found")
    
    metadata = read_csv_metadata(token) or {}
    columns = metadata.get("columns") or _extract_csv_columns(path)
    filename = metadata.get("filename")
    
    if not metadata.get("columns"):
        write_csv_metadata(token, {"filename": filename or path.name, "columns": columns})
    
    return JSONResponse({"columns": columns, "filename": filename or path.name})
