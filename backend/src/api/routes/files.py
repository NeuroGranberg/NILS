"""File system routes for directory listing."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse


router = APIRouter(prefix="/api", tags=["files"])

# Module-level state - will be set during app initialization
_data_roots: List[Path] = []


def set_data_roots(roots: List[Path]) -> None:
    """Set the allowed data roots for file operations."""
    global _data_roots
    _data_roots = roots


def get_data_roots() -> List[Path]:
    """Get the configured data roots."""
    return _data_roots


def parse_data_roots() -> List[Path]:
    """Parse DATA_ROOTS from env (JSON array) or fallback to single DATA_ROOT."""
    roots_json = os.getenv("DATA_ROOTS")
    if roots_json:
        try:
            roots_list = json.loads(roots_json)
            return [Path(r).resolve() for r in roots_list]
        except (json.JSONDecodeError, TypeError):
            pass
    
    # Fallback to single DATA_ROOT
    single_root = os.getenv("DATA_ROOT", "/app/data")
    return [Path(single_root).resolve()]


def list_subdirectories_clamped(allowed_roots: List[Path], requested: Path) -> List[dict]:
    """List subdirectories under requested path if it's under any allowed root."""
    target = requested.resolve() if requested.is_absolute() else requested
    
    # Check if target is under any allowed root
    valid = False
    for root in allowed_roots:
        if str(target).startswith(str(root)):
            valid = True
            break
    
    if not valid:
        raise HTTPException(status_code=400, detail="Path outside allowed data roots")

    try:
        entries: List[dict] = []
        with os.scandir(target) as it:
            for entry in it:
                if entry.is_dir(follow_symlinks=False):
                    absolute_child = Path(entry.path)
                    display_path = str(absolute_child)
                    entries.append({
                        "name": entry.name,
                        "path": display_path.replace("\\", "/"),
                        "type": "directory",
                    })
        return entries
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Path not found")


@router.get("/data-roots")
def get_data_roots_endpoint():
    """Return list of available data root paths."""
    return JSONResponse([str(r) for r in _data_roots])


@router.get("/files")
def get_directories(path: str = Query(default=None)):
    """List subdirectories at the given path."""
    if path:
        requested = Path(path)
    else:
        # Default to first root if no path provided
        requested = _data_roots[0] if _data_roots else Path("/app/data")
    
    items = list_subdirectories_clamped(_data_roots, requested)
    return JSONResponse(items)
