"""QC Pipeline API routes."""

from __future__ import annotations

import functools
from pathlib import Path
import json
import time

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse, Response

from pydantic import BaseModel

from qc.service import qc_service
from qc.dicom_service import dicom_service
from qc.axes_service import axes_qc_service, get_axis_options_from_yaml
from qc.main_acquisition_service import (
    FilterState,
    main_acquisition_service,
)
from qc.models import (
    CreateQCSessionPayload,
    UpdateQCItemPayload,
    ConfirmQCChangesPayload,
    MASQCSession,
    MASQCSessionDTO,
    CreateMASQCSessionPayload,
    UpdateMASQCSessionPayload,
)


router = APIRouter(prefix="/api/qc", tags=["qc"])


# =============================================================================
# Session Management
# =============================================================================


@router.post("/sessions")
def create_session(payload: CreateQCSessionPayload):
    """Create a new QC session for a cohort."""
    try:
        session = qc_service.create_session(payload)
        return JSONResponse(session.model_dump(mode="json"), status_code=201)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sessions/{session_id}")
def get_session(session_id: int):
    """Get QC session details with category counts."""
    session = qc_service.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return JSONResponse(session.model_dump(mode="json"))


@router.get("/cohorts/{cohort_id}/session")
def get_session_for_cohort(cohort_id: int):
    """Get or create QC session for a cohort."""
    try:
        session = qc_service.get_or_create_session(cohort_id)
        return JSONResponse(session.model_dump(mode="json"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sessions/{session_id}/summary")
def get_session_summary(session_id: int):
    """Get summary counts by category and status."""
    summary = qc_service.get_session_summary(session_id)
    if not summary:
        raise HTTPException(status_code=404, detail="Session not found")
    return JSONResponse(summary)


@router.post("/sessions/{session_id}/refresh")
def refresh_session(session_id: int):
    """Refresh session items from metadata DB."""
    session = qc_service.refresh_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return JSONResponse(session.model_dump(mode="json"))


# =============================================================================
# Item Listing by Category
# =============================================================================


@router.get("/sessions/{session_id}/categories/{category}")
def get_items_for_category(
    session_id: int,
    category: str,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    status: str = Query(None),
):
    """Get paginated QC items for a category."""
    valid_categories = ["base", "provenance", "technique", "body_part", "contrast"]
    if category not in valid_categories:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid category. Must be one of: {valid_categories}",
        )

    items, total = qc_service.get_items_for_category(
        session_id, category, offset=offset, limit=limit, status=status
    )
    return JSONResponse(
        {
            "items": [item.model_dump(mode="json") for item in items],
            "total": total,
            "offset": offset,
            "limit": limit,
        }
    )


# =============================================================================
# Individual Item Operations
# =============================================================================


@router.get("/items/{item_id}")
def get_item_detail(item_id: int):
    """Get full item details including metadata and DICOM info."""
    item = qc_service.get_item_detail(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return JSONResponse(item.model_dump(mode="json"))


@router.patch("/items/{item_id}")
def update_item(item_id: int, payload: UpdateQCItemPayload):
    """Save draft changes for an item."""
    item = qc_service.update_item(item_id, payload)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return JSONResponse(item.model_dump(mode="json"))


@router.delete("/items/{item_id}/changes")
def discard_item_changes(item_id: int):
    """Discard all draft changes for an item."""
    item = qc_service.discard_changes(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return JSONResponse(item.model_dump(mode="json"))


@router.post("/items/{item_id}/skip")
def skip_item(item_id: int):
    """Mark item as skipped (no changes needed)."""
    item = qc_service.skip_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return JSONResponse(item.model_dump(mode="json"))


# =============================================================================
# Confirmation / Push
# =============================================================================


@router.post("/sessions/{session_id}/confirm")
def confirm_changes(session_id: int, payload: ConfirmQCChangesPayload):
    """Confirm and push draft changes to metadata DB."""
    try:
        count = qc_service.confirm_items(session_id, payload)
        return JSONResponse({"confirmed_count": count})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sessions/{session_id}/confirm-all")
def confirm_all_changes(session_id: int):
    """Confirm all reviewed items with draft changes."""
    try:
        count = qc_service.confirm_all_reviewed(session_id)
        return JSONResponse({"confirmed_count": count})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Data Viewer (Subject -> Session -> Stack)
# =============================================================================


@router.get("/cohorts/{cohort_id}/subjects")
def get_subjects_for_cohort(
    cohort_id: int,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    search: str = Query(None),
    sort_by: str = Query("code"),
):
    """Get or create QC session for a cohort."""
    try:
        subjects, total = qc_service.get_subjects_for_cohort(
            cohort_id, offset=offset, limit=limit, search=search, sort_by=sort_by
        )
        return JSONResponse(
            {
                "subjects": subjects,
                "total": total,
                "offset": offset,
                "limit": limit,
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/subjects/{subject_id}/sessions")
def get_sessions_for_subject(subject_id: int):
    """Get sessions (study dates) for a subject."""
    try:
        sessions = qc_service.get_sessions_for_subject(subject_id)
        return JSONResponse({"sessions": sessions})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/subjects/{subject_id}/sessions/{date}/stacks")
def get_stacks_for_session(subject_id: int, date: str):
    """Get stacks for a specific session (date)."""
    try:
        stacks = qc_service.get_stacks_for_session(subject_id, date)
        return JSONResponse(
            {"stacks": [stack.model_dump(mode="json") for stack in stacks]}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Classification Options (for dropdowns)
# =============================================================================


@router.get("/options")
def get_classification_options():
    """Get available options for classification dropdowns."""
    # These are the valid values for each field
    options = {
        "bases": [
            "T1w",
            "T2w",
            "PDw",
            "DWI",
            "SWI",
            "MTw",
            "PWI",
            "T2starw",
            "FLAIR",
            "STIR",
            "DIR",
            "angio",
            "phase",
            "magnitude",
        ],
        "techniques": [
            "TSE",
            "SPACE",
            "MPRAGE",
            "FLASH",
            "GRE",
            "ME-GRE",
            "EPI",
            "DWI-EPI",
            "BOLD-EPI",
            "ASL",
            "PC-MRA",
            "TOF-MRA",
            "SWI",
            "CISS",
            "FISP",
            "TrueFISP",
            "HASTE",
            "BLADE",
            "PROPELLER",
            "RESOLVE",
            "MDME",
        ],
        "provenances": [
            None,
            "SyMRI",
            "SWIRecon",
            "DTIRecon",
            "PerfusionRecon",
            "ASLRecon",
            "BOLDRecon",
            "ProjectionDerived",
        ],
        "directory_types": [
            "anat",
            "dwi",
            "func",
            "fmap",
            "perf",
            "localizer",
            "misc",
            "excluded",
        ],
        "post_contrast_options": [
            {"value": None, "label": "Unknown"},
            {"value": 0, "label": "Pre-contrast"},
            {"value": 1, "label": "Post-contrast"},
        ],
        "localizer_options": [
            {"value": 0, "label": "No"},
            {"value": 1, "label": "Yes"},
        ],
        "spinal_cord_options": [
            {"value": None, "label": "Unknown"},
            {"value": 0, "label": "No (Brain)"},
            {"value": 1, "label": "Yes (Spine)"},
        ],
        "body_part_options": [
            {"value": None, "label": "Unknown"},
            {"value": "brain", "label": "Brain"},
            {"value": "brain-neck", "label": "Brain + Neck"},
            {"value": "neck", "label": "Neck"},
            {"value": "spine", "label": "Spine"},
        ],
    }
    return JSONResponse(options)


# =============================================================================
# DICOM Viewing - Cornerstone.js Compatible
# =============================================================================


@router.get("/dicom/{series_uid}/metadata")
def get_series_metadata(
    series_uid: str,
    stack_index: int = Query(0, ge=0),
):
    """
    Get series metadata for Cornerstone.js viewer.

    Returns metadata in a format compatible with Cornerstone/OHIF,
    including instance URLs and all necessary rendering parameters.
    """
    try:
        metadata = dicom_service.get_series_metadata(series_uid, stack_index)
        if metadata is None:
            raise HTTPException(status_code=404, detail="Series not found")
        return JSONResponse(metadata)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dicom/file/{instance_id}")
def get_dicom_file(instance_id: int, frame: int | None = Query(None)):
    """
    Serve a raw DICOM file by instance ID.

    This endpoint streams the DICOM file directly for Cornerstone.js
    to parse and render on the client side (much faster than server-side conversion).
    """
    try:
        file_path = dicom_service.get_instance_file_path(instance_id)
        if file_path is None or not Path(file_path).exists():
            raise HTTPException(status_code=404, detail="DICOM file not found")

        return FileResponse(
            path=file_path,
            media_type="application/dicom",
            headers={
                "Cache-Control": "max-age=86400",
                "Access-Control-Allow-Origin": "*",
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dicom/wado")
def wado_retrieve(
    studyUID: str = Query(..., alias="studyUID"),
    seriesUID: str = Query(..., alias="seriesUID"),
    objectUID: str = Query(..., alias="objectUID"),
):
    """
    WADO-URI compatible endpoint for DICOM retrieval.

    This allows Cornerstone.js to use standard WADO URLs.
    Format: /api/qc/dicom/wado?studyUID=...&seriesUID=...&objectUID=...
    """
    try:
        file_path = dicom_service.get_instance_file_path_by_uid(objectUID)
        if file_path is None or not Path(file_path).exists():
            raise HTTPException(status_code=404, detail="DICOM file not found")

        return FileResponse(
            path=file_path,
            media_type="application/dicom",
            headers={
                "Cache-Control": "max-age=86400",
                "Access-Control-Allow-Origin": "*",
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Simple Image Viewer (PNG rendering - works without Cornerstone.js)
# =============================================================================


@router.get("/dicom/image/{instance_id}")
def get_instance_image(
    instance_id: int,
    window_center: float = Query(None),
    window_width: float = Query(None),
    frame: int = Query(None),
):
    """
    Get a DICOM instance rendered as PNG.

    This is a simple viewer endpoint that renders DICOM to PNG server-side.
    For high-performance viewing, use the raw DICOM endpoints with Cornerstone.js.

    Args:
        instance_id: Instance ID
        window_center: Optional window center override
        window_width: Optional window width override
        frame: Frame index for multi-frame DICOM (optional)
    """
    try:
        image_bytes = dicom_service.render_instance_to_png(
            instance_id,
            window_center=window_center,
            window_width=window_width,
            frame=frame,
        )
        if image_bytes is None:
            raise HTTPException(
                status_code=404, detail="Instance not found or cannot be rendered"
            )

        return Response(
            content=image_bytes,
            media_type="image/png",
            headers={"Cache-Control": "max-age=3600"},
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dicom/{series_uid}/thumbnail")
def get_series_thumbnail(
    series_uid: str,
    stack_index: int = Query(0, ge=0),
    size: int = Query(128, ge=32, le=512),
):
    """
    Get a thumbnail image for a series (middle slice).

    Args:
        series_uid: Series Instance UID
        stack_index: Stack index for multi-stack series
        size: Thumbnail size (max dimension)
    """
    try:
        instance_id = dicom_service.get_middle_instance_id(series_uid, stack_index)
        if instance_id is None:
            raise HTTPException(status_code=404, detail="Series not found")

        image_bytes = dicom_service.render_instance_to_png(
            instance_id,
            size=size,
        )
        if image_bytes is None:
            raise HTTPException(status_code=404, detail="Cannot render thumbnail")

        return Response(
            content=image_bytes,
            media_type="image/png",
            headers={"Cache-Control": "max-age=3600"},
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@functools.lru_cache(maxsize=2048)
def _render_stack_thumbnail(stack_id: int, slice_index: int, size: int) -> bytes:
    """Render and cache a thumbnail in memory. LRU evicts old entries."""
    from sqlalchemy import text
    from metadata_db.session import SessionLocal as MetadataSessionLocal

    with MetadataSessionLocal() as db:
        rows = db.execute(
            text(
                """
                SELECT i.instance_id,
                       COALESCE(i.number_of_frames, 1) AS num_frames
                FROM instance i
                JOIN series_stack ss ON i.series_stack_id = ss.series_stack_id
                WHERE ss.series_stack_id = :sid
                ORDER BY
                    COALESCE(i.slice_location, i.instance_number, 0) ASC,
                    i.instance_number ASC
                """
            ),
            {"sid": int(stack_id)},
        ).fetchall()

    expanded: list[tuple[int, int]] = []
    for r in rows:
        nf = int(r.num_frames or 1)
        for f in range(nf):
            expanded.append((int(r.instance_id), f))

    if not expanded:
        raise HTTPException(status_code=404, detail="Stack has no instances")
    if slice_index >= len(expanded):
        raise HTTPException(
            status_code=404,
            detail=f"slice_index {slice_index} out of range (n={len(expanded)})",
        )

    instance_id, frame = expanded[slice_index]
    image_bytes = dicom_service.render_instance_to_png(
        instance_id,
        size=size,
        frame=frame if frame > 0 else None,
    )
    if image_bytes is None:
        raise HTTPException(status_code=404, detail="Cannot render thumbnail")
    return image_bytes


@router.get("/dicom/stack/{stack_id}/slice/{slice_index}/thumbnail")
def get_stack_slice_thumbnail(
    stack_id: int,
    slice_index: int,
    size: int = Query(256, ge=32, le=1024),
):
    """Render a thumbnail for the (stack_id, slice_index) pair.

    Results are held in an in-memory LRU cache (2048 entries, ~50 MB)
    so repeated requests for the same page of the Changes view are
    near-instant. The browser ``Cache-Control`` header provides an
    additional client-side layer.
    """
    if slice_index < 0:
        raise HTTPException(status_code=400, detail="slice_index must be >= 0")
    try:
        image_bytes = _render_stack_thumbnail(stack_id, slice_index, size)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return Response(
        content=image_bytes,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=86400"},
    )


@router.get("/dicom/{series_uid}/instances")
def get_series_instances(
    series_uid: str,
    stack_index: int = Query(0, ge=0),
):
    """
    Get list of frames for a series (for slice navigation).

    Returns frame list ordered by slice location. For multi-frame Enhanced DICOM,
    each frame within an instance is expanded as a separate entry.
    
    Response format:
    - frames: list of {instance_id, frame} objects
    - total: total number of navigable slices
    - instance_ids: (legacy) list of unique instance IDs for backwards compatibility
    
    Frontend should build image URLs as:
    - Classic DICOM: wadouri:.../file/{instance_id}
    - Multi-frame DICOM: wadouri:.../file/{instance_id}?frame={frame}
    """
    try:
        # Get expanded frame list (handles both classic and Enhanced DICOM)
        frames = dicom_service.get_series_frame_list(series_uid, stack_index)
        
        # Also include legacy instance_ids for backwards compatibility
        instance_ids = dicom_service.get_series_instance_ids(series_uid, stack_index)
        
        return JSONResponse(
            {
                "series_uid": series_uid,
                "stack_index": stack_index,
                "frames": frames,  # New: expanded frame list
                "instance_ids": instance_ids,  # Legacy: just instance IDs
                "total": len(frames),  # Total navigable slices
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Contrast Comparison - Sister Series
# =============================================================================


@router.get("/dicom/{series_uid}/sisters")
def get_sister_series(series_uid: str):
    """
    Get related series from the same study for comparison.

    Used for contrast QC to find potential pre/post contrast pairs.
    """
    try:
        sisters = dicom_service.get_sister_series(series_uid)
        return JSONResponse(
            {
                "series_uid": series_uid,
                "sisters": sisters,
                "total": len(sisters),
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dicom/{series_uid}/contrast-pairs")
def get_contrast_pairs(series_uid: str):
    """
    Get T1w series grouped by contrast status for comparison.

    Returns series categorized as pre-contrast, post-contrast, or unknown.
    """
    try:
        pairs = dicom_service.get_t1w_contrast_pairs(series_uid)
        return JSONResponse(pairs)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Axes Prediction QC - Compact classification QC module
# =============================================================================


@router.get("/cohorts/{cohort_id}/axes/items")
def get_axes_qc_items(
    cohort_id: int,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    axis: str = Query(None, description="Filter by axis (base, technique, modifier, provenance, construct)"),
    flag_type: str = Query(None, description="Filter by flag type (missing, conflict, low_confidence, ambiguous, review)"),
):
    """
    Get stacks needing axes QC for classification review.

    Sorted by: subject_code, study_date, field_strength (desc), manufacturer, model

    Optional filters:
    - axis: Filter to show only items with flags on a specific axis
    - flag_type: Filter to show only items with a specific flag type

    Returns compact data for the Axes QC viewer.
    """
    try:
        items, total = axes_qc_service.get_axes_qc_items(
            cohort_id, offset=offset, limit=limit, axis=axis, flag_type=flag_type
        )
        return JSONResponse({
            "items": items,
            "total": total,
            "offset": offset,
            "limit": limit,
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/axes/items/{stack_id}")
def get_axes_qc_item(stack_id: int, cohort_id: int = Query(None)):
    """Get a single stack with full details for axes QC."""
    try:
        item = axes_qc_service.get_axes_qc_item(stack_id, cohort_id=cohort_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Stack not found")
        return JSONResponse(item)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/cohorts/{cohort_id}/axes/items/{stack_id}")
def update_axis_value(
    cohort_id: int,
    stack_id: int,
    axis: str = Query(...),
    value: str = Query(None)
):
    """
    Save an axis value change as a draft.

    Changes are stored in application_db until confirmed.
    Use POST /cohorts/{cohort_id}/axes/confirm to push changes to metadata_db.
    """
    try:
        result = axes_qc_service.save_axis_draft(cohort_id, stack_id, axis, value)
        return JSONResponse(result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cohorts/{cohort_id}/axes/session")
def get_axes_session(cohort_id: int):
    """
    Get or create the axes QC session for a cohort.

    Returns session info including draft change counts.
    """
    try:
        session = axes_qc_service.get_or_create_session(cohort_id)
        return JSONResponse(session)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cohorts/{cohort_id}/axes/confirm")
def confirm_axes_changes(cohort_id: int):
    """
    Confirm and push all draft changes to metadata_db.

    This persists all axis value changes and clears the draft state.
    """
    try:
        result = axes_qc_service.confirm_axes_changes(cohort_id)
        return JSONResponse(result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cohorts/{cohort_id}/axes/discard")
def discard_axes_changes(cohort_id: int):
    """
    Discard all draft changes for a cohort's axes QC.

    This removes all pending changes without persisting them.
    """
    try:
        result = axes_qc_service.discard_axes_changes(cohort_id)
        return JSONResponse(result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/axes/options")
def get_axis_options():
    """Get available options for each classification axis from YAML configs."""
    return JSONResponse(get_axis_options_from_yaml())


@router.get("/cohorts/{cohort_id}/axes/filters")
def get_axes_available_filters(cohort_id: int):
    """
    Get available axes and flag types that have QC items for this cohort.

    Returns only filter options that have at least one QC item.
    Used to populate filter dropdowns with relevant options only.
    """
    try:
        filters = axes_qc_service.get_available_filters(cohort_id)
        return JSONResponse(filters)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/axes/items/{stack_id}/image-comments")
def get_image_comments(stack_id: int):
    """Get image_comments from a representative instance of the stack."""
    try:
        comments = axes_qc_service.get_image_comments_for_stack(stack_id)
        return JSONResponse({"image_comments": comments})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Main Acquisition Selection QC
# =============================================================================


class MainAcqFilterRequest(BaseModel):
    directory_type: list[str] | None = None
    provenance: list[str] | None = None
    orientation: list[str] | None = None
    base: list[str] | None = None
    mr_acquisition_type: list[str] | None = None
    technique: list[str] | None = None
    modifier_csv: list[str] | None = None


class MainAcqRolePayload(BaseModel):
    role: str  # "main" | "pre" | "post"
    value: bool


@router.post("/cohorts/{cohort_id}/main-acq/filter-options")
def main_acq_filter_options(cohort_id: int, payload: MainAcqFilterRequest):
    """Return cascading filter options for the Main Acquisition QC filter bar."""
    try:
        filters = FilterState.from_dict(payload.model_dump())
        options = main_acquisition_service.get_filter_options(cohort_id, filters)
        return JSONResponse({"options": options})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cohorts/{cohort_id}/main-acq/sessions")
def main_acq_sessions(
    cohort_id: int,
    payload: MainAcqFilterRequest,
    display_id_type: str = Query(None),
    only_multi_stack: bool = Query(False),
):
    """List sessions matching the Main Acquisition QC filter state."""
    try:
        filters = FilterState.from_dict(payload.model_dump())
        return JSONResponse(
            main_acquisition_service.get_sessions(
                cohort_id, filters,
                display_id_type=display_id_type,
                only_multi_stack=only_multi_stack,
            )
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cohorts/{cohort_id}/main-acq/sessions/{session_index}/bundles")
def main_acq_session_bundles(
    cohort_id: int,
    session_index: int,
    payload: MainAcqFilterRequest,
    display_id_type: str = Query(None),
    only_multi_stack: bool = Query(False),
):
    """Return bundles for a single session under the current filter state."""
    try:
        filters = FilterState.from_dict(payload.model_dump())
        return JSONResponse(
            main_acquisition_service.get_session_bundles(
                cohort_id, filters, session_index,
                display_id_type=display_id_type,
                only_multi_stack=only_multi_stack,
            )
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/main-acq/resolve-subject")
def main_acq_resolve_subject(identifier: str = Query(...)):
    """Resolve any identifier (subject_code or other_identifier) to a subject_code."""
    code = main_acquisition_service.resolve_subject_identifier(identifier)
    if not code:
        raise HTTPException(status_code=404, detail="subject not found")
    return JSONResponse({"identifier": identifier, "subject_code": code})


@router.post("/main-acq/stacks/{series_stack_id}/role")
def main_acq_set_role(series_stack_id: int, payload: MainAcqRolePayload):
    """Set or clear a role (main/pre/post) for a stack."""
    role = payload.role.lower().strip()
    if role not in ("main", "pre", "post"):
        raise HTTPException(status_code=400, detail=f"invalid role: {payload.role}")
    try:
        result = main_acquisition_service.set_stack_role(
            series_stack_id=series_stack_id,
            role=role,  # type: ignore[arg-type]
            value=bool(payload.value),
        )
        return JSONResponse(result)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# MASQC Saved Sessions
# =============================================================================


@router.get("/cohorts/{cohort_id}/main-acq/saved-sessions")
def list_masqc_sessions(cohort_id: int):
    """List saved MASQC sessions for a cohort."""
    from db.session import session_scope
    with session_scope() as db:
        rows = db.query(MASQCSession).filter(
            MASQCSession.cohort_id == cohort_id
        ).order_by(MASQCSession.updated_at.desc()).all()
        return JSONResponse([MASQCSessionDTO.model_validate(r).model_dump(mode="json") for r in rows])


@router.post("/cohorts/{cohort_id}/main-acq/saved-sessions")
def create_masqc_session(cohort_id: int, payload: CreateMASQCSessionPayload):
    """Create a new saved MASQC session."""
    from db.session import session_scope
    with session_scope() as db:
        session = MASQCSession(
            cohort_id=cohort_id,
            name=payload.name,
            filters=payload.filters,
            session_index=payload.session_index,
            seen_indices=[],
            display_id_type=payload.display_id_type,
            only_multi_stack=payload.only_multi_stack,
        )
        db.add(session)
        db.flush()
        return JSONResponse(MASQCSessionDTO.model_validate(session).model_dump(mode="json"), status_code=201)


@router.patch("/main-acq/saved-sessions/{session_id}")
def update_masqc_session(session_id: int, payload: UpdateMASQCSessionPayload):
    """Update a saved MASQC session (position, seen, filters, name)."""
    from db.session import session_scope
    with session_scope() as db:
        session = db.query(MASQCSession).filter(MASQCSession.id == session_id).first()
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        if payload.session_index is not None:
            session.session_index = payload.session_index
        if payload.seen_indices is not None:
            session.seen_indices = payload.seen_indices
        if payload.filters is not None:
            session.filters = payload.filters
        if payload.name is not None:
            session.name = payload.name
        if payload.display_id_type is not None:
            session.display_id_type = payload.display_id_type
        if payload.only_multi_stack is not None:
            session.only_multi_stack = payload.only_multi_stack
        db.flush()
        return JSONResponse(MASQCSessionDTO.model_validate(session).model_dump(mode="json"))


@router.delete("/main-acq/saved-sessions/{session_id}")
def delete_masqc_session(session_id: int):
    """Delete a saved MASQC session."""
    from db.session import session_scope
    with session_scope() as db:
        session = db.query(MASQCSession).filter(MASQCSession.id == session_id).first()
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        db.delete(session)
        return Response(status_code=204)
