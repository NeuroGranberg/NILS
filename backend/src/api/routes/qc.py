"""QC Pipeline API routes."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse, Response

from qc.service import qc_service
from qc.dicom_service import dicom_service
from qc.axes_service import axes_qc_service, get_axis_options_from_yaml
from qc.models import (
    CreateQCSessionPayload,
    UpdateQCItemPayload,
    ConfirmQCChangesPayload,
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
def get_dicom_file(instance_id: int):
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
):
    """
    Get a DICOM instance rendered as PNG.

    This is a simple viewer endpoint that renders DICOM to PNG server-side.
    For high-performance viewing, use the raw DICOM endpoints with Cornerstone.js.

    Args:
        instance_id: Instance ID
        window_center: Optional window center override
        window_width: Optional window width override
    """
    try:
        image_bytes = dicom_service.render_instance_to_png(
            instance_id,
            window_center=window_center,
            window_width=window_width,
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


@router.get("/dicom/{series_uid}/instances")
def get_series_instances(
    series_uid: str,
    stack_index: int = Query(0, ge=0),
):
    """
    Get list of instance IDs for a series (for slice navigation).

    Returns instance IDs ordered by slice location for use with
    the simple image viewer.
    """
    try:
        instance_ids = dicom_service.get_series_instance_ids(series_uid, stack_index)
        return JSONResponse(
            {
                "series_uid": series_uid,
                "stack_index": stack_index,
                "instance_ids": instance_ids,
                "total": len(instance_ids),
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
