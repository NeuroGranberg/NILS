"""Standalone BIDS export routes.

Provides endpoints for exporting a subset of data based on a selection manifest
(CSV or JSON specifying subject/session/stack combinations).
"""
from __future__ import annotations

import logging
import threading
from pathlib import Path

from fastapi import APIRouter, Body, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse

from sqlalchemy import text as sa_text

from bids import (
    BidsExportConfig,
    parse_manifest,
    resolve_manifest,
    run_bids_export,
)
from cohorts.service import cohort_service
from jobs.service import job_service
from api.utils.serializers import serialize_job
from metadata_db.session import SessionLocal as MetadataSessionLocal

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/export", tags=["export"])


def _parse_subject_id_source(raw: str | int | None) -> str | int:
    """Parse subject_identifier_source from frontend config."""
    if raw is None or raw == "" or raw == "subject_code":
        return "subject_code"
    try:
        return int(raw)
    except (TypeError, ValueError):
        return "subject_code"


@router.post("/resolve")
async def resolve_export_manifest(
    manifest_text: str | None = Body(default=None),
    manifest_file: UploadFile | None = File(default=None),
):
    """Resolve a selection manifest to concrete series_stack_ids.

    Accepts either:
      - JSON body with manifest_text (CSV or JSON string)
      - Multipart file upload (manifest_file)

    Returns resolved stack IDs with summary statistics and warnings.
    """
    content: str | None = None

    if manifest_file is not None:
        raw = await manifest_file.read()
        content = raw.decode("utf-8")
    elif manifest_text is not None:
        content = manifest_text
    else:
        raise HTTPException(status_code=400, detail="Provide manifest_text or manifest_file")

    if not content or not content.strip():
        raise HTTPException(status_code=400, detail="Manifest is empty")

    try:
        entries = parse_manifest(content)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to parse manifest: {exc}")

    if not entries:
        raise HTTPException(status_code=400, detail="Manifest contains no valid entries")

    result = resolve_manifest(entries)

    return JSONResponse({
        "total_subjects": result.total_subjects,
        "total_sessions": result.total_sessions,
        "total_stacks": result.total_stacks,
        "resolved_stack_ids": result.resolved_stack_ids,
        "resolved_entries": result.resolved_entries,
        "warnings": result.warnings,
        "detected_cohorts": result.detected_cohorts,
    })


@router.post("/resolve-text")
async def resolve_export_manifest_text(body: dict = Body(...)):
    """Resolve a selection manifest from a JSON body.

    Body: { "manifest_text": "..csv or json string.." }
    """
    content = body.get("manifest_text", "")
    if not content or not str(content).strip():
        raise HTTPException(status_code=400, detail="manifest_text is empty")

    try:
        entries = parse_manifest(str(content))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to parse manifest: {exc}")

    if not entries:
        raise HTTPException(status_code=400, detail="Manifest contains no valid entries")

    result = resolve_manifest(entries)

    return JSONResponse({
        "total_subjects": result.total_subjects,
        "total_sessions": result.total_sessions,
        "total_stacks": result.total_stacks,
        "resolved_stack_ids": result.resolved_stack_ids,
        "resolved_entries": result.resolved_entries,
        "warnings": result.warnings,
        "detected_cohorts": result.detected_cohorts,
    })


@router.post("/subject-identifiers")
def get_subject_identifiers(body: dict = Body(...)):
    """Look up alternative identifiers for a list of subject codes.

    Body: { "subject_codes": ["PAT001", "PAT002"], "id_type_id": 1 }
    Returns: { "mapping": { "PAT001": "KI-12345", "PAT002": "KI-67890" } }

    Subjects without a matching identifier are omitted from the mapping
    (frontend falls back to subject_code).
    """
    subject_codes = body.get("subject_codes", [])
    id_type_id = body.get("id_type_id")

    if not subject_codes or not id_type_id:
        return JSONResponse({"mapping": {}})

    try:
        id_type_id = int(id_type_id)
    except (TypeError, ValueError):
        return JSONResponse({"mapping": {}})

    placeholders = ", ".join(f":sc_{i}" for i in range(len(subject_codes)))
    sql = f"""
        SELECT subj.subject_code, soi.other_identifier
        FROM subject subj
        JOIN subject_other_identifiers soi ON subj.subject_id = soi.subject_id
        WHERE soi.id_type_id = :id_type_id
          AND subj.subject_code IN ({placeholders})
    """
    params: dict = {"id_type_id": id_type_id}
    for i, sc in enumerate(subject_codes):
        params[f"sc_{i}"] = sc

    with MetadataSessionLocal() as db:
        rows = db.execute(sa_text(sql), params).fetchall()

    mapping = {row.subject_code: row.other_identifier for row in rows}
    return JSONResponse({"mapping": mapping})


def _run_export_background(
    job_id: int,
    raw_root: Path,
    derivatives_root: Path,
    config_model: BidsExportConfig,
) -> None:
    """Run BIDS export in a background thread, updating job progress/status."""

    def progress_cb(processed: int, total: int) -> None:
        if total <= 0:
            return
        pct = 1 + int((processed / total) * 98)
        pct = max(1, min(pct, 99))
        try:
            job_service.update_progress(job_id, pct)
        except Exception:
            pass

    try:
        result = run_bids_export(
            raw_root=raw_root,
            derivatives_root=derivatives_root,
            config=config_model,
            progress_cb=progress_cb,
        )
    except Exception as exc:
        logger.exception("Subset export job %s failed", job_id)
        job_service.mark_failed(job_id, str(exc))
        return

    metrics = {
        "total_stacks": result.total_stacks,
        "exported_stacks": result.exported_stacks,
        "copied_files": result.copied_files,
        "skipped_files": result.skipped_files,
        "errors": result.errors,
        "warnings": result.warnings,
        "skipped_nifti_provenances": result.skipped_nifti_provenances,
        "nifti_conversion_errors": result.nifti_conversion_errors,
        "dicom_copy_errors": result.dicom_copy_errors,
    }
    job_service.update_metrics(job_id, metrics)
    job_service.update_progress(job_id, 100)

    has_errors = bool(result.errors)
    has_warnings = bool(result.warnings)
    exported_something = result.exported_stacks > 0

    if has_errors and not exported_something:
        err_msg = "All exports failed: " + "; ".join(result.errors[:3])
        if len(result.errors) > 3:
            err_msg += f" ... and {len(result.errors) - 3} more"
        job_service.mark_failed(job_id, err_msg)
    elif has_errors or has_warnings:
        warning_count = len(result.errors) + len(result.warnings)
        job_service.mark_completed_with_warnings(job_id, warning_count)
    else:
        job_service.mark_completed(job_id)

    logger.info(
        "Subset export job %s finished: %s stacks exported, %s files copied",
        job_id, result.exported_stacks, result.copied_files,
    )


@router.post("/run", status_code=202)
def run_export(body: dict = Body(...)):
    """Start a BIDS export for a resolved set of stack IDs.

    Returns immediately with the created job (HTTP 202). The export runs
    in a background thread, updating progress and status via the job service.

    Body:
    {
        "stack_ids": [1, 2, 3, ...],
        "detected_cohorts": ["CohortName"],
        "output_path": "/path/to/output",
        "manifest_text": "...original CSV/JSON...",
        "resolve_summary": { "total_subjects": 12, "total_sessions": 24 },
        "config": { "layout": "bids", "outputModes": ["dcm"], ... }
    }
    """
    stack_ids = body.get("stack_ids")
    if not stack_ids or not isinstance(stack_ids, list):
        raise HTTPException(status_code=400, detail="stack_ids is required and must be a non-empty list")

    output_path_str = body.get("output_path")
    if not output_path_str or not str(output_path_str).strip():
        raise HTTPException(status_code=400, detail="output_path is required")

    detected_cohorts = body.get("detected_cohorts", [])
    if not detected_cohorts:
        raise HTTPException(
            status_code=400,
            detail="No source cohort detected. The resolved stacks must belong to at least one cohort.",
        )

    cohort_name = detected_cohorts[0]
    cohort = cohort_service.get_cohort_by_name(cohort_name)
    if not cohort:
        raise HTTPException(
            status_code=404,
            detail=f"Cohort '{cohort_name}' not found. Cannot locate source DICOM files.",
        )

    from anonymize.config import setup_derivatives_folders
    selected_root = Path(cohort.source_path)
    derivatives_setup = setup_derivatives_folders(selected_root)
    raw_root = derivatives_setup.output_path
    derivatives_root = Path(output_path_str)

    export_name = str(body.get("export_name", "")).strip()
    export_description = str(body.get("export_description", "")).strip()

    cfg = body.get("config", {})
    try:
        raw_output_modes = cfg.get("outputModes")
        if not raw_output_modes:
            legacy_mode = cfg.get("outputMode", "dcm")
            raw_output_modes = [legacy_mode] if legacy_mode else ["dcm"]

        layout = cfg.get("layout", "bids")

        config_model = BidsExportConfig(
            output_modes=raw_output_modes,
            layout=layout,
            overwrite_mode=cfg.get("overwriteMode", "skip"),
            include_intents=cfg.get("includeIntents") or ["anat", "dwi", "func", "fmap", "perf"],
            include_provenance=cfg.get("includeProvenance", []),
            exclude_provenance=cfg.get("excludeProvenance", ["ProjectionDerived"]),
            group_symri=bool(cfg.get("groupSyMRI", True)),
            copy_workers=int(cfg.get("copyWorkers", 8)),
            convert_workers=int(cfg.get("convertWorkers", 8)),
            bids_dcm_root_name=cfg.get("bidsDcmRootName", "bids-dcm"),
            bids_nifti_root_name=cfg.get("bidsNiftiRootName", "bids-nifti"),
            flat_dcm_root_name=cfg.get("flatDcmRootName", "flat-dcm"),
            flat_nifti_root_name=cfg.get("flatNiftiRootName", "flat-nifti"),
            dcm2niix_path=cfg.get("dcm2niixPath", "dcm2niix"),
            include_field_strengths=cfg.get("includeFieldStrengths", []),
            include_stack_ids=[int(sid) for sid in stack_ids],
            subject_identifier_source=_parse_subject_id_source(cfg.get("subjectIdentifierSource", "subject_code")),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid config: {exc}")

    # Persist manifest + resolve summary + naming in job config
    resolve_summary = body.get("resolve_summary", {})
    job_config = config_model.model_dump(mode="json")
    job_config["source_cohort_id"] = cohort.id
    job_config["source_cohort_name"] = cohort.name
    job_config["detected_cohorts"] = detected_cohorts
    job_config["export_type"] = "standalone"
    job_config["output_path"] = output_path_str
    job_config["stack_count"] = len(stack_ids)
    job_config["manifest_text"] = body.get("manifest_text", "")
    job_config["total_subjects"] = resolve_summary.get("total_subjects", 0)
    job_config["total_sessions"] = resolve_summary.get("total_sessions", 0)
    job_config["export_name"] = export_name
    job_config["export_description"] = export_description

    job_name = f"{export_name} - {len(stack_ids)} stacks" if export_name else f"Export - {len(stack_ids)} stacks"
    job = job_service.create_job(
        stage="subset_export",
        config=job_config,
        name=job_name,
    )
    job_service.mark_running(job.id)

    try:
        job_service.update_progress(job.id, 1)
    except Exception:
        pass

    # Run export in background thread
    thread = threading.Thread(
        target=_run_export_background,
        args=(job.id, raw_root, derivatives_root, config_model),
        daemon=True,
        name=f"subset-export-{job.id}",
    )
    thread.start()

    logger.info("Subset export job %s started in background thread", job.id)
    return JSONResponse(
        status_code=202,
        content={"job": serialize_job(job_service.get_job(job.id))},
    )
