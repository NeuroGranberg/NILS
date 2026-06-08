"""Unified background runner + config builder for BIDS exports.

Both the cohort ``bids`` pipeline stage and the standalone ``export`` job
(formerly ``subset_export``) run the same underlying
:func:`bids.run_bids_export` engine. This module holds the single copy of both
the export-config construction (:func:`build_export_config`) and the
progress / metrics / terminal-status logic (:func:`run_export_job`), so the two
callers only differ in scope (``cohort_name`` vs ``include_stack_ids``), output
root, and pipeline coupling.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from bids import BidsExportConfig, run_bids_export
from jobs.service import job_service

from api.stage_sync import complete_pipeline_step, fail_pipeline_step

logger = logging.getLogger(__name__)


def _parse_subject_id_source(raw: "str | int | None") -> "str | int":
    """Parse ``subjectIdentifierSource`` from a frontend config dict.

    Accepts ``"subject_code"`` (default), an ``id_type_id`` integer (or its
    string form), or empty/None. Anything unparseable falls back to
    ``"subject_code"``.
    """
    if raw is None or raw == "" or raw == "subject_code":
        return "subject_code"
    try:
        return int(raw)
    except (TypeError, ValueError):
        return "subject_code"


def build_export_config(
    raw_cfg: dict,
    *,
    cohort_name: Optional[str] = None,
    include_stack_ids: "Optional[list[int]]" = None,
) -> BidsExportConfig:
    """Build a :class:`BidsExportConfig` from a frontend config dict.

    Single source of truth for export-config construction. Both export entry
    points feed it the same camelCase ``raw_cfg`` and differ only in *scope*:

    - **Cohort export** passes ``cohort_name`` (filters on the metadata-DB
      ``dicom_origin_cohort`` name).
    - **Subset export** passes ``include_stack_ids`` (globally-unique,
      cross-cohort stack ids).

    Both scope filters are independent optional WHERE clauses on the same
    engine query (``exporter._build_fetch_stacks_sql``), so populating either,
    both, or neither is valid. Everything downstream (naming, conversion,
    provenance routing) is identical.
    """
    raw_output_modes = raw_cfg.get("outputModes")
    if not raw_output_modes:
        legacy_mode = raw_cfg.get("outputMode", "dcm")
        raw_output_modes = [legacy_mode] if legacy_mode else ["dcm"]

    return BidsExportConfig(
        output_modes=raw_output_modes,
        layout=raw_cfg.get("layout", "bids"),
        overwrite_mode=raw_cfg.get("overwriteMode", "skip"),
        include_intents=raw_cfg.get("includeIntents") or ["anat", "dwi", "func", "fmap", "perf"],
        include_provenance=raw_cfg.get("includeProvenance", []),
        exclude_provenance=raw_cfg.get("excludeProvenance", ["ProjectionDerived"]),
        group_symri=bool(raw_cfg.get("groupSyMRI", True)),
        copy_workers=int(raw_cfg.get("copyWorkers", 8)),
        convert_workers=int(raw_cfg.get("convertWorkers", 8)),
        bids_dcm_root_name=raw_cfg.get("bidsDcmRootName", "bids-dcm"),
        bids_nifti_root_name=raw_cfg.get("bidsNiftiRootName", "bids-nifti"),
        flat_dcm_root_name=raw_cfg.get("flatDcmRootName", "flat-dcm"),
        flat_nifti_root_name=raw_cfg.get("flatNiftiRootName", "flat-nifti"),
        dcm2niix_path=raw_cfg.get("dcm2niixPath", "dcm2niix"),
        subject_identifier_source=_parse_subject_id_source(
            raw_cfg.get("subjectIdentifierSource", "subject_code")
        ),
        include_field_strengths=raw_cfg.get("includeFieldStrengths", []),
        include_acceleration_in_name=bool(raw_cfg.get("includeAccelerationInName", True)),
        cohort_name=cohort_name,
        include_stack_ids=list(include_stack_ids or []),
    )


def _build_metrics(result) -> dict:
    return {
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


def run_export_job(
    job_id: int,
    raw_root: Path,
    derivatives_root: Path,
    config_model: BidsExportConfig,
    *,
    pipeline_cohort_id: Optional[int] = None,
    pipeline_stage: Optional[str] = None,
) -> None:
    """Run a BIDS/subset export in the background, updating job + pipeline state.

    Args:
        job_id: The job record to update with progress / status / metrics.
        raw_root: Source ``dcm-raw`` root for the export.
        derivatives_root: Destination root for the export output.
        config_model: Resolved BIDS export configuration.
        pipeline_cohort_id: When set (cohort ``bids`` stage), the matching
            pipeline step is completed/failed alongside the job.
        pipeline_stage: Pipeline stage id to sync (e.g. ``"bids"``). Required
            together with ``pipeline_cohort_id``.
    """
    sync_pipeline = pipeline_cohort_id is not None and pipeline_stage is not None

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
        logger.exception("Export job %s failed", job_id)
        job_service.mark_failed(job_id, str(exc))
        if sync_pipeline:
            fail_pipeline_step(pipeline_cohort_id, pipeline_stage, error=str(exc))
        return

    metrics = _build_metrics(result)
    job_service.update_metrics(job_id, metrics)
    job_service.update_progress(job_id, 100)

    has_errors = bool(result.errors)
    has_warnings = bool(result.warnings)
    exported_something = result.exported_stacks > 0

    if has_errors and not exported_something:
        # Total failure - nothing was exported.
        err_msg = "All exports failed: " + "; ".join(result.errors[:3])
        if len(result.errors) > 3:
            err_msg += f" ... and {len(result.errors) - 3} more"
        job_service.mark_failed(job_id, err_msg)
        if sync_pipeline:
            fail_pipeline_step(pipeline_cohort_id, pipeline_stage, error=err_msg)
    elif has_errors or has_warnings:
        # Partial success - some exported, some had issues.
        warning_count = len(result.errors) + len(result.warnings)
        job_service.mark_completed_with_warnings(job_id, warning_count)
        if sync_pipeline:
            complete_pipeline_step(pipeline_cohort_id, pipeline_stage, metrics=metrics)
    else:
        # Complete success.
        job_service.mark_completed(job_id)
        if sync_pipeline:
            complete_pipeline_step(pipeline_cohort_id, pipeline_stage, metrics=metrics)

    logger.info(
        "Export job %s finished: %s stacks exported, %s files copied",
        job_id, result.exported_stacks, result.copied_files,
    )
