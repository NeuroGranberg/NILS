"""Analysis-pipeline run orchestration (frozen design §6.4).

``AnalysisPipelineRunner`` is the lifecycle owner for a single run:

    create_run (sync, 202-style)
        └─ pin repo_sha + image_digest from the pipeline
        └─ persist AnalysisPipelineRun (status=pending) + a mirrored Job
        └─ submit _execute_run to the shared _job_executor (lazy import)

    _execute_run (background, on _job_executor)
        a. materialize  — db_subset: build_export_config(include_stack_ids=...)
                          + run_export_job → BIDS on scratch (REAL reuse, the
                          §1.5.4 on-ramp); external_path: skip materialization.
        b. apply_needs  — x-nils.needs → binds/env/flags/work_dir.
        c. slice units  — by work_unit over the materialized BIDS tree.
        d. launch       — bridge.create_run(...) → store prefect_run_id.
        e. poll + mirror — bridge.get_state until terminal → update_progress /
                          update_metrics(counts) → mark_completed /
                          mark_completed_with_warnings / mark_failed; set
                          run.status + provenance snapshot.

External infrastructure is reached ONLY through the injected
:class:`PipelineBridge` (defaults to :class:`StubBridge`), so the whole lifecycle
is unit-testable with no Prefect/apptainer and with ``build_export_config`` /
``run_export_job`` monkeypatched. ``_job_executor`` is imported LAZILY inside
``create_run`` to avoid a circular import with ``api.server`` (exactly how
``cohorts.py`` / ``export.py`` do it).

App-DB I/O goes through ``db.session.session_scope``. The pipeline's descriptor
is read from the stored JSONB column and re-parsed; the runner does NOT depend on
the (separately-owned) registry module.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import select

from db.session import session_scope

from analysis_pipeline import config as scratch
from analysis_pipeline.bridge import BridgeRunState, PipelineBridge, StubBridge
from analysis_pipeline.descriptor import Descriptor
from analysis_pipeline.models import (
    AnalysisPipeline,
    AnalysisPipelineRun,
    AnalysisPipelineRunStatus,
    InputSource,
)
from analysis_pipeline.needs import NeedsContext, apply_needs
from analysis_pipeline.results import RunResults, parse_results_json

logger = logging.getLogger(__name__)

_TERMINAL_BRIDGE_STATES = frozenset({"completed", "failed", "crashed", "canceled"})

# Job stage for the run lifecycle (status mirror). Distinct from the
# "ingest_derivatives" stage handled in ingest.py.
RUN_JOB_STAGE = "analysis_pipeline_run"


def _coerce_input_source(value: "InputSource | str") -> InputSource:
    if isinstance(value, InputSource):
        return value
    return InputSource(str(value))


def _has_subjects(path: Path) -> bool:
    """True when ``path`` is a dir containing at least one ``sub-*`` subdir."""
    if not path.is_dir():
        return False
    return any(p.is_dir() and p.name.startswith("sub-") for p in path.iterdir())


class AnalysisPipelineRunner:
    """Owns the create → materialize → launch → mirror lifecycle of a run."""

    def __init__(self, bridge: Optional[PipelineBridge] = None) -> None:
        # Default substrate is the in-memory StubBridge (no real Prefect). The
        # real PrefectBridge is a drop-in of the same Protocol.
        self.bridge: PipelineBridge = bridge if bridge is not None else StubBridge()

    # -- public API --------------------------------------------------------

    def create_run(
        self,
        *,
        pipeline_id: int,
        input_source: "InputSource | str",
        stack_ids: Optional[list[int]] = None,
        external_path: Optional[str] = None,
        params: Optional[dict] = None,
        output_path: Optional[str] = None,
        retain_input: bool = True,
        raw_cfg: Optional[dict] = None,
        name: Optional[str] = None,
    ) -> int:
        """Create + launch a run; returns the new ``AnalysisPipelineRun.id``.

        Synchronous and fast (202-style): pins immutable refs, writes the run +
        a mirrored Job, then hands the lifecycle to the background executor.
        """
        src = _coerce_input_source(input_source)
        if src is InputSource.EXTERNAL_PATH and not external_path:
            raise ValueError("external_path is required for input_source=external_path")
        if src is InputSource.DB_SUBSET and not stack_ids:
            raise ValueError("stack_ids is required for input_source=db_subset")

        # 1. Load the pipeline → pin repo_sha + image_digest (immutable-per-run).
        with session_scope() as session:
            pipeline = session.get(AnalysisPipeline, pipeline_id)
            if pipeline is None:
                raise ValueError(f"AnalysisPipeline {pipeline_id} not found")
            repo = pipeline.repo
            repo_sha = repo.sha if repo is not None else ""
            image_digest = pipeline.image_digest
            pipeline_name = pipeline.name

            run = AnalysisPipelineRun(
                pipeline_id=pipeline_id,
                input_source=src.value,
                stack_ids=list(stack_ids) if stack_ids else None,
                external_path=external_path,
                params=dict(params or {}),
                repo_sha=repo_sha,
                image_digest=image_digest,
                output_path=output_path,
                retain_input=retain_input,
                status=AnalysisPipelineRunStatus.PENDING.value,
                provenance={
                    "stack_ids": list(stack_ids) if stack_ids else [],
                    "repo_sha": repo_sha,
                    "image_digest": image_digest,
                    "work_unit": pipeline.work_unit,
                },
            )
            session.add(run)
            session.flush()
            run_id = run.id

        # 2. Mirror a Job (status only; the bridge owns logs/cancel transport).
        job_config = {
            "pipeline_id": pipeline_id,
            "run_id": run_id,
            "input_source": src.value,
            "stack_ids": list(stack_ids) if stack_ids else [],
            "external_path": external_path,
            "raw_cfg": dict(raw_cfg or {}),
        }
        from jobs.service import job_service  # local import keeps module import light

        job = job_service.create_job(
            stage=RUN_JOB_STAGE,
            config=job_config,
            name=name or f"Pipeline run: {pipeline_name}",
        )
        with session_scope() as session:
            run = session.get(AnalysisPipelineRun, run_id)
            if run is not None:
                run.job_id = job.id

        # 3. Submit the background lifecycle on the shared executor (lazy import
        #    of _job_executor to dodge the api.server circular import).
        from api.server import _job_executor

        _job_executor.submit(self._execute_run, run_id, raw_cfg)
        logger.info(
            "AnalysisPipeline run %s (pipeline %s) submitted; job=%s",
            run_id,
            pipeline_id,
            job.id,
        )
        return run_id

    def get_run(self, run_id: int) -> Optional[AnalysisPipelineRun]:
        with session_scope() as session:
            run = session.get(AnalysisPipelineRun, run_id)
            if run is not None:
                session.expunge(run)
            return run

    def list_runs(self, pipeline_id: int) -> list[AnalysisPipelineRun]:
        """All runs for a pipeline, newest first (detached snapshots)."""
        with session_scope() as session:
            rows = (
                session.execute(
                    select(AnalysisPipelineRun)
                    .where(AnalysisPipelineRun.pipeline_id == pipeline_id)
                    .order_by(AnalysisPipelineRun.created_at.desc())
                )
                .scalars()
                .all()
            )
            for r in rows:
                session.expunge(r)
            return list(rows)

    def get_state(self, run_id: int) -> BridgeRunState:
        """Proxy the bridge state for a run (per-unit counts)."""
        prefect_run_id = self._prefect_run_id(run_id)
        if not prefect_run_id:
            return BridgeRunState(prefect_run_id="", state="pending")
        return self.bridge.get_state(prefect_run_id)

    def get_logs(self, run_id: int, since: int = 0) -> list[str]:
        """Proxy per-unit logs for a run (``since`` cursor)."""
        prefect_run_id = self._prefect_run_id(run_id)
        if not prefect_run_id:
            return []
        return self.bridge.get_logs(prefect_run_id, since=since)

    def cancel_run(self, run_id: int) -> None:
        """Cancel a run: bridge.cancel → run.status=canceled → cancel the Job.

        The bridge owns the cancel transport (net-new, §0.5(4)); the in-process
        JobControl is NOT the cancel path for a bridge-owned run.
        """
        prefect_run_id = self._prefect_run_id(run_id)
        if prefect_run_id:
            try:
                self.bridge.cancel(prefect_run_id)
            except Exception as exc:  # pragma: no cover - bridge transport
                logger.warning("bridge.cancel failed for run %s: %s", run_id, exc)
        job_id = self._set_status(run_id, AnalysisPipelineRunStatus.CANCELED)
        if job_id is not None:
            from jobs.service import job_service

            job_service.cancel_job(job_id)

    # -- background lifecycle ---------------------------------------------

    def _execute_run(self, run_id: int, raw_cfg: Optional[dict]) -> None:
        """Background body (on ``_job_executor``)."""
        from jobs.service import job_service

        # Load the run + its pipeline descriptor (detached snapshots).
        snap = self._load_run_snapshot(run_id)
        if snap is None:
            logger.error("run %s vanished before execution", run_id)
            return
        job_id = snap["job_id"]

        try:
            if job_id is not None:
                job_service.mark_running(job_id)

            # a. Materialize input. A failed db_subset materialization RAISES
            #    here (see _materialize) → caught below → run marked FAILED.
            self._set_status(run_id, AnalysisPipelineRunStatus.MATERIALIZING)
            bids_dir = self._materialize(run_id, snap, raw_cfg)

            # Honor a cancel issued DURING materialization: no bridge run exists
            # yet, so cancel_run() could not reach the bridge and _execute_run
            # would otherwise overwrite the CANCELED status. Bail, leaving it.
            if self._is_canceled(run_id):
                logger.info("run %s canceled during materialization", run_id)
                return

            output_dir = scratch.run_output_dir(run_id)
            work_dir = scratch.run_work_dir(run_id)
            output_dir.mkdir(parents=True, exist_ok=True)

            # b. Apply needs → binds/env/flags/work_dir. work_dir is a DECLARED
            #    need (§7.3): provision it only when the descriptor asks, and feed
            #    the run's scratch work path through the declared value so the
            #    '/work' bind targets the right dir (a bare True would make
            #    provision_work_dir mint a throwaway tmp dir instead).
            descriptor = snap["descriptor"]
            ctx = NeedsContext(work_dir=str(work_dir))
            needs = dict(descriptor.x_nils.needs.model_dump(by_alias=False))
            if needs.get("work_dir"):
                needs["work_dir"] = str(work_dir)
            apply_needs(needs, ctx)

            # c. Slice units by work_unit over the materialized tree.
            units = self._slice_units(snap, bids_dir)

            # d. Launch via the bridge.
            self._set_status(run_id, AnalysisPipelineRunStatus.LAUNCHING)
            image = self._image_ref(descriptor)
            prefect_run_id = self.bridge.create_run(
                image=image,
                units=units,
                descriptor=descriptor.model_dump(by_alias=True),
                binds=ctx.binds,
                env=ctx.env,
                bids_dir=str(bids_dir),
                output_dir=str(output_dir),
                work_dir=ctx.work_dir,
            )
            self._store_launch(
                run_id,
                prefect_run_id=prefect_run_id,
                input_path=str(bids_dir),
                output_path=str(output_dir),
                work_dir=ctx.work_dir,
            )

            # Honor a cancel issued DURING launch: a bridge run now exists, so
            # tell the bridge and stop before flipping to RUNNING.
            if self._is_canceled(run_id):
                logger.info("run %s canceled during launch", run_id)
                try:
                    self.bridge.cancel(prefect_run_id)
                except Exception as exc:  # pragma: no cover - bridge transport
                    logger.warning("bridge.cancel failed for run %s: %s", run_id, exc)
                return

            self._set_status(run_id, AnalysisPipelineRunStatus.RUNNING)

            # e. Poll the bridge to a terminal state + mirror to the job.
            state = self._poll_to_terminal(prefect_run_id)
            results = parse_results_json(output_dir)
            self._finalize(run_id, job_id, state, results, ctx)
        except Exception as exc:
            logger.exception("run %s failed", run_id)
            self._record_error(run_id, str(exc))
            self._set_status(run_id, AnalysisPipelineRunStatus.FAILED)
            if job_id is not None:
                job_service.mark_failed(job_id, str(exc))

    # -- lifecycle helpers -------------------------------------------------

    def _materialize(self, run_id: int, snap: dict, raw_cfg: Optional[dict]) -> Path:
        """Produce the BIDS input root on scratch (or use external_path).

        db_subset reuses the export engine VERBATIM (no new export code path):
        ``build_export_config(raw_cfg, include_stack_ids=...)`` then
        ``run_export_job(... derivatives_root=run_bids_dir)`` — the §1.5.4 on-ramp.
        external_path skips materialization entirely.
        """
        src = snap["input_source"]
        if src == InputSource.EXTERNAL_PATH.value:
            external_path = snap["external_path"]
            self._store_input_path(run_id, external_path)
            return Path(external_path)

        # db_subset → materialize a stack subset to scratch BIDS.
        bids_dir = scratch.run_bids_dir(run_id)
        bids_dir.mkdir(parents=True, exist_ok=True)
        raw_root = self._resolve_raw_root(snap, raw_cfg)

        from api.services.export_runner import build_export_config, run_export_job
        from jobs.service import job_service

        cfg = dict(raw_cfg or {})
        # Defensive: never let the exporter's clean-root logic reach the run's
        # scratch siblings (output/, work/). An empty bids_*_root_name makes the
        # exporter write to (and, under overwrite=clean, wipe) the parent dir.
        if not cfg.get("bidsNiftiRootName"):
            cfg["bidsNiftiRootName"] = "bids-nifti"
        if not cfg.get("bidsDcmRootName"):
            cfg["bidsDcmRootName"] = "bids-dcm"
        config_model = build_export_config(
            cfg, include_stack_ids=[int(s) for s in snap["stack_ids"]]
        )

        # Materialization runs on its OWN child export job, NOT the run's mirror
        # job: run_export_job drives the job it is handed to a TERMINAL status and
        # swallows export failures (mark_failed + return, never raises). Reusing
        # the run's job would corrupt the lifecycle mirror (§9.2) and hide a
        # failed materialization. Use a separate job and inspect its outcome.
        mat_job = job_service.create_job(
            stage="export",
            config={
                "run_id": run_id,
                "materialize": True,
                "stack_ids": list(snap["stack_ids"]),
            },
            name=f"Pipeline run {run_id}: materialize",
        )
        job_service.mark_running(mat_job.id)
        run_export_job(mat_job.id, raw_root, bids_dir, config_model)

        mat = job_service.get_job(mat_job.id)
        mat_status = str(getattr(mat, "status", "failed"))
        mat_status = getattr(getattr(mat, "status", None), "value", mat_status)
        if mat is None or str(mat_status) == "failed":
            raise RuntimeError(
                f"Materialization failed for run {run_id}: "
                f"{(getattr(mat, 'last_error', None) if mat else None) or 'export produced no BIDS tree'}"
            )

        # run_bids_export writes subjects under <derivatives_root>/<bids root
        # name>/sub-*, NOT directly under derivatives_root. Resolve the real root.
        real_root = self._resolve_bids_root(bids_dir, config_model, snap["descriptor"])
        self._store_input_path(run_id, str(real_root))
        return real_root

    def _resolve_raw_root(self, snap: dict, raw_cfg: Optional[dict]) -> Path:
        """Resolve the source ``dcm-raw`` root for a db_subset materialization.

        Mirrors the cohort resolution in ``api/routes/export.py``: take the
        source cohort (by name/id from ``raw_cfg``), then
        ``setup_derivatives_folders(source_path).output_path``.
        """
        cfg = dict(raw_cfg or {})
        # Allow a caller to pass an explicit raw_root (tests, advanced callers).
        explicit = cfg.get("raw_root") or cfg.get("source_dcm_raw")
        if explicit:
            return Path(explicit)

        cohort_name = cfg.get("source_cohort_name") or (
            (cfg.get("detected_cohorts") or [None])[0]
        )
        cohort_id = cfg.get("source_cohort_id")
        from cohorts.service import cohort_service

        cohort = None
        if cohort_id is not None:
            cohort = cohort_service.get_cohort(int(cohort_id))
        if cohort is None and cohort_name:
            cohort = cohort_service.get_cohort_by_name(str(cohort_name))
        if cohort is None:
            raise ValueError(
                "Cannot resolve source cohort for db_subset materialization; "
                "pass source_cohort_name/source_cohort_id or raw_root in config."
            )
        from anonymize.config import setup_derivatives_folders

        derivatives_setup = setup_derivatives_folders(Path(cohort.source_path))
        return derivatives_setup.output_path

    @staticmethod
    def _resolve_bids_root(derivatives_root: Path, config_model, descriptor: Descriptor) -> Path:
        """Resolve the actual BIDS root produced by ``run_bids_export``.

        The exporter writes subjects under
        ``<derivatives_root>/<bids_nifti_root_name|bids_dcm_root_name>/sub-*``
        (or the ``flat_*`` names for flat layout), NOT directly under
        ``derivatives_root``. Pick the root for the format the descriptor
        consumes, preferring whichever actually contains ``sub-*`` dirs; fall
        back to ``derivatives_root`` so a stubbed/empty materialize still yields
        a path (the slice step then surfaces the empty tree).
        """
        from bids.exporter import Layout

        is_bids = getattr(config_model.layout, "value", config_model.layout) == Layout.BIDS.value
        if is_bids:
            nifti_name = config_model.bids_nifti_root_name
            dcm_name = config_model.bids_dcm_root_name
        else:
            nifti_name = config_model.flat_nifti_root_name
            dcm_name = config_model.flat_dcm_root_name

        formats = [str(f).lower() for f in (descriptor.x_nils.input.formats or [])]
        wants_dicom = "dicom" in formats and "nifti" not in formats
        ordered = [dcm_name, nifti_name] if wants_dicom else [nifti_name, dcm_name]

        candidates = [derivatives_root / n if n else derivatives_root for n in ordered]
        candidates.append(derivatives_root)
        for cand in candidates:
            if _has_subjects(cand):
                return cand
        return candidates[0]

    def _is_canceled(self, run_id: int) -> bool:
        """True when the run has been moved to CANCELED out-of-band."""
        with session_scope() as session:
            run = session.get(AnalysisPipelineRun, run_id)
            return run is not None and run.status == AnalysisPipelineRunStatus.CANCELED.value

    def _slice_units(self, snap: dict, bids_dir: Path) -> list[dict]:
        """Slice work units by the pipeline's ``work_unit`` over the BIDS tree.

        v1 slicing is intentionally simple and tolerant: scan ``sub-*`` (and
        ``ses-*`` for session units) directories under ``bids_dir``. For
        ``group`` there is a single dataset-level unit. For ``stack`` units with
        an explicit stack subset, fall back to the stack ids. When the tree is
        empty (e.g. a stubbed materialize), synthesize units from inputs so the
        bridge still has something to run.
        """
        work_unit = snap["work_unit"]
        units: list[dict] = []

        if work_unit == "group":
            return [{"unit_id": "dataset", "work_unit": "group"}]

        subjects: list[str] = []
        if bids_dir.is_dir():
            subjects = sorted(
                p.name for p in bids_dir.iterdir() if p.is_dir() and p.name.startswith("sub-")
            )

        if work_unit in ("subject", "session", "stack") and subjects:
            if work_unit == "subject":
                return [{"unit_id": s, "work_unit": "subject"} for s in subjects]
            # session/stack: one unit per ses-* under each subject (fallback to
            # the subject itself when no sessions exist).
            for s in subjects:
                sub_dir = bids_dir / s
                sessions = sorted(
                    p.name
                    for p in sub_dir.iterdir()
                    if p.is_dir() and p.name.startswith("ses-")
                ) if sub_dir.is_dir() else []
                if sessions:
                    for ses in sessions:
                        units.append(
                            {"unit_id": f"{s}_{ses}", "work_unit": work_unit}
                        )
                else:
                    units.append({"unit_id": s, "work_unit": work_unit})
            if units:
                return units

        # Fallbacks when the materialized tree is empty (e.g. stubbed export).
        if work_unit == "stack" and snap.get("stack_ids"):
            return [
                {"unit_id": f"stack-{sid}", "work_unit": "stack"}
                for sid in snap["stack_ids"]
            ]
        # Last-resort single unit so a launch is never empty.
        return [{"unit_id": "unit-0", "work_unit": work_unit}]

    def _poll_to_terminal(self, prefect_run_id: str) -> BridgeRunState:
        """Poll the bridge until a terminal state.

        The StubBridge reaches terminal in one (immediate) or two (walking) calls,
        so a bounded loop is safe and infra-free. The real poller is an asyncio
        task (§0.5(3)) — designed-for, not built here.
        """
        state = self.bridge.get_state(prefect_run_id)
        guard = 0
        while state.state not in _TERMINAL_BRIDGE_STATES and guard < 1000:
            guard += 1
            state = self.bridge.get_state(prefect_run_id)
        return state

    def _finalize(
        self,
        run_id: int,
        job_id: Optional[int],
        state: BridgeRunState,
        results: RunResults,
        ctx: NeedsContext,
    ) -> None:
        """Mirror the terminal bridge state to the run + job."""
        from jobs.service import job_service

        counts = results.counts
        provenance_extra = {
            "results_counts": counts,
            "bridge_state": state.state,
            "needs_warnings": ctx.warnings,
        }

        if job_id is not None:
            try:
                job_service.update_metrics(job_id, counts)
                job_service.update_progress(job_id, 100)
            except Exception:  # pragma: no cover - best effort
                pass

        if state.state == "canceled":
            self._set_status(run_id, AnalysisPipelineRunStatus.CANCELED, provenance_extra)
            if job_id is not None:
                job_service.cancel_job(job_id)
            return

        failed = counts.get("failed", 0)
        succeeded = counts.get("succeeded", 0)
        total = counts.get("total", 0)

        if state.state in ("failed", "crashed") or (failed and succeeded == 0 and total):
            self._set_status(run_id, AnalysisPipelineRunStatus.FAILED, provenance_extra)
            if job_id is not None:
                job_service.mark_failed(
                    job_id, f"Run failed: {failed}/{total} units failed."
                )
            return

        if failed:
            self._set_status(
                run_id,
                AnalysisPipelineRunStatus.COMPLETED_WITH_WARNINGS,
                provenance_extra,
            )
            if job_id is not None:
                job_service.mark_completed_with_warnings(job_id, failed)
            return

        self._set_status(run_id, AnalysisPipelineRunStatus.COMPLETED, provenance_extra)
        if job_id is not None:
            job_service.mark_completed(job_id)

    # -- App-DB access -----------------------------------------------------

    def _load_run_snapshot(self, run_id: int) -> Optional[dict]:
        """Load a detached snapshot of the run + its pipeline descriptor."""
        with session_scope() as session:
            run = session.get(AnalysisPipelineRun, run_id)
            if run is None:
                return None
            pipeline = session.get(AnalysisPipeline, run.pipeline_id)
            if pipeline is None:
                return None
            descriptor = self._descriptor_from_pipeline(pipeline)
            return {
                "run_id": run.id,
                "job_id": run.job_id,
                "input_source": run.input_source,
                "stack_ids": list(run.stack_ids or []),
                "external_path": run.external_path,
                "work_unit": pipeline.work_unit,
                "descriptor": descriptor,
            }

    @staticmethod
    def _descriptor_from_pipeline(pipeline: AnalysisPipeline) -> Descriptor:
        """Rebuild a :class:`Descriptor` from the stored JSONB column."""
        return Descriptor.model_validate(pipeline.descriptor)

    @staticmethod
    def _image_ref(descriptor: Descriptor) -> str:
        ci = descriptor.container_image
        if ci is not None and ci.image:
            return ci.image
        return descriptor.image_digest or descriptor.slug

    def _prefect_run_id(self, run_id: int) -> Optional[str]:
        with session_scope() as session:
            run = session.get(AnalysisPipelineRun, run_id)
            return run.prefect_run_id if run is not None else None

    def _set_status(
        self,
        run_id: int,
        status: AnalysisPipelineRunStatus,
        provenance_extra: Optional[dict] = None,
    ) -> Optional[int]:
        """Set run.status (+ merge provenance); return the run's job_id."""
        with session_scope() as session:
            run = session.get(AnalysisPipelineRun, run_id)
            if run is None:
                return None
            run.status = status.value
            if provenance_extra:
                merged = dict(run.provenance or {})
                merged.update(provenance_extra)
                run.provenance = merged
            return run.job_id

    def _store_launch(
        self,
        run_id: int,
        *,
        prefect_run_id: str,
        input_path: str,
        output_path: str,
        work_dir: Optional[str],
    ) -> None:
        with session_scope() as session:
            run = session.get(AnalysisPipelineRun, run_id)
            if run is None:
                return
            run.prefect_run_id = prefect_run_id
            run.input_path = input_path
            run.output_path = output_path
            run.work_dir = work_dir

    def _store_input_path(self, run_id: int, input_path: str) -> None:
        with session_scope() as session:
            run = session.get(AnalysisPipelineRun, run_id)
            if run is not None:
                run.input_path = input_path

    def _record_error(self, run_id: int, error: str) -> None:
        with session_scope() as session:
            run = session.get(AnalysisPipelineRun, run_id)
            if run is not None:
                run.last_error = error


__all__ = ["AnalysisPipelineRunner", "RUN_JOB_STAGE"]
