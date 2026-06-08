"""``ingest_derivatives`` skeleton (frozen design §6.5, §17.8).

After a run finishes, its successful units' derivatives can be ingested back
into the METADATA DB (provenance + derivative rows) so downstream queries see
them. That DB-WRITE adapter is DEFERRED (§17.8): in this slice only the PURE
planning surface is implemented and tested. The write path is a clearly-marked,
gated seam behind ``INGEST_DB_WRITE_ENABLED = False``.

* :func:`plan_ingest` — PURE: scan ``output_dir`` + parse ``results.json`` and
  select the SUCCEEDED units + their derivative paths. No DB. Drives both the
  ``--dry-run`` report and the UI preview.
* :func:`run_ingest_derivatives_job` — the ``stage="ingest_derivatives"`` job
  body (runs on ``_job_executor``). Computes the plan, records it in job
  metrics, and — because the write adapter is gated OFF — completes-with-warnings
  noting "DB-write deferred (§17.8)". When the gate is later flipped on, the
  commented seam invokes ``x-nils.ingest.entrypoint`` to write provenance +
  derivative rows. ``entrypoint`` is recorded but never invoked in v1.
"""

from __future__ import annotations

import logging
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from analysis_pipeline.results import UnitResult, UnitStatus, parse_results_json

logger = logging.getLogger(__name__)


# GATE — the metadata-DB write adapter is DEFERRED (§17.8). While False, the job
# computes + records the plan but writes NOTHING. Flipping this on is a separate,
# scoped change that wires the ingest adapter (the commented seam below).
INGEST_DB_WRITE_ENABLED = False


class IngestPlan(BaseModel):
    """What ``ingest_derivatives`` WOULD ingest (pure, DB-free)."""

    model_config = ConfigDict(extra="allow")

    run_id: int
    output_dir: str
    units_to_ingest: list[UnitResult] = Field(default_factory=list)  # succeeded only
    skipped: list[str] = Field(default_factory=list)  # failed/skipped/absent unit ids
    derivative_count: int = 0

    @property
    def counts(self) -> dict[str, int]:
        return {
            "units_to_ingest": len(self.units_to_ingest),
            "skipped": len(self.skipped),
            "derivative_count": self.derivative_count,
        }


def plan_ingest(run_id: int, output_dir: "str | Path") -> IngestPlan:
    """Scan ``output_dir`` + parse ``results.json`` → an :class:`IngestPlan`.

    PURE: no DB reads or writes. Selects SUCCEEDED units only (§9.3 per-unit) and
    tallies their declared derivative paths. Failed/skipped units are recorded in
    ``skipped``. A missing/empty ``results.json`` yields an empty plan (not an
    error).
    """
    out_str = str(output_dir)
    results = parse_results_json(output_dir)

    units_to_ingest: list[UnitResult] = list(results.succeeded)
    # Partition by STATUS, not pydantic value-equality (which would mis-bucket a
    # failed unit that happens to be field-equal to a succeeded one).
    skipped = [u.unit_id for u in results.units if u.status != UnitStatus.SUCCEEDED]
    derivative_count = sum(len(u.derivatives) for u in units_to_ingest)

    return IngestPlan(
        run_id=run_id,
        output_dir=out_str,
        units_to_ingest=units_to_ingest,
        skipped=skipped,
        derivative_count=derivative_count,
    )


def run_ingest_derivatives_job(
    job_id: int, run_id: int, output_dir: "str | Path"
) -> None:
    """Job body for ``stage="ingest_derivatives"`` (runs on ``_job_executor``).

    Always computes + records the plan and mirrors job status. The actual
    metadata-DB write is gated OFF (§17.8): when disabled, the job
    completes-with-warnings noting the deferral; the real adapter is a drop-in.
    """
    from jobs.service import job_service

    try:
        job_service.mark_running(job_id)
        plan = plan_ingest(run_id, output_dir)
        job_service.update_metrics(job_id, plan.counts)

        if not INGEST_DB_WRITE_ENABLED:
            # ---- DEFERRED metadata-DB write adapter (§17.8) ------------------
            # When INGEST_DB_WRITE_ENABLED flips True, this is where the ingest
            # adapter runs: resolve x-nils.ingest.entrypoint and, for each
            # succeeded unit, write provenance + derivative rows to the METADATA
            # DB (metadata_db.session). Intentionally NOT implemented here — the
            # contract is deferred (§17.8); only the plan is computed in v1.
            #
            #   from metadata_db.session import SessionLocal as MetadataSessionLocal
            #   adapter = load_ingest_adapter(entrypoint)
            #   with MetadataSessionLocal() as db:
            #       for unit in plan.units_to_ingest:
            #           adapter.write(db, run_id, unit)
            # ------------------------------------------------------------------
            logger.info(
                "ingest_derivatives run=%s: planned %d units / %d derivatives "
                "(DB-write deferred, §17.8)",
                run_id,
                len(plan.units_to_ingest),
                plan.derivative_count,
            )
            job_service.mark_completed_with_warnings(job_id, len(plan.units_to_ingest))
            return

        # Reached only when the gate is on (not in v1). Guards against silently
        # claiming a write happened when the adapter is not yet wired.
        raise NotImplementedError(
            "ingest DB-write adapter is deferred (§17.8); set "
            "INGEST_DB_WRITE_ENABLED only once the adapter is implemented."
        )
    except Exception as exc:
        logger.exception("ingest_derivatives job %s failed", job_id)
        job_service.mark_failed(job_id, str(exc))


__all__ = [
    "INGEST_DB_WRITE_ENABLED",
    "IngestPlan",
    "plan_ingest",
    "run_ingest_derivatives_job",
]
