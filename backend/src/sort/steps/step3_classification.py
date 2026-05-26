"""
Step 3: Classification

This step runs the classification pipeline on each fingerprint from Step 2
and populates the series_classification_cache table.

Key features:
- Batch processing for memory efficiency
- Progress streaming to frontend
- Bulk upsert for performance
- Comprehensive metrics tracking
"""

from __future__ import annotations

import asyncio
import functools
import logging
from collections import Counter
from typing import Any

from sqlalchemy import text

from .base import BaseStep, StepContext, StepResult
from ..models import Step3Handover, Step3Metrics

# Import classification pipeline
from classification.pipeline import ClassificationPipeline
from classification.core.context import ClassificationContext
from classification.core.output import ClassificationResult

logger = logging.getLogger(__name__)

# Batch size for processing fingerprints
CLASSIFICATION_BATCH_SIZE = 1000


class Step3Classification(BaseStep):
    """Step 3: Classification.

    Runs the classification pipeline on each fingerprint and populates
    series_classification_cache table.
    
    Processing flow:
    1. Get handover from Step 2 (fingerprint_ids, series_stack_ids)
    2. Load fingerprints in batches from stack_fingerprint table
    3. Run ClassificationPipeline on each fingerprint
    4. Bulk upsert results to series_classification_cache
    5. Compute metrics and build handover for next step
    """

    step_id = "classification"
    step_title = "Classification"

    def __init__(self, progress_callback=None):
        super().__init__(progress_callback)
        # The pipeline is instantiated per-run inside execute() so we can
        # apply this cohort's keyword overrides. See
        # classification.overrides.merge_overrides.
        self._pipeline: ClassificationPipeline | None = None

    def _resolve_app_cohort_id(self, cohort_name: str) -> int | None:
        """Resolve the **application DB** cohort_id by cohort name.

        Cached on the step instance so the same lookup can be reused by
        the pipeline builder and the body-part QC protection pre-flight.
        Returns ``None`` if the cohort is not found or any error occurs.
        """
        if hasattr(self, "_cached_app_cohort_id"):
            return self._cached_app_cohort_id  # type: ignore[attr-defined]
        try:
            from cohorts.repository import get_cohort_by_name
            from db.session import session_scope

            with session_scope() as session:
                app_cohort = get_cohort_by_name(session, cohort_name)
                app_cohort_id = app_cohort.id if app_cohort else None
        except Exception as exc:
            logger.warning(
                "Failed to resolve application cohort_id for %r: %s",
                cohort_name, exc,
            )
            app_cohort_id = None
        self._cached_app_cohort_id = app_cohort_id  # type: ignore[attr-defined]
        return app_cohort_id

    def _build_pipeline(self, cohort_name: str) -> ClassificationPipeline:
        """Build a ClassificationPipeline with this cohort's keyword overrides merged.

        Resolves the cohort by name against the application DB, because
        ``StepContext.cohort_id`` is the **metadata DB** cohort_id but
        keyword overrides are keyed on the **application DB** cohort.id.
        The two DBs use independent auto-increment sequences; the cohort
        name is the only stable bridging key (unique in both tables).
        See ``metadata_db/resolve.py`` for the broader pattern.

        Defers to defaults when the cohort has no override rows, is not
        found in the application DB, or any error occurs during loading.
        """
        try:
            from classification.overrides import merge_overrides
            from cohorts.keyword_override_service import (
                load_override_map,
            )

            app_cohort_id = self._resolve_app_cohort_id(cohort_name)

            if app_cohort_id is None:
                logger.warning(
                    "Cohort %r not found in application DB; "
                    "classification will use global defaults",
                    cohort_name,
                )
                return ClassificationPipeline()

            overrides = load_override_map(app_cohort_id)
            if overrides:
                merged = merge_overrides(None, overrides)
                self.log(
                    f"Classification: applied {len(overrides)} keyword "
                    f"override bucket(s) for cohort {cohort_name!r}"
                )
                return ClassificationPipeline(merged_configs=merged)
        except Exception as exc:
            # Never fail classification because overrides couldn't be loaded.
            logger.warning(
                "Failed to load keyword overrides for cohort %r: %s; "
                "falling back to global defaults",
                cohort_name, exc,
            )
        return ClassificationPipeline()

    async def execute(self, context: StepContext) -> StepResult:
        """Execute Step 3: Classification."""
        metrics = Step3Metrics()
        conn = context.conn
        self._pipeline = self._build_pipeline(context.cohort_name)

        try:
            # ═══════════════════════════════════════════════════════════
            # PHASE 1: GET HANDOVER FROM STEP 2
            # ═══════════════════════════════════════════════════════════
            step2_handover = context.previous_handover
            if not step2_handover:
                error = "No handover from Step 2"
                await self.emit_error(error, metrics.to_dict())
                return StepResult(success=False, error=error, metrics=metrics.to_dict())

            fingerprint_ids = step2_handover.fingerprint_ids
            series_stack_ids = step2_handover.series_stack_ids
            
            self.log(f"Received handover with {len(fingerprint_ids):,} fingerprints from Step 2")
            await self.emit_progress(
                1, "Processing handover from Step 2...",
                current_action="Loading fingerprint data"
            )

            if not fingerprint_ids:
                metrics.warnings.append("No fingerprints to classify from Step 2")
                self.log("WARNING: No fingerprints to classify")

                handover = Step3Handover(
                    classified_stack_ids=[],
                    stacks_requiring_review=[],
                    cohort_id=step2_handover.cohort_id,
                    cohort_name=step2_handover.cohort_name,
                    processing_mode=step2_handover.processing_mode,
                    total_classified=0,
                    review_required_count=0,
                    metrics=metrics,
                )

                await self.emit_warning(metrics.to_dict())
                return StepResult(success=True, handover=handover, metrics=metrics.to_dict())

            # ═══════════════════════════════════════════════════════════
            # PHASE 2: LOAD FINGERPRINTS
            # ═══════════════════════════════════════════════════════════
            self.log("Loading fingerprints from database...")
            await self.emit_progress(
                5, "Loading fingerprints...",
                current_action="Querying stack_fingerprint table"
            )

            # Load all fingerprints (we need full data for classification)
            fingerprints = self._load_fingerprints(conn, fingerprint_ids)
            self.log(f"Loaded {len(fingerprints):,} fingerprints")

            if not fingerprints:
                error = "No fingerprints found in database"
                await self.emit_error(error, metrics.to_dict())
                return StepResult(success=False, error=error, metrics=metrics.to_dict())

            # Session-aware rescue: if a whole session (subject_id + study_date)
            # has zero ORIGINAL+PRIMARY stacks, treat its ORIGINAL+SECONDARY
            # stacks as primary so they don't all get excluded by Stage-0.
            self._mark_session_rescue(fingerprints)

            # ═══════════════════════════════════════════════════════════
            # PHASE 3: CLASSIFY IN BATCHES
            # ═══════════════════════════════════════════════════════════
            self.log("Starting classification...")
            await self.emit_progress(
                10, "Classifying stacks...",
                current_action="Running classification pipeline"
            )

            total_fingerprints = len(fingerprints)
            classification_results: list[tuple[dict, ClassificationResult]] = []
            
            # Process in batches for memory efficiency and progress updates
            batch_count = (total_fingerprints + CLASSIFICATION_BATCH_SIZE - 1) // CLASSIFICATION_BATCH_SIZE
            
            loop = asyncio.get_running_loop()

            for batch_idx in range(batch_count):
                start_idx = batch_idx * CLASSIFICATION_BATCH_SIZE
                end_idx = min(start_idx + CLASSIFICATION_BATCH_SIZE, total_fingerprints)
                batch = fingerprints[start_idx:end_idx]

                # Run classification in executor (blocking operation)
                batch_results = await loop.run_in_executor(
                    None,
                    functools.partial(self._classify_batch, batch)
                )
                classification_results.extend(batch_results)

                # Calculate progress (10-80%)
                progress = 10 + int((end_idx / total_fingerprints) * 70)
                await self.emit_progress(
                    progress,
                    f"Classified {end_idx:,}/{total_fingerprints:,} stacks...",
                    current_action=f"Batch {batch_idx + 1}/{batch_count}"
                )
                self.log(f"Batch {batch_idx + 1}/{batch_count}: classified {len(batch)} stacks")

            self.log(f"Classification complete: {len(classification_results):,} results")

            # ═══════════════════════════════════════════════════════════
            # PHASE 4: BULK UPSERT TO DATABASE
            # ═══════════════════════════════════════════════════════════
            self.log("Inserting classification results...")
            await self.emit_progress(
                82, "Inserting results to database...",
                current_action="Bulk upsert to series_classification_cache"
            )

            # Prepare values for upsert (CPU-bound, can run in executor)
            values = self._prepare_upsert_values(
                classification_results,
                step2_handover.cohort_name
            )

            # Body Part QC protection: do not let the keyword detector
            # overwrite ``body_part`` for stacks that the cohort's QC
            # module has already labeled. We swap the keyword-derived
            # value with whatever's currently in the cache so the
            # ON CONFLICT UPDATE is a no-op for the column.
            self._apply_body_part_qc_protection(
                conn, values, step2_handover.cohort_name,
            )

            # Insert in batches, yielding control between batches
            # This keeps the event loop responsive for health checks and progress updates
            rows_inserted = await self._async_batch_upsert(conn, values)

            self.log(f"Inserted {rows_inserted:,} classification records")

            # ═══════════════════════════════════════════════════════════
            # PHASE 5: COMPUTE METRICS
            # ═══════════════════════════════════════════════════════════
            self.log("Computing metrics...")
            await self.emit_progress(
                92, "Computing metrics...",
                current_action="Analyzing classification results"
            )

            # Compute metrics from results
            self._compute_metrics(classification_results, metrics)

            # Identify stacks requiring review
            stacks_requiring_review = [
                fp["series_stack_id"]
                for fp, result in classification_results
                if result.manual_review_required == 1
            ]

            # ═══════════════════════════════════════════════════════════
            # PHASE 6: BUILD HANDOVER
            # ═══════════════════════════════════════════════════════════
            self.log("Building handover for next step...")
            await self.emit_progress(
                95, "Building handover...",
                current_action="Preparing for next step"
            )

            classified_stack_ids = [fp["series_stack_id"] for fp, _ in classification_results]

            handover = Step3Handover(
                classified_stack_ids=classified_stack_ids,
                stacks_requiring_review=stacks_requiring_review,
                cohort_id=step2_handover.cohort_id,
                cohort_name=step2_handover.cohort_name,
                processing_mode=step2_handover.processing_mode,
                total_classified=len(classified_stack_ids),
                review_required_count=len(stacks_requiring_review),
                metrics=metrics,
            )

            await self.emit_complete(metrics.to_dict())

            self.log(
                f"Step 3 complete: {len(classified_stack_ids):,} classified, "
                f"{len(stacks_requiring_review):,} requiring review"
            )
            logger.info(
                "Step 3 complete: %d classified, %d requiring review",
                len(classified_stack_ids), len(stacks_requiring_review)
            )

            return StepResult(success=True, handover=handover, metrics=metrics.to_dict())

        except Exception as e:
            logger.exception("Step 3 failed")
            self.log(f"ERROR: {str(e)}")
            metrics.errors.append(str(e))
            await self.emit_error(str(e), metrics.to_dict())
            return StepResult(success=False, error=str(e), metrics=metrics.to_dict())

    def _load_fingerprints(self, conn, fingerprint_ids: list[int]) -> list[dict]:
        """Load fingerprints from database.

        Args:
            conn: Database connection
            fingerprint_ids: List of fingerprint IDs to load

        Returns:
            List of fingerprint dicts
        """
        if not fingerprint_ids:
            return []

        result = conn.execute(text("""
            SELECT
                fp.fingerprint_id,
                fp.series_stack_id,
                fp.modality,
                fp.manufacturer,
                fp.manufacturer_model,
                fp.stack_sequence_name,
                fp.text_search_blob,
                fp.contrast_search_blob,
                fp.stack_orientation,
                fp.fov_x,
                fp.fov_y,
                fp.aspect_ratio,
                fp.image_type,
                fp.scanning_sequence,
                fp.sequence_variant,
                fp.scan_options,
                fp.mr_te,
                fp.mr_tr,
                fp.mr_ti,
                fp.mr_flip_angle,
                fp.mr_echo_train_length,
                fp.mr_echo_number,
                fp.mr_acquisition_type,
                fp.mr_diffusion_b_value,
                fp.stack_n_instances,
                -- Get series info for cache table
                ss.series_id,
                s.series_instance_uid,
                -- Get subject and study for cohort lookup
                s.subject_id,
                s.study_id,
                -- Study date is used for session-aware rescue rules
                -- (a single session can span multiple study_ids when
                -- brain + spine are acquired together, but they share
                -- the same date).
                st.study_date,
                -- Geometry: through-plane resolution (prefer SpacingBetweenSlices,
                -- fall back to SliceThickness when the former is absent).
                COALESCE(s.spacing_between_slices, s.slice_thickness) AS slice_thickness_mm,
                -- One representative instance per stack to derive in-plane
                -- resolution and matrix size. ~99.6% of stacks are uniform on
                -- these values; mosaic / variable-rows edge cases get the
                -- first non-null row.
                inst.pixel_spacing AS instance_pixel_spacing,
                inst.rows AS instance_rows,
                inst.columns AS instance_columns
            FROM stack_fingerprint fp
            JOIN series_stack ss ON fp.series_stack_id = ss.series_stack_id
            JOIN series s ON ss.series_id = s.series_id
            JOIN study st ON s.study_id = st.study_id
            LEFT JOIN LATERAL (
                SELECT pixel_spacing, rows, columns
                FROM instance i
                WHERE i.series_stack_id = fp.series_stack_id
                  AND i.pixel_spacing IS NOT NULL
                LIMIT 1
            ) inst ON true
            WHERE fp.fingerprint_id = ANY(:fingerprint_ids)
        """), {"fingerprint_ids": fingerprint_ids})

        return [dict(row._mapping) for row in result]

    def _mark_session_rescue(self, fingerprints: list[dict]) -> None:
        """Session-aware rescue for ORIGINAL\\SECONDARY-only sessions.

        Some scanners/exports (e.g. certain Philips workflows) tag every
        reconstructed image as ``ORIGINAL\\SECONDARY`` without ever marking
        anything as ``PRIMARY``. The default Stage-0 rule excludes
        ``SECONDARY && !PRIMARY``, so those sessions end up 100% excluded
        and the subject becomes unusable.

        Rescue rule (applied in-place, sets ``treat_secondary_as_primary``
        on individual fingerprint dicts):

        Group fingerprints by ``(subject_id, study_date)``. Date is used
        instead of ``study_id`` so brain + spine acquired on the same day
        but split across multiple ``study_id`` rows are treated as one
        session.

        For each group:
          * If ANY fingerprint already has ImageType containing BOTH
            ``ORIGINAL`` AND ``PRIMARY``, do nothing (normal session).
          * Otherwise, for every fingerprint whose ImageType matches:
              - contains ``ORIGINAL``
              - contains ``SECONDARY``
              - does NOT contain ``PRIMARY``
              - does NOT contain ``DERIVED``
              - does NOT contain ``SCREENSHOT`` / ``PASTED`` / ``ERROR``
            set ``fp["treat_secondary_as_primary"] = True``.

        Downstream classifiers (ProjectionDerived for MPR/PROJECTION,
        DTIRecon for ADC/EADC, etc.) still route those rescued stacks
        correctly because the rescue only bypasses the Stage-0
        SECONDARY-without-PRIMARY exclusion; it does not change the
        per-stack classification pipeline.
        """
        if not fingerprints:
            return

        # Group by (subject_id, study_date).
        groups: dict[tuple, list[dict]] = {}
        for fp in fingerprints:
            key = (fp.get("subject_id"), fp.get("study_date"))
            groups.setdefault(key, []).append(fp)

        sessions_rescued = 0
        stacks_marked = 0
        for key, group in groups.items():
            # Need a real (subject_id, date) — skip incomplete keys.
            if key[0] is None or key[1] is None:
                continue

            has_any_original_primary = False
            for fp in group:
                img = (fp.get("image_type") or "").upper()
                if "ORIGINAL" in img and "PRIMARY" in img:
                    has_any_original_primary = True
                    break
            if has_any_original_primary:
                continue

            # No primary anywhere in the session — rescue eligible stacks.
            marked_in_session = 0
            for fp in group:
                img = (fp.get("image_type") or "").upper()
                if "ORIGINAL" not in img:
                    continue
                if "SECONDARY" not in img:
                    continue
                if "PRIMARY" in img:
                    continue
                if "DERIVED" in img:
                    continue
                if "SCREENSHOT" in img or "PASTED" in img or "ERROR" in img:
                    continue
                fp["treat_secondary_as_primary"] = True
                marked_in_session += 1

            if marked_in_session:
                sessions_rescued += 1
                stacks_marked += marked_in_session

        if stacks_marked:
            self.log(
                f"Session-aware rescue: marked {stacks_marked:,} stacks "
                f"across {sessions_rescued:,} sessions as "
                f"treat_secondary_as_primary"
            )

    def _classify_batch(
        self,
        fingerprints: list[dict]
    ) -> list[tuple[dict, ClassificationResult]]:
        """Classify a batch of fingerprints.

        Args:
            fingerprints: List of fingerprint dicts

        Returns:
            List of (fingerprint, ClassificationResult) tuples
        """
        results = []
        for fp in fingerprints:
            try:
                # Create context from fingerprint
                ctx = ClassificationContext.from_fingerprint(fp)
                
                # Run classification
                result = self._pipeline.classify(ctx)
                results.append((fp, result))
            except Exception as e:
                # Log error but continue with other fingerprints
                logger.warning(
                    "Failed to classify fingerprint %s: %s",
                    fp.get("fingerprint_id"), str(e)
                )
                # Create a minimal error result
                result = ClassificationResult(
                    directory_type="misc",
                    manual_review_required=1,
                )
                result.add_review_reason("classification:error")
                results.append((fp, result))

        return results

    # Batch size for bulk inserts to avoid PostgreSQL OOM
    UPSERT_BATCH_SIZE = 10000

    # SQL for upserting classification results
    UPSERT_SQL = text("""
        INSERT INTO series_classification_cache (
            series_stack_id, series_id, series_instance_uid,
            subject_id, study_id, dicom_origin_cohort,
            directory_type, base, technique, modifier_csv, construct_csv,
            provenance, acceleration_csv, post_contrast, localizer,
            spinal_cord, body_part, manual_review_required, manual_review_reasons_csv,
            fov_x_mm, fov_y_mm, slices_count, orientation_patient, echo_number,
            pixsp_row_mm, pixsp_col_mm, rows, columns, slice_thickness_mm
        ) VALUES (
            :series_stack_id, :series_id, :series_instance_uid,
            :subject_id, :study_id, :dicom_origin_cohort,
            :directory_type, :base, :technique, :modifier_csv, :construct_csv,
            :provenance, :acceleration_csv, :post_contrast, :localizer,
            :spinal_cord, :body_part, :manual_review_required, :manual_review_reasons_csv,
            :fov_x_mm, :fov_y_mm, :slices_count, :orientation_patient, :echo_number,
            :pixsp_row_mm, :pixsp_col_mm, :rows, :columns, :slice_thickness_mm
        )
        ON CONFLICT (series_stack_id) DO UPDATE SET
            series_id = EXCLUDED.series_id,
            series_instance_uid = EXCLUDED.series_instance_uid,
            subject_id = EXCLUDED.subject_id,
            study_id = EXCLUDED.study_id,
            -- Sticky to the cohort that first ingested the stack: never repaint
            -- the origin when re-sorting a subject that already lives under
            -- another cohort. (See cross-cohort safeguard.)
            dicom_origin_cohort = COALESCE(series_classification_cache.dicom_origin_cohort, EXCLUDED.dicom_origin_cohort),
            directory_type = EXCLUDED.directory_type,
            base = EXCLUDED.base,
            technique = EXCLUDED.technique,
            modifier_csv = EXCLUDED.modifier_csv,
            construct_csv = EXCLUDED.construct_csv,
            provenance = EXCLUDED.provenance,
            acceleration_csv = EXCLUDED.acceleration_csv,
            post_contrast = EXCLUDED.post_contrast,
            localizer = EXCLUDED.localizer,
            spinal_cord = EXCLUDED.spinal_cord,
            body_part = EXCLUDED.body_part,
            manual_review_required = EXCLUDED.manual_review_required,
            manual_review_reasons_csv = EXCLUDED.manual_review_reasons_csv,
            fov_x_mm = EXCLUDED.fov_x_mm,
            fov_y_mm = EXCLUDED.fov_y_mm,
            slices_count = EXCLUDED.slices_count,
            orientation_patient = EXCLUDED.orientation_patient,
            echo_number = EXCLUDED.echo_number,
            pixsp_row_mm = EXCLUDED.pixsp_row_mm,
            pixsp_col_mm = EXCLUDED.pixsp_col_mm,
            rows = EXCLUDED.rows,
            columns = EXCLUDED.columns,
            slice_thickness_mm = EXCLUDED.slice_thickness_mm
    """)

    def _prepare_upsert_values(
        self,
        results: list[tuple[dict, ClassificationResult]],
        cohort_name: str = ""
    ) -> list[dict]:
        """Prepare values for bulk upsert (CPU-bound, no DB access).

        Args:
            results: List of (fingerprint, ClassificationResult) tuples
            cohort_name: Name of the cohort being processed

        Returns:
            List of value dictionaries ready for SQL execution
        """
        values = []
        for fp, result in results:
            values.append({
                "series_stack_id": fp["series_stack_id"],
                "series_id": fp["series_id"],
                "series_instance_uid": fp["series_instance_uid"],
                "subject_id": fp.get("subject_id"),
                "study_id": fp.get("study_id"),
                "dicom_origin_cohort": cohort_name or None,
                "directory_type": result.directory_type,
                "base": result.base,
                "technique": result.technique,
                "modifier_csv": result.modifier_csv,
                "construct_csv": result.construct_csv,
                "provenance": result.provenance,
                "acceleration_csv": result.acceleration_csv,
                "post_contrast": result.post_contrast,
                "localizer": result.localizer,
                "spinal_cord": result.spinal_cord,
                "body_part": result.body_part,
                "manual_review_required": result.manual_review_required,
                "manual_review_reasons_csv": result.manual_review_reasons_csv,
                # Geometry from fingerprint
                "fov_x_mm": fp.get("fov_x"),
                "fov_y_mm": fp.get("fov_y"),
                "slices_count": fp.get("stack_n_instances"),
                "orientation_patient": fp.get("stack_orientation"),
                "echo_number": self._parse_echo_number(fp.get("mr_echo_number")),
                # Geometry derived from a representative instance + series.
                # pixel_spacing is the DICOM "row\col" string; rows/columns are
                # ints; slice_thickness_mm prefers SpacingBetweenSlices.
                **self._derive_resolution(
                    fp.get("instance_pixel_spacing"),
                    fp.get("instance_rows"),
                    fp.get("instance_columns"),
                    fp.get("slice_thickness_mm"),
                ),
            })
        return values

    @staticmethod
    def _derive_resolution(
        pixel_spacing: str | None,
        rows: int | None,
        columns: int | None,
        slice_thickness_mm: float | None,
    ) -> dict:
        """Parse DICOM pixel_spacing ("row\\col") and pass through matrix size
        and through-plane resolution. Returns the four cache columns. Any
        component that fails to parse becomes NULL — the rest still land. """
        row_sp: float | None = None
        col_sp: float | None = None
        if pixel_spacing:
            parts = pixel_spacing.split("\\")
            if len(parts) >= 2:
                try:
                    row_sp = float(parts[0])
                except (TypeError, ValueError):
                    row_sp = None
                try:
                    col_sp = float(parts[1])
                except (TypeError, ValueError):
                    col_sp = None
        return {
            "pixsp_row_mm": row_sp,
            "pixsp_col_mm": col_sp,
            "rows": int(rows) if rows is not None else None,
            "columns": int(columns) if columns is not None else None,
            "slice_thickness_mm": (
                float(slice_thickness_mm) if slice_thickness_mm is not None else None
            ),
        }

    def _apply_body_part_qc_protection(
        self, conn, values: list[dict], cohort_name: str,
    ) -> None:
        """Preserve QC-decided ``body_part`` values across step3 reclassifications.

        For every value row whose ``series_stack_id`` is in the cohort's
        Body Part QC protected set, replace the keyword-detector-derived
        ``body_part`` with whatever's currently in
        ``series_classification_cache``. The upsert then becomes a
        no-op for that column even though every other column still
        gets updated.

        Defensive: any failure here is logged and the upsert proceeds
        unchanged. This step must never block reclassification.
        """
        if not values:
            return
        try:
            from qc.body_part.protection import (
                get_qc_protected_body_part_labels,
            )

            app_cohort_id = self._resolve_app_cohort_id(cohort_name)
            if app_cohort_id is None:
                return
            protected_labels = get_qc_protected_body_part_labels(app_cohort_id)
            if not protected_labels:
                return
            protected = set(protected_labels)

            affected = [
                v for v in values
                if v["series_stack_id"] in protected
            ]
            if not affected:
                return

            n_overridden = 0
            for v in affected:
                sid = v["series_stack_id"]
                bp = protected_labels.get(sid)
                sc = self._spinal_cord_from_body_part(bp)
                if v["body_part"] != bp or v.get("spinal_cord") != sc:
                    v["body_part"] = bp
                    v["spinal_cord"] = sc
                    n_overridden += 1

            if n_overridden:
                self.log(
                    f"Body Part QC protection: preserved existing "
                    f"body_part for {n_overridden} stack(s) "
                    f"(out of {len(protected)} protected)"
                )
        except Exception:
            logger.exception(
                "_apply_body_part_qc_protection failed for cohort %r; "
                "step3 will proceed without QC protection",
                cohort_name,
            )

    @staticmethod
    def _spinal_cord_from_body_part(bp: str | None) -> int | None:
        if not bp:
            return None
        low = bp.lower()
        if low in ("spine", "neck"):
            return 1
        if low in ("brain", "brain-neck"):
            return 0
        return None

    async def _async_batch_upsert(self, conn, values: list[dict]) -> int:
        """Insert values in batches, yielding control between batches.

        This approach:
        1. Runs DB operations synchronously (required - connections aren't thread-safe)
        2. Yields control between batches via asyncio.sleep(0)
        3. Keeps event loop responsive for health checks and progress updates

        Args:
            conn: Database connection
            values: List of value dictionaries to insert

        Returns:
            Number of rows inserted/updated
        """
        if not values:
            return 0

        total_rows = len(values)
        batch_count = (total_rows + self.UPSERT_BATCH_SIZE - 1) // self.UPSERT_BATCH_SIZE

        for batch_idx in range(batch_count):
            start = batch_idx * self.UPSERT_BATCH_SIZE
            end = min(start + self.UPSERT_BATCH_SIZE, total_rows)
            batch = values[start:end]

            # Execute batch synchronously (connection not thread-safe)
            conn.execute(self.UPSERT_SQL, batch)

            # Yield control to event loop between batches
            # This allows health checks and other tasks to run
            if batch_idx < batch_count - 1:
                await asyncio.sleep(0)

        return total_rows

    def _parse_echo_number(self, echo_number_str: str | None) -> int | None:
        """Parse echo number from string (could be comma-separated)."""
        if not echo_number_str:
            return None
        try:
            # Take first value if comma-separated
            first_val = echo_number_str.split(",")[0].strip()
            return int(float(first_val))
        except (ValueError, IndexError):
            return None

    def _compute_metrics(
        self,
        results: list[tuple[dict, ClassificationResult]],
        metrics: Step3Metrics
    ) -> None:
        """Compute classification metrics from results.

        Args:
            results: List of (fingerprint, ClassificationResult) tuples
            metrics: Step3Metrics to populate
        """
        # Count totals
        metrics.total_classified = len(results)

        # Use Counter for efficient counting
        directory_types = Counter()
        provenances = Counter()
        bases = Counter()
        techniques = Counter()
        review_reasons = Counter()
        low_confidence_axes = Counter()

        for fp, result in results:
            # Directory type
            directory_types[result.directory_type or "misc"] += 1

            # Check for excluded
            if result.directory_type == "excluded":
                metrics.excluded_count += 1

            # Provenance
            if result.provenance:
                provenances[result.provenance] += 1

            # Base
            if result.base:
                bases[result.base] += 1

            # Technique
            if result.technique:
                techniques[result.technique] += 1

            # Review required
            if result.manual_review_required == 1:
                metrics.review_required_count += 1
                
                # Parse review reasons
                for reason in result.get_review_reasons():
                    review_reasons[reason] += 1
                    
                    # Track low confidence by axis
                    if ":low_confidence" in reason:
                        axis = reason.split(":")[0]
                        low_confidence_axes[axis] += 1

            # Special flags
            if result.spinal_cord == 1:
                metrics.spine_detected_count += 1
            if result.post_contrast == 1:
                metrics.post_contrast_count += 1
            if result.localizer == 1:
                metrics.localizer_count += 1

        # Store counts in metrics
        metrics.breakdown_by_directory_type = dict(directory_types)
        metrics.breakdown_by_provenance = dict(provenances)
        metrics.breakdown_by_base = dict(bases)
        metrics.breakdown_by_technique = dict(techniques)
        metrics.review_reasons = dict(review_reasons)
        metrics.low_confidence_axes = dict(low_confidence_axes)
