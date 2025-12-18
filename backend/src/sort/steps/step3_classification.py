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
        # Initialize the classification pipeline once
        self._pipeline = ClassificationPipeline()

    async def execute(self, context: StepContext) -> StepResult:
        """Execute Step 3: Classification."""
        metrics = Step3Metrics()
        conn = context.conn

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
                s.study_id
            FROM stack_fingerprint fp
            JOIN series_stack ss ON fp.series_stack_id = ss.series_stack_id
            JOIN series s ON ss.series_id = s.series_id
            WHERE fp.fingerprint_id = ANY(:fingerprint_ids)
        """), {"fingerprint_ids": fingerprint_ids})

        return [dict(row._mapping) for row in result]

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
            spinal_cord, manual_review_required, manual_review_reasons_csv,
            fov_x_mm, fov_y_mm, slices_count, orientation_patient, echo_number
        ) VALUES (
            :series_stack_id, :series_id, :series_instance_uid,
            :subject_id, :study_id, :dicom_origin_cohort,
            :directory_type, :base, :technique, :modifier_csv, :construct_csv,
            :provenance, :acceleration_csv, :post_contrast, :localizer,
            :spinal_cord, :manual_review_required, :manual_review_reasons_csv,
            :fov_x_mm, :fov_y_mm, :slices_count, :orientation_patient, :echo_number
        )
        ON CONFLICT (series_stack_id) DO UPDATE SET
            series_id = EXCLUDED.series_id,
            series_instance_uid = EXCLUDED.series_instance_uid,
            subject_id = EXCLUDED.subject_id,
            study_id = EXCLUDED.study_id,
            dicom_origin_cohort = EXCLUDED.dicom_origin_cohort,
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
            manual_review_required = EXCLUDED.manual_review_required,
            manual_review_reasons_csv = EXCLUDED.manual_review_reasons_csv,
            fov_x_mm = EXCLUDED.fov_x_mm,
            fov_y_mm = EXCLUDED.fov_y_mm,
            slices_count = EXCLUDED.slices_count,
            orientation_patient = EXCLUDED.orientation_patient,
            echo_number = EXCLUDED.echo_number
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
                "manual_review_required": result.manual_review_required,
                "manual_review_reasons_csv": result.manual_review_reasons_csv,
                # Geometry from fingerprint
                "fov_x_mm": fp.get("fov_x"),
                "fov_y_mm": fp.get("fov_y"),
                "slices_count": fp.get("stack_n_instances"),
                "orientation_patient": fp.get("stack_orientation"),
                "echo_number": self._parse_echo_number(fp.get("mr_echo_number")),
            })
        return values

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
