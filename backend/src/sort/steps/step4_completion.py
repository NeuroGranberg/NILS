"""
Step 4: Completion & Gap Filling

This step ensures every stack has complete classification data by:
0. Normalizing MR field strength to standard values (1, 1.5, 3, 7 T)
1. Flagging low-confidence orientations for review
2. Filling missing mr_acquisition_type (2D/3D) from text/flags
3. Filling missing base and technique via physics-based similarity
4. Re-synthesizing directory_type for misc stacks after filling

Key features:
- Cohort-agnostic reference database (uses entire metadata DB)
- Physics-based binning for efficient similarity lookup
- All filled values flagged for manual review
"""

from __future__ import annotations

import asyncio
import functools
import logging
from collections import Counter
from typing import Any

from sqlalchemy import text

from .base import BaseStep, StepContext, StepResult
from ..models import Step4Handover, Step4Metrics
from ..gap_filling import (
    ORIENTATION_CONFIDENCE_THRESHOLD,
    ReferenceDatabase,
    build_reference_database,
    compute_physics_key,
    find_best_match,
    infer_acquisition_type,
    synthesize_directory_type,
    add_review_reason,
    remove_review_reason,
)

# Import for SWI branch re-routing in Phase 3B
from classification.core.context import ClassificationContext
from classification.branches.swi import apply_swi_logic

logger = logging.getLogger(__name__)

# Batch size for processing stacks
COMPLETION_BATCH_SIZE = 1000

# Standard MR field strengths in Tesla
STANDARD_FIELD_STRENGTHS = [0.5, 1.0, 1.5, 3.0, 7.0]


def normalize_field_strength(value: float | None) -> float | None:
    """Normalize MR field strength to standard values.

    Standard field strengths: 0.5, 1.0, 1.5, 3.0, 7.0 T

    Handles various input formats:
    - Already normalized (1.5, 3.0, etc.) → keep as-is
    - Slight variations (1.493806, 2.89362) → round to nearest standard
    - Gauss scale (15000, 10000, 5000) → convert (1T = 10000 Gauss)
    - Low-field variations (~0.95) → round to 1.0 T

    Args:
        value: Raw field strength value

    Returns:
        Normalized field strength or None if input is None
    """
    if value is None:
        return None

    # Handle Gauss scale (values > 100 are assumed to be in Gauss)
    # 1 Tesla = 10,000 Gauss
    if value > 100:
        value = value / 10000.0

    # Find the nearest standard field strength
    # Use tolerance-based matching for more accurate assignment
    tolerances = {
        0.5: 0.15,   # 0.35 - 0.65 → 0.5 T
        1.0: 0.15,   # 0.85 - 1.15 → 1.0 T
        1.5: 0.15,   # 1.35 - 1.65 → 1.5 T
        3.0: 0.3,    # 2.7 - 3.3 → 3.0 T
        7.0: 0.5,    # 6.5 - 7.5 → 7.0 T
    }

    for standard in STANDARD_FIELD_STRENGTHS:
        tolerance = tolerances.get(standard, 0.15)
        if abs(value - standard) <= tolerance:
            return standard

    # If no match found within tolerance, return nearest
    # This handles edge cases like very low field (0.2 T) or unusual values
    nearest = min(STANDARD_FIELD_STRENGTHS, key=lambda x: abs(x - value))
    return nearest


class Step4Completion(BaseStep):
    """Step 4: Completion & Gap Filling.

    Ensures every stack has complete classification by filling gaps
    and flagging low-confidence results for review.

    Processing flow:
    0. Phase 0: Normalize MR field strength to standard values
    1. Phase 1: Check orientation confidence, flag low values
    2. Phase 2: Fill mr_acquisition_type from flags/text/technique
    3. Phase 3: Fill base & technique via physics similarity
    4. Phase 4: Re-synthesize directory_type for misc stacks
    5. Phase 5: Persist updates and build handover
    """

    step_id = "completion"
    step_title = "Completion"

    def __init__(self, progress_callback=None):
        super().__init__(progress_callback)
        self._reference_db: ReferenceDatabase | None = None

    async def execute(self, context: StepContext) -> StepResult:
        """Execute Step 4: Completion & Gap Filling."""
        metrics = Step4Metrics()
        conn = context.conn

        try:
            # ═══════════════════════════════════════════════════════════
            # SETUP: GET HANDOVER FROM STEP 3
            # ═══════════════════════════════════════════════════════════
            step3_handover = context.previous_handover
            if not step3_handover:
                error = "No handover from Step 3"
                await self.emit_error(error, metrics.to_dict())
                return StepResult(success=False, error=error, metrics=metrics.to_dict())

            classified_stack_ids = step3_handover.classified_stack_ids
            existing_review_stacks = set(step3_handover.stacks_requiring_review)
            
            self.log(f"Received {len(classified_stack_ids):,} stacks from Step 3")
            logger.info("Step 4: Received %d stacks from Step 3", len(classified_stack_ids))
            await self.emit_progress(
                1, "Processing handover from Step 3...",
                current_action="Loading stack data"
            )

            if not classified_stack_ids:
                metrics.warnings.append("No stacks to process from Step 3")
                self.log("WARNING: No stacks to process")

                handover = Step4Handover(
                    completed_stack_ids=[],
                    stacks_requiring_review=list(existing_review_stacks),
                    cohort_id=step3_handover.cohort_id,
                    cohort_name=step3_handover.cohort_name,
                    processing_mode=step3_handover.processing_mode,
                    total_completed=0,
                    gaps_filled=0,
                    misc_resolved=0,
                    review_required_count=len(existing_review_stacks),
                    metrics=metrics,
                )

                await self.emit_complete(metrics.to_dict())
                return StepResult(success=True, handover=handover, metrics=metrics.to_dict())

            # Load stack data for processing
            logger.info("Step 4: Loading stack data for %d stacks...", len(classified_stack_ids))
            stacks = self._load_stacks_for_completion(conn, classified_stack_ids)
            metrics.total_processed = len(stacks)
            self.log(f"Loaded {len(stacks):,} stacks for completion")
            logger.info("Step 4: Loaded %d stacks for completion", len(stacks))

            # Track updates and new review flags
            updates: list[dict[str, Any]] = []
            new_review_stacks: set[int] = set()

            loop = asyncio.get_running_loop()

            # ═══════════════════════════════════════════════════════════
            # PHASE 0: NORMALIZE MR FIELD STRENGTH (1-5%)
            # ═══════════════════════════════════════════════════════════
            self.log("Phase 0: Normalizing MR field strength...")
            await self.emit_progress(
                1, "Normalizing MR field strength...",
                current_action="Phase 0: Field strength"
            )

            field_strength_updates = await loop.run_in_executor(
                None,
                functools.partial(self._normalize_field_strength, conn)
            )
            metrics.field_strength_normalized_count = field_strength_updates
            self.log(f"Phase 0 complete: {field_strength_updates} series normalized")

            await self.emit_progress(
                4, f"Normalized {field_strength_updates} field strength values",
                current_action="Phase 0 complete"
            )

            # ═══════════════════════════════════════════════════════════
            # PHASE 1: ORIENTATION CONFIDENCE CHECK (5-10%)
            # ═══════════════════════════════════════════════════════════
            self.log("Phase 1: Checking orientation confidence...")
            await self.emit_progress(
                5, "Checking orientation confidence...",
                current_action="Phase 1: Orientation"
            )

            orientation_flagged = await loop.run_in_executor(
                None,
                functools.partial(self._check_orientation_confidence, stacks)
            )
            
            for stack_id in orientation_flagged:
                new_review_stacks.add(stack_id)
                metrics.new_review_reasons["orientation:low_confidence"] = \
                    metrics.new_review_reasons.get("orientation:low_confidence", 0) + 1
            
            metrics.orientation_flagged_count = len(orientation_flagged)
            self.log(f"Phase 1 complete: {metrics.orientation_flagged_count} stacks flagged for orientation")

            # ═══════════════════════════════════════════════════════════
            # PHASE 2: FILL MR_ACQUISITION_TYPE (10-25%)
            # ═══════════════════════════════════════════════════════════
            self.log("Phase 2: Filling acquisition type gaps...")
            await self.emit_progress(
                10, "Filling acquisition type gaps...",
                current_action="Phase 2: 2D/3D"
            )

            acq_updates, acq_stats = await loop.run_in_executor(
                None,
                functools.partial(self._fill_acquisition_type, stacks)
            )
            
            for stack_id, new_acq_type, method in acq_updates:
                # Find the stack and update in-memory
                for s in stacks:
                    if s["series_stack_id"] == stack_id:
                        s["mr_acquisition_type"] = new_acq_type
                        break
                
                new_review_stacks.add(stack_id)
                metrics.acquisition_type_by_method[method] = \
                    metrics.acquisition_type_by_method.get(method, 0) + 1
                metrics.new_review_reasons["acquisition_type:inferred"] = \
                    metrics.new_review_reasons.get("acquisition_type:inferred", 0) + 1
            
            metrics.acquisition_type_filled_count = len(acq_updates)
            self.log(f"Phase 2 complete: {metrics.acquisition_type_filled_count} acquisition types filled")

            await self.emit_progress(
                25, f"Filled {metrics.acquisition_type_filled_count} acquisition types",
                current_action="Phase 2 complete"
            )

            # ═══════════════════════════════════════════════════════════
            # PHASE 3: FILL BASE & TECHNIQUE VIA SIMILARITY (25-75%)
            # ═══════════════════════════════════════════════════════════
            self.log("Phase 3: Building reference database...")
            await self.emit_progress(
                28, "Building reference database...",
                current_action="Phase 3: Loading references"
            )

            # Build reference database from entire metadata DB
            logger.info("Step 4: Loading reference database...")
            ref_rows = self._load_reference_stacks(conn)
            logger.info("Step 4: Loaded %d reference rows, building database...", len(ref_rows))
            self._reference_db = build_reference_database(ref_rows)
            self.log(f"Reference database: {self._reference_db.total_count:,} stacks in {self._reference_db.bin_count:,} bins")
            logger.info("Step 4: Reference database built: %d stacks in %d bins", self._reference_db.total_count, self._reference_db.bin_count)

            await self.emit_progress(
                35, f"Reference DB: {self._reference_db.total_count:,} stacks",
                current_action="Phase 3: Similarity matching"
            )

            # Find stacks needing base/technique fill
            # NOTE: Localizers are now included to benefit from physics-based gap filling
            # EXCLUDE SyMRI, SWIRecon, EPIMix: their classification is branch-determined
            # EXCLUDE BOLDRecon: BOLD intentionally has no base (measures hemodynamic signal)
            stacks_needing_fill = [
                s for s in stacks
                if s["modality"] == "MR"
                and s["directory_type"] != "excluded"
                and s.get("provenance") not in ("SyMRI", "SWIRecon", "EPIMix", "BOLDRecon")
                and (s["base"] is None or s["base"] == "" or s["base"] == "Unknown"
                     or s["technique"] is None or s["technique"] == "Unknown")
            ]
            
            self.log(f"Phase 3: {len(stacks_needing_fill)} stacks need base/technique filling")
            logger.info("Step 4: Phase 3 starting - %d stacks need base/technique filling", len(stacks_needing_fill))

            # Process in batches - use larger batch size for efficiency
            batch_size = 5000  # Larger batch for faster processing
            fill_results = []
            total_to_fill = len(stacks_needing_fill)
            
            for i in range(0, total_to_fill, batch_size):
                batch = stacks_needing_fill[i:i + batch_size]
                
                batch_results = await loop.run_in_executor(
                    None,
                    functools.partial(self._fill_base_technique_batch, batch)
                )
                fill_results.extend(batch_results)
                
                processed = min(i + batch_size, total_to_fill)
                progress = 35 + int((processed / max(total_to_fill, 1)) * 35)
                self.log(f"Processed {processed:,}/{total_to_fill:,} stacks")
                logger.info("Step 4: Phase 3 progress - %d/%d stacks processed", processed, total_to_fill)
                await self.emit_progress(
                    progress,
                    f"Processed {processed:,}/{total_to_fill:,} stacks...",
                    current_action="Phase 3: Similarity matching"
                )

            # Apply fill results - use dict for O(1) lookup instead of O(n) search
            logger.info("Step 4: Applying %d fill results to %d stacks...", len(fill_results), len(stacks))
            stacks_by_id = {s["series_stack_id"]: s for s in stacks}
            
            for stack_id, base, technique, method in fill_results:
                # Update in-memory stack using O(1) dict lookup
                s = stacks_by_id.get(stack_id)
                if s:
                    existing_reasons = s.get("manual_review_reasons_csv") or ""

                    if base and (s["base"] is None or s["base"] == ""):
                        s["base"] = base
                        metrics.base_filled_count += 1
                        metrics.new_review_reasons["base:low_confidence"] = \
                            metrics.new_review_reasons.get("base:low_confidence", 0) + 1
                        # Remove :missing flag since we now have a value, add :low_confidence
                        existing_reasons = remove_review_reason(existing_reasons, "base:missing")
                        existing_reasons = add_review_reason(existing_reasons, "base:low_confidence")

                    if technique and (s["technique"] is None or s["technique"] == "Unknown"):
                        s["technique"] = technique
                        metrics.technique_filled_count += 1
                        metrics.new_review_reasons["technique:low_confidence"] = \
                            metrics.new_review_reasons.get("technique:low_confidence", 0) + 1
                        # Remove :missing flag since we now have a value, add :low_confidence
                        existing_reasons = remove_review_reason(existing_reasons, "technique:missing")
                        existing_reasons = add_review_reason(existing_reasons, "technique:low_confidence")

                    s["manual_review_reasons_csv"] = existing_reasons
                    new_review_stacks.add(stack_id)
                
                metrics.similarity_match_counts[method] = \
                    metrics.similarity_match_counts.get(method, 0) + 1
            
            logger.info("Step 4: Fill results applied")

            # Count no-match stacks
            no_match_count = sum(1 for _, _, _, m in fill_results if m in ("no_match", "insufficient_matches"))
            metrics.stacks_with_no_match = no_match_count

            self.log(f"Phase 3 complete: {metrics.base_filled_count} base, {metrics.technique_filled_count} technique filled")

            await self.emit_progress(
                70, f"Filled {metrics.base_filled_count} base, {metrics.technique_filled_count} technique",
                current_action="Phase 3 complete"
            )

            # ═══════════════════════════════════════════════════════════
            # PHASE 3B: SWI BRANCH RE-ROUTING (70-72%)
            # ═══════════════════════════════════════════════════════════
            # Stacks that received base=SWI from gap filling need to be
            # re-routed through the SWI branch for proper construct detection
            # (Phase, Magnitude, MinIP, MIP, QSM, SWI)
            self.log("Phase 3B: Re-routing SWI stacks...")
            await self.emit_progress(
                71, "Re-routing SWI stacks through branch logic...",
                current_action="Phase 3B: SWI re-routing"
            )

            swi_rerouted_count = 0
            for stack_id, base, technique, method in fill_results:
                if base == "SWI":
                    s = stacks_by_id.get(stack_id)
                    if s and s.get("provenance") not in ("SWIRecon", "SyMRI", "EPIMix"):
                        ctx = ClassificationContext.from_fingerprint(s)
                        branch_result = apply_swi_logic(ctx)

                        s["provenance"] = "SWIRecon"
                        s["base"] = branch_result.base
                        s["technique"] = branch_result.technique
                        s["construct_csv"] = branch_result.construct
                        s["directory_type"] = branch_result.directory_type

                        swi_rerouted_count += 1

            self.log(f"Phase 3B complete: {swi_rerouted_count} stacks re-routed to SWI branch")

            # ═══════════════════════════════════════════════════════════
            # PHASE 4: RE-SYNTHESIZE MISC INTENT (72-85%)
            # ═══════════════════════════════════════════════════════════
            self.log("Phase 4: Re-synthesizing misc intent...")
            await self.emit_progress(
                72, "Re-synthesizing misc stacks...",
                current_action="Phase 4: Intent synthesis"
            )

            # Find misc stacks that we just filled
            misc_stacks = [
                s for s in stacks
                if s["directory_type"] == "misc"
                and s["series_stack_id"] in new_review_stacks
            ]
            
            metrics.misc_initial_count = len(misc_stacks)
            self.log(f"Phase 4: {metrics.misc_initial_count} misc stacks to re-synthesize")

            for stack in misc_stacks:
                new_intent = synthesize_directory_type(
                    base=stack["base"],
                    technique=stack["technique"],
                    construct_csv=stack.get("construct_csv") or "",
                    provenance=stack.get("provenance"),
                    localizer=stack.get("localizer") or 0,
                )
                
                if new_intent != "misc":
                    stack["directory_type"] = new_intent
                    metrics.misc_resolved_count += 1
                    metrics.resolved_to[new_intent] = metrics.resolved_to.get(new_intent, 0) + 1
                else:
                    metrics.misc_remaining_count += 1
                    metrics.new_review_reasons["intent:unresolved"] = \
                        metrics.new_review_reasons.get("intent:unresolved", 0) + 1

            self.log(f"Phase 4 complete: {metrics.misc_resolved_count} misc resolved, {metrics.misc_remaining_count} remaining")

            await self.emit_progress(
                80, f"Resolved {metrics.misc_resolved_count} misc stacks",
                current_action="Phase 4 complete"
            )

            # ═══════════════════════════════════════════════════════════
            # PHASE 4B: CONTRAST CROSS-STACK CONFLICT CHECK (80-85%)
            # ═══════════════════════════════════════════════════════════
            self.log("Phase 4B: Checking contrast conflicts...")
            await self.emit_progress(
                82, "Checking contrast conflicts...",
                current_action="Phase 4B: Contrast conflicts"
            )

            contrast_conflict_stacks = await loop.run_in_executor(
                None,
                functools.partial(self._check_contrast_conflicts, stacks)
            )

            # Convert to set for O(1) lookup
            contrast_conflict_set = set(contrast_conflict_stacks)

            # Update stacks using O(1) dict lookup instead of O(n) search
            for stack_id in contrast_conflict_stacks:
                new_review_stacks.add(stack_id)
                # Update in-memory stack using dict lookup
                s = stacks_by_id.get(stack_id)
                if s:
                    existing = s.get("manual_review_reasons_csv") or ""
                    s["manual_review_reasons_csv"] = add_review_reason(
                        existing, "contrast:duplicate_prediction"
                    )

            metrics.contrast_conflict_count = len(contrast_conflict_stacks)
            metrics.new_review_reasons["contrast:duplicate_prediction"] = \
                metrics.new_review_reasons.get("contrast:duplicate_prediction", 0) + len(contrast_conflict_stacks)
            self.log(f"Phase 4B complete: {metrics.contrast_conflict_count} contrast conflicts detected")

            await self.emit_progress(
                85, f"Detected {metrics.contrast_conflict_count} contrast conflicts",
                current_action="Phase 4B complete"
            )

            # ═══════════════════════════════════════════════════════════
            # PHASE 5: PERSIST UPDATES (85-95%)
            # ═══════════════════════════════════════════════════════════
            self.log("Phase 5: Persisting updates...")
            await self.emit_progress(
                87, "Persisting updates...",
                current_action="Phase 5: Database updates"
            )

            # Persist classification cache updates
            # NOTE: Running synchronously - do NOT use run_in_executor for DB ops
            # as SQLAlchemy connections are not thread-safe
            classification_updates = [
                s for s in stacks
                if s["series_stack_id"] in new_review_stacks
            ]
            
            logger.info("Step 4: Persisting %d classification updates...", len(classification_updates))
            self._persist_classification_updates(conn, classification_updates, orientation_flagged)
            logger.info("Step 4: Classification updates persisted")

            # Persist fingerprint updates (acquisition type)
            if acq_updates:
                logger.info("Step 4: Persisting %d fingerprint updates...", len(acq_updates))
                self._persist_fingerprint_updates(conn, acq_updates)
                logger.info("Step 4: Fingerprint updates persisted")

            # Commit all updates in a single transaction
            conn.commit()
            logger.info("Step 4: All updates committed")

            self.log(f"Phase 5 complete: {len(classification_updates)} classification updates persisted")

            await self.emit_progress(
                95, f"Persisted {len(classification_updates)} updates",
                current_action="Phase 5 complete"
            )

            # ═══════════════════════════════════════════════════════════
            # BUILD HANDOVER
            # ═══════════════════════════════════════════════════════════
            self.log("Building handover...")
            await self.emit_progress(
                98, "Building handover...",
                current_action="Finalizing"
            )

            # Combine review stacks
            all_review_stacks = existing_review_stacks | new_review_stacks
            metrics.stacks_newly_flagged = len(new_review_stacks)

            handover = Step4Handover(
                completed_stack_ids=classified_stack_ids,
                stacks_requiring_review=list(all_review_stacks),
                cohort_id=step3_handover.cohort_id,
                cohort_name=step3_handover.cohort_name,
                processing_mode=step3_handover.processing_mode,
                total_completed=len(classified_stack_ids),
                gaps_filled=metrics.base_filled_count + metrics.technique_filled_count,
                misc_resolved=metrics.misc_resolved_count,
                review_required_count=len(all_review_stacks),
                metrics=metrics,
            )

            await self.emit_complete(metrics.to_dict())

            self.log(
                f"Step 4 complete: {handover.total_completed:,} stacks, "
                f"{handover.gaps_filled} gaps filled, "
                f"{handover.review_required_count:,} requiring review"
            )
            logger.info(
                "Step 4 complete: %d stacks, %d gaps filled, %d requiring review",
                handover.total_completed, handover.gaps_filled, handover.review_required_count
            )

            return StepResult(success=True, handover=handover, metrics=metrics.to_dict())

        except Exception as e:
            logger.exception("Step 4 failed")
            self.log(f"ERROR: {str(e)}")
            metrics.errors.append(str(e))
            await self.emit_error(str(e), metrics.to_dict())
            return StepResult(success=False, error=str(e), metrics=metrics.to_dict())

    # =========================================================================
    # Data Loading Methods
    # =========================================================================

    def _load_stacks_for_completion(
        self,
        conn,
        stack_ids: list[int]
    ) -> list[dict[str, Any]]:
        """Load stack data needed for completion phases."""
        if not stack_ids:
            return []

        result = conn.execute(text("""
            SELECT
                scc.series_stack_id,
                scc.base,
                scc.technique,
                scc.construct_csv,
                scc.provenance,
                scc.directory_type,
                scc.localizer,
                scc.post_contrast,
                scc.manual_review_required,
                scc.manual_review_reasons_csv,
                fp.modality,
                fp.mr_acquisition_type,
                fp.mr_tr,
                fp.mr_te,
                fp.mr_ti,
                fp.mr_flip_angle,
                fp.stack_n_instances,
                fp.text_search_blob,
                fp.image_type,
                fp.scanning_sequence,
                fp.sequence_variant,
                fp.scan_options,
                fp.stack_sequence_name,
                fp.stack_orientation,
                ss.stack_orientation_confidence,
                s.series_instance_uid,
                s.study_id,
                st.study_date,
                st.subject_id
            FROM series_classification_cache scc
            JOIN stack_fingerprint fp ON scc.series_stack_id = fp.series_stack_id
            JOIN series_stack ss ON scc.series_stack_id = ss.series_stack_id
            JOIN series s ON ss.series_id = s.series_id
            JOIN study st ON s.study_id = st.study_id
            WHERE scc.series_stack_id = ANY(:stack_ids)
        """), {"stack_ids": stack_ids})

        return [dict(row._mapping) for row in result]

    def _load_reference_stacks(self, conn) -> list[dict[str, Any]]:
        """Load all classified MR stacks as reference for similarity matching.

        Includes localizers to enable physics-based gap filling for unclassified localizers.
        Excludes stacks with technique='Unknown' to avoid contaminating similarity matches.
        """
        result = conn.execute(text("""
            SELECT
                scc.series_stack_id,
                scc.base,
                scc.technique,
                fp.mr_tr,
                fp.mr_te,
                fp.mr_ti,
                fp.mr_flip_angle,
                fp.stack_n_instances
            FROM series_classification_cache scc
            JOIN stack_fingerprint fp ON scc.series_stack_id = fp.series_stack_id
            WHERE scc.base IS NOT NULL
              AND scc.base != 'Unknown'
              AND scc.technique IS NOT NULL
              AND scc.technique != 'Unknown'
              AND scc.directory_type != 'excluded'
              AND fp.modality = 'MR'
        """))

        return [dict(row._mapping) for row in result]

    # =========================================================================
    # Phase 0: Field Strength Normalization
    # =========================================================================

    def _normalize_field_strength(self, conn) -> int:
        """Normalize MR field strength values in mri_series_details.

        Updates magnetic_field_strength to standard values (0.5, 1, 1.5, 3, 7 T).
        Handles variations and Gauss-scale values.

        Returns:
            Number of series updated
        """
        # Get all distinct non-null field strength values that need normalization
        result = conn.execute(text("""
            SELECT DISTINCT magnetic_field_strength
            FROM mri_series_details
            WHERE magnetic_field_strength IS NOT NULL
        """))

        update_count = 0
        for row in result:
            raw_value = row[0]
            normalized = normalize_field_strength(raw_value)

            # Skip if already normalized (same value)
            if normalized is not None and abs(raw_value - normalized) > 0.001:
                # Update all series with this raw value
                update_result = conn.execute(text("""
                    UPDATE mri_series_details
                    SET magnetic_field_strength = :normalized
                    WHERE magnetic_field_strength = :raw_value
                """), {"normalized": normalized, "raw_value": raw_value})
                update_count += update_result.rowcount

        # NOTE: Do NOT commit here - let Phase 5 manage the transaction
        return update_count

    # =========================================================================
    # Phase 1: Orientation Confidence
    # =========================================================================

    def _check_orientation_confidence(
        self,
        stacks: list[dict[str, Any]]
    ) -> list[int]:
        """Check orientation confidence and return stack IDs to flag."""
        flagged = []
        
        for stack in stacks:
            if stack["modality"] != "MR":
                continue
            
            confidence = stack.get("stack_orientation_confidence")
            if confidence is not None and confidence < ORIENTATION_CONFIDENCE_THRESHOLD:
                flagged.append(stack["series_stack_id"])
        
        return flagged

    # =========================================================================
    # Phase 2: Fill Acquisition Type
    # =========================================================================

    def _fill_acquisition_type(
        self,
        stacks: list[dict[str, Any]]
    ) -> tuple[list[tuple[int, str, str]], dict[str, int]]:
        """
        Fill missing mr_acquisition_type.
        
        Returns:
            Tuple of (updates, stats)
            updates: List of (stack_id, new_value, method)
            stats: Dict of method -> count
        """
        updates = []
        stats: dict[str, int] = {}
        
        for stack in stacks:
            if stack["modality"] != "MR":
                continue
            
            if stack.get("mr_acquisition_type"):
                continue  # Already set
            
            # Build minimal unified flags for inference
            unified_flags = self._build_minimal_unified_flags(stack)
            
            new_value, method = infer_acquisition_type(
                mr_acquisition_type=stack.get("mr_acquisition_type"),
                unified_flags=unified_flags,
                text_search_blob=stack.get("text_search_blob"),
                technique=stack.get("technique"),
            )
            
            if new_value:
                updates.append((stack["series_stack_id"], new_value, method))
                stats[method] = stats.get(method, 0) + 1
        
        return updates, stats

    def _build_minimal_unified_flags(self, stack: dict[str, Any]) -> dict[str, bool]:
        """Build minimal unified flags needed for acquisition type inference."""
        # Check image_type for 2D/3D markers
        image_type = (stack.get("image_type") or "").upper()
        
        is_3d = "DIS3D" in image_type or "3D" in image_type
        is_2d = "DIS2D" in image_type or "2D" in image_type
        
        # Check sequence name patterns
        seq_name = (stack.get("stack_sequence_name") or "").lower()
        if "3d" in seq_name or "spc" in seq_name or "space" in seq_name:
            is_3d = True
        if "2d" in seq_name:
            is_2d = True
        
        return {
            "is_3d": is_3d,
            "is_2d": is_2d,
        }

    # =========================================================================
    # Phase 3: Fill Base & Technique
    # =========================================================================

    def _fill_base_technique_batch(
        self,
        stacks: list[dict[str, Any]]
    ) -> list[tuple[int, str | None, str | None, str]]:
        """
        Fill base and technique for a batch of stacks.
        
        Returns:
            List of (stack_id, base, technique, method)
        """
        results = []
        
        for stack in stacks:
            result = find_best_match(
                ref_db=self._reference_db,
                tr=stack.get("mr_tr"),
                te=stack.get("mr_te"),
                ti=stack.get("mr_ti"),
                fa=stack.get("mr_flip_angle"),
                n_instances=stack.get("stack_n_instances"),
                scanning_sequence=stack.get("scanning_sequence"),
            )

            # Only fill if we got a match
            current_base = stack.get("base")
            base_missing = current_base is None or current_base == ""
            base_is_unknown = (
                isinstance(current_base, str)
                and current_base.strip().lower() == "unknown"
            )
            base = result.base if (result.base and (base_missing or base_is_unknown)) else None

            current_technique = stack.get("technique")
            technique_missing = current_technique is None
            technique_is_unknown = (
                isinstance(current_technique, str)
                and current_technique.strip().lower() == "unknown"
            )
            technique = (
                result.technique
                if result.technique and (technique_missing or technique_is_unknown)
                else None
            )

            if base or technique:
                results.append((
                    stack["series_stack_id"],
                    base,
                    technique,
                    result.method,
                ))
            elif result.method in ("no_match", "insufficient_matches", "no_compatible_match"):
                # Record that we couldn't fill
                results.append((
                    stack["series_stack_id"],
                    None,
                    None,
                    result.method,
                ))
        
        return results

    # =========================================================================
    # Phase 4B: Contrast Conflict Detection
    # =========================================================================

    def _check_contrast_conflicts(
        self,
        stacks: list[dict[str, Any]]
    ) -> list[int]:
        """
        Check for contrast conflicts within sessions.

        A conflict occurs when two stacks in the same session (subject + study_date)
        have identical fingerprints but the same contrast prediction (both PRE or
        both POST). One should likely be PRE and one POST.

        Only checks single-stack series (not multi-echo where same prediction is OK).

        Returns:
            List of stack IDs that have contrast conflicts
        """
        from collections import defaultdict

        # Pre-filter: only check stacks with non-null contrast prediction
        # This significantly reduces the working set
        stacks_with_contrast = [
            s for s in stacks
            if s.get("post_contrast") is not None
        ]

        if not stacks_with_contrast:
            return []

        # Count series per series_instance_uid (only for stacks with contrast)
        series_stack_counts: dict[str, int] = defaultdict(int)
        for stack in stacks_with_contrast:
            uid = stack.get("series_instance_uid")
            if uid:
                series_stack_counts[uid] += 1

        # Filter to single-stack series only (not multi-echo)
        single_stack_stacks = [
            s for s in stacks_with_contrast
            if series_stack_counts.get(s.get("series_instance_uid"), 0) == 1
        ]

        if not single_stack_stacks:
            return []

        # Group by session (subject_id + study_date) AND fingerprint signature together
        # Key: (subject_id, study_date, fingerprint_sig)
        groups: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
        for stack in single_stack_stacks:
            subject_id = stack.get("subject_id")
            study_date = stack.get("study_date")
            if subject_id is None or study_date is None:
                continue
            sig = self._get_fingerprint_signature(stack)
            key = (subject_id, study_date, sig)
            groups[key].append(stack)

        # Check each group for conflicts
        flagged_stack_ids: list[int] = []
        for key, group in groups.items():
            if len(group) < 2:
                continue

            # Get contrast values (all non-None since we pre-filtered)
            contrast_values = [s["post_contrast"] for s in group]

            # All same value = conflict (both PRE or both POST)
            if len(set(contrast_values)) == 1:
                flagged_stack_ids.extend(s["series_stack_id"] for s in group)

        return flagged_stack_ids

    def _get_fingerprint_signature(self, stack: dict[str, Any]) -> str:
        """
        Generate signature for fingerprint comparison.

        Two stacks are considered "identical" if they match on:
        - base, technique, orientation, TE, TR

        Args:
            stack: Stack data dict

        Returns:
            Fingerprint signature string
        """
        return "|".join([
            str(stack.get("base") or ""),
            str(stack.get("technique") or ""),
            str(stack.get("stack_orientation") or ""),
            str(stack.get("mr_te") or ""),
            str(stack.get("mr_tr") or ""),
        ])

    # =========================================================================
    # Phase 5: Persistence
    # =========================================================================

    def _persist_classification_updates(
        self,
        conn,
        stacks: list[dict[str, Any]],
        orientation_flagged: list[int],
    ) -> None:
        """Persist updates to series_classification_cache."""
        if not stacks:
            return

        for stack in stacks:
            stack_id = stack["series_stack_id"]
            
            # Build updated review reasons
            existing_reasons = stack.get("manual_review_reasons_csv") or ""
            
            # Add orientation flag if needed
            if stack_id in orientation_flagged:
                existing_reasons = add_review_reason(existing_reasons, "orientation:low_confidence")
            
            # Update the row
            conn.execute(text("""
                UPDATE series_classification_cache
                SET base = :base,
                    technique = :technique,
                    construct_csv = :construct_csv,
                    provenance = :provenance,
                    directory_type = :directory_type,
                    manual_review_required = 1,
                    manual_review_reasons_csv = :reasons
                WHERE series_stack_id = :stack_id
            """), {
                "stack_id": stack_id,
                "base": stack.get("base"),
                "technique": stack.get("technique"),
                "construct_csv": stack.get("construct_csv"),
                "provenance": stack.get("provenance"),
                "directory_type": stack.get("directory_type"),
                "reasons": existing_reasons,
            })
        # NOTE: Do NOT commit here - let caller manage transaction

    def _persist_fingerprint_updates(
        self,
        conn,
        updates: list[tuple[int, str, str]],
    ) -> None:
        """Persist mr_acquisition_type updates to stack_fingerprint."""
        if not updates:
            return

        for stack_id, new_value, _ in updates:
            conn.execute(text("""
                UPDATE stack_fingerprint
                SET mr_acquisition_type = :value
                WHERE series_stack_id = :stack_id
            """), {
                "stack_id": stack_id,
                "value": new_value,
            })
        # NOTE: Do NOT commit here - let caller manage transaction
