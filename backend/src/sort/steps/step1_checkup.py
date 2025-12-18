"""Step 1: Checkup - Verify cohort scope and data integrity.

This step determines exactly which data is in-scope for sorting:
1.1 Cohort Subject Resolution - Get subjects belonging to this cohort
1.2 Study Discovery - Find all studies for cohort subjects
1.3 Study Date Validation & Repair - Impute missing dates or exclude
1.4 Series Collection - Get series from valid studies
1.5 Existing Classification Check - Skip or reprocess based on config
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from .base import BaseStep, StepContext, StepResult
from ..models import (
    SeriesForProcessing,
    Step1Handover,
    Step1Metrics,
    SortingStepId,
)
from ..queries import (
    get_cohort_subjects,
    get_studies_for_subjects,
    get_series_date_for_study,
    get_acquisition_date_for_study,
    get_content_date_for_study,
    update_study_date,
    get_series_for_studies,
    get_classified_series_ids,
    filter_series_by_modality,
)

logger = logging.getLogger(__name__)


class Step1Checkup(BaseStep):
    """Step 1: Checkup - Verify cohort scope and data integrity.

    Purpose:
    - Determine exactly which data is in-scope for this sorting run
    - Validate required fields (especially study dates)
    - Repair missing dates from alternative sources
    - Filter already-classified series if skip_classified=True

    Output:
    - Step1Handover containing the list of SeriesForProcessing
    """

    step_id = SortingStepId.CHECKUP.value
    step_title = "Checkup"

    async def execute(self, context: StepContext) -> StepResult:
        """Execute the validation step.

        Args:
            context: Step context with cohort_id, config, and db connection

        Returns:
            StepResult with Step1Handover as handover data
        """
        metrics = Step1Metrics()
        conn = context.conn

        try:
            # ═══════════════════════════════════════════════════════════════
            # 1.1 COHORT SUBJECT RESOLUTION
            # ═══════════════════════════════════════════════════════════════
            await self.emit_progress(
                5,
                "Resolving cohort subjects...",
                current_action="Querying subject_cohorts table",
            )

            subjects = get_cohort_subjects(conn, context.cohort_id)
            metrics.subjects_in_cohort = len(subjects)

            if not subjects:
                metrics.validation_passed = False
                metrics.errors.append(f"No subjects found in cohort {context.cohort_id}")
                await self.emit_error("No subjects found in cohort", metrics.to_dict())
                return StepResult(
                    success=False,
                    error="No subjects found in cohort",
                    metrics=metrics.to_dict(),
                )

            subject_ids = [s["subject_id"] for s in subjects]
            logger.info(
                "Step 1.1: Found %d subjects in cohort %d",
                len(subjects),
                context.cohort_id,
            )

            # ═══════════════════════════════════════════════════════════════
            # 1.2 STUDY DISCOVERY
            # ═══════════════════════════════════════════════════════════════
            await self.emit_progress(
                20,
                f"Found {len(subjects)} subjects, discovering studies...",
                metrics={"subjects_in_cohort": len(subjects)},
                current_action="Querying study table",
            )

            studies = get_studies_for_subjects(conn, subject_ids)
            metrics.total_studies = len(studies)

            if not studies:
                metrics.validation_passed = False
                metrics.errors.append("No studies found for cohort subjects")
                await self.emit_error("No studies found for cohort subjects", metrics.to_dict())
                return StepResult(
                    success=False,
                    error="No studies found for cohort subjects",
                    metrics=metrics.to_dict(),
                )

            logger.info(
                "Step 1.2: Found %d studies for %d subjects",
                len(studies),
                len(subjects),
            )

            # ═══════════════════════════════════════════════════════════════
            # 1.3 STUDY DATE VALIDATION & REPAIR
            # ═══════════════════════════════════════════════════════════════
            await self.emit_progress(
                40,
                f"Validating dates for {len(studies)} studies...",
                metrics={"subjects_in_cohort": len(subjects), "total_studies": len(studies)},
                current_action="Checking and repairing study dates",
            )

            valid_studies, excluded_studies, imputed_count = await self._validate_study_dates(
                conn, studies
            )

            metrics.studies_with_valid_date = len(valid_studies)
            metrics.studies_date_imputed = imputed_count
            metrics.studies_excluded_no_date = len(excluded_studies)
            metrics.excluded_study_uids = [s["study_instance_uid"] for s in excluded_studies]

            if excluded_studies:
                metrics.warnings.append(
                    f"{len(excluded_studies)} studies excluded due to missing dates"
                )
                logger.warning(
                    "Step 1.3: Excluded %d studies with no recoverable date",
                    len(excluded_studies),
                )

            if not valid_studies:
                metrics.validation_passed = False
                metrics.errors.append("All studies excluded - no valid dates")
                await self.emit_error("All studies excluded - no valid dates", metrics.to_dict())
                return StepResult(
                    success=False,
                    error="All studies excluded due to missing dates",
                    metrics=metrics.to_dict(),
                )

            logger.info(
                "Step 1.3: %d valid studies, %d imputed, %d excluded",
                len(valid_studies),
                imputed_count,
                len(excluded_studies),
            )

            valid_study_ids = [s["study_id"] for s in valid_studies]

            # ═══════════════════════════════════════════════════════════════
            # 1.4 SERIES COLLECTION
            # ═══════════════════════════════════════════════════════════════
            await self.emit_progress(
                60,
                f"Collecting series from {len(valid_studies)} valid studies...",
                metrics={
                    "subjects_in_cohort": len(subjects),
                    "total_studies": len(studies),
                    "studies_with_valid_date": len(valid_studies),
                    "studies_date_imputed": imputed_count,
                    "studies_excluded_no_date": len(excluded_studies),
                },
                current_action="Querying series table",
            )

            series_rows = get_series_for_studies(conn, valid_study_ids)
            metrics.total_series = len(series_rows)

            if not series_rows:
                metrics.validation_passed = False
                metrics.errors.append("No series found in valid studies")
                await self.emit_error("No series found in valid studies", metrics.to_dict())
                return StepResult(
                    success=False,
                    error="No series found in valid studies",
                    metrics=metrics.to_dict(),
                )

            logger.info("Step 1.4: Found %d series in valid studies", len(series_rows))

            # ═══════════════════════════════════════════════════════════════
            # 1.4.5 MODALITY FILTERING
            # ═══════════════════════════════════════════════════════════════
            await self.emit_progress(
                70,
                f"Filtering {len(series_rows)} series by selected modalities...",
                metrics={
                    "subjects_in_cohort": len(subjects),
                    "total_studies": len(studies),
                    "studies_with_valid_date": len(valid_studies),
                    "total_series": len(series_rows),
                },
                current_action="Applying modality filter",
            )

            series_rows, modality_counts = filter_series_by_modality(
                series_rows,
                context.config.selected_modalities
            )

            metrics.series_by_modality = modality_counts
            metrics.selected_modalities = context.config.selected_modalities
            metrics.total_series = len(series_rows)

            logger.info(
                "Step 1.4.5: Filtered to %d series with modalities %s (breakdown: %s)",
                len(series_rows),
                context.config.selected_modalities,
                modality_counts,
            )

            if not series_rows:
                metrics.warnings.append(
                    f"No series found matching selected modalities: {', '.join(context.config.selected_modalities)}"
                )
                logger.warning(
                    "Step 1.4.5: No series match selected modalities %s - returning with warning",
                    context.config.selected_modalities,
                )
                
                # Build empty handover when no series match
                processing_mode = "incremental" if context.config.skip_classified else "full_reprocess"
                handover = Step1Handover(
                    series_to_process=[],
                    series_ids=set(),
                    series_uids=set(),
                    study_ids=set(),
                    subject_ids=set(),
                    cohort_id=context.cohort_id,
                    cohort_name=context.cohort_name,
                    processing_mode=processing_mode,
                    metrics=metrics,
                )
                
                await self.emit_warning(metrics.to_dict())
                
                return StepResult(
                    success=True,  # Not an error, just no matching data
                    handover=handover,
                    metrics=metrics.to_dict(),
                )

            # ═══════════════════════════════════════════════════════════════
            # 1.5 EXISTING CLASSIFICATION CHECK
            # ═══════════════════════════════════════════════════════════════
            await self.emit_progress(
                80,
                f"Checking existing classifications for {len(series_rows)} series...",
                metrics={
                    "subjects_in_cohort": len(subjects),
                    "total_studies": len(studies),
                    "studies_with_valid_date": len(valid_studies),
                    "total_series": len(series_rows),
                },
                current_action="Checking series_classification_cache",
            )

            series_to_process, skipped_series = await self._filter_by_classification(
                conn, series_rows, context.config.skip_classified
            )

            metrics.series_already_classified = len(skipped_series)
            metrics.series_to_process_count = len(series_to_process)
            metrics.skipped_series_uids = [s.series_instance_uid for s in skipped_series]

            processing_mode = "incremental" if context.config.skip_classified else "full_reprocess"

            if context.config.skip_classified and skipped_series:
                metrics.warnings.append(
                    f"{len(skipped_series)} series skipped (already classified)"
                )
                logger.info(
                    "Step 1.5: Skipping %d already-classified series (incremental mode)",
                    len(skipped_series),
                )

            if not series_to_process and context.config.skip_classified:
                # All series already classified - this is OK, just nothing to do
                metrics.warnings.append("All series already classified - nothing to process")
                logger.info("Step 1.5: All series already classified, nothing to process")

            logger.info(
                "Step 1.5: %d series to process, %d skipped, mode=%s",
                len(series_to_process),
                len(skipped_series),
                processing_mode,
            )

            # ═══════════════════════════════════════════════════════════════
            # BUILD HANDOVER
            # ═══════════════════════════════════════════════════════════════
            await self.emit_progress(
                95,
                "Building handover for next step...",
                metrics=metrics.to_dict(),
                current_action="Preparing Step1Handover",
            )

            handover = Step1Handover(
                series_to_process=series_to_process,
                series_ids={s.series_id for s in series_to_process},
                series_uids={s.series_instance_uid for s in series_to_process},
                study_ids={s.study_id for s in series_to_process},
                subject_ids={s.subject_id for s in series_to_process},
                cohort_id=context.cohort_id,
                cohort_name=context.cohort_name,
                processing_mode=processing_mode,
                metrics=metrics,
            )

            # Final completion - emit warning if there are excluded studies
            if metrics.studies_excluded_no_date > 0:
                await self.emit_warning(metrics.to_dict())
                logger.info(
                    "Step 1 complete with warnings: %d series ready, %d studies excluded",
                    len(series_to_process),
                    metrics.studies_excluded_no_date,
                )
            else:
                await self.emit_complete(metrics.to_dict())
                logger.info(
                    "Step 1 complete: %d series ready for processing",
                    len(series_to_process),
                )

            return StepResult(
                success=True,
                handover=handover,
                metrics=metrics.to_dict(),
            )

        except Exception as e:
            logger.exception("Step 1 failed with exception")
            metrics.validation_passed = False
            metrics.errors.append(str(e))
            await self.emit_error(str(e), metrics.to_dict())
            return StepResult(
                success=False,
                error=str(e),
                metrics=metrics.to_dict(),
            )

    async def _validate_study_dates(
        self,
        conn: Any,
        studies: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
        """Validate and repair study dates.

        For studies with NULL study_date, attempt to impute from:
        1. series.series_date
        2. instance.acquisition_date
        3. instance.content_date

        If still NULL, exclude the study from sorting (don't delete it).

        Args:
            conn: Database connection
            studies: List of study dicts

        Returns:
            Tuple of (valid_studies, excluded_studies, imputed_count)
        """
        valid_studies = []
        excluded_studies = []
        imputed_count = 0

        for study in studies:
            if study["study_date"] is not None:
                valid_studies.append(study)
                continue

            # Attempt repair
            imputed_date = None
            source = None

            # Priority 1: series.series_date
            series_date = get_series_date_for_study(conn, study["study_id"])
            if series_date:
                imputed_date = series_date
                source = "series.series_date"

            # Priority 2: instance.acquisition_date
            if not imputed_date:
                acq_date = get_acquisition_date_for_study(conn, study["study_id"])
                if acq_date:
                    imputed_date = acq_date
                    source = "instance.acquisition_date"

            # Priority 3: instance.content_date
            if not imputed_date:
                content_date = get_content_date_for_study(conn, study["study_id"])
                if content_date:
                    imputed_date = content_date
                    source = "instance.content_date"

            if imputed_date:
                # Persist the imputed date
                update_study_date(conn, study["study_id"], imputed_date)
                study["study_date"] = imputed_date
                valid_studies.append(study)
                imputed_count += 1
                logger.debug(
                    "Imputed study_date for study %s from %s",
                    study["study_instance_uid"],
                    source,
                )
            else:
                # Exclude from sorting
                excluded_studies.append(study)
                logger.debug(
                    "Excluding study %s: no date available",
                    study["study_instance_uid"],
                )

        return valid_studies, excluded_studies, imputed_count

    async def _filter_by_classification(
        self,
        conn: Any,
        series_rows: list[dict[str, Any]],
        skip_classified: bool,
    ) -> tuple[list[SeriesForProcessing], list[SeriesForProcessing]]:
        """Filter series by existing classification status.

        Args:
            conn: Database connection
            series_rows: List of series dicts from query
            skip_classified: If True, skip already-classified series

        Returns:
            Tuple of (series_to_process, skipped_series)
        """
        # Convert to SeriesForProcessing objects
        all_series = [
            SeriesForProcessing(
                series_id=row["series_id"],
                series_instance_uid=row["series_instance_uid"],
                modality=row["modality"],
                study_id=row["study_id"],
                subject_id=row["subject_id"],
                study_instance_uid=row["study_instance_uid"],
                study_date=row["study_date"],
                subject_code=row["subject_code"],
            )
            for row in series_rows
        ]

        if not skip_classified:
            # Full reprocess mode - process everything
            return all_series, []

        # Check which series are already classified
        series_ids = [s.series_id for s in all_series]
        classified_ids = get_classified_series_ids(conn, series_ids)

        to_process = []
        skipped = []

        for series in all_series:
            if series.series_id in classified_ids:
                skipped.append(series)
            else:
                to_process.append(series)

        return to_process, skipped
