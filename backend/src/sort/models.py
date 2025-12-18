"""Pydantic and dataclass models for the sorting pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class SortingStepId(str, Enum):
    """Identifiers for sorting pipeline steps."""

    CHECKUP = "checkup"
    STACK_FINGERPRINT = "stack_fingerprint"
    CLASSIFICATION = "classification"
    COMPLETION = "completion"
    # Future steps TBD (deduplication, verification)


class StepStatus(str, Enum):
    """Status of a sorting step."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    WARNING = "warning"  # Complete but with data quality issues
    ERROR = "error"
    SKIPPED = "skipped"


class SortingConfig(BaseModel):
    """Configuration for sorting pipeline."""

    skip_classified: bool = Field(default=True, alias="skipClassified")
    force_reprocess: bool = Field(default=False, alias="forceReprocess")
    profile: str = Field(default="standard")
    selected_modalities: list[str] = Field(default=["MR", "CT", "PT"], alias="selectedModalities")

    class Config:
        populate_by_name = True


@dataclass
class SeriesForProcessing:
    """Single series record passed between steps.

    Contains denormalized fields to avoid repeated JOINs in downstream steps.
    """

    series_id: int
    series_instance_uid: str
    modality: str
    study_id: int
    subject_id: int
    study_instance_uid: str
    study_date: date | None
    subject_code: str


@dataclass
class Step1Metrics:
    """Metrics for Step 1: Data Validation & Scope Resolution.

    These metrics are displayed in the UI and logged for review.
    """

    # Counts
    subjects_in_cohort: int = 0
    total_studies: int = 0
    studies_with_valid_date: int = 0
    studies_date_imputed: int = 0
    studies_excluded_no_date: int = 0
    total_series: int = 0
    series_already_classified: int = 0
    series_to_process_count: int = 0

    # Modality breakdown
    series_by_modality: dict[str, int] = field(default_factory=dict)
    selected_modalities: list[str] = field(default_factory=list)

    # For review/debugging
    excluded_study_uids: list[str] = field(default_factory=list)
    skipped_series_uids: list[str] = field(default_factory=list)

    # Status
    validation_passed: bool = True
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "subjects_in_cohort": self.subjects_in_cohort,
            "total_studies": self.total_studies,
            "studies_with_valid_date": self.studies_with_valid_date,
            "studies_date_imputed": self.studies_date_imputed,
            "studies_excluded_no_date": self.studies_excluded_no_date,
            "total_series": self.total_series,
            "series_already_classified": self.series_already_classified,
            "series_to_process_count": self.series_to_process_count,
            "series_by_modality": self.series_by_modality,
            "selected_modalities": self.selected_modalities,
            "excluded_study_uids": self.excluded_study_uids[:10],  # Limit for UI
            "skipped_series_uids": self.skipped_series_uids[:10],  # Limit for UI
            "validation_passed": self.validation_passed,
            "warnings": self.warnings,
            "errors": self.errors,
        }


@dataclass
class Step1Handover:
    """Handover from Step 1 to Step 2.

    This is the PRIMARY output of Step 1 - the actual data to process.
    Everything else (metrics) is for UI display and logging.
    """

    # ═══════════════════════════════════════════════════════════════════
    # PRIMARY OUTPUT: The actual data to process
    # ═══════════════════════════════════════════════════════════════════
    series_to_process: list[SeriesForProcessing]

    # Quick lookup structures (derived from series_to_process)
    series_ids: set[int]  # For fast membership checks
    series_uids: set[str]  # SeriesInstanceUIDs
    study_ids: set[int]  # Unique studies in scope
    subject_ids: set[int]  # Unique subjects in scope

    # ═══════════════════════════════════════════════════════════════════
    # CONTEXT: Passed through for downstream steps
    # ═══════════════════════════════════════════════════════════════════
    cohort_id: int
    cohort_name: str
    processing_mode: str  # "incremental" | "full_reprocess"

    # ═══════════════════════════════════════════════════════════════════
    # METRICS: For UI display and logging (not used by Step 2)
    # ═══════════════════════════════════════════════════════════════════
    metrics: Step1Metrics

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization and database persistence.
        
        Note: We only store series_ids (not full SeriesForProcessing objects)
        because the full series data can be re-queried from the metadata DB
        when reconstructing the handover. This keeps the stored JSON small.
        """
        return {
            "type": "step1",
            "series_ids": list(self.series_ids),  # Convert set to list for JSON
            "cohort_id": self.cohort_id,
            "cohort_name": self.cohort_name,
            "processing_mode": self.processing_mode,
            "metrics": self.metrics.to_dict() if hasattr(self.metrics, 'to_dict') else self.metrics,
        }


@dataclass
class StepProgress:
    """Progress update for a single step."""

    step_id: str
    status: StepStatus
    progress: int  # 0-100
    message: str
    metrics: dict[str, Any] = field(default_factory=dict)
    current_action: str | None = None
    error: str | None = None
    logs: list[str] = field(default_factory=list)  # Recent log lines for streaming

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "step_id": self.step_id,
            "status": self.status.value,
            "progress": self.progress,
            "message": self.message,
            "metrics": self.metrics,
            "current_action": self.current_action,
            "error": self.error,
            "logs": self.logs,
        }


class ProgressEvent(BaseModel):
    """SSE event for progress streaming."""

    type: str  # step_start, step_progress, step_complete, step_error, pipeline_complete
    step_id: str | None = None
    step_title: str | None = None
    progress: int | None = None
    message: str | None = None
    metrics: dict[str, Any] | None = None
    current_action: str | None = None
    error: str | None = None
    summary: dict[str, Any] | None = None
    logs: list[str] | None = None  # Recent log lines for streaming display


@dataclass
class Step2Metrics:
    """Metrics for Step 2: Stack Fingerprint."""

    # Fingerprint generation metrics
    total_fingerprints_created: int = 0
    stacks_processed: int = 0
    stacks_with_missing_fov: int = 0
    stacks_with_contrast: int = 0
    
    # Modality and manufacturer breakdown
    breakdown_by_modality: dict[str, int] = field(default_factory=dict)
    breakdown_by_manufacturer: dict[str, int] = field(default_factory=dict)
    
    # Processing stats per modality
    mr_stacks_with_3d: int = 0
    mr_stacks_with_diffusion: int = 0
    ct_stacks_calcium_score: int = 0
    pet_stacks_attn_corrected: int = 0

    # Stack analysis
    series_with_multiple_stacks: int = 0
    series_with_single_stack: int = 0
    max_stacks_per_series: int = 0

    # Orientation confidence metrics
    stacks_with_low_confidence: int = 0  # Confidence < 0.85 (oblique orientations)
    avg_orientation_confidence: float | None = None
    min_orientation_confidence: float | None = None

    # Examples of multi-stack series for QC (limit to 10)
    multi_stack_examples: list[dict[str, Any]] = field(default_factory=list)

    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "total_fingerprints_created": self.total_fingerprints_created,
            "stacks_processed": self.stacks_processed,
            "stacks_with_missing_fov": self.stacks_with_missing_fov,
            "stacks_with_contrast": self.stacks_with_contrast,
            "breakdown_by_modality": self.breakdown_by_modality,
            "breakdown_by_manufacturer": self.breakdown_by_manufacturer,
            "mr_stacks_with_3d": self.mr_stacks_with_3d,
            "mr_stacks_with_diffusion": self.mr_stacks_with_diffusion,
            "ct_stacks_calcium_score": self.ct_stacks_calcium_score,
            "pet_stacks_attn_corrected": self.pet_stacks_attn_corrected,
            "series_with_multiple_stacks": self.series_with_multiple_stacks,
            "series_with_single_stack": self.series_with_single_stack,
            "max_stacks_per_series": self.max_stacks_per_series,
            "stacks_with_low_confidence": self.stacks_with_low_confidence,
            "avg_orientation_confidence": self.avg_orientation_confidence,
            "min_orientation_confidence": self.min_orientation_confidence,
            "multi_stack_examples": self.multi_stack_examples[:10],
            "warnings": self.warnings,
            "errors": self.errors,
        }


@dataclass
class Step2Handover:
    """Handover from Step 2 to Step 3."""

    # Primary output: fingerprint IDs for classification
    fingerprint_ids: list[int] = field(default_factory=list)
    
    # Stack IDs (for reference)
    series_stack_ids: list[int] = field(default_factory=list)

    # Context from Step 1
    cohort_id: int = 0
    cohort_name: str = ""
    processing_mode: str = ""

    # Summary metrics
    fingerprints_created: int = 0
    stacks_processed: int = 0
    series_with_multiple_stacks: int = 0
    breakdown_by_modality: dict[str, int] = field(default_factory=dict)

    metrics: Step2Metrics = field(default_factory=Step2Metrics)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization and database persistence."""
        return {
            "type": "step2",
            "fingerprint_ids": self.fingerprint_ids,
            "series_stack_ids": self.series_stack_ids,
            "cohort_id": self.cohort_id,
            "cohort_name": self.cohort_name,
            "processing_mode": self.processing_mode,
            "fingerprints_created": self.fingerprints_created,
            "stacks_processed": self.stacks_processed,
            "series_with_multiple_stacks": self.series_with_multiple_stacks,
            "breakdown_by_modality": self.breakdown_by_modality,
        }


@dataclass
class Step3Metrics:
    """Metrics for Step 3: Classification.

    Tracks classification results for UI display and logging.
    """

    # Primary counts
    total_classified: int = 0
    excluded_count: int = 0
    review_required_count: int = 0

    # Breakdown by classification axes
    breakdown_by_directory_type: dict[str, int] = field(default_factory=dict)
    breakdown_by_provenance: dict[str, int] = field(default_factory=dict)
    breakdown_by_base: dict[str, int] = field(default_factory=dict)
    breakdown_by_technique: dict[str, int] = field(default_factory=dict)

    # Review-related
    low_confidence_axes: dict[str, int] = field(default_factory=dict)  # {base: 5, technique: 3}
    review_reasons: dict[str, int] = field(default_factory=dict)  # {base:missing: 10, ...}

    # Special flags
    spine_detected_count: int = 0
    post_contrast_count: int = 0
    localizer_count: int = 0

    # Status
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "total_classified": self.total_classified,
            "excluded_count": self.excluded_count,
            "review_required_count": self.review_required_count,
            "breakdown_by_directory_type": self.breakdown_by_directory_type,
            "breakdown_by_provenance": self.breakdown_by_provenance,
            "breakdown_by_base": self.breakdown_by_base,
            "breakdown_by_technique": self.breakdown_by_technique,
            "low_confidence_axes": self.low_confidence_axes,
            "review_reasons": self.review_reasons,
            "spine_detected_count": self.spine_detected_count,
            "post_contrast_count": self.post_contrast_count,
            "localizer_count": self.localizer_count,
            "warnings": self.warnings,
            "errors": self.errors,
        }


@dataclass
class Step3Handover:
    """Handover from Step 3 to Step 4 (future deduplication step)."""

    # Primary output: classified stack IDs
    classified_stack_ids: list[int] = field(default_factory=list)

    # Stacks requiring manual review
    stacks_requiring_review: list[int] = field(default_factory=list)

    # Context passed through
    cohort_id: int = 0
    cohort_name: str = ""
    processing_mode: str = ""

    # Summary
    total_classified: int = 0
    review_required_count: int = 0

    metrics: Step3Metrics = field(default_factory=Step3Metrics)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization and database persistence."""
        return {
            "type": "step3",
            "classified_stack_ids": self.classified_stack_ids,
            "stacks_requiring_review": self.stacks_requiring_review,
            "cohort_id": self.cohort_id,
            "cohort_name": self.cohort_name,
            "processing_mode": self.processing_mode,
            "total_classified": self.total_classified,
            "review_required_count": self.review_required_count,
        }


@dataclass
class Step4Metrics:
    """Metrics for Step 4: Completion & Gap Filling.

    Tracks gap filling results for UI display and logging.
    """

    # Totals
    total_processed: int = 0

    # Phase 0: Field strength normalization
    field_strength_normalized_count: int = 0

    # Phase 1: Orientation confidence
    orientation_flagged_count: int = 0

    # Phase 2: Acquisition type filling
    acquisition_type_filled_count: int = 0
    acquisition_type_by_method: dict[str, int] = field(default_factory=dict)
    # {"unified_flag": 10, "text_pattern": 25, "technique_inference": 5}

    # Phase 3: Base & Technique via similarity
    base_filled_count: int = 0
    technique_filled_count: int = 0
    stacks_with_no_match: int = 0
    similarity_match_counts: dict[str, int] = field(default_factory=dict)
    # {"exact_bin": 100, "expanded_search": 30}

    # Phase 4: Intent re-synthesis
    misc_initial_count: int = 0
    misc_resolved_count: int = 0
    misc_remaining_count: int = 0
    resolved_to: dict[str, int] = field(default_factory=dict)
    # {"anat": 50, "dwi": 10, "func": 5}

    # Phase 4B: Contrast conflict detection
    contrast_conflict_count: int = 0

    # Review summary
    stacks_newly_flagged: int = 0
    new_review_reasons: dict[str, int] = field(default_factory=dict)

    # Status
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "total_processed": self.total_processed,
            "field_strength_normalized_count": self.field_strength_normalized_count,
            "orientation_flagged_count": self.orientation_flagged_count,
            "acquisition_type_filled_count": self.acquisition_type_filled_count,
            "acquisition_type_by_method": self.acquisition_type_by_method,
            "base_filled_count": self.base_filled_count,
            "technique_filled_count": self.technique_filled_count,
            "stacks_with_no_match": self.stacks_with_no_match,
            "similarity_match_counts": self.similarity_match_counts,
            "misc_initial_count": self.misc_initial_count,
            "misc_resolved_count": self.misc_resolved_count,
            "misc_remaining_count": self.misc_remaining_count,
            "resolved_to": self.resolved_to,
            "contrast_conflict_count": self.contrast_conflict_count,
            "stacks_newly_flagged": self.stacks_newly_flagged,
            "new_review_reasons": self.new_review_reasons,
            "warnings": self.warnings,
            "errors": self.errors,
        }


@dataclass
class Step4Handover:
    """Handover from Step 4 to Step 5 (future deduplication step)."""

    # Primary output: completed stack IDs
    completed_stack_ids: list[int] = field(default_factory=list)

    # Stacks requiring manual review (updated from Step 3 + new flags)
    stacks_requiring_review: list[int] = field(default_factory=list)

    # Context passed through
    cohort_id: int = 0
    cohort_name: str = ""
    processing_mode: str = ""

    # Summary
    total_completed: int = 0
    gaps_filled: int = 0  # base + technique fills
    misc_resolved: int = 0
    review_required_count: int = 0

    metrics: Step4Metrics = field(default_factory=Step4Metrics)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization and database persistence."""
        return {
            "type": "step4",
            "completed_stack_ids": self.completed_stack_ids,
            "stacks_requiring_review": self.stacks_requiring_review,
            "cohort_id": self.cohort_id,
            "cohort_name": self.cohort_name,
            "processing_mode": self.processing_mode,
            "total_completed": self.total_completed,
            "gaps_filled": self.gaps_filled,
            "misc_resolved": self.misc_resolved,
            "review_required_count": self.review_required_count,
        }


# Step metadata for UI display
SORTING_STEPS = [
    {
        "id": SortingStepId.CHECKUP,
        "title": "Checkup",
        "description": "Verify cohort scope and data integrity",
    },
    {
        "id": SortingStepId.STACK_FINGERPRINT,
        "title": "Stack Fingerprint",
        "description": "Build classification features for each stack",
    },
    {
        "id": SortingStepId.CLASSIFICATION,
        "title": "Classification",
        "description": "Classify each stack using detection rules",
    },
    {
        "id": SortingStepId.COMPLETION,
        "title": "Completion",
        "description": "Fill gaps and flag for review",
    },
    # Future steps TBD (deduplication, verification)
]
