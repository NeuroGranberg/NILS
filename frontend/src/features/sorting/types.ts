/**
 * Types for the sorting pipeline UI.
 */

export type SortingStepId =
  | 'checkup'
  | 'stack_fingerprint'
  | 'classification'
  | 'completion';
  // Future steps TBD (deduplication, verification)

export type SortingStepStatus = 'pending' | 'running' | 'complete' | 'warning' | 'error' | 'skipped';

export interface SortingStep {
  id: SortingStepId;
  title: string;
  description: string;
}

export const SORTING_STEPS: SortingStep[] = [
  {
    id: 'checkup',
    title: 'Checkup',
    description: 'Verify cohort scope and data integrity',
  },
  {
    id: 'stack_fingerprint',
    title: 'Stack Fingerprint',
    description: 'Build classification features for each stack',
  },
  {
    id: 'classification',
    title: 'Classification',
    description: 'Classify each stack using detection rules',
  },
  {
    id: 'completion',
    title: 'Completion',
    description: 'Fill gaps and flag for review',
  },
  // Future steps TBD (deduplication, verification)
];

export interface SortingConfig {
  skipClassified: boolean;
  forceReprocess: boolean;
  profile: 'standard' | 'neuro' | 'cardiac' | 'oncology';
  selectedModalities: string[];
  previewMode?: boolean;  // Step 2 only: generate preview without DB insert
}

export interface Step1Metrics {
  subjects_in_cohort: number;
  total_studies: number;
  studies_with_valid_date: number;
  studies_date_imputed: number;
  studies_excluded_no_date: number;
  total_series: number;
  series_already_classified: number;
  series_to_process_count: number;
  series_by_modality?: Record<string, number>;
  selected_modalities?: string[];
  excluded_study_uids?: string[];
  skipped_series_uids?: string[];
  validation_passed?: boolean;
  warnings?: string[];
  errors?: string[];
}

export interface StackPreviewRow {
  series_id: number;
  stack_index: number;
  stack_key: string;
  instance_count: number;
  modality?: string;
  image_orientation_patient?: string;
  pixel_spacing?: string;
  rows?: number;
  columns?: number;
  slice_thickness?: number;
  echo_time?: number;
  repetition_time?: number;
  flip_angle?: number;
}

export interface Step2Metrics {
  // Fingerprint metrics
  total_fingerprints_created: number;
  stacks_processed: number;
  stacks_with_missing_fov: number;
  stacks_with_contrast: number;
  breakdown_by_modality?: Record<string, number>;
  breakdown_by_manufacturer?: Record<string, number>;
  // Modality-specific stats
  mr_stacks_with_3d: number;
  mr_stacks_with_diffusion: number;
  ct_stacks_calcium_score: number;
  pet_stacks_attn_corrected: number;
  // Stack analysis
  series_with_multiple_stacks: number;
  series_with_single_stack: number;
  max_stacks_per_series: number;
  // Orientation confidence
  stacks_with_low_confidence: number;
  avg_orientation_confidence?: number;
  min_orientation_confidence?: number;
  // QC examples
  multi_stack_examples?: Array<{
    series_id: number;
    stack_count: number;
    modality: string;
  }>;
  warnings?: string[];
  errors?: string[];
}

export interface Step3Metrics {
  // Primary counts
  total_classified: number;
  excluded_count: number;
  review_required_count: number;
  // Breakdown by classification axes
  breakdown_by_directory_type?: Record<string, number>;
  breakdown_by_provenance?: Record<string, number>;
  breakdown_by_base?: Record<string, number>;
  breakdown_by_technique?: Record<string, number>;
  // Review-related
  low_confidence_axes?: Record<string, number>;
  review_reasons?: Record<string, number>;
  // Special flags
  spine_detected_count: number;
  post_contrast_count: number;
  localizer_count: number;
  // Status
  warnings?: string[];
  errors?: string[];
}

export interface Step4Metrics {
  // Totals
  total_processed: number;
  // Phase 1: Orientation
  orientation_flagged_count: number;
  // Phase 2: Acquisition type
  acquisition_type_filled_count: number;
  acquisition_type_by_method?: Record<string, number>;
  // Phase 3: Base & Technique
  base_filled_count: number;
  technique_filled_count: number;
  stacks_with_no_match: number;
  similarity_match_counts?: Record<string, number>;
  // Phase 4: Intent
  misc_initial_count: number;
  misc_resolved_count: number;
  misc_remaining_count: number;
  resolved_to?: Record<string, number>;
  // Review summary
  stacks_newly_flagged: number;
  new_review_reasons?: Record<string, number>;
  // Status
  warnings?: string[];
  errors?: string[];
}

export interface StepState {
  status: SortingStepStatus;
  progress: number;
  message?: string;
  metrics?: Step1Metrics | Step2Metrics | Step3Metrics | Step4Metrics | Record<string, unknown>;
  error?: string;
}

export interface SortingJobInfo {
  job_id: number;
  stream_url: string;
}

export interface DateRecoveryConfig {
  minYear: number;
  maxYear: number;
}

export interface DateRecoveryResult {
  recovered_count: number;
  failed_count: number;
  recovered_study_ids: number[];
  updated_metrics?: Step1Metrics;
}

// SSE Event types
export interface SSEStepStartEvent {
  type: 'step_start';
  step_id: string;
  step_title: string;
}

export interface SSEStepProgressEvent {
  type: 'step_progress';
  step_id: string;
  progress: number;
  message?: string;
  metrics?: Record<string, unknown>;
  current_action?: string;
}

export interface SSEStepCompleteEvent {
  type: 'step_complete';
  step_id: string;
  metrics?: Record<string, unknown>;
}

export interface SSEStepErrorEvent {
  type: 'step_error';
  step_id: string;
  error: string;
  metrics?: Record<string, unknown>;
}

export interface SSEPipelineCompleteEvent {
  type: 'pipeline_complete';
  summary?: {
    steps_completed: number;
    total_steps: number;
    series_to_process: number;
    stacks_discovered?: number;
    stacks_classified?: number;
    gaps_filled?: number;
    review_required?: number;
    processing_mode: string;
  };
}

export interface SSEPipelineErrorEvent {
  type: 'pipeline_error';
  error: string;
}

export type SSEEvent =
  | SSEStepStartEvent
  | SSEStepProgressEvent
  | SSEStepCompleteEvent
  | SSEStepErrorEvent
  | SSEPipelineCompleteEvent
  | SSEPipelineErrorEvent;
