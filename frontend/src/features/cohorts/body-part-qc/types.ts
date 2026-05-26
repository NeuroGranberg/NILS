/**
 * Type definitions for the cohort-wide Body Part QC.
 * Mirrors the Pydantic DTOs in backend/src/cohorts/models.py.
 */

export interface BodyPartCandidate {
  stack_id: number;
  slice_index: number;
  orientation: string;          // "axial" | "sagittal" | "coronal" | "unknown"
  zs_prob: number;
  margin: number;
  top2_label: string | null;
  series_instance_uid: string;
  study_id: number;
  subject_code: string;
  session_date: string | null;
  thumbnail_url: string;
}

export interface BodyPartTrainingSample {
  stack_id: number;
  slice_index: number;
  label: string;
  orientation: string;
  approved_at: string;
  thumbnail_url: string | null;
  subject_code: string | null;
}

export interface BodyPartOverrideConflict {
  /** What the new model predicts for this stack on re-Apply. */
  label: string;
  /** Top probability of that prediction (≥ override_conflict_prob). */
  prob: number;
}

export interface BodyPartStackPick {
  stack_id: number;
  label: string | null;
  confidence: number | null;
  probs: Record<string, number>;
  is_override: boolean;
  needs_check: boolean;
  series_instance_uid: string | null;
  technique: string | null;
  orientation: string | null;
  previous_label: string | null;
  prior_source: string | null;     // "text_keyword" | "qc_v1" | "manual" | null
  changed: boolean;
  /** Set when the user override survived re-Apply but the new model
   * strongly disagrees with the kept label (≥ override_conflict_prob).
   * Null when there is no disagreement. */
  override_conflict?: BodyPartOverrideConflict | null;
}

export interface BodyPartSessionPick {
  /**
   * Sessions are keyed by (subject_id, session_date) — one calendar visit
   * may include multiple StudyInstanceUIDs (e.g. brain study + spine
   * study), and they are aggregated into a single session.
   */
  subject_id: number;
  session_date: string | null;
  subject_code: string;
  /** All study_ids contributing to this session, ascending. */
  study_ids: number[];
  /** Canonical study (most stacks; ties → min). Used for any single-study fallback. */
  primary_study_id: number;
  stacks: BodyPartStackPick[];
  session_combo: string[];
  session_combo_key: string;
  session_prev_combo_key: string;
  session_changed: boolean;
  stacks_changed: number;
  low_conf_count: number;
  needs_check: boolean;
  subject_other_ids: Record<string, string>;
}

export interface BodyPartSummary {
  total_sessions: number;
  by_combo: Record<string, number>;
  needs_check: number;
  total_stacks: number;
  stacks_changed: number;
  sessions_changed: number;
  change_matrix: Record<string, Record<string, number>>;
  /** Number of stacks whose carried-forward override conflicts with the
   * new model's prediction. Surface in the Changes pane. */
  override_conflicts_count: number;
}

/** Stage of the cohort's current_picks vs. the metadata DB.
 *
 * - `none`: no Apply yet.
 * - `staged`: Apply produced picks but they have NOT been written to the
 *   metadata DB. The user must commit.
 * - `committed`: current_picks match the most recent commit. Edits flip
 *   to `dirty`.
 * - `dirty`: there were committed picks, then the user made edits
 *   (override / reset) so the on-disk state has been updated for those
 *   sessions but the cohort no longer has a single coherent committed
 *   snapshot. Re-commit to clear.
 */
export type BodyPartStageStatus = 'none' | 'staged' | 'committed' | 'dirty';

/** Per-category training counts keyed by orientation. */
export interface BodyPartTrainingSummaryEntry {
  axial: number;
  sagittal: number;
  coronal: number;
  total: number;
}

export interface BodyPartState {
  cohort_id: number;
  has_current: boolean;
  has_previous: boolean;
  current_run_at: string | null;
  previous_run_at: string | null;
  categories: string[];
  training_summary: Record<string, BodyPartTrainingSummaryEntry>;
  classifier_meta: Record<string, unknown> | null;
  summary: BodyPartSummary;
  picks: BodyPartSessionPick[];
  profile: Record<string, unknown>;
  available_id_types: string[];
  /** Stage→commit gate (Milestone C). */
  stage_status: BodyPartStageStatus;
  /** UTC timestamp of the most recent commit (null until first commit). */
  last_committed_at: string | null;
  /** Number of stacks with pending changes vs. the metadata DB; only
   * non-zero in `staged` or `dirty` states. */
  pending_changes_count: number;
  /** ID of the selected global model (null = use legacy per-cohort classifier). */
  selected_model_id: number | null;
  /** Display name of the selected model (null if none selected). */
  selected_model_name: string | null;
}

// ---------------------------------------------------------------------------
// Diff / Changes pane
// ---------------------------------------------------------------------------

export interface BodyPartChangeRow {
  /** Owning StudyInstanceUID for this stack (used for traceability + image paths). */
  study_id: number;
  /** Session this row belongs to: (subject_id, session_date). */
  subject_id: number;
  stack_id: number;
  subject_code: string;
  session_date: string | null;
  series_description: string | null;
  technique: string | null;
  orientation: string | null;
  previous_label: string | null;
  new_label: string;
  prior_source: string | null;
  confidence: number;
  needs_check: boolean;
  is_override: boolean;
  thumbnail_url: string;
  /** Middle slice (num_slices // 2) for visual review. */
  middle_slice_url?: string | null;
  /** Present when the user's override conflicts with the latest model
   * prediction (Milestone B). UI should render a "model says X" badge. */
  override_conflict?: BodyPartOverrideConflict | null;
}

export interface BodyPartChangesPage {
  total: number;
  offset: number;
  limit: number;
  rows: BodyPartChangeRow[];
}

// ---------------------------------------------------------------------------
// Request payloads
// ---------------------------------------------------------------------------

export interface BodyPartCategoriesPayload {
  categories: string[];
}

export interface BodyPartSeedPayload {
  category: string;
  n_target?: number;
}

export type BodyPartSampleOpKind = 'approve' | 'remove' | 'replace' | 'move';

export interface BodyPartSampleOp {
  op: BodyPartSampleOpKind;
  stack_id: number;
  slice_index: number;
  label?: string | null;
  new_label?: string | null;
}

export interface BodyPartSamplesPayload {
  ops: BodyPartSampleOp[];
}

export interface BodyPartOverridePayload {
  subject_id: number;
  session_date: string;
  stack_id: number;
  label: string;
  note?: string | null;
}

export interface BodyPartSessionResetPayload {
  subject_id: number;
  session_date: string;
}

export interface BodyPartCommitPayload {
  stack_ids?: number[];
  min_confidence?: number;
  from_label?: string;
  to_label?: string;
}

export interface BodyPartDestagePayload {
  stack_ids: number[];
}

export interface BodyPartResetResponse {
  tokens_removed: number;
  had_state: boolean;
}

export interface SelectModelPayload {
  model_id: number | null;
}

// ---------------------------------------------------------------------------
// Global model registry
// ---------------------------------------------------------------------------

export interface BodyPartPoolSummary {
  by_label: Record<string, number>;
  total: number;
}

export interface BodyPartModelEntry {
  id: number;
  name: string;
  description: string | null;
  classes: string[];
  label_remap: Record<string, string>;
  accuracy: number | null;
  n_samples: number;
  trained_at: string;
  is_default: boolean;
  meta: Record<string, unknown>;
}

export interface TrainModelPayload {
  name: string;
  classes: string[];
  label_remap: Record<string, string>;
  description?: string;
  estimator_kind?: string;   // "logreg" | "rf" | "svm"
  use_pca?: boolean;         // default true
  pca_components?: number | null;  // null → auto-tune picks
}

export interface PushToPoolResult {
  inserted: number;
  updated: number;
  total_pool_size: number;
}
