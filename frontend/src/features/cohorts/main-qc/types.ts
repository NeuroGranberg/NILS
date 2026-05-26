/**
 * Type definitions for the cohort-wide Main Acquisition QC.
 * Mirrors the Pydantic DTOs in backend/src/cohorts/models.py.
 */

export type MainQCAxis = 't1w' | 'flair';

export type AxisDisplayMode = 't1w' | 'flair' | 'both';

export type Density = 'compact' | 'medium' | 'comfortable';

export interface MainQCSummary {
  green: number;
  amber: number;
  red: number;
  needs_check: number;
  total_sessions: number;
}

export interface MainQCCandidate {
  stack_id: number;
  score: number;
  technique: string | null;
  modifier_csv: string | null;
  construct_csv: string | null;
  dim: string | null; // "2D" | "3D" | null
  slices: number | null;
  fov_x_mm: number | null;
  post_contrast: number | null;
  provenance: string | null;
  is_main: boolean;
  in_winning_family: boolean;
  is_canonical_in_family: boolean;
  /** Image-addressing fields (added for the cohort Main QC modal preview). */
  series_instance_uid?: string | null;
  stack_index?: number;
  /** Display fields used by the modal tile caption. */
  orientation?: string | null;       // "Axial" | "Coronal" | "Sagittal"
  base?: string | null;              // "T1w" | "T2w_FLAIR" | ...
  fov_y_mm?: number | null;
  pixsp_row_mm?: number | null;
  pixsp_col_mm?: number | null;
  slice_thickness_mm?: number | null;
}

export interface MainQCSessionContent {
  technique: string | null;
  dim: string | null;          // "2D" | "3D" | null
  family: string | null;       // "dixon" | "waterexc" | "plain" | null
  slice_bucket: string | null; // "hi" | "std" | "lo" | null
  slices: number | null;
}

export interface MainQCSessionPick {
  /**
   * Session identity = ``(subject_id, session_date)``.
   * A single calendar visit can span multiple StudyInstanceUIDs (brain study
   * + spine study, etc.); they are aggregated into one session.
   */
  subject_id: number;
  session_date: string | null;
  subject_code: string;
  axis: MainQCAxis;
  /** All study_ids contributing to this session, sorted ascending. */
  study_ids: number[];
  /**
   * Primary study (the one owning the winning stack, or the lowest study_id
   * when no winner) — used for image previews and any single-study fallback.
   */
  primary_study_id: number;
  winning_stack_ids: number[];
  score: number | null;
  needs_check: boolean;
  needs_check_reasons: string[];
  candidate_summary: MainQCCandidate[];
  content: MainQCSessionContent | null;
  subject_other_ids: Record<string, string>;
  /** User has reviewed this pick and confirmed the algorithm's choice. */
  acknowledged?: boolean;
}

export interface MainQCSessionAcknowledgePayload {
  subject_id: number;
  session_date: string;
  axis: MainQCAxis;
}

export interface MainQCState {
  cohort_id: number;
  has_current: boolean;
  has_previous: boolean;
  current_run_at: string | null;
  previous_run_at: string | null;
  summary: Partial<Record<MainQCAxis, MainQCSummary>>;
  picks: MainQCSessionPick[];
  profile: Record<string, unknown>;
  available_id_types: string[];
}

export interface MainQCSessionPickPayload {
  subject_id: number;
  session_date: string;
  axis: MainQCAxis;
  stack_ids: number[];
  note?: string | null;
}

export interface MainQCSessionResetPayload {
  subject_id: number;
  session_date: string;
  axis: MainQCAxis;
}

// ---------------------------------------------------------------------------
// Legend filter — multi-axis content filter
// ---------------------------------------------------------------------------

/**
 * The content filter is the union of selections across multiple axes.
 *
 * Semantics:
 *   - AND across axes (a cell must match every non-empty axis)
 *   - OR  within an axis (any selected chip on that axis is enough)
 *   - Empty axis = no constraint on that axis
 */
export interface ContentFilter {
  dim: string[];           // "3D" | "2D" | "unknown"
  technique: string[];     // dynamic from picks
  family: string[];        // "dixon" | "waterexc" | "plain"
  slice_bucket: string[];  // "hi" | "std" | "lo"
  border: string[];        // "needs_check" | "manual_pick"
}

export const EMPTY_FILTER: ContentFilter = {
  dim: [], technique: [], family: [], slice_bucket: [], border: [],
};

/** Border colors for flag/manual overrides. */
export const CELL_BORDER_COLORS: Record<'needs-check' | 'manual-pick', string> = {
  'needs-check': '#fd7e14',
  'manual-pick': '#1c7ed6',
};

/** Empty / no-eligible-stacks fill — used outside the score gradient. */
export const EMPTY_CELL_FILL = '#dee2e6';
