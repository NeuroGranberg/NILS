/**
 * TypeScript types for QC Pipeline feature.
 */

// =============================================================================
// Axes Prediction QC Types
// =============================================================================

export type AxisType = 'base' | 'technique' | 'modifier' | 'provenance' | 'construct';
export type AxisFlagType = 'missing' | 'conflict' | 'low_confidence' | 'ambiguous' | 'review';

export interface AxesQCScanner {
  field_strength?: number;
  manufacturer?: string;
  model?: string;
}

export interface AxesQCParams {
  modality?: string;
  key?: string;
  te?: number;
  tr?: number;
  ti?: number;
  fa?: number;
  acq?: string;
  fov?: string;
}

export interface AxesQCTags {
  image_type?: string;
  scanning_seq?: string;
  seq_variant?: string;
  scan_options?: string;
  seq_name?: string;
  protocol?: string;
  description?: string;
  comments?: string;
}

export interface AxesQCCurrent {
  base: string | null;
  technique: string | null;
  modifier: string | null;
  provenance: string | null;
  construct: string | null;
  acceleration: string | null;
}

export interface AxesQCIntent {
  directory_type: string | null;
  spinal_cord: number | null;  // 1=yes, 0=no, null=unknown
  post_contrast: number | null;  // 1=yes, 0=no, null=unknown
}

// Draft changes keyed by column name (base, technique, modifier_csv, provenance, construct_csv)
export type AxesQCDraftChanges = Partial<Record<string, string | null>>;

export interface AxesQCItem {
  stack_id: number;
  series_uid: string;
  stack_index: number;
  subject_code: string | null;
  subject_id: number | null;
  study_date: string | null;
  scanner: AxesQCScanner;
  params: AxesQCParams;
  tags: AxesQCTags;
  flags: Partial<Record<AxisType, AxisFlagType>>;
  current: AxesQCCurrent;
  intent: AxesQCIntent;
  // Draft changes (pending, not yet saved to metadata_db)
  draft_changes?: AxesQCDraftChanges;
  has_draft?: boolean;
}

export interface AxesQCItemsResponse {
  items: AxesQCItem[];
  total: number;
  offset: number;
  limit: number;
}

export interface AxisOptions {
  base: string[];
  technique: string[];
  technique_metadata?: Record<string, { name: string; family: string }>;
  modifier: string[];
  provenance: (string | null)[];
  construct: string[];
}

// Flag display metadata
export const AXIS_FLAG_META: Record<AxisFlagType, { icon: string; color: string; label: string }> = {
  missing: { icon: 'IconQuestionMark', color: 'yellow', label: 'Missing' },
  conflict: { icon: 'IconAlertTriangle', color: 'orange', label: 'Conflict' },
  low_confidence: { icon: 'IconTrendingDown', color: 'blue', label: 'Low Confidence' },
  ambiguous: { icon: 'IconHelp', color: 'violet', label: 'Ambiguous' },
  review: { icon: 'IconEye', color: 'gray', label: 'Review' },
};
