/**
 * Type definitions for per-cohort classification keyword overrides.
 * Mirrors the Pydantic DTOs in backend/src/cohorts/models.py.
 */

export type ClassificationAxis =
  | 'base'
  | 'construct'
  | 'contrast'
  | 'modifier'
  | 'technique'
  | 'provenance'
  | 'body_part';

export interface KeywordBucketView {
  axis: ClassificationAxis | string;
  bucket_path: string;
  display_name: string;
  group_label: string | null;
  description: string | null;
  defaults: string[];
  added: string[];
  removed: string[];
  effective: string[];
}

export interface AxisView {
  axis: ClassificationAxis | string;
  label: string;
  description: string | null;
  buckets: KeywordBucketView[];
}

export interface CohortKeywordConfig {
  cohort_id: number;
  axes: AxisView[];
}

export interface BucketUpdatePayload {
  axis: string;
  bucket_path: string;
  added: string[];
  removed: string[];
}

/**
 * Visual state of a single keyword chip in the editor.
 *   'default'  - in global defaults, untouched by the user
 *   'added'    - user added on top of defaults
 *   'removed'  - in defaults, user marked for removal (strikethrough)
 */
export type ChipState = 'default' | 'added' | 'removed';
