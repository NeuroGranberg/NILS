import type { StageSummary, StageId, StageStatus } from './stage';
import type { AnonymizeStageConfig } from './anonymize';
import type { JobSummary } from './job';

export interface Cohort {
  id: number;
  name: string;
  description?: string;
  source_path: string;
  created_at: string;
  updated_at: string;
  anonymization_enabled: boolean;
  tags: string[];
  status: StageStatus;
  total_subjects: number;
  total_sessions: number;
  total_series: number;
  completion_percentage: number;
  stages: StageSummary[];
  anonymize_job?: JobSummary | null;
  anonymize_history?: JobSummary[];
  extract_job?: JobSummary | null;
  extract_history?: JobSummary[];
}

export interface CohortStageRequest {
  cohort_id: number;
  stage_id: StageId;
  config: Record<string, unknown>;
}

export interface CreateCohortPayload {
  name: string;
  description?: string;
  source_path: string;
  anonymization_enabled: boolean;
  tags: string[];
  anonymize_config?: AnonymizeStageConfig;
}

export type CohortSummary = Pick<
  Cohort,
  'id' | 'name' | 'tags' | 'created_at' | 'stages'
>;
