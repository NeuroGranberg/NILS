import type { StageId } from './stage';

export type JobStatus = 'queued' | 'running' | 'paused' | 'completed' | 'failed' | 'canceled';

export interface JobPerformanceMetrics {
  [key: string]: unknown;
}

export interface JobMetrics {
  subjects: number;
  studies: number;
  series: number;
  instances: number;
  safe_batch_rows?: number;
  performance?: JobPerformanceMetrics;
}

export interface JobSummary {
  id: number;
  stageId: StageId | string;
  stepId?: string | null;
  status: JobStatus;
  progress: number;
  submittedAt: string;
  startedAt?: string | null;
  finishedAt?: string | null;
  elapsedMs?: number | null;
  etaMs?: number | null;
  totalMs?: number | null;
  errorMessage?: string | null;
  cohortId?: number | null;
  cohortName?: string | null;
  config: Record<string, unknown>;
  metrics?: JobMetrics;
}

export type Job = JobSummary;

export type JobAction = 'pause' | 'resume' | 'cancel' | 'retry';
