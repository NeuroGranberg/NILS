/**
 * Centralized status color and configuration constants.
 *
 * This file provides a single source of truth for all status-related
 * colors, labels, and styling used throughout the application.
 */

import type { StageStatus } from '../types/stage';
import type { JobStatus } from '../types/job';

/** Configuration for status display */
export interface StatusConfig {
  /** Primary color (CSS variable) */
  color: string;
  /** Background color with opacity for badges */
  bgColor: string;
  /** Mantine color name for components */
  mantineColor: string;
  /** Human-readable label */
  label: string;
}

/**
 * Stage status configuration.
 * Used by PipelineStepper, StageCard, CohortCard, etc.
 */
export const STAGE_STATUS_CONFIG: Record<StageStatus, StatusConfig> = {
  idle: {
    color: 'var(--nils-stage-idle)',
    bgColor: 'rgba(110, 118, 129, 0.15)',
    mantineColor: 'gray',
    label: 'Idle',
  },
  pending: {
    color: 'var(--nils-stage-pending)',
    bgColor: 'rgba(163, 113, 247, 0.15)',
    mantineColor: 'violet',
    label: 'Pending',
  },
  running: {
    color: 'var(--nils-stage-running)',
    bgColor: 'rgba(88, 166, 255, 0.15)',
    mantineColor: 'blue',
    label: 'Running',
  },
  completed: {
    color: 'var(--nils-stage-completed)',
    bgColor: 'rgba(63, 185, 80, 0.15)',
    mantineColor: 'teal',
    label: 'Completed',
  },
  failed: {
    color: 'var(--nils-stage-failed)',
    bgColor: 'rgba(248, 81, 73, 0.15)',
    mantineColor: 'red',
    label: 'Failed',
  },
  paused: {
    color: 'var(--nils-stage-paused)',
    bgColor: 'rgba(210, 153, 34, 0.15)',
    mantineColor: 'yellow',
    label: 'Paused',
  },
  blocked: {
    color: 'var(--nils-stage-blocked)',
    bgColor: 'rgba(72, 79, 88, 0.15)',
    mantineColor: 'gray',
    label: 'Blocked',
  },
};

/**
 * Job status configuration.
 * Used by JobCard, JobHistoryTable, SystemJobCard, etc.
 */
export const JOB_STATUS_CONFIG: Record<JobStatus, StatusConfig> = {
  queued: {
    color: 'var(--nils-stage-pending)',
    bgColor: 'rgba(163, 113, 247, 0.15)',
    mantineColor: 'violet',
    label: 'Queued',
  },
  running: {
    color: 'var(--nils-stage-running)',
    bgColor: 'rgba(88, 166, 255, 0.15)',
    mantineColor: 'blue',
    label: 'Running',
  },
  paused: {
    color: 'var(--nils-stage-paused)',
    bgColor: 'rgba(210, 153, 34, 0.15)',
    mantineColor: 'yellow',
    label: 'Paused',
  },
  completed: {
    color: 'var(--nils-stage-completed)',
    bgColor: 'rgba(63, 185, 80, 0.15)',
    mantineColor: 'teal',
    label: 'Completed',
  },
  completed_with_warnings: {
    color: 'var(--nils-stage-completed)',
    bgColor: 'rgba(210, 153, 34, 0.15)',
    mantineColor: 'yellow',
    label: 'Completed with Warnings',
  },
  failed: {
    color: 'var(--nils-stage-failed)',
    bgColor: 'rgba(248, 81, 73, 0.15)',
    mantineColor: 'red',
    label: 'Failed',
  },
  canceled: {
    color: 'var(--nils-stage-idle)',
    bgColor: 'rgba(110, 118, 129, 0.15)',
    mantineColor: 'gray',
    label: 'Canceled',
  },
};

/**
 * Get status configuration for a stage status.
 */
export function getStageStatusConfig(status: StageStatus): StatusConfig {
  return STAGE_STATUS_CONFIG[status];
}

/**
 * Get status configuration for a job status.
 */
export function getJobStatusConfig(status: JobStatus): StatusConfig {
  return JOB_STATUS_CONFIG[status];
}

/**
 * Check if a status represents an active/in-progress state.
 */
export function isActiveStatus(status: StageStatus | JobStatus): boolean {
  return status === 'running' || status === 'pending' || status === 'queued';
}

/**
 * Check if a status requires user attention.
 */
export function isAttentionStatus(status: StageStatus | JobStatus): boolean {
  return status === 'failed' || status === 'paused';
}
