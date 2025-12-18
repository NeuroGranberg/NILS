/**
 * API functions for the sorting pipeline.
 */

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { apiClient } from '../../utils/api-client';
import type { SortingConfig, SortingJobInfo, SortingStep, Step1Metrics, Step2Metrics, DateRecoveryConfig, DateRecoveryResult } from './types';

// Query keys
export const sortingKeys = {
  all: ['sorting'] as const,
  steps: (cohortId: number) => [...sortingKeys.all, 'steps', cohortId] as const,
  status: (cohortId: number) => [...sortingKeys.all, 'status', cohortId] as const,
  stepMetrics: (cohortId: number, stepId: string) => [...sortingKeys.all, 'metrics', cohortId, stepId] as const,
};

interface SortingRunResponse {
  job_id: number;
  stream_url: string;
}

interface SortingStepsResponse {
  steps: SortingStep[];
}

/**
 * Response from GET /cohorts/{id}/stages/sort/status
 */
export interface SortingStatusResponse {
  steps: Record<string, 'completed' | 'pending'>;
  metrics: Record<string, Step1Metrics | Step2Metrics>;  // step_id -> metrics (any step type)
  next_step: string | null;
}

/**
 * Response from GET /cohorts/{id}/stages/sort/steps/{step_id}/metrics
 */
interface StepMetricsResponse {
  step_id: string;
  metrics: Step1Metrics;
}

/**
 * Start the sorting pipeline for a cohort.
 */
export const startSorting = async (
  cohortId: number,
  config: Partial<SortingConfig>,
): Promise<SortingJobInfo> => {
  const response = await apiClient.post<SortingRunResponse>(`/cohorts/${cohortId}/stages/sort/run`, {
    skipClassified: config.skipClassified ?? true,
    forceReprocess: config.forceReprocess ?? false,
    profile: config.profile ?? 'standard',
    selectedModalities: config.selectedModalities ?? ['MR', 'CT', 'PT'],
  });
  return {
    job_id: response.job_id,
    stream_url: response.stream_url,
  };
};

/**
 * Get sorting step metadata.
 */
export const getSortingSteps = async (cohortId: number): Promise<SortingStep[]> => {
  const response = await apiClient.get<SortingStepsResponse>(`/cohorts/${cohortId}/stages/sort/steps`);
  return response.steps;
};

/**
 * Run a single sorting step independently (step-wise execution).
 */
export const runSortingStep = async (
  cohortId: number,
  stepId: string,
  config: Partial<SortingConfig>,
): Promise<SortingJobInfo> => {
  console.log('[API] runSortingStep called:', { cohortId, stepId, config });

  try {
    const response = await apiClient.post<SortingRunResponse>(`/cohorts/${cohortId}/stages/sort/run-step/${stepId}`, {
      skipClassified: config.skipClassified ?? true,
      forceReprocess: config.forceReprocess ?? false,
      profile: config.profile ?? 'standard',
      selectedModalities: config.selectedModalities ?? ['MR', 'CT', 'PT'],
      previewMode: config.previewMode ?? false,  // Support preview mode (Step 2 only)
    });

    console.log('[API] runSortingStep response:', response);

    return {
      job_id: response.job_id,
      stream_url: response.stream_url,
    };
  } catch (error) {
    console.error('[API] runSortingStep error:', error);
    throw error;
  }
};

/**
 * Re-run a specific sorting step.
 */
export const rerunSortingStep = async (
  cohortId: number,
  stepId: string,
): Promise<{ stream_url: string }> => {
  // This returns an SSE stream directly, so we just need the URL
  return {
    stream_url: `/api/cohorts/${cohortId}/stages/sort/rerun/${stepId}`,
  };
};

/**
 * Get sorting status (completed steps and their metrics).
 */
export const getSortingStatus = async (cohortId: number): Promise<SortingStatusResponse> => {
  return apiClient.get<SortingStatusResponse>(`/cohorts/${cohortId}/stages/sort/status`);
};

/**
 * Get metrics for a specific sorting step.
 */
export const getStepMetrics = async (cohortId: number, stepId: string): Promise<Step1Metrics> => {
  const response = await apiClient.get<StepMetricsResponse>(`/cohorts/${cohortId}/stages/sort/steps/${stepId}/metrics`);
  return response.metrics;
};

// React Query hooks

/**
 * Hook to get sorting step metadata.
 */
export const useSortingSteps = (cohortId: number) => {
  return useQuery({
    queryKey: sortingKeys.steps(cohortId),
    queryFn: () => getSortingSteps(cohortId),
    enabled: !!cohortId,
  });
};

/**
 * Hook to start the sorting pipeline.
 */
export const useStartSorting = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ cohortId, config }: { cohortId: number; config: Partial<SortingConfig> }) =>
      startSorting(cohortId, config),
    onSuccess: () => {
      // Invalidate cohort queries to refresh stage status
      queryClient.invalidateQueries({ queryKey: ['cohorts'] });
    },
  });
};

/**
 * Hook to run a single sorting step (step-wise execution).
 */
export const useRunSortingStep = () => {
  return useMutation({
    mutationFn: ({
      cohortId,
      stepId,
      config,
    }: {
      cohortId: number;
      stepId: string;
      config: Partial<SortingConfig>;
    }) => runSortingStep(cohortId, stepId, config),
  });
};

/**
 * Hook to re-run a sorting step.
 */
export const useRerunSortingStep = () => {
  return useMutation({
    mutationFn: ({
      cohortId,
      stepId,
    }: {
      cohortId: number;
      stepId: string;
    }) => rerunSortingStep(cohortId, stepId),
  });
};

/**
 * Hook to get sorting status (completed steps and metrics).
 * This is the main hook for restoring state when returning to a cohort.
 */
export const useSortingStatus = (cohortId: number) => {
  return useQuery({
    queryKey: sortingKeys.status(cohortId),
    queryFn: () => getSortingStatus(cohortId),
    enabled: !!cohortId,
    staleTime: 1000 * 60, // 1 minute
  });
};

/**
 * Hook to get metrics for a specific step.
 */
export const useStepMetrics = (cohortId: number, stepId: string, enabled: boolean = true) => {
  return useQuery({
    queryKey: sortingKeys.stepMetrics(cohortId, stepId),
    queryFn: () => getStepMetrics(cohortId, stepId),
    enabled: enabled && !!cohortId && !!stepId,
  });
};

/**
 * Recover missing study dates from DICOM UIDs.
 */
export const recoverMissingDates = async (
  cohortId: number,
  config: DateRecoveryConfig
): Promise<DateRecoveryResult> => {
  return apiClient.post(`/cohorts/${cohortId}/stages/sort/recover-dates`, {
    min_year: config.minYear,
    max_year: config.maxYear,
  });
};

/**
 * Hook to recover missing dates.
 */
export const useRecoverDates = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ cohortId, config }: {
      cohortId: number;
      config: DateRecoveryConfig
    }) => recoverMissingDates(cohortId, config),
    onSuccess: (_, { cohortId }) => {
      // Invalidate sorting status to refresh metrics
      queryClient.invalidateQueries({
        queryKey: sortingKeys.status(cohortId)
      });
    },
  });
};
