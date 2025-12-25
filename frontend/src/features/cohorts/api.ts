/**
 * Cohort API hooks.
 *
 * Performance optimized:
 * - Added staleTime to reduce unnecessary refetches
 * - Increased refetch intervals (list: 30s, detail: 15s)
 * - Disabled refetchOnWindowFocus for less aggressive polling
 */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { notifications } from '@mantine/notifications';
import type { Cohort, CohortStageRequest, CreateCohortPayload, StageSummary } from '../../types';
import { apiClient } from '../../utils/api-client';
import { QUERY_KEYS, STALE_TIMES } from '../../constants/api';

export const COHORTS_QUERY_KEY = QUERY_KEYS.cohorts;

export const useCohortsQuery = () =>
  useQuery<Cohort[]>({
    queryKey: QUERY_KEYS.cohorts,
    queryFn: () => apiClient.get<Cohort[]>('/cohorts'),
    staleTime: STALE_TIMES.short, // Cache for 30s before considering stale
    // No refetchInterval - manual refresh or invalidation only (memory optimization)
    refetchOnWindowFocus: false, // Don't refetch on tab focus
  });

export const useCohortQuery = (cohortId: string | undefined) =>
  useQuery<Cohort>({
    queryKey: QUERY_KEYS.cohort(cohortId ?? ''),
    queryFn: () => apiClient.get<Cohort>(`/cohorts/${cohortId}`),
    enabled: Boolean(cohortId),
    staleTime: 10 * 1000, // Cache for 10s (detail view needs fresher data)
    refetchInterval: 30000, // Poll every 30s for detail view (reduced from 15s)
    refetchOnWindowFocus: false,
  });

export const useCreateCohortMutation = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (payload: CreateCohortPayload) => apiClient.post<Cohort>('/cohorts', payload),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.cohorts });
      notifications.show({
        title: 'Cohort Created',
        message: `Created cohort: ${data.name}`,
        color: 'green',
      });
    },
    onError: (error: Error) => {
      notifications.show({
        title: 'Create Failed',
        message: error.message,
        color: 'red',
      });
    },
  });
};

export const useRunStageMutation = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ cohort_id, stage_id, config }: CohortStageRequest) =>
      apiClient.post<StageSummary>(`/cohorts/${cohort_id}/stages/${stage_id}/run`, config),
    onSuccess: (_data, variables) => {
      notifications.show({
        title: 'Stage Started',
        message: `Started ${variables.stage_id} stage`,
        color: 'green',
      });
    },
    onError: (error: Error) => {
      notifications.show({
        title: 'Stage Failed',
        message: error.message,
        color: 'red',
      });
    },
    onSettled: (_data, _error, variables) => {
      // Only invalidate the specific cohort, not the entire list
      // This prevents refetching all cohorts when one job changes
      if (variables?.cohort_id != null) {
        void queryClient.invalidateQueries({
          queryKey: QUERY_KEYS.cohort(variables.cohort_id.toString()),
        });
      }
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.jobs });
    },
  });
};
