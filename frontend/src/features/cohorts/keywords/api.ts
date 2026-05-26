/**
 * React Query hooks for cohort classification keyword overrides.
 *
 * Endpoints (see backend/src/api/routes/cohort_keywords.py):
 *   GET    /api/cohorts/{id}/classification-config
 *   PUT    /api/cohorts/{id}/classification-config/bucket
 *   DELETE /api/cohorts/{id}/classification-config/bucket?axis=..&bucket_path=..
 *   DELETE /api/cohorts/{id}/classification-config
 *   GET    /api/classification-config/catalog
 */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { notifications } from '@mantine/notifications';

import { apiClient } from '../../../utils/api-client';
import { STALE_TIMES } from '../../../constants/api';
import type {
  AxisView,
  BucketUpdatePayload,
  CohortKeywordConfig,
  KeywordBucketView,
} from './types';

const keywordKeys = {
  all: ['cohort-keywords'] as const,
  config: (cohortId: number | string) =>
    ['cohort-keywords', 'config', String(cohortId)] as const,
  catalog: ['cohort-keywords', 'catalog'] as const,
};

// ---------------------------------------------------------------------------
// Global catalog (defaults only, no cohort context)
// ---------------------------------------------------------------------------

export const useKeywordCatalogQuery = (enabled = true) =>
  useQuery<AxisView[]>({
    queryKey: keywordKeys.catalog,
    queryFn: () => apiClient.get<AxisView[]>('/classification-config/catalog'),
    staleTime: STALE_TIMES.long,
    enabled,
    refetchOnWindowFocus: false,
  });

// ---------------------------------------------------------------------------
// Cohort-scoped config
// ---------------------------------------------------------------------------

export const useCohortKeywordConfigQuery = (
  cohortId: number | string | undefined,
) =>
  useQuery<CohortKeywordConfig>({
    queryKey: keywordKeys.config(cohortId ?? ''),
    queryFn: () =>
      apiClient.get<CohortKeywordConfig>(
        `/cohorts/${cohortId}/classification-config`,
      ),
    enabled: cohortId !== undefined && cohortId !== null && cohortId !== '',
    staleTime: STALE_TIMES.medium,
    refetchOnWindowFocus: false,
  });

export const useUpdateKeywordBucketMutation = (cohortId: number | string) => {
  const queryClient = useQueryClient();

  return useMutation<KeywordBucketView, Error, BucketUpdatePayload>({
    mutationFn: (payload) =>
      apiClient.put<KeywordBucketView>(
        `/cohorts/${cohortId}/classification-config/bucket`,
        payload,
      ),
    onSuccess: (updated) => {
      // Patch the single bucket into the cached config (avoid full refetch).
      queryClient.setQueryData<CohortKeywordConfig | undefined>(
        keywordKeys.config(cohortId),
        (prev) => {
          if (!prev) return prev;
          return {
            ...prev,
            axes: prev.axes.map((axis) => {
              if (axis.axis !== updated.axis) return axis;
              return {
                ...axis,
                buckets: axis.buckets.map((b) =>
                  b.bucket_path === updated.bucket_path ? updated : b,
                ),
              };
            }),
          };
        },
      );
      notifications.show({
        title: 'Keywords saved',
        message: 'Changes will apply on the next sort run.',
        color: 'green',
      });
    },
    onError: (error) => {
      notifications.show({
        title: 'Failed to save keywords',
        message: error.message,
        color: 'red',
      });
    },
  });
};

export const useResetKeywordBucketMutation = (cohortId: number | string) => {
  const queryClient = useQueryClient();

  return useMutation<KeywordBucketView, Error, { axis: string; bucket_path: string }>({
    mutationFn: ({ axis, bucket_path }) =>
      apiClient.delete<KeywordBucketView>(
        `/cohorts/${cohortId}/classification-config/bucket?axis=${encodeURIComponent(
          axis,
        )}&bucket_path=${encodeURIComponent(bucket_path)}`,
      ),
    onSuccess: (updated) => {
      queryClient.setQueryData<CohortKeywordConfig | undefined>(
        keywordKeys.config(cohortId),
        (prev) => {
          if (!prev) return prev;
          return {
            ...prev,
            axes: prev.axes.map((axis) => {
              if (axis.axis !== updated.axis) return axis;
              return {
                ...axis,
                buckets: axis.buckets.map((b) =>
                  b.bucket_path === updated.bucket_path ? updated : b,
                ),
              };
            }),
          };
        },
      );
      notifications.show({
        title: 'Bucket reset',
        message: 'Reverted to global defaults.',
        color: 'blue',
      });
    },
    onError: (error) => {
      notifications.show({
        title: 'Reset failed',
        message: error.message,
        color: 'red',
      });
    },
  });
};

export const useResetAllKeywordsMutation = (cohortId: number | string) => {
  const queryClient = useQueryClient();

  return useMutation<void, Error, void>({
    mutationFn: () =>
      apiClient.delete<void>(`/cohorts/${cohortId}/classification-config`),
    onSuccess: () => {
      void queryClient.invalidateQueries({
        queryKey: keywordKeys.config(cohortId),
      });
      notifications.show({
        title: 'All keywords reset',
        message: 'Every bucket reverted to global defaults.',
        color: 'blue',
      });
    },
    onError: (error) => {
      notifications.show({
        title: 'Reset failed',
        message: error.message,
        color: 'red',
      });
    },
  });
};

export const keywordQueryKeys = keywordKeys;
