/**
 * React Query hooks for the global Body Part QC — sample pool + model registry.
 *
 * Endpoints:
 *   GET    /api/body-part-qc/pool/summary
 *   POST   /api/body-part-qc/pool/push
 *   GET    /api/body-part-qc/models
 *   POST   /api/body-part-qc/models/train
 *   GET    /api/body-part-qc/models/{model_id}
 *   POST   /api/body-part-qc/models/{model_id}/set-default
 *   DELETE /api/body-part-qc/models/{model_id}
 */
import { notifications } from '@mantine/notifications';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import { apiClient } from '../../../utils/api-client';
import type {
  BodyPartModelEntry,
  BodyPartPoolSummary,
  PushToPoolResult,
  TrainModelPayload,
} from './types';

export const globalBodyPartKeys = {
  poolSummary: ['body-part-qc', 'pool', 'summary'] as const,
  models: ['body-part-qc', 'models'] as const,
  model: (id: number) => ['body-part-qc', 'models', id] as const,
};

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

export const usePoolSummaryQuery = () =>
  useQuery<BodyPartPoolSummary>({
    queryKey: globalBodyPartKeys.poolSummary,
    queryFn: () => apiClient.get<BodyPartPoolSummary>('/body-part-qc/pool/summary'),
    staleTime: 30_000,
  });

export const usePushToPoolMutation = () => {
  const qc = useQueryClient();
  return useMutation<PushToPoolResult, Error, { cohort_id: number }>({
    mutationFn: (payload) =>
      apiClient.post<PushToPoolResult>('/body-part-qc/pool/push', payload),
    onSuccess: (result) => {
      qc.invalidateQueries({ queryKey: globalBodyPartKeys.poolSummary });
      notifications.show({
        title: 'Pushed to global pool',
        message: `${result.inserted} new, ${result.updated} updated. Pool total: ${result.total_pool_size}.`,
        color: 'green',
      });
    },
    onError: (err) =>
      notifications.show({
        title: 'Push failed',
        message: err.message,
        color: 'red',
      }),
  });
};

// ---------------------------------------------------------------------------
// Models
// ---------------------------------------------------------------------------

export const useModelsQuery = () =>
  useQuery<BodyPartModelEntry[]>({
    queryKey: globalBodyPartKeys.models,
    queryFn: () => apiClient.get<BodyPartModelEntry[]>('/body-part-qc/models'),
    staleTime: 30_000,
  });

export const useTrainModelMutation = () => {
  const qc = useQueryClient();
  return useMutation<BodyPartModelEntry, Error, TrainModelPayload>({
    mutationFn: (payload) =>
      apiClient.post<BodyPartModelEntry>('/body-part-qc/models/train', payload),
    onSuccess: (model) => {
      qc.invalidateQueries({ queryKey: globalBodyPartKeys.models });
      notifications.show({
        title: 'Model trained',
        message: `"${model.name}" — ${model.classes.join(', ')} — ${
          model.accuracy != null ? `${(model.accuracy * 100).toFixed(1)}%` : '—'
        } accuracy`,
        color: 'green',
      });
    },
    onError: (err) =>
      notifications.show({
        title: 'Model training failed',
        message: err.message,
        color: 'red',
      }),
  });
};

export const useSetDefaultModelMutation = () => {
  const qc = useQueryClient();
  return useMutation<BodyPartModelEntry, Error, number>({
    mutationFn: (modelId) =>
      apiClient.post<BodyPartModelEntry>(
        `/body-part-qc/models/${modelId}/set-default`,
      ),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: globalBodyPartKeys.models });
      notifications.show({
        title: 'Default model updated',
        message: 'This model will be used for cohorts without an explicit selection.',
        color: 'blue',
      });
    },
    onError: (err) =>
      notifications.show({
        title: 'Failed to set default',
        message: err.message,
        color: 'red',
      }),
  });
};

export const useDeleteModelMutation = () => {
  const qc = useQueryClient();
  return useMutation<unknown, Error, number>({
    mutationFn: (modelId) =>
      apiClient.delete(`/body-part-qc/models/${modelId}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: globalBodyPartKeys.models });
      notifications.show({
        title: 'Model deleted',
        message: 'Model removed from the registry.',
        color: 'orange',
      });
    },
    onError: (err) =>
      notifications.show({
        title: 'Delete failed',
        message: err.message,
        color: 'red',
      }),
  });
};
