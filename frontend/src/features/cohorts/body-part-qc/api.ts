/**
 * React Query hooks for the cohort-wide Body Part QC.
 *
 * Endpoints (see backend/src/api/routes/cohort_body_part_qc.py):
 *   GET    /api/cohorts/{id}/body-part-qc
 *   PUT    /api/cohorts/{id}/body-part-qc/categories
 *   POST   /api/cohorts/{id}/body-part-qc/seed
 *   POST   /api/cohorts/{id}/body-part-qc/samples
 *   POST   /api/cohorts/{id}/body-part-qc/train
 *   POST   /api/cohorts/{id}/body-part-qc/apply
 *   POST   /api/cohorts/{id}/body-part-qc/commit
 *   POST   /api/cohorts/{id}/body-part-qc/restore-previous
 *   GET    /api/cohorts/{id}/body-part-qc/changes
 *   POST   /api/cohorts/{id}/sessions/{study_id}/body-part-qc/override
 *   POST   /api/cohorts/{id}/sessions/{study_id}/body-part-qc/reset
 */
import { notifications } from '@mantine/notifications';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import { STALE_TIMES } from '../../../constants/api';
import { apiClient } from '../../../utils/api-client';
import type {
  BodyPartCandidate,
  BodyPartCategoriesPayload,
  BodyPartChangesPage,
  BodyPartCommitPayload,
  BodyPartDestagePayload,
  BodyPartOverridePayload,
  BodyPartResetResponse,
  BodyPartSamplesPayload,
  BodyPartSeedPayload,
  BodyPartSessionPick,
  BodyPartSessionResetPayload,
  BodyPartState,
  BodyPartTrainingSample,
  SelectModelPayload,
} from './types';

/** Match a session pick by (subject_id, session_date) — the new session key. */
const sameSession = (a: BodyPartSessionPick, b: BodyPartSessionPick) =>
  a.subject_id === b.subject_id && a.session_date === b.session_date;

export const bodyPartQCKeys = {
  all: ['cohort-body-part-qc'] as const,
  state: (cohortId: number | string) =>
    ['cohort-body-part-qc', 'state', String(cohortId)] as const,
  samples: (cohortId: number | string, label?: string) =>
    ['cohort-body-part-qc', 'samples', String(cohortId), label ?? null] as const,
  changes: (cohortId: number | string, params: Record<string, unknown>) =>
    ['cohort-body-part-qc', 'changes', String(cohortId), params] as const,
};

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

export const useBodyPartQCStateQuery = (cohortId: number | string | undefined) =>
  useQuery<BodyPartState>({
    queryKey: bodyPartQCKeys.state(cohortId ?? ''),
    queryFn: () =>
      apiClient.get<BodyPartState>(`/cohorts/${cohortId}/body-part-qc`),
    enabled: cohortId !== undefined && cohortId !== null && cohortId !== '',
    staleTime: STALE_TIMES.medium,
    refetchOnWindowFocus: false,
  });

export const useBodyPartSamplesQuery = (
  cohortId: number | string | undefined,
  label?: string,
) => {
  const search = new URLSearchParams();
  if (label) search.set('label', label);
  const qs = search.toString();
  return useQuery<BodyPartTrainingSample[]>({
    queryKey: bodyPartQCKeys.samples(cohortId ?? '', label),
    queryFn: () =>
      apiClient.get<BodyPartTrainingSample[]>(
        `/cohorts/${cohortId}/body-part-qc/samples${qs ? `?${qs}` : ''}`,
      ),
    enabled:
      cohortId !== undefined && cohortId !== null && cohortId !== '',
    staleTime: STALE_TIMES.short,
    refetchOnWindowFocus: false,
  });
};

export interface ChangesQueryParams {
  from_label?: string;
  to_label?: string;
  prior_source?: string;
  min_confidence?: number;
  offset?: number;
  limit?: number;
}

export const useBodyPartChangesQuery = (
  cohortId: number | string | undefined,
  params: ChangesQueryParams,
  enabled = true,
) => {
  const search = new URLSearchParams();
  if (params.from_label) search.set('from', params.from_label);
  if (params.to_label) search.set('to', params.to_label);
  if (params.prior_source) search.set('prior_source', params.prior_source);
  if (params.min_confidence !== undefined)
    search.set('min_confidence', String(params.min_confidence));
  if (params.offset !== undefined) search.set('offset', String(params.offset));
  if (params.limit !== undefined) search.set('limit', String(params.limit));
  const qs = search.toString();
  return useQuery<BodyPartChangesPage>({
    queryKey: bodyPartQCKeys.changes(
      cohortId ?? '',
      params as unknown as Record<string, unknown>,
    ),
    queryFn: () =>
      apiClient.get<BodyPartChangesPage>(
        `/cohorts/${cohortId}/body-part-qc/changes${qs ? `?${qs}` : ''}`,
      ),
    enabled:
      enabled &&
      cohortId !== undefined &&
      cohortId !== null &&
      cohortId !== '',
    staleTime: STALE_TIMES.short,
    refetchOnWindowFocus: false,
  });
};

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

export const useUpdateCategoriesMutation = (cohortId: number | string) => {
  const qc = useQueryClient();
  return useMutation<BodyPartState, Error, BodyPartCategoriesPayload>({
    mutationFn: (payload) =>
      apiClient.put<BodyPartState>(
        `/cohorts/${cohortId}/body-part-qc/categories`,
        payload,
      ),
    onSuccess: (state) => {
      qc.setQueryData(bodyPartQCKeys.state(cohortId), state);
      notifications.show({
        title: 'Categories updated',
        message: state.categories.join(', '),
        color: 'green',
      });
    },
    onError: (err) =>
      notifications.show({
        title: 'Could not update categories',
        message: err.message,
        color: 'red',
      }),
  });
};

export const useSeedCandidatesMutation = (cohortId: number | string) =>
  useMutation<BodyPartCandidate[], Error, BodyPartSeedPayload>({
    mutationFn: (payload) =>
      apiClient.post<BodyPartCandidate[]>(
        `/cohorts/${cohortId}/body-part-qc/seed`,
        payload,
      ),
    onError: (err) =>
      notifications.show({
        title: 'Seeding failed',
        message: err.message,
        color: 'red',
      }),
  });

export const useUpdateSamplesMutation = (cohortId: number | string) => {
  const qc = useQueryClient();
  return useMutation<BodyPartState, Error, BodyPartSamplesPayload>({
    mutationFn: (payload) =>
      apiClient.post<BodyPartState>(
        `/cohorts/${cohortId}/body-part-qc/samples`,
        payload,
      ),
    onSuccess: (state) => {
      qc.setQueryData(bodyPartQCKeys.state(cohortId), state);
      qc.invalidateQueries({
        queryKey: ['cohort-body-part-qc', 'samples', String(cohortId)],
      });
    },
    onError: (err) =>
      notifications.show({
        title: 'Sample edit failed',
        message: err.message,
        color: 'red',
      }),
  });
};

export const useApplyBodyPartMutation = (cohortId: number | string) => {
  const qc = useQueryClient();
  return useMutation<BodyPartState, Error, void>({
    mutationFn: () =>
      apiClient.post<BodyPartState>(`/cohorts/${cohortId}/body-part-qc/apply`),
    onSuccess: (state) => {
      qc.setQueryData(bodyPartQCKeys.state(cohortId), state);
      qc.invalidateQueries({
        queryKey: ['cohort-body-part-qc', 'changes', String(cohortId)],
      });
      const conflictsSuffix =
        state.summary.override_conflicts_count > 0
          ? ` · ${state.summary.override_conflicts_count} override conflict(s)`
          : '';
      notifications.show({
        title: 'Body Part QC applied (staged)',
        message:
          `${state.summary.total_sessions} sessions, ` +
          `${state.summary.stacks_changed} stack labels changed${conflictsSuffix}. ` +
          `Press Commit to write to the metadata DB.`,
        color: 'green',
      });
    },
    onError: (err) =>
      notifications.show({
        title: 'Apply failed',
        message: err.message,
        color: 'red',
      }),
  });
};

export const useCommitBodyPartMutation = (cohortId: number | string) => {
  const qc = useQueryClient();
  return useMutation<BodyPartState, Error, BodyPartCommitPayload | void>({
    mutationFn: (payload) =>
      apiClient.post<BodyPartState>(
        `/cohorts/${cohortId}/body-part-qc/commit`,
        payload || undefined,
      ),
    onSuccess: (state) => {
      qc.setQueryData(bodyPartQCKeys.state(cohortId), state);
      qc.invalidateQueries({
        queryKey: ['cohort-body-part-qc', 'changes', String(cohortId)],
      });
      notifications.show({
        title: 'Body Part QC committed',
        message:
          `Wrote stacks to the metadata DB. ${state.summary.total_stacks} stacks remaining.`,
        color: 'green',
      });
    },
    onError: (err) =>
      notifications.show({
        title: 'Commit failed',
        message: err.message,
        color: 'red',
      }),
  });
};

export const useDestageBodyPartMutation = (cohortId: number | string) => {
  const qc = useQueryClient();
  return useMutation<BodyPartState, Error, BodyPartDestagePayload>({
    mutationFn: (payload) =>
      apiClient.post<BodyPartState>(
        `/cohorts/${cohortId}/body-part-qc/destage`,
        payload,
      ),
    onSuccess: (state) => {
      qc.setQueryData(bodyPartQCKeys.state(cohortId), state);
      qc.invalidateQueries({
        queryKey: ['cohort-body-part-qc', 'changes', String(cohortId)],
      });
      notifications.show({
        title: 'Stacks destaged',
        message: `Removed from staging. ${state.summary.total_stacks} stacks remaining.`,
        color: 'blue',
      });
    },
    onError: (err) =>
      notifications.show({
        title: 'Destage failed',
        message: err.message,
        color: 'red',
      }),
  });
};

export const useRestorePreviousBodyPartMutation = (cohortId: number | string) => {
  const qc = useQueryClient();
  return useMutation<BodyPartState, Error, void>({
    mutationFn: () =>
      apiClient.post<BodyPartState>(
        `/cohorts/${cohortId}/body-part-qc/restore-previous`,
      ),
    onSuccess: (state) => {
      qc.setQueryData(bodyPartQCKeys.state(cohortId), state);
      qc.invalidateQueries({
        queryKey: ['cohort-body-part-qc', 'changes', String(cohortId)],
      });
      notifications.show({
        title: 'Restored previous run',
        message: 'The current run has been swapped out.',
        color: 'blue',
      });
    },
    onError: (err) =>
      notifications.show({
        title: 'Restore failed',
        message: err.message,
        color: 'red',
      }),
  });
};

export const useOverrideStackMutation = (cohortId: number | string) => {
  const qc = useQueryClient();
  return useMutation<BodyPartSessionPick, Error, BodyPartOverridePayload>({
    mutationFn: (payload) =>
      apiClient.post<BodyPartSessionPick>(
        `/cohorts/${cohortId}/body-part-qc/session-override`,
        payload,
      ),
    onSuccess: (updated) => {
      qc.setQueryData<BodyPartState | undefined>(
        bodyPartQCKeys.state(cohortId),
        (prev) => {
          if (!prev) return prev;
          const idx = prev.picks.findIndex((p) => sameSession(p, updated));
          const next = [...prev.picks];
          if (idx >= 0) next[idx] = updated;
          else next.push(updated);
          return { ...prev, picks: next };
        },
      );
      // The Changes table is derived from the same picks; invalidate
      // it so the relabeled row drops out (or updates its badges).
      qc.invalidateQueries({
        queryKey: ['cohort-body-part-qc', 'changes', String(cohortId)],
      });
    },
    onError: (err) =>
      notifications.show({
        title: 'Override failed',
        message: err.message,
        color: 'red',
      }),
  });
};

export const useSelectModelMutation = (cohortId: number | string) => {
  const qc = useQueryClient();
  return useMutation<BodyPartState, Error, SelectModelPayload>({
    mutationFn: (payload) =>
      apiClient.post<BodyPartState>(
        `/cohorts/${cohortId}/body-part-qc/select-model`,
        payload,
      ),
    onSuccess: (state) => {
      qc.setQueryData(bodyPartQCKeys.state(cohortId), state);
      notifications.show({
        title: 'Model selected',
        message: state.selected_model_name
          ? `Using "${state.selected_model_name}" for Apply.`
          : 'Cleared model selection.',
        color: 'blue',
      });
    },
    onError: (err) =>
      notifications.show({
        title: 'Model selection failed',
        message: err.message,
        color: 'red',
      }),
  });
};

export const useResetBodyPartMutation = (cohortId: number | string) => {
  const qc = useQueryClient();
  return useMutation<BodyPartResetResponse, Error, void>({
    mutationFn: () =>
      apiClient.post<BodyPartResetResponse>(
        `/cohorts/${cohortId}/body-part-qc/reset`,
      ),
    onSuccess: () => {
      qc.invalidateQueries({
        queryKey: bodyPartQCKeys.state(cohortId),
      });
      qc.invalidateQueries({
        queryKey: ['cohort-body-part-qc', 'changes', String(cohortId)],
      });
      qc.invalidateQueries({
        queryKey: ['cohort-body-part-qc', 'samples', String(cohortId)],
      });
      notifications.show({
        title: 'Body Part QC reset',
        message:
          'All QC state has been removed. The next resort will restore keyword-detector labels.',
        color: 'orange',
      });
    },
    onError: (err) =>
      notifications.show({
        title: 'Reset failed',
        message: err.message,
        color: 'red',
      }),
  });
};

export const useResetSessionMutation = (cohortId: number | string) => {
  const qc = useQueryClient();
  return useMutation<BodyPartSessionPick, Error, BodyPartSessionResetPayload>({
    mutationFn: (payload) =>
      apiClient.post<BodyPartSessionPick>(
        `/cohorts/${cohortId}/body-part-qc/session-reset`,
        payload,
      ),
    onSuccess: (updated) => {
      qc.setQueryData<BodyPartState | undefined>(
        bodyPartQCKeys.state(cohortId),
        (prev) => {
          if (!prev) return prev;
          const idx = prev.picks.findIndex((p) => sameSession(p, updated));
          const next = [...prev.picks];
          if (idx >= 0) next[idx] = updated;
          else next.push(updated);
          return { ...prev, picks: next };
        },
      );
      notifications.show({
        title: 'Session reset',
        message: 'Inference re-applied for this session.',
        color: 'blue',
      });
    },
    onError: (err) =>
      notifications.show({
        title: 'Reset failed',
        message: err.message,
        color: 'red',
      }),
  });
};
