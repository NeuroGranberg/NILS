/**
 * React Query hooks for the cohort-wide Main Acquisition QC.
 *
 * Endpoints (see backend/src/api/routes/cohort_main_qc.py):
 *   GET    /api/cohorts/{id}/main-qc
 *   POST   /api/cohorts/{id}/main-qc/apply
 *   POST   /api/cohorts/{id}/main-qc/restore-previous
 *   POST   /api/cohorts/{id}/main-qc/session-pick
 *   POST   /api/cohorts/{id}/main-qc/session-reset
 *   POST   /api/cohorts/{id}/main-qc/session-acknowledge
 *
 * Sessions are identified by (subject_id, session_date) in the request body.
 */
import { notifications } from '@mantine/notifications';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import { STALE_TIMES } from '../../../constants/api';
import { apiClient } from '../../../utils/api-client';
import type {
  MainQCSessionAcknowledgePayload,
  MainQCSessionPick,
  MainQCSessionPickPayload,
  MainQCSessionResetPayload,
  MainQCState,
} from './types';

export const mainQCKeys = {
  all: ['cohort-main-qc'] as const,
  state: (cohortId: number | string) =>
    ['cohort-main-qc', 'state', String(cohortId)] as const,
};

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

export const useMainQCStateQuery = (cohortId: number | string | undefined) =>
  useQuery<MainQCState>({
    queryKey: mainQCKeys.state(cohortId ?? ''),
    queryFn: () => apiClient.get<MainQCState>(`/cohorts/${cohortId}/main-qc`),
    enabled: cohortId !== undefined && cohortId !== null && cohortId !== '',
    staleTime: STALE_TIMES.medium,
    refetchOnWindowFocus: false,
  });

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

export const useApplyMainQCMutation = (cohortId: number | string) => {
  const qc = useQueryClient();
  return useMutation<MainQCState, Error, void>({
    mutationFn: () => apiClient.post<MainQCState>(`/cohorts/${cohortId}/main-qc/apply`),
    onSuccess: (state) => {
      qc.setQueryData(mainQCKeys.state(cohortId), state);
      notifications.show({
        title: 'Main acquisition QC applied',
        message:
          `T1w: ${state.summary.t1w?.green ?? 0} green / ${state.summary.t1w?.amber ?? 0} amber / ` +
          `${state.summary.t1w?.red ?? 0} red. ` +
          `FLAIR: ${state.summary.flair?.green ?? 0} green / ${state.summary.flair?.amber ?? 0} amber / ` +
          `${state.summary.flair?.red ?? 0} red.`,
        color: 'green',
      });
    },
    onError: (err) =>
      notifications.show({ title: 'Apply failed', message: err.message, color: 'red' }),
  });
};

export const useRestorePreviousMainQCMutation = (cohortId: number | string) => {
  const qc = useQueryClient();
  return useMutation<MainQCState, Error, void>({
    mutationFn: () =>
      apiClient.post<MainQCState>(`/cohorts/${cohortId}/main-qc/restore-previous`),
    onSuccess: (state) => {
      qc.setQueryData(mainQCKeys.state(cohortId), state);
      notifications.show({
        title: 'Restored previous run',
        message: 'The current run has been swapped out.',
        color: 'blue',
      });
    },
    onError: (err) =>
      notifications.show({ title: 'Restore failed', message: err.message, color: 'red' }),
  });
};

/** Match a pick by the new (subject_id, session_date, axis) session key. */
const sameSession = (a: MainQCSessionPick, b: MainQCSessionPick) =>
  a.subject_id === b.subject_id
  && a.session_date === b.session_date
  && a.axis === b.axis;

export const useUpdateSessionPickMutation = (
  cohortId: number | string,
) => {
  const qc = useQueryClient();
  return useMutation<MainQCSessionPick, Error, MainQCSessionPickPayload>({
    mutationFn: (payload) =>
      apiClient.post<MainQCSessionPick>(
        `/cohorts/${cohortId}/main-qc/session-pick`,
        payload,
      ),
    onSuccess: (updated) => {
      qc.setQueryData<MainQCState | undefined>(mainQCKeys.state(cohortId), (prev) => {
        if (!prev) return prev;
        const idx = prev.picks.findIndex((p) => sameSession(p, updated));
        const next = [...prev.picks];
        if (idx >= 0) next[idx] = updated;
        else next.push(updated);
        return { ...prev, picks: next };
      });
    },
    onError: (err) =>
      notifications.show({ title: 'Save failed', message: err.message, color: 'red' }),
  });
};

export const useResetSessionPickMutation = (cohortId: number | string) => {
  const qc = useQueryClient();
  return useMutation<MainQCSessionPick, Error, MainQCSessionResetPayload>({
    mutationFn: (payload) =>
      apiClient.post<MainQCSessionPick>(
        `/cohorts/${cohortId}/main-qc/session-reset`,
        payload,
      ),
    onSuccess: (updated) => {
      qc.setQueryData<MainQCState | undefined>(mainQCKeys.state(cohortId), (prev) => {
        if (!prev) return prev;
        const idx = prev.picks.findIndex((p) => sameSession(p, updated));
        const next = [...prev.picks];
        if (idx >= 0) next[idx] = updated;
        else next.push(updated);
        return { ...prev, picks: next };
      });
      notifications.show({
        title: 'Session reset',
        message: 'Auto-pick re-applied for this session.',
        color: 'blue',
      });
    },
    onError: (err) =>
      notifications.show({ title: 'Reset failed', message: err.message, color: 'red' }),
  });
};

export const useAcknowledgeSessionPickMutation = (cohortId: number | string) => {
  const qc = useQueryClient();
  return useMutation<MainQCSessionPick, Error, MainQCSessionAcknowledgePayload>({
    mutationFn: (payload) =>
      apiClient.post<MainQCSessionPick>(
        `/cohorts/${cohortId}/main-qc/session-acknowledge`,
        payload,
      ),
    onSuccess: (updated) => {
      qc.setQueryData<MainQCState | undefined>(mainQCKeys.state(cohortId), (prev) => {
        if (!prev) return prev;
        const idx = prev.picks.findIndex((p) => sameSession(p, updated));
        const next = [...prev.picks];
        if (idx >= 0) next[idx] = updated;
        else next.push(updated);
        return { ...prev, picks: next };
      });
    },
    onError: (err) =>
      notifications.show({ title: 'Acknowledge failed', message: err.message, color: 'red' }),
  });
};
