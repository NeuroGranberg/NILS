/**
 * React Query hooks + types for the Main Acquisition Selection QC module.
 */

import { useMutation, useQuery, useQueryClient, keepPreviousData } from '@tanstack/react-query';
import { apiClient } from '../../../utils/api-client';

// --------------------------------------------------------------------------- //
// Types
// --------------------------------------------------------------------------- //

export const FACETS = [
  'directory_type',
  'provenance',
  'orientation',
  'base',
  'mr_acquisition_type',
  'technique',
  'modifier_csv',
] as const;

export type Facet = typeof FACETS[number];

export const NONE_SENTINEL = '__none__';

export type FilterState = {
  [K in Facet]?: string[] | null;
};

export interface FilterOptionsResponse {
  options: Record<Facet, string[]>;
}

export interface SessionSummary {
  index: number;
  subject_code: string;
  study_date: string;
  stack_count: number;
  display_id: string | null;
}

export interface SessionsResponse {
  total_subjects: number;
  total_sessions: number;
  total_stacks: number;
  sessions: SessionSummary[];
  available_id_types: string[];
}

export interface BundleStack {
  series_stack_id: number;
  series_instance_uid: string;
  stack_index: number;
  series_time: string | null;
  series_number: number | null;
  slices_count: number | null;
  post_contrast: number | null;
  is_main: boolean;
  stack_key: string | null;
  provenance: string | null;
  echo_time: number | null;
  repetition_time: number | null;
  inversion_time: number | null;
  flip_angle: number | null;
  thumbnail_url: string;
}

export interface Bundle {
  bundle_id: string;
  title: string;
  directory_type: string | null;
  base: string | null;
  technique: string | null;
  modifier_csv: string | null;
  orientation: string | null;
  body_part: string | null;
  provenance: string | null;
  acceleration_csv: string | null;
  echo_time: number | null;
  repetition_time: number | null;
  inversion_time: number | null;
  flip_angle: number | null;
  stacks: BundleStack[];
  suggested_main_stack_id: number | null;
}

export interface BundlesResponse {
  session_index: number;
  session: SessionSummary | null;
  bundles: Bundle[];
  total_sessions: number;
}

export interface RoleUpdateResponse {
  series_stack_id: number;
  post_contrast: number | null;
  is_main: boolean;
  cleared_main_stack_ids: number[];
}

export interface SavedMASQCSession {
  id: number;
  cohort_id: number;
  name: string;
  filters: FilterState;
  session_index: number;
  seen_indices: number[];
  display_id_type: string | null;
  only_multi_stack: boolean;
  created_at: string;
  updated_at: string;
}

// --------------------------------------------------------------------------- //
// Query keys
// --------------------------------------------------------------------------- //

export const mainAcqKeys = {
  all: ['main-acq'] as const,
  options: (cohortId: number, filters: FilterState) =>
    [...mainAcqKeys.all, 'options', cohortId, filters] as const,
  sessions: (cohortId: number, filters: FilterState, displayIdType: string, onlyMultiStack: boolean) =>
    [...mainAcqKeys.all, 'sessions', cohortId, filters, displayIdType, onlyMultiStack] as const,
  bundles: (cohortId: number, filters: FilterState, sessionIndex: number, displayIdType: string, onlyMultiStack: boolean) =>
    [...mainAcqKeys.all, 'bundles', cohortId, sessionIndex, filters, displayIdType, onlyMultiStack] as const,
  savedSessions: (cohortId: number) =>
    [...mainAcqKeys.all, 'saved-sessions', cohortId] as const,
};

// --------------------------------------------------------------------------- //
// Hooks
// --------------------------------------------------------------------------- //

export const useMainAcqFilterOptions = (cohortId: number | null, filters: FilterState) =>
  useQuery({
    queryKey: cohortId ? mainAcqKeys.options(cohortId, filters) : ['main-acq', 'options', 'none'],
    queryFn: () =>
      apiClient.post<FilterOptionsResponse>(
        `/qc/cohorts/${cohortId}/main-acq/filter-options`,
        filters
      ),
    enabled: !!cohortId,
    staleTime: 10_000,
  });

export const useMainAcqSessions = (cohortId: number | null, filters: FilterState, displayIdType: string = 'code', onlyMultiStack: boolean = false) =>
  useQuery({
    queryKey: cohortId ? mainAcqKeys.sessions(cohortId, filters, displayIdType, onlyMultiStack) : ['main-acq', 'sessions', 'none'],
    queryFn: () => {
      const qp = new URLSearchParams();
      if (displayIdType && displayIdType !== 'code') qp.set('display_id_type', displayIdType);
      if (onlyMultiStack) qp.set('only_multi_stack', 'true');
      const qs = qp.toString() ? `?${qp.toString()}` : '';
      return apiClient.post<SessionsResponse>(`/qc/cohorts/${cohortId}/main-acq/sessions${qs}`, filters);
    },
    enabled: !!cohortId,
    staleTime: 10_000,
  });

/**
 * Shared fetcher for one session's bundles. Used by both the live query
 * (`useMainAcqBundles`) and the prefetch path in the page (which warms
 * the cache for sessionIndex ± 1 so "Next/Prev" lands instantly).
 */
export const fetchMainAcqBundles = (
  cohortId: number,
  filters: FilterState,
  sessionIndex: number,
  displayIdType: string = 'code',
  onlyMultiStack: boolean = false,
) => {
  const qp = new URLSearchParams();
  if (displayIdType && displayIdType !== 'code') qp.set('display_id_type', displayIdType);
  if (onlyMultiStack) qp.set('only_multi_stack', 'true');
  const qs = qp.toString() ? `?${qp.toString()}` : '';
  return apiClient.post<BundlesResponse>(
    `/qc/cohorts/${cohortId}/main-acq/sessions/${sessionIndex}/bundles${qs}`,
    filters,
  );
};

export const useMainAcqBundles = (
  cohortId: number | null,
  filters: FilterState,
  sessionIndex: number,
  displayIdType: string = 'code',
  onlyMultiStack: boolean = false,
) =>
  useQuery({
    queryKey: cohortId
      ? mainAcqKeys.bundles(cohortId, filters, sessionIndex, displayIdType, onlyMultiStack)
      : ['main-acq', 'bundles', 'none'],
    queryFn: () =>
      fetchMainAcqBundles(cohortId!, filters, sessionIndex, displayIdType, onlyMultiStack),
    enabled: !!cohortId && sessionIndex >= 0,
    staleTime: 60_000,
    placeholderData: keepPreviousData,
  });

export interface ResolveSubjectResponse {
  identifier: string;
  subject_code: string;
}

export const resolveSubjectIdentifier = (identifier: string) =>
  apiClient.get<ResolveSubjectResponse>(
    `/qc/main-acq/resolve-subject?identifier=${encodeURIComponent(identifier)}`
  );

export const useSavedMASQCSessions = (cohortId: number | null) =>
  useQuery({
    queryKey: cohortId ? mainAcqKeys.savedSessions(cohortId) : ['main-acq', 'saved-sessions', 'none'],
    queryFn: () =>
      apiClient.get<SavedMASQCSession[]>(`/qc/cohorts/${cohortId}/main-acq/saved-sessions`),
    enabled: !!cohortId,
  });

export const useCreateSavedSession = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ cohortId, ...body }: {
      cohortId: number;
      name: string;
      filters: FilterState;
      session_index: number;
      display_id_type?: string | null;
      only_multi_stack?: boolean;
    }) =>
      apiClient.post<SavedMASQCSession>(`/qc/cohorts/${cohortId}/main-acq/saved-sessions`, body),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: mainAcqKeys.savedSessions(variables.cohortId) });
    },
  });
};

export const useUpdateSavedSession = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ sessionId, ...body }: {
      sessionId: number;
      cohortId: number;
      session_index?: number;
      seen_indices?: number[];
      filters?: FilterState;
      name?: string;
      display_id_type?: string | null;
      only_multi_stack?: boolean;
    }) =>
      apiClient.patch<SavedMASQCSession>(`/qc/main-acq/saved-sessions/${sessionId}`, body),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: mainAcqKeys.savedSessions(variables.cohortId) });
    },
  });
};

export const useDeleteSavedSession = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ sessionId, cohortId }: { sessionId: number; cohortId: number }) =>
      apiClient.delete(`/qc/main-acq/saved-sessions/${sessionId}`),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: mainAcqKeys.savedSessions(variables.cohortId) });
    },
  });
};

export const useSetStackRole = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      seriesStackId,
      role,
      value,
    }: {
      seriesStackId: number;
      role: 'main' | 'pre' | 'post';
      value: boolean;
    }) =>
      apiClient.post<RoleUpdateResponse>(`/qc/main-acq/stacks/${seriesStackId}/role`, {
        role,
        value,
      }),
    onSuccess: () => {
      // The bundle list caches what we just mutated; invalidate all bundle
      // pages under ``main-acq``. Session counts and filter options don't
      // change as a result of a role toggle, so we leave them alone.
      queryClient.invalidateQueries({ queryKey: ['main-acq', 'bundles'] });
    },
  });
};

// --------------------------------------------------------------------------- //
// Helpers
// --------------------------------------------------------------------------- //

export const formatFacetValue = (facet: Facet, value: string): string => {
  if (value === NONE_SENTINEL) return '(none)';
  return value;
};
