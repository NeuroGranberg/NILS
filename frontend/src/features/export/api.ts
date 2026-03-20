import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { apiClient } from '../../utils/api-client';
import type { Job } from '../../types';
import { QUERY_KEYS } from '../../constants/api';
import type { ResolveResponse, RunExportPayload, RunExportResponse } from './types';

export const exportKeys = {
  all: ['export'] as const,
  jobs: [...['export'], 'jobs'] as const,
  job: (jobId: number) => [...['export'], 'job', jobId] as const,
  subjectIdentifiers: (idTypeId: number, subjectCodes: string[]) =>
    [...['export'], 'subject-identifiers', idTypeId, subjectCodes.join(',')] as const,
};

export const resolveManifest = async (manifestText: string): Promise<ResolveResponse> => {
  return apiClient.post<ResolveResponse>('/export/resolve-text', {
    manifest_text: manifestText,
  });
};

export const runExport = async (payload: RunExportPayload): Promise<RunExportResponse> => {
  return apiClient.post<RunExportResponse>('/export/run', payload);
};

export const useResolveManifest = () =>
  useMutation({
    mutationFn: resolveManifest,
  });

export const useRunExport = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: runExport,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: exportKeys.jobs });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.jobs });
    },
  });
};

export const useExportJobs = () =>
  useQuery<Job[]>({
    queryKey: exportKeys.jobs,
    queryFn: async () => {
      const allJobs = await apiClient.get<Job[]>('/jobs');
      return allJobs.filter((j) => j.stageId === 'subset_export');
    },
    refetchInterval: (query) => {
      const jobs = query.state.data;
      const hasActive = jobs?.some((j) => ['running', 'queued', 'paused'].includes(j.status));
      return hasActive ? 5000 : 30000;
    },
  });

const TERMINAL_STATUSES = ['completed', 'completed_with_warnings', 'failed', 'canceled'];

export const useExportJobQuery = (jobId: number | null) =>
  useQuery<Job>({
    queryKey: exportKeys.job(jobId ?? 0),
    queryFn: () => apiClient.get<Job>(`/jobs/${jobId}`),
    enabled: jobId !== null && jobId > 0,
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      if (status && TERMINAL_STATUSES.includes(status)) return false;
      return 3000;
    },
  });

interface SubjectIdentifiersResponse {
  mapping: Record<string, string>;
}

export const fetchSubjectIdentifiers = async (
  subjectCodes: string[],
  idTypeId: number,
): Promise<Record<string, string>> => {
  const resp = await apiClient.post<SubjectIdentifiersResponse>('/export/subject-identifiers', {
    subject_codes: subjectCodes,
    id_type_id: idTypeId,
  });
  return resp.mapping;
};

export const useSubjectIdentifiers = (subjectCodes: string[], idTypeId: number | null) =>
  useQuery({
    queryKey: exportKeys.subjectIdentifiers(idTypeId ?? 0, subjectCodes),
    queryFn: () => fetchSubjectIdentifiers(subjectCodes, idTypeId!),
    enabled: idTypeId !== null && idTypeId > 0 && subjectCodes.length > 0,
    staleTime: 5 * 60 * 1000,
  });


