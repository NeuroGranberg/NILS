import { useQuery, type UseQueryOptions } from '@tanstack/react-query';
import { apiClient } from '../../utils/api-client';

interface FolderSamplesResponse {
  paths: string[];
}

type FolderSamplesKey = ['cohort-folder-samples', number | null | undefined];

type FolderSamplesQueryOptions = Omit<
  UseQueryOptions<FolderSamplesResponse, Error, FolderSamplesResponse, FolderSamplesKey>,
  'queryKey' | 'queryFn'
>;

export const useFolderSamples = (
  cohortId: number | null | undefined,
  options?: FolderSamplesQueryOptions,
) =>
  useQuery<FolderSamplesResponse, Error, FolderSamplesResponse, FolderSamplesKey>({
    queryKey: ['cohort-folder-samples', cohortId],
    queryFn: () => apiClient.get<FolderSamplesResponse>(`/cohorts/${cohortId}/examples/folders`),
    enabled: Boolean(cohortId) && (options?.enabled ?? true),
    staleTime: 5 * 60 * 1000,
    refetchOnWindowFocus: false,
    ...options,
  });

interface CsvColumnsResponse {
  columns: string[];
}

type CsvColumnsKey = ['csv-columns', string | null | undefined];

type CsvColumnsQueryOptions = Omit<
  UseQueryOptions<CsvColumnsResponse, Error, CsvColumnsResponse, CsvColumnsKey>,
  'queryKey' | 'queryFn'
>;

export const useCsvColumns = (token: string | null | undefined, options?: CsvColumnsQueryOptions) =>
  useQuery<CsvColumnsResponse, Error, CsvColumnsResponse, CsvColumnsKey>({
    queryKey: ['csv-columns', token],
    queryFn: () => apiClient.get<CsvColumnsResponse>(`/uploads/csv/${token}/columns`),
    enabled: Boolean(token) && (options?.enabled ?? true),
    staleTime: 5 * 60 * 1000,
    refetchOnWindowFocus: false,
    ...options,
  });