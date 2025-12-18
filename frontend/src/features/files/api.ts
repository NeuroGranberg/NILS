import { useQuery } from '@tanstack/react-query';
import type { DirectoryEntry } from '../../types';
import { apiClient } from '../../utils/api-client';
import { QUERY_KEYS, STALE_TIMES } from '../../constants/api';

export const useDirectoryQuery = (path: string) =>
  useQuery<DirectoryEntry[]>({
    queryKey: QUERY_KEYS.directory(path),
    queryFn: () => apiClient.get<DirectoryEntry[]>(`/files?path=${encodeURIComponent(path)}`),
    enabled: Boolean(path),
    staleTime: STALE_TIMES.short,
  });

export const useDataRootsQuery = () =>
  useQuery<string[]>({
    queryKey: QUERY_KEYS.dataRoots,
    queryFn: () => apiClient.get<string[]>('/data-roots'),
    staleTime: STALE_TIMES.medium,
  });
