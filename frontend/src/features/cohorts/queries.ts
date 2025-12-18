import { useQuery } from '@tanstack/react-query';
import { apiClient } from '../../utils/api-client';
import type { SystemResources } from '../../types';
import { QUERY_KEYS, STALE_TIMES } from '../../constants/api';

export const useSystemResources = () =>
  useQuery<SystemResources>({
    queryKey: QUERY_KEYS.systemResources,
    queryFn: () => apiClient.get<SystemResources>('/system/resources'),
    staleTime: STALE_TIMES.long,
    enabled: false,
    refetchOnWindowFocus: false,
    refetchOnMount: false,
    refetchOnReconnect: false,
  });
