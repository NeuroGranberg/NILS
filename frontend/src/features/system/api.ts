/**
 * System health and status API hooks.
 */

import { useQuery } from '@tanstack/react-query';
import { apiClient } from '../../utils/api-client';
import type { SystemResources } from '../../types';
import { QUERY_KEYS, STALE_TIMES, REFETCH_INTERVALS } from '../../constants/api';

export interface HealthResponse {
  status: string;
}

export interface ReadinessResponse {
  status: string;
  database: string;
}

/**
 * Check if the API is healthy.
 */
export const useHealthCheck = () =>
  useQuery<HealthResponse>({
    queryKey: QUERY_KEYS.health,
    queryFn: () => apiClient.get<HealthResponse>('/health'),
    staleTime: STALE_TIMES.short,
    // No refetchInterval - health checks on demand only (memory optimization)
    retry: 1,
  });

/**
 * Check if the API is ready (database connected).
 */
export const useReadinessCheck = () =>
  useQuery<ReadinessResponse>({
    queryKey: QUERY_KEYS.ready,
    queryFn: () => apiClient.get<ReadinessResponse>('/ready'),
    staleTime: STALE_TIMES.short,
    // No refetchInterval - readiness checks on demand only (memory optimization)
    retry: 1,
  });

/**
 * Get system resource information and recommendations.
 * Note: This is disabled by default and must be manually triggered.
 */
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
