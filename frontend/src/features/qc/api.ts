/**
 * React Query hooks and API functions for QC Pipeline.
 */

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { apiClient } from '../../utils/api-client';
import type { AxesQCItem, AxesQCItemsResponse, AxisOptions, AxisType } from './types';

// =============================================================================
// DICOM Viewer Queries
// =============================================================================

export interface DicomSeriesMetadata {
  seriesInstanceUid: string;
  seriesDescription: string | null;
  modality: string | null;
  seriesNumber: number | null;
  studyInstanceUid: string;
  studyDescription: string | null;
  studyDate: string | null;
  stackIndex: number;
  totalInstances: number;
  instances: DicomInstanceMetadata[];
}

export interface DicomInstanceMetadata {
  instanceId: number;
  sopInstanceUid: string;
  instanceNumber: number | null;
  sliceLocation: number | null;
  sliceIndex: number;
  url: string;
  rows: number | null;
  columns: number | null;
  windowCenter: number | null;
  windowWidth: number | null;
}

export interface DicomInstancesResponse {
  series_uid: string;
  stack_index: number;
  instance_ids: number[];
  total: number;
}

export const dicomKeys = {
  all: ['dicom'] as const,
  metadata: (seriesUid: string, stackIndex: number) =>
    [...dicomKeys.all, 'metadata', seriesUid, stackIndex] as const,
  instances: (seriesUid: string, stackIndex: number) =>
    [...dicomKeys.all, 'instances', seriesUid, stackIndex] as const,
};

export const useDicomSeriesMetadata = (seriesUid: string | null, stackIndex: number = 0) =>
  useQuery({
    queryKey: dicomKeys.metadata(seriesUid ?? '', stackIndex),
    queryFn: () =>
      apiClient.get<DicomSeriesMetadata>(
        `/qc/dicom/${seriesUid}/metadata?stack_index=${stackIndex}`
      ),
    enabled: !!seriesUid,
    staleTime: 60000, // 1 minute
  });

export const useDicomInstances = (seriesUid: string | null, stackIndex: number = 0) =>
  useQuery({
    queryKey: dicomKeys.instances(seriesUid ?? '', stackIndex),
    queryFn: () =>
      apiClient.get<DicomInstancesResponse>(
        `/qc/dicom/${seriesUid}/instances?stack_index=${stackIndex}`
      ),
    enabled: !!seriesUid,
    staleTime: 60000, // 1 minute
  });

// Helper to build image URLs
export const getDicomImageUrl = (instanceId: number, windowCenter?: number, windowWidth?: number) => {
  let url = `/api/qc/dicom/image/${instanceId}`;
  const params = new URLSearchParams();
  if (windowCenter !== undefined) params.set('window_center', String(windowCenter));
  if (windowWidth !== undefined) params.set('window_width', String(windowWidth));
  if (params.toString()) url += `?${params.toString()}`;
  return url;
};

export const getDicomThumbnailUrl = (seriesUid: string, stackIndex: number = 0, size: number = 128) =>
  `/api/qc/dicom/${seriesUid}/thumbnail?stack_index=${stackIndex}&size=${size}`;

// =============================================================================
// Sister Series for Contrast Comparison
// =============================================================================

export interface SisterSeries {
  seriesInstanceUid: string;
  seriesDescription: string | null;
  seriesNumber: number | null;
  modality: string | null;
  base: string | null;
  technique: string | null;
  postContrast: number | null;
  directoryType: string | null;
  instanceCount: number;
  thumbnailUrl: string;
}

export interface SisterSeriesResponse {
  series_uid: string;
  sisters: SisterSeries[];
  total: number;
}

export interface ContrastPairsResponse {
  preContrast: SisterSeries[];
  postContrast: SisterSeries[];
  unknownContrast: SisterSeries[];
  allSisters: SisterSeries[];
}

export const sisterKeys = {
  all: ['sisters'] as const,
  sisters: (seriesUid: string) => [...sisterKeys.all, 'list', seriesUid] as const,
  contrastPairs: (seriesUid: string) => [...sisterKeys.all, 'contrast-pairs', seriesUid] as const,
};

export const useSisterSeries = (seriesUid: string | null) =>
  useQuery({
    queryKey: sisterKeys.sisters(seriesUid ?? ''),
    queryFn: () => apiClient.get<SisterSeriesResponse>(`/qc/dicom/${seriesUid}/sisters`),
    enabled: !!seriesUid,
    staleTime: 60000,
  });

export const useContrastPairs = (seriesUid: string | null) =>
  useQuery({
    queryKey: sisterKeys.contrastPairs(seriesUid ?? ''),
    queryFn: () => apiClient.get<ContrastPairsResponse>(`/qc/dicom/${seriesUid}/contrast-pairs`),
    enabled: !!seriesUid,
    staleTime: 60000,
  });

// =============================================================================
// Axes Prediction QC
// =============================================================================

export const axesQCKeys = {
  all: ['axes-qc'] as const,
  items: (cohortId: number) => [...axesQCKeys.all, 'items', cohortId] as const,
  item: (stackId: number) => [...axesQCKeys.all, 'item', stackId] as const,
  options: () => [...axesQCKeys.all, 'options'] as const,
  imageComments: (stackId: number) => [...axesQCKeys.all, 'image-comments', stackId] as const,
  session: (cohortId: number) => [...axesQCKeys.all, 'session', cohortId] as const,
};

// Session type for axes QC
export interface AxesQCSession {
  id: number;
  cohort_id: number;
  status: string;
  draft_item_count: number;
  draft_change_count: number;
  created_at: string | null;
  updated_at: string | null;
}

export interface AxesQCFilters {
  axis?: string | null;
  flagType?: string | null;
}

export const useAxesQCItems = (
  cohortId: number | null,
  offset: number = 0,
  limit: number = 100,
  filters: AxesQCFilters = {}
) => {
  const { axis, flagType } = filters;
  return useQuery({
    queryKey: [...axesQCKeys.items(cohortId ?? 0), { offset, limit, axis, flagType }],
    queryFn: () => {
      const params = new URLSearchParams({
        offset: String(offset),
        limit: String(limit),
      });
      if (axis) params.set('axis', axis);
      if (flagType) params.set('flag_type', flagType);
      return apiClient.get<AxesQCItemsResponse>(
        `/qc/cohorts/${cohortId}/axes/items?${params.toString()}`
      );
    },
    enabled: !!cohortId,
    staleTime: 30000, // 30 seconds
  });
};

export const useAxesQCItem = (stackId: number | null) =>
  useQuery({
    queryKey: axesQCKeys.item(stackId ?? 0),
    queryFn: () => apiClient.get<AxesQCItem>(`/qc/axes/items/${stackId}`),
    enabled: !!stackId,
    staleTime: 10000,
  });

export const useAxisOptions = () =>
  useQuery({
    queryKey: axesQCKeys.options(),
    queryFn: () => apiClient.get<AxisOptions>('/qc/axes/options'),
    staleTime: 300000, // 5 minutes
  });

export const useImageComments = (stackId: number | null) =>
  useQuery({
    queryKey: axesQCKeys.imageComments(stackId ?? 0),
    queryFn: () =>
      apiClient.get<{ image_comments: string | null }>(`/qc/axes/items/${stackId}/image-comments`),
    enabled: !!stackId,
    staleTime: 60000,
  });

// Get or create axes QC session for a cohort
export const useAxesQCSession = (cohortId: number | null) =>
  useQuery({
    queryKey: axesQCKeys.session(cohortId ?? 0),
    queryFn: () => apiClient.get<AxesQCSession>(`/qc/cohorts/${cohortId}/axes/session`),
    enabled: !!cohortId,
    staleTime: 10000, // 10 seconds - refresh frequently to show draft counts
  });

// Get available axes and flags that have QC items for this cohort
export interface AvailableFilters {
  available_axes: string[];
  available_flags: string[];
}

export const useAvailableFilters = (cohortId: number | null) =>
  useQuery({
    queryKey: ['axes-qc', 'filters', cohortId],
    queryFn: () => apiClient.get<AvailableFilters>(`/qc/cohorts/${cohortId}/axes/filters`),
    enabled: !!cohortId,
    staleTime: 60000, // 1 minute - doesn't change often
  });

// Save an axis value as draft (does NOT persist to metadata_db)
export const useUpdateAxisValue = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      cohortId,
      stackId,
      axis,
      value,
    }: {
      cohortId: number;
      stackId: number;
      axis: AxisType;
      value: string | null;
    }) =>
      apiClient.patch<{ success: boolean; stack_id: number; axis: string; value: string | null; draft: boolean }>(
        `/qc/cohorts/${cohortId}/axes/items/${stackId}?axis=${axis}${value ? `&value=${encodeURIComponent(value)}` : ''}`
      ),
    onSuccess: (_data, { cohortId, stackId }) => {
      // Invalidate item cache
      queryClient.invalidateQueries({ queryKey: axesQCKeys.item(stackId) });
      // Invalidate items list to refresh draft_changes
      queryClient.invalidateQueries({ queryKey: axesQCKeys.items(cohortId) });
      // Invalidate session to update draft counts
      queryClient.invalidateQueries({ queryKey: axesQCKeys.session(cohortId) });
    },
  });
};

// Confirm all draft changes (push to metadata_db)
export const useConfirmAxesChanges = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (cohortId: number) =>
      apiClient.post<{ success: boolean; confirmed_items: number; confirmed_changes: number }>(
        `/qc/cohorts/${cohortId}/axes/confirm`
      ),
    onSuccess: (_data, cohortId) => {
      // Invalidate all axes QC queries to refresh state
      queryClient.invalidateQueries({ queryKey: axesQCKeys.all });
      queryClient.invalidateQueries({ queryKey: axesQCKeys.session(cohortId) });
    },
  });
};

// Discard all draft changes
export const useDiscardAxesChanges = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (cohortId: number) =>
      apiClient.post<{ success: boolean; discarded_items: number; discarded_changes: number }>(
        `/qc/cohorts/${cohortId}/axes/discard`
      ),
    onSuccess: (_data, cohortId) => {
      // Invalidate all axes QC queries to refresh state
      queryClient.invalidateQueries({ queryKey: axesQCKeys.all });
      queryClient.invalidateQueries({ queryKey: axesQCKeys.session(cohortId) });
    },
  });
};
