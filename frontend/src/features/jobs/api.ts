import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { notifications } from '@mantine/notifications';
import type { ExtractPerformanceConfigPatch, Job, JobAction } from '../../types';
import { apiClient } from '../../utils/api-client';
import { QUERY_KEYS, REFETCH_INTERVALS } from '../../constants/api';

export const useJobsQuery = () =>
  useQuery<Job[]>({
    queryKey: QUERY_KEYS.jobs,
    queryFn: () => apiClient.get<Job[]>('/jobs'),
    refetchInterval: REFETCH_INTERVALS.fast,
  });

export const useJobAction = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ jobId, action }: { jobId: number; action: JobAction }) =>
      apiClient.post<Job>(`/jobs/${jobId}/${action}`),
    onSuccess: (_data, variables) => {
      notifications.show({
        title: 'Job Action',
        message: `Job ${variables.action} successful`,
        color: 'green',
      });
    },
    onError: (error: Error) => {
      notifications.show({
        title: 'Action Failed',
        message: error.message,
        color: 'red',
      });
    },
    onSettled: () => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.jobs });
    },
  });
};

export const useUpdateJobConfig = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ jobId, payload }: { jobId: number; payload: ExtractPerformanceConfigPatch }) =>
      apiClient.patch<Job>(`/jobs/${jobId}/config`, payload),
    onSuccess: (job) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.jobs });
      const cohortId = job?.config?.cohort_id ?? job?.cohortId;
      if (cohortId != null) {
        void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.cohort(cohortId.toString()) });
      }
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.cohorts });
      notifications.show({
        title: 'Config Updated',
        message: 'Job configuration has been updated',
        color: 'green',
      });
    },
    onError: (error: Error) => {
      notifications.show({
        title: 'Update Failed',
        message: error.message,
        color: 'red',
      });
    },
  });
};

export const useDeleteJob = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (jobId: number) => apiClient.delete(`/jobs/${jobId}`),
    onSuccess: () => {
      notifications.show({
        title: 'Job Deleted',
        message: 'Job record has been removed',
        color: 'green',
      });
    },
    onError: (error: Error) => {
      notifications.show({
        title: 'Delete Failed',
        message: error.message,
        color: 'red',
      });
    },
    onSettled: () => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.jobs });
    },
  });
};
