import { useRef } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { apiClient } from '../../utils/api-client';
import { QUERY_KEYS } from '../../constants/api';
import { isTerminalRunStatus } from './types';
import type {
  ReposResponse,
  RegisterRepoResponse,
  PipelinesResponse,
  PipelineResponse,
  RunResponse,
  CreateRunResponse,
  RunStateResponse,
  RunLogsResponse,
  CancelRunResponse,
  IngestResponse,
  RunsResponse,
  RunResultsResponse,
  RegisterRepoBody,
  RefreshRepoBody,
  CreateRunBody,
} from './types';

export const pipelineKeys = {
  all: ['pipelines'] as const,
  repos: ['pipelines', 'repos'] as const,
  pipelines: (repoId?: number) => ['pipelines', 'list', repoId ?? null] as const,
  pipeline: (id: number) => ['pipelines', 'detail', id] as const,
  run: (runId: number) => ['pipelines', 'run', runId] as const,
  runState: (runId: number) => ['pipelines', 'run', runId, 'state'] as const,
  runLogs: (runId: number) => ['pipelines', 'run', runId, 'logs'] as const,
  runResults: (runId: number) => ['pipelines', 'run', runId, 'results'] as const,
  pipelineRuns: (pipelineId: number) => ['pipelines', 'runs', pipelineId] as const,
};

// ---- Repos ----

export const useRepos = () =>
  useQuery({
    queryKey: pipelineKeys.repos,
    queryFn: () => apiClient.get<ReposResponse>('/analysis-pipelines/repos'),
    select: (r) => r.repos,
  });

export const useRegisterRepo = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: RegisterRepoBody) =>
      apiClient.post<RegisterRepoResponse>('/analysis-pipelines/repos', body),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: pipelineKeys.repos });
      void qc.invalidateQueries({ queryKey: ['pipelines', 'list'] });
    },
  });
};

export const useRefreshRepo = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, ref }: { id: number; ref?: string }) =>
      apiClient.post<RegisterRepoResponse>(`/analysis-pipelines/repos/${id}/refresh`, {
        ref,
      } satisfies RefreshRepoBody),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: pipelineKeys.repos });
      void qc.invalidateQueries({ queryKey: ['pipelines', 'list'] });
    },
  });
};

export const useRemoveRepo = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: number) => apiClient.delete<void>(`/analysis-pipelines/repos/${id}`),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: pipelineKeys.repos });
      void qc.invalidateQueries({ queryKey: ['pipelines', 'list'] });
    },
  });
};

// ---- Pipelines ----

export const usePipelines = (repoId?: number) =>
  useQuery({
    queryKey: pipelineKeys.pipelines(repoId),
    queryFn: () =>
      apiClient.get<PipelinesResponse>(
        repoId ? `/analysis-pipelines?repo_id=${repoId}` : '/analysis-pipelines',
      ),
    select: (r) => r.pipelines,
  });

export const usePipeline = (id: number) =>
  useQuery({
    queryKey: pipelineKeys.pipeline(id),
    queryFn: () => apiClient.get<PipelineResponse>(`/analysis-pipelines/${id}`),
    select: (r) => r.pipeline,
    enabled: id > 0,
  });

// ---- Runs ----

export const usePipelineRuns = (pipelineId: number) =>
  useQuery({
    queryKey: pipelineKeys.pipelineRuns(pipelineId),
    queryFn: () =>
      apiClient.get<RunsResponse>(`/analysis-pipelines/${pipelineId}/runs`),
    select: (r) => r.runs,
    enabled: pipelineId > 0,
    // Keep the list live while any run is still progressing.
    refetchInterval: (q) =>
      q.state.data?.runs.some((r) => !isTerminalRunStatus(r.status)) ? 3000 : false,
  });

export const useCreateRun = (pipelineId: number) => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: CreateRunBody) =>
      apiClient.post<CreateRunResponse>(`/analysis-pipelines/${pipelineId}/runs`, body),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: pipelineKeys.pipeline(pipelineId) });
      void qc.invalidateQueries({ queryKey: pipelineKeys.pipelineRuns(pipelineId) });
      void qc.invalidateQueries({ queryKey: ['pipelines', 'run'] });
      void qc.invalidateQueries({ queryKey: QUERY_KEYS.jobs });
    },
  });
};

export const useRunResults = (runId: number, enabled: boolean) =>
  useQuery({
    queryKey: pipelineKeys.runResults(runId),
    queryFn: () =>
      apiClient.get<RunResultsResponse>(`/analysis-pipelines/runs/${runId}/results`),
    select: (r) => r.results,
    enabled: runId > 0 && enabled,
  });

export const useRun = (runId: number) =>
  useQuery({
    queryKey: pipelineKeys.run(runId),
    queryFn: () => apiClient.get<RunResponse>(`/analysis-pipelines/runs/${runId}`),
    enabled: runId > 0,
    refetchInterval: (q) => (isTerminalRunStatus(q.state.data?.run.status) ? false : 3000),
  });

export const useRunState = (runId: number, enabled: boolean) =>
  useQuery({
    queryKey: pipelineKeys.runState(runId),
    queryFn: () =>
      apiClient.get<RunStateResponse>(`/analysis-pipelines/runs/${runId}/state`),
    select: (r) => r.state,
    enabled: runId > 0 && enabled,
    refetchInterval: (q) => {
      const s = q.state.data?.state.state; // bridge state string
      const done = s === 'completed' || s === 'failed' || s === 'crashed' || s === 'canceled';
      return done ? false : 2000;
    },
  });

export const useRunLogs = (runId: number, enabled: boolean) => {
  const cursorRef = useRef(0);
  const linesRef = useRef<string[]>([]);
  return useQuery({
    queryKey: pipelineKeys.runLogs(runId),
    enabled: runId > 0 && enabled,
    queryFn: async () => {
      const r = await apiClient.get<RunLogsResponse>(
        `/analysis-pipelines/runs/${runId}/logs?since=${cursorRef.current}`,
      );
      if (r.lines.length) linesRef.current = [...linesRef.current, ...r.lines];
      cursorRef.current = r.next_cursor;
      return linesRef.current; // return the ACCUMULATED array
    },
    refetchInterval: enabled ? 2000 : false,
  });
};

export const useCancelRun = (runId: number) => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () =>
      apiClient.post<CancelRunResponse>(`/analysis-pipelines/runs/${runId}/cancel`),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: pipelineKeys.run(runId) });
      void qc.invalidateQueries({ queryKey: pipelineKeys.runState(runId) });
    },
  });
};

export const useIngest = (runId: number) => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ dryRun }: { dryRun: boolean }) =>
      apiClient.post<IngestResponse>(
        `/analysis-pipelines/runs/${runId}/ingest?dry_run=${dryRun}`,
      ),
    onSuccess: (_d, { dryRun }) => {
      if (!dryRun) {
        void qc.invalidateQueries({ queryKey: pipelineKeys.run(runId) });
        void qc.invalidateQueries({ queryKey: QUERY_KEYS.jobs });
      }
    },
  });
};
