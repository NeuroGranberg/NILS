import {
  Anchor,
  Badge,
  Button,
  Card,
  Checkbox,
  Collapse,
  Group,
  Loader,
  NumberInput,
  Progress,
  Select,
  SimpleGrid,
  SegmentedControl,
  Stack,
  Switch,
  Text,
  TextInput,
  Title,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { IconExternalLink } from '@tabler/icons-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { useCohortQuery, useRunStageMutation } from '../api';
import { useSystemResources } from '../queries';
import { PipelineStepper } from '../../shared/components/PipelineStepper';
import { findSuggestedActiveIndex } from '../../shared/components/pipelineStepperUtils';
import { StageCard } from '../../shared/components/StageCard';
import {
  STAGE_ORDER,
  type AnonymizeStageConfig,
  type ExtractStageConfig,
  type StageConfigById,
  type StageId,
  type StageSummary,
  type JobAction,
  type SystemResources,
} from '../../../types';
import { formatDateTime } from '../../../utils/formatters';
import { AnonymizeStageForm } from '../../anonymization/AnonymizeStageForm';
import { buildAnonymizeConfigFromExisting, buildDefaultAnonymizeConfig } from '../../anonymization/defaults';
import { ExtractStageForm } from '../../extraction/ExtractStageForm';
import { buildDefaultExtractConfig } from '../../extraction/defaults';
import { buildNonAnonymizeStageDefaults, type NonAnonymizeStageConfigDefaults } from '../../stages/defaults';
import type { JobSummary } from '../../../types';
import { ApiError } from '../../../utils/api-client';
import { useJobAction } from '../../jobs/api';
import { useJobsQuery } from '../../jobs/api';
import { useQueryClient } from '@tanstack/react-query';
import { SortingPipelineSimple } from '../../sorting/components/SortingPipelineSimple';
import { useRunSortingStep, sortingKeys, type SortingConfig } from '../../sorting';
import { useIdTypes } from '../../database/api';

// Debug logging disabled for production - uncomment for local debugging
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const debugLog = (_hypothesisId: string, _location: string, _message: string, _data: Record<string, unknown>) => {
  // No-op in production
};

type StageConfigState = Partial<StageConfigById> & NonAnonymizeStageConfigDefaults;
type GenericStageId = 'sort' | 'bids';

interface SortingState {
  config: SortingConfig;
  jobId: number | null;
  streamUrl: string | null;
}

export const CohortDetailPage = () => {
  const { cohortId } = useParams<{ cohortId: string }>();
  const { data: cohort, isLoading, isError, error } = useCohortQuery(cohortId);
  const { data: jobs } = useJobsQuery();
  const queryClient = useQueryClient();
  const systemResourcesQuery = useSystemResources();
  const { data: systemResources, isFetching: systemResourcesLoading, refetch: fetchSystemResources } =
    systemResourcesQuery;
  const [configs, setConfigs] = useState<StageConfigState>(() => ({
    ...buildNonAnonymizeStageDefaults(),
  } as StageConfigState));
  const [activeStageIndex, setActiveStageIndex] = useState(0);
  const runStageMutation = useRunStageMutation();
  const jobActionMutation = useJobAction();
  const [anonymizeConflict, setAnonymizeConflict] = useState<{ message: string; path?: string } | null>(null);
  const [configInitialized, setConfigInitialized] = useState(false);
  const lastInitializedCohortIdRef = useRef<number | null>(null);
  const [stageSelectionInitialized, setStageSelectionInitialized] = useState(false);

  // Sorting state
  const runSortingStepMutation = useRunSortingStep();
  const [sortingState, setSortingState] = useState<SortingState>({
    config: {
      skipClassified: true,
      forceReprocess: false,
      profile: 'standard',
      selectedModalities: ['MR', 'CT', 'PT'],
      previewMode: false,  // Will be set to true for Step 2's first run
    },
    jobId: null,
    streamUrl: null,
  });

  const intentOptions = [
    { label: 'Anatomical (anat)', value: 'anat' },
    { label: 'Diffusion (dwi)', value: 'dwi' },
    { label: 'Functional (func)', value: 'func' },
    { label: 'Field map (fmap)', value: 'fmap' },
    { label: 'Perfusion (perf)', value: 'perf' },
    { label: 'Localizer', value: 'localizer' },
    { label: 'Misc', value: 'misc' },
  ];

  const defaultIntentSelection = intentOptions
    .filter((option) => option.value !== 'localizer' && option.value !== 'misc')
    .map((option) => option.value);

  const defaultProvenanceSelection = ['SyMRI', 'SWIRecon', 'EPIMix'];

  const provenanceOptions = [
    { label: 'SyMRI', value: 'SyMRI' },
    { label: 'SWI', value: 'SWIRecon' },
    { label: 'EPIMix', value: 'EPIMix' },
    { label: 'Projections/MPRs', value: 'ProjectionDerived' },
  ];

  // Fetch available identifier types for BIDS subject naming
  const { data: idTypesResponse } = useIdTypes();
  const subjectIdentifierOptions = useMemo(() => {
    const options = [{ label: 'Subject Code (default)', value: 'subject_code' }];
    if (idTypesResponse?.items) {
      for (const idType of idTypesResponse.items) {
        options.push({ label: idType.name, value: String(idType.id) });
      }
    }
    return options;
  }, [idTypesResponse]);

  useEffect(() => {
    debugLog('H-empty', 'CohortDetailPage', 'status', {
      cohortIdParam: cohortId,
      isLoading,
      isError,
      hasCohort: Boolean(cohort),
      errorMessage: error ? String((error as Error).message ?? error) : null,
    });
  }, [cohortId, cohort, isLoading, isError, error]);

  useEffect(() => {
    if (isError) {
      debugLog('H-empty', 'CohortDetailPage', 'error-state', {
        cohortIdParam: cohortId,
        errorMessage: error ? String((error as Error).message ?? error) : 'unknown',
      });
    }
  }, [isError, error, cohortId]);

  useEffect(() => {
    if (!cohort) {
      return;
    }
    if (lastInitializedCohortIdRef.current !== cohort.id) {
      setConfigInitialized(false);
      setStageSelectionInitialized(false);
      lastInitializedCohortIdRef.current = cohort.id;
    }
    debugLog('H-empty', 'CohortDetailPage', 'cohort-loaded', {
      cohortId: cohort.id,
      stageCount: Array.isArray(cohort.stages) ? cohort.stages.length : null,
    });
  }, [cohort]);

  useEffect(() => {
    if (!cohort || configInitialized) {
      return;
    }

    const base = {
      ...buildNonAnonymizeStageDefaults(),
    } as StageConfigState;

    debugLog('H3', 'CohortDetailPage', 'cohort-load', {
      cohortId,
      hasCohort: Boolean(cohort),
      isLoading,
      isError,
    });

    const stagesArray = Array.isArray(cohort.stages) ? cohort.stages : [];
    const anonymizeStage = stagesArray.find((stage) => stage.id === 'anonymize');

    const recommendationContext = systemResources
      ? {
        recommendedProcesses: systemResources.recommended_processes,
        recommendedWorkers: systemResources.recommended_workers,
      }
      : undefined;

    stagesArray.forEach((stage) => {
      if (stage.id === 'anonymize') {
        base.anonymize = buildAnonymizeConfigFromExisting(
          stage.config as AnonymizeStageConfig | undefined,
          {
            cohortName: cohort.name,
            sourcePath: cohort.source_path,
          },
          recommendationContext,
        );
      } else if (stage.id === 'extract') {
        const defaultExtract = buildDefaultExtractConfig();
        const existingExtract = (stage.config as Partial<ExtractStageConfig> | undefined) ?? {};
        base.extract = {
          ...defaultExtract,
          ...existingExtract,
          resumeByPath: existingExtract.resumeByPath ?? (existingExtract.resume ?? defaultExtract.resumeByPath),
        };
      } else if (stage.config) {
        base[stage.id] = {
          ...(base[stage.id] as any),
          ...(stage.config as any),
        } as any;
      }
    });

    if (!anonymizeStage) {
      base.anonymize = buildDefaultAnonymizeConfig(
        {
          cohortName: cohort.name,
          sourcePath: cohort.source_path,
        },
        recommendationContext,
      );
    }

    setConfigs(base);
    setConfigInitialized(true);
  }, [cohort, systemResources, configInitialized]);

  const orderedStages = useMemo(() => {
    if (!cohort) return [];
    const stagesArray = Array.isArray(cohort.stages) ? cohort.stages : [];
    const stageById = Object.fromEntries(stagesArray.map((stage) => [stage.id, stage]));
    return STAGE_ORDER.map((id) => stageById[id]).filter((stage): stage is StageSummary => Boolean(stage));
  }, [cohort]);

  const suggestedStageIndex = useMemo(() => findSuggestedActiveIndex(orderedStages), [orderedStages]);

  useEffect(() => {
    if (orderedStages.length === 0) {
      setActiveStageIndex(0);
      return;
    }

    // On initial load for this cohort, always use the suggested index
    if (!stageSelectionInitialized) {
      setActiveStageIndex(suggestedStageIndex);
      setStageSelectionInitialized(true);
      return;
    }

    // After initial load, only change if current selection is invalid
    setActiveStageIndex((prev) => (orderedStages[prev] ? prev : suggestedStageIndex));
  }, [orderedStages, suggestedStageIndex, stageSelectionInitialized]);

  const resolvedActiveIndex = orderedStages[activeStageIndex] ? activeStageIndex : suggestedStageIndex;
  const activeStage = orderedStages[resolvedActiveIndex];
  const activeStageConfig = activeStage ? configs[activeStage.id] : undefined;
  const activeGenericConfig =
    activeStage && (activeStage.id === 'sort' || activeStage.id === 'bids')
      ? (activeStageConfig as Record<string, unknown> | undefined)
      : undefined;

  const bidsStageJobId =
    activeStage?.id === 'bids' && activeStage.jobId ? Number(activeStage.jobId) : null;

  const bidsJobs = useMemo(() => {
    if (!jobs) return [];
    const filtered = jobs.filter((job) => {
      if (job.stageId !== 'bids') return false;
      const matchesCohort = cohort ? job.cohortId === cohort.id : true;
      const matchesStageJob = bidsStageJobId != null ? job.id === bidsStageJobId : false;
      return matchesCohort || matchesStageJob;
    });
    return filtered;
  }, [jobs, cohort, bidsStageJobId]);

  const activeBidsJob = useMemo(
    () => bidsJobs.find((job) => ['running', 'queued', 'paused'].includes(job.status)),
    [bidsJobs],
  );

  const lastBidsJob = useMemo(() => {
    if (!bidsJobs.length) return null;
    return [...bidsJobs].sort(
      (a, b) => new Date(b.submittedAt).getTime() - new Date(a.submittedAt).getTime(),
    )[0];
  }, [bidsJobs]);

  const anonymizeConfig = configs.anonymize as AnonymizeStageConfig | undefined;

  const stageBlocked = activeStage?.status === 'blocked';
  const blockingStage = stageBlocked
    ? orderedStages.slice(0, resolvedActiveIndex).find((stage) => stage.status !== 'completed')
    : undefined;
  const blockedReason = stageBlocked
    ? blockingStage
      ? `${blockingStage.title} must complete before this step becomes available.`
      : 'Complete the previous stage to unlock this step.'
    : undefined;

  const anonymizeJob = (cohort?.anonymize_job as JobSummary | undefined) ?? null;
  const anonymizeHistory = (cohort?.anonymize_history as JobSummary[] | undefined) ?? [];
  const extractJob = (cohort?.extract_job as JobSummary | undefined) ?? null;
  const extractHistory = (cohort?.extract_history as JobSummary[] | undefined) ?? [];
  const anonymizeJobStatus = anonymizeJob?.status;
  const anonymizeBusy = anonymizeJobStatus === 'running' || anonymizeJobStatus === 'queued';
  const extractBusy = extractJob ? ['queued', 'running', 'paused'].includes(extractJob.status) : false;
  const [showAnonymizeProgress, setShowAnonymizeProgress] = useState(false);
  useEffect(() => {
    if (anonymizeBusy) {
      setShowAnonymizeProgress(true);
    }
  }, [anonymizeBusy]);

  const handleGenericConfigChange = (
    stageId: GenericStageId,
    key: string,
    value: string | number | boolean | string[],
  ) => {
    setConfigs((prev) => {
      const nextStageConfig = {
        ...(prev[stageId] as Record<string, unknown> | undefined),
      };
      nextStageConfig[key] = value;
      return {
        ...prev,
        [stageId]: nextStageConfig,
      } as StageConfigState;
    });
  };

  const handleAnonymizeConfigChange = (next: AnonymizeStageConfig) => {
    setConfigs((prev) => ({
      ...prev,
      anonymize: next,
    }));
  };

  const handleExtractConfigChange = (next: ExtractStageConfig) => {
    setConfigs((prev) => ({
      ...prev,
      extract: next,
    }));
  };

  const applySystemRecommendations = async (apply: (resources: SystemResources) => void) => {
    const result = await fetchSystemResources();
    if (result.error) {
      notifications.show({
        color: 'red',
        message: result.error instanceof Error ? result.error.message : 'Unable to fetch system resources.',
      });
      return;
    }
    const resources = result.data ?? systemResources;
    if (!resources) return;
    apply(resources);
  };

  const handleRecommendAnonymizeResources = () =>
    applySystemRecommendations((resources) => {
      const recommendedProcesses = Math.max(1, resources.recommended_processes ?? 1);
      const recommendedWorkers = Math.max(1, resources.recommended_workers ?? recommendedProcesses);
      setConfigs((prev) => {
        const current = prev.anonymize as AnonymizeStageConfig | undefined;
        if (!current) {
          return prev;
        }
        if (current.processCount === recommendedProcesses && current.workerCount === recommendedWorkers) {
          return prev;
        }
        return {
          ...prev,
          anonymize: {
            ...current,
            processCount: recommendedProcesses,
            workerCount: recommendedWorkers,
          },
        } as StageConfigState;
      });
    });

  const handleRecommendExtractResources = () =>
    applySystemRecommendations((resources) => {
      setConfigs((prev) => {
        const current = prev.extract as ExtractStageConfig | undefined;
        if (!current) {
          return prev;
        }
        const workerCap = resources.max_workers_cap ?? 128;
        const batchCap = resources.max_batch_cap ?? 5000;
        const queueCap = resources.max_queue_cap ?? 500;
        const adaptiveCap = resources.max_adaptive_batch_cap ?? 20000;
        const safeBatchCap = resources.safe_instance_batch_rows ?? batchCap;
        const dbWriterPoolCap = resources.max_db_writer_pool_cap ?? 16;
        const recommendedWorkers = Math.min(workerCap, resources.recommended_workers ?? current.maxWorkers);
        const recommendedProcesses = Math.min(workerCap, resources.recommended_processes ?? recommendedWorkers);
        const recommendedBatch = Math.min(
          batchCap,
          safeBatchCap,
          resources.recommended_batch_size ?? current.batchSize,
        );
        const recommendedQueue = Math.min(queueCap, resources.recommended_queue_depth ?? current.queueSize);
        const recommendedAdaptiveMin = Math.min(
          recommendedBatch,
          safeBatchCap,
          resources.recommended_adaptive_min_batch ?? current.adaptiveMinBatchSize,
        );
        const recommendedAdaptiveMax = Math.min(
          adaptiveCap,
          safeBatchCap,
          resources.recommended_adaptive_max_batch ?? current.adaptiveMaxBatchSize,
        );

        const next: ExtractStageConfig = {
          ...current,
          maxWorkers: recommendedWorkers,
          processPoolWorkers: recommendedProcesses,
          batchSize: recommendedBatch,
          queueSize: recommendedQueue,
          adaptiveMinBatchSize: recommendedAdaptiveMin,
          adaptiveMaxBatchSize: Math.max(recommendedAdaptiveMin, recommendedAdaptiveMax),
          seriesWorkersPerSubject:
            resources.recommended_series_workers_per_subject ?? current.seriesWorkersPerSubject,
          dbWriterPoolSize: Math.min(
            dbWriterPoolCap,
            resources.recommended_db_writer_pool ?? current.dbWriterPoolSize ?? 3,
          ),
        };
        return {
          ...prev,
          extract: next,
        } as StageConfigState;
      });
    });

  const handleRunStage = (stageId: StageId, retryMode?: 'clean' | 'overwrite') => {
    if (!cohort) return;

    const targetStage = orderedStages.find((stage) => stage?.id === stageId);
    if (targetStage?.status === 'blocked') {
      notifications.show({ color: 'gray', message: 'Complete the previous stage to unlock this step.' });
      return;
    }

    const baseConfig = (configs[stageId] as Record<string, unknown> | undefined) ?? {};
    const payloadConfig: Record<string, unknown> = { ...baseConfig };

    if (stageId === 'anonymize') {
      setAnonymizeConflict(null);
      payloadConfig.derivativesRetryMode = retryMode ?? 'prompt';
      if (retryMode === 'overwrite') {
        payloadConfig.resume = true;
      } else if (!retryMode) {
        payloadConfig.resume = false;
      }
    }
    if (stageId === 'extract') {
      const resumeValue = typeof payloadConfig['resume'] === 'boolean' ? (payloadConfig['resume'] as boolean) : true;
      payloadConfig.resumeByPath = resumeValue;
    }


    runStageMutation.mutate(
      {
        cohort_id: cohort.id,
        stage_id: stageId,
        config: payloadConfig,
      },
      {
        onSuccess: () => {
          notifications.show({ color: 'teal', message: `${stageId} queued.` });
          if (stageId === 'anonymize') {
            setShowAnonymizeProgress(true);
          }
        },
        onError: (error) => {
          if (stageId === 'anonymize' && error instanceof ApiError && error.status === 409 && !retryMode) {
            const detail = (error.body ?? {}) as { message?: string; path?: string };
            setAnonymizeConflict({
              message:
                detail?.message ??
                'Existing anonymized files were detected under derivatives/dcm-raw. Choose how to proceed.',
              path: detail?.path,
            });
            setShowAnonymizeProgress(false);
            return;
          }
          notifications.show({ color: 'red', message: (error as Error).message });
        },
      },
    );
  };

  const handleExtractionAction = (action: JobAction) => {
    if (!extractJob || jobActionMutation.isPending) {
      return;
    }
    jobActionMutation.mutate({ jobId: extractJob.id, action });
  };

  const handlePauseExtraction = () => handleExtractionAction('pause');
  const handleResumeExtraction = () => handleExtractionAction('resume');
  const handleCancelExtraction = () => handleExtractionAction('cancel');

  const renderAnonymizeConflict = () => {
    if (!anonymizeConflict) return null;
    return (
      <Card withBorder radius="md" padding="md" bg="var(--mantine-color-red-0)">
        <Stack gap="sm">
          <Text fw={600}>Existing anonymized files detected</Text>
          <Text size="sm">
            {anonymizeConflict.message}
            {anonymizeConflict.path ? ` (Path: ${anonymizeConflict.path})` : ''}
          </Text>
          <Group gap="sm">
            <Button color="red" variant="filled" onClick={() => handleRunStage('anonymize', 'clean')}>
              Clean processed folder &amp; retry
            </Button>
            <Button color="blue" variant="light" onClick={() => handleRunStage('anonymize', 'overwrite')}>
              Continue and skip existing files
            </Button>
            <Button variant="default" onClick={() => setAnonymizeConflict(null)}>
              Cancel
            </Button>
          </Group>
        </Stack>
      </Card>
    );
  };

  const renderAnonymizeExecution = () => {
    if (!anonymizeJob || !anonymizeBusy) return null;
    const jobConfig = (anonymizeJob.config ?? {}) as Record<string, unknown>;
    const sourceRoot = (jobConfig.source_root as string | undefined) ?? cohort?.source_path;
    const outputRoot = jobConfig.output_root as string | undefined;
    const strategy = (jobConfig.patient_id as { strategy?: string } | undefined)?.strategy ?? 'unknown';

    return (
      <Card withBorder padding="md" radius="md">
        <Stack gap="sm">
          <Group justify="space-between" align="center">
            <Text fw={600}>Anonymization in progress</Text>
            <Badge color={anonymizeJob.status === 'running' ? 'blue' : 'yellow'}>
              {anonymizeJob.status.toUpperCase()}
            </Badge>
          </Group>
          <Stack gap={4}>
            <Text size="sm">
              <strong>Source:</strong> {sourceRoot}
            </Text>
            {outputRoot && (
              <Text size="sm">
                <strong>Output:</strong> {outputRoot}
              </Text>
            )}
            <Text size="sm">
              <strong>Patient ID strategy:</strong> {strategy}
            </Text>
          </Stack>
          <Stack gap={4}>
            <Text size="xs" c="dimmed">
              Job progress
            </Text>
            <Progress value={anonymizeJob.progress} size="lg" radius="md" transitionDuration={200} />
            <Text size="sm" fw={600}>
              {anonymizeJob.progress}%
            </Text>
          </Stack>
          <Group justify="space-between" align="center">
            <Text size="xs" c="dimmed">
              Started {anonymizeJob.startedAt ? formatDateTime(anonymizeJob.startedAt) : 'pending'}
            </Text>
            <Button size="xs" variant="light" component={Link} to="/jobs">
              View all jobs
            </Button>
          </Group>
        </Stack>
      </Card>
    );
  };

  const renderAnonymizeHistory = () => {
    if (!anonymizeHistory.length) return null;
    return (
      <Card withBorder padding="md" radius="md">
        <Stack gap="xs">
          <Text fw={600}>Recent anonymization runs</Text>
          {anonymizeHistory.slice(0, 5).map((job) => (
            <Group key={job.id} justify="space-between" align="center">
              <Stack gap={0}>
                <Text size="sm" fw={500}>
                  Job #{job.id} · {job.status}
                </Text>
                <Text size="xs" c="dimmed">
                  Started {job.startedAt ? formatDateTime(job.startedAt) : formatDateTime(job.submittedAt)}
                </Text>
              </Stack>
              <Badge color={job.status === 'completed' ? 'teal' : job.status === 'failed' ? 'red' : 'blue'}>
                {job.progress}%
              </Badge>
            </Group>
          ))}
        </Stack>
      </Card>
    );
  };

  const renderExtractSummary = () => {
    if (!extractHistory.length) return null;
    const latest = extractHistory[0];
    const badgeColor =
      latest.status === 'completed'
        ? 'teal'
        : latest.status === 'failed'
          ? 'red'
          : latest.status === 'running'
            ? 'blue'
            : latest.status === 'queued'
              ? 'yellow'
              : 'gray';
    const metrics = latest.metrics;

    return (
      <Card withBorder padding="md" radius="md">
        <Stack gap="sm">
          <Group justify="space-between" align="center">
            <Text fw={600}>Last extraction summary</Text>
            <Badge color={badgeColor}>{latest.status.toUpperCase()}</Badge>
          </Group>
          <Stack gap={0}>
            <Text size="xs" c="dimmed">
              Started {latest.startedAt ? formatDateTime(latest.startedAt) : formatDateTime(latest.submittedAt)}
            </Text>
            {latest.finishedAt && (
              <Text size="xs" c="dimmed">
                Finished {formatDateTime(latest.finishedAt)}
              </Text>
            )}
          </Stack>
          {metrics ? (
            <SimpleGrid cols={{ base: 2, sm: 4 }} spacing="md">
              {[
                { label: 'Subjects', value: metrics.subjects },
                { label: 'Studies', value: metrics.studies },
                { label: 'Series', value: metrics.series },
                { label: 'Instances', value: metrics.instances },
              ].map((entry) => (
                <Stack key={entry.label} gap={2} align="flex-start">
                  <Text size="xs" c="dimmed">
                    {entry.label}
                  </Text>
                  <Text fw={600}>{entry.value.toLocaleString()}</Text>
                </Stack>
              ))}
            </SimpleGrid>
          ) : (
            <Text size="xs" c="dimmed">
              Metrics unavailable for the latest run.
            </Text>
          )}
        </Stack>
      </Card>
    );
  };

  if (isLoading) {
    return (
      <Group justify="center" py="xl">
        <Loader />
      </Group>
    );
  }

  if (isError || !cohort) {
    return (
      <Stack gap="sm" p="md">
        <Title order={3}>Cohort not found</Title>
        <Text c="dimmed">The requested cohort does not exist in the mock dataset.</Text>
        <Anchor component={Link} to="/cohorts">
          <Group gap={4} wrap="nowrap">
            <IconExternalLink size={14} />
            <Text size="sm">Back to cohorts</Text>
          </Group>
        </Anchor>
      </Stack>
    );
  }

  return (
    <Stack gap="lg" p="md">
      <Stack gap={2}>
        <Title order={2}>{cohort.name}</Title>
        <Text c="dimmed" size="sm">
          Source path: {cohort.source_path}
        </Text>
        <Text size="xs" c="dimmed">
          Last updated {formatDateTime(cohort.updated_at)}
        </Text>
      </Stack>

      <PipelineStepper
        stages={orderedStages}
        activeStageIndex={resolvedActiveIndex}
        onStageClick={(index) => {
          if (index >= 0 && index < orderedStages.length) {
            setActiveStageIndex(index);
          }
        }}
      />

      <Card withBorder radius="md" padding="md">
        <Group justify="space-between">
          <Stack gap={2}>
            <Text size="sm" c="dimmed">
              Metrics
            </Text>
            <Text fw={600}>
              {cohort.total_subjects} subjects · {cohort.total_sessions} sessions
            </Text>
          </Stack>
          <Anchor component={Link} to="/jobs" size="sm">
            View job history
          </Anchor>
        </Group>
      </Card>

      {activeStage && (
        <StageCard
          stage={activeStage}
          disabled={
            stageBlocked ||
            (activeStage.id === 'anonymize' && (anonymizeBusy || Boolean(anonymizeConflict)))
          }
          onRun={activeStage.id === 'sort' ? undefined : () => handleRunStage(activeStage.id)}
          blockedReason={blockedReason}
          onPause={
            activeStage.id === 'extract' &&
              extractJob &&
              extractJob.status === 'running' &&
              !jobActionMutation.isPending
              ? handlePauseExtraction
              : undefined
          }
        >
          {activeStage.id === 'anonymize' && (
            <Stack gap="md">
              {renderAnonymizeConflict()}
              {anonymizeBusy && renderAnonymizeExecution()}
              <Collapse in={!anonymizeBusy && !anonymizeConflict && (!showAnonymizeProgress || anonymizeJobStatus === 'completed')}>
                {anonymizeConfig && (
                  <AnonymizeStageForm
                    cohortName={cohort.name}
                    cohortId={cohort.id}
                    config={anonymizeConfig}
                    onChange={handleAnonymizeConfigChange}
                    onRecommendResources={handleRecommendAnonymizeResources}
                    recommendLoading={systemResourcesLoading}
                    recommendation={systemResources}
                  />
                )}
              </Collapse>
              {!anonymizeBusy && showAnonymizeProgress && anonymizeJob && (
                <Card withBorder padding="md" radius="md">
                  <Stack gap="sm">
                    <Group justify="space-between" align="center">
                      <Text fw={600}>Last anonymization summary</Text>
                      <Button size="xs" variant="light" onClick={() => setShowAnonymizeProgress(false)}>
                        Show configuration
                      </Button>
                    </Group>
                    <Text size="sm">Status: {anonymizeJob.status}</Text>
                    <Text size="sm">
                      Started {anonymizeJob.startedAt ? formatDateTime(anonymizeJob.startedAt) : formatDateTime(anonymizeJob.submittedAt)}
                    </Text>
                    {anonymizeJob.finishedAt && (
                      <Text size="sm">Finished {formatDateTime(anonymizeJob.finishedAt)}</Text>
                    )}
                  </Stack>
                </Card>
              )}
              {renderAnonymizeHistory()}
            </Stack>
          )}

          {activeStage.id === 'extract' && cohort && configs.extract && (
            <Stack gap="md">
              <ExtractStageForm
                sourcePath={cohort.source_path}
                config={configs.extract}
                job={extractJob}
                onChange={handleExtractConfigChange}
                onRecommendResources={handleRecommendExtractResources}
                recommendLoading={systemResourcesLoading}
                recommendation={systemResources ?? undefined}
                onPauseJob={extractJob ? handlePauseExtraction : undefined}
                onResumeJob={extractJob ? handleResumeExtraction : undefined}
                onCancelJob={extractJob ? handleCancelExtraction : undefined}
                jobActionPending={jobActionMutation.isPending}
              />
              {!extractBusy && renderExtractSummary()}
            </Stack>
          )}

          {activeStage.id === 'sort' && cohort && (
            <SortingPipelineSimple
              cohortId={cohort.id}
              config={sortingState.config}
              onConfigChange={(config) => setSortingState(prev => ({ ...prev, config }))}
              onRunStep={(stepId) => {
                console.log('[Sort] Running step:', stepId);

                // Invalidate sorting status cache so we get fresh data
                queryClient.invalidateQueries({ queryKey: sortingKeys.status(cohort.id) });

                // Always run individual step (step-wise is the only mode)
                runSortingStepMutation.mutate(
                  { cohortId: cohort.id, stepId, config: sortingState.config },
                  {
                    onSuccess: (result) => {
                      setSortingState(prev => ({
                        ...prev,
                        jobId: result.job_id,
                        streamUrl: result.stream_url,
                      }));
                    },
                    onError: (error) => {
                      notifications.show({
                        color: 'red',
                        title: 'Step Execution Failed',
                        message: (error as Error).message
                      });
                    },
                  }
                );
              }}
              isLoading={runSortingStepMutation.isPending}
              disabled={stageBlocked}
              jobId={sortingState.jobId}
              streamUrl={sortingState.streamUrl}
            />
          )}

          {activeStage.id === 'bids' && activeGenericConfig && (
            <Stack gap="sm">
              {(() => {
                const bidsConfig = (activeGenericConfig as Record<string, unknown>) || {};
                const rawIncludeIntents = bidsConfig.includeIntents as string[] | undefined;
                const includeIntentsVal =
                  rawIncludeIntents && rawIncludeIntents.length === 0
                    ? defaultIntentSelection
                    : rawIncludeIntents ?? defaultIntentSelection;
                const rawIncludeProvenance = bidsConfig.includeProvenance as string[] | undefined;
                const includeProvVal =
                  rawIncludeProvenance && rawIncludeProvenance.length === 0
                    ? defaultProvenanceSelection
                    : rawIncludeProvenance ?? defaultProvenanceSelection;
                const showProvenanceSelection = includeIntentsVal.includes('anat');
                const copyWorkersVal = Number((bidsConfig.copyWorkers as number | string | boolean | undefined) ?? 8);
                const convertWorkersVal = Number((bidsConfig.convertWorkers as number | string | boolean | undefined) ?? 8);
                const bidsDcmRootNameVal = String((bidsConfig.bidsDcmRootName as string | undefined) ?? 'bids-dcm');
                const bidsNiftiRootNameVal = String((bidsConfig.bidsNiftiRootName as string | undefined) ?? 'bids-nifti');
                const rawFlatDcmRoot = bidsConfig.flatDcmRootName as string | undefined;
                const rawFlatNiftiRoot = bidsConfig.flatNiftiRootName as string | undefined;
                const flatDcmRootNameVal =
                  rawFlatDcmRoot === 'dcm-flat'
                    ? 'flat-dcm'
                    : String(rawFlatDcmRoot ?? 'flat-dcm');
                const flatNiftiRootNameVal =
                  rawFlatNiftiRoot === 'nii-flat'
                    ? 'flat-nifti'
                    : String(rawFlatNiftiRoot ?? 'flat-nifti');
                const layoutVal = String((bidsConfig.layout as string | undefined) ?? 'bids');
                const layoutLabel = layoutVal === 'flat' ? 'Flat' : 'BIDS';
                const outputModesVal =
                  (bidsConfig.outputModes as string[] | undefined) ??
                  ((bidsConfig.outputMode as string | undefined) ? [String(bidsConfig.outputMode)] : ['dcm']);
                const exportDicom = outputModesVal.includes('dcm');
                const niftiModeVal = outputModesVal.find(
                  (mode) => mode === 'nii' || mode === 'nii.gz',
                ) as 'nii' | 'nii.gz' | undefined;
                const applyOutputModes = (nextDicom: boolean, nextNifti: 'nii' | 'nii.gz' | null) => {
                  const next: string[] = [];
                  if (nextDicom) next.push('dcm');
                  if (nextNifti) next.push(nextNifti);
                  const safeNext = next.length ? next : ['dcm'];
                  handleGenericConfigChange('bids', 'outputModes', safeNext);
                  // Keep legacy field in sync for older configs
                  handleGenericConfigChange('bids', 'outputMode', nextNifti ?? (nextDicom ? 'dcm' : 'dcm'));
                };
                const overwriteRaw = (bidsConfig.overwriteMode as string | undefined) ?? 'skip';
                const overwriteModeVal = ['skip', 'clean', 'overwrite'].includes(overwriteRaw) ? overwriteRaw : 'skip';
                const subjectIdentifierSourceVal = String(
                  (bidsConfig.subjectIdentifierSource as string | number | undefined) ?? 'subject_code',
                );
                const jobStatusColor = (status: string) => {
                  switch (status) {
                    case 'running':
                      return 'blue';
                    case 'queued':
                      return 'yellow';
                    case 'paused':
                      return 'orange';
                    case 'completed':
                      return 'teal';
                    case 'failed':
                      return 'red';
                    default:
                      return 'gray';
                  }
                };
                const bidsRunningStatuses = ['running', 'queued', 'paused'];
                const renderBidsJobCard = (job: JobSummary, title: string) => (
                  <Card withBorder padding="md" radius="md">
                    <Stack gap="xs">
                      <Group justify="space-between" align="center">
                        <Text fw={600}>{title}</Text>
                        <Badge color={jobStatusColor(job.status)}>{job.status.toUpperCase()}</Badge>
                      </Group>
                      <Progress value={job.progress ?? 0} size="lg" radius="md" transitionDuration={200} />
                      <Group justify="space-between">
                        <Text size="xs" c="dimmed">
                          Started {job.startedAt ? formatDateTime(job.startedAt) : formatDateTime(job.submittedAt)}
                        </Text>
                        <Text size="xs" c="dimmed">
                          Job #{job.id}
                        </Text>
                      </Group>
                      {job.finishedAt && (
                        <Text size="xs" c="dimmed">
                          Finished {formatDateTime(job.finishedAt)}
                        </Text>
                      )}
                      {job.errorMessage && (
                        <Text size="xs" c="red">
                          Error: {job.errorMessage}
                        </Text>
                      )}
                    </Stack>
                  </Card>
                );

                if (activeBidsJob && bidsRunningStatuses.includes(activeBidsJob.status)) {
                  return (
                    <>
                      {renderBidsJobCard(activeBidsJob, 'BIDS export in progress')}
                      {activeBidsJob.status === 'paused' && (
                        <Text size="xs" c="dimmed">
                          Job paused. Manage actions from the Jobs page.
                        </Text>
                      )}
                    </>
                  );
                }

                return (
                  <>
              <Stack gap="xs">
                <SegmentedControl
                  data={[
                    { label: 'BIDS layout', value: 'bids' },
                    { label: 'Flat layout', value: 'flat' },
                  ]}
                  value={layoutVal}
                  onChange={(value) => handleGenericConfigChange('bids', 'layout', value ?? 'bids')}
                />
                <SegmentedControl
                  value={overwriteModeVal}
                  onChange={(value) => handleGenericConfigChange('bids', 'overwriteMode', value ?? 'skip')}
                  data={[
                    { label: 'Skip existing', value: 'skip' },
                    { label: 'Clean', value: 'clean' },
                    { label: 'Overwrite', value: 'overwrite' },
                  ]}
                />
                <Group gap="md" align="center">
                  <Switch
                    label="Export DICOM"
                    checked={exportDicom}
                    onChange={(event) => applyOutputModes(event.currentTarget.checked, niftiModeVal ?? null)}
                  />
                  {exportDicom && (
                    <TextInput
                      label={`${layoutLabel} DICOM root (under derivatives)`}
                      value={layoutVal === 'flat' ? flatDcmRootNameVal : bidsDcmRootNameVal}
                      style={{ flex: 1 }}
                      onChange={(event) =>
                        handleGenericConfigChange(
                          'bids',
                          layoutVal === 'flat' ? 'flatDcmRootName' : 'bidsDcmRootName',
                          event.currentTarget.value,
                        )
                      }
                    />
                  )}
                </Group>
                <Group gap="md" align="center">
                  <Switch
                    label="Export NIfTI"
                    checked={Boolean(niftiModeVal)}
                    onChange={(event) =>
                      applyOutputModes(exportDicom, event.currentTarget.checked ? (niftiModeVal ?? 'nii.gz') : null)
                    }
                  />
                  {Boolean(niftiModeVal) && (
                    <TextInput
                      label={`${layoutLabel} NIfTI root (under derivatives)`}
                      value={layoutVal === 'flat' ? flatNiftiRootNameVal : bidsNiftiRootNameVal}
                      style={{ flex: 1 }}
                      onChange={(event) =>
                        handleGenericConfigChange(
                          'bids',
                          layoutVal === 'flat' ? 'flatNiftiRootName' : 'bidsNiftiRootName',
                          event.currentTarget.value,
                        )
                      }
                    />
                  )}
                </Group>
                {Boolean(niftiModeVal) && (
                  <SegmentedControl
                    data={[
                      { label: 'NIfTI (.nii)', value: 'nii' },
                      { label: 'Compressed (.nii.gz)', value: 'nii.gz' },
                    ]}
                    value={niftiModeVal}
                    onChange={(value) => applyOutputModes(exportDicom, value as 'nii' | 'nii.gz')}
                  />
                )}
              </Stack>
              <Select
                label="Subject identifier"
                description="Choose which identifier to use for subject naming (sub-*)"
                value={subjectIdentifierSourceVal}
                data={subjectIdentifierOptions}
                onChange={(value) => {
                  // Convert numeric string back to number for id_type_id values
                  const parsed = value === 'subject_code' ? 'subject_code' : Number(value);
                  handleGenericConfigChange('bids', 'subjectIdentifierSource', parsed);
                }}
              />
              <Checkbox.Group
                label="Include intents"
                description="Select which directory types to include in the export"
                value={includeIntentsVal}
                onChange={(value) => handleGenericConfigChange('bids', 'includeIntents', value)}
              >
                <Group mt="xs">
                  {intentOptions.map((option) => (
                    <Checkbox key={option.value} value={option.value} label={option.label} />
                  ))}
                </Group>
              </Checkbox.Group>
              {showProvenanceSelection && (
                <Checkbox.Group
                  label="Include provenance (anat only)"
                  description="Leave empty to include all provenance types"
                  value={includeProvVal}
                  onChange={(value) => handleGenericConfigChange('bids', 'includeProvenance', value)}
                >
                  <Group mt="xs">
                    {provenanceOptions.map((option) => (
                      <Checkbox key={option.value} value={option.value} label={option.label} />
                    ))}
                  </Group>
                </Checkbox.Group>
              )}
              <Group grow>
                <NumberInput
                  label="Copy workers (DICOM)"
                  min={1}
                  value={copyWorkersVal}
                  onChange={(value) => handleGenericConfigChange('bids', 'copyWorkers', Number(value ?? 8))}
                />
                <NumberInput
                  label="Convert workers (NIfTI)"
                  min={1}
                  value={convertWorkersVal}
                  onChange={(value) => handleGenericConfigChange('bids', 'convertWorkers', Number(value ?? 8))}
                />
              </Group>
              {lastBidsJob && renderBidsJobCard(lastBidsJob, 'Last BIDS export')}
                  </>
                );
              })()}
            </Stack>
          )}
        </StageCard>
      )}
    </Stack>
  );
};
