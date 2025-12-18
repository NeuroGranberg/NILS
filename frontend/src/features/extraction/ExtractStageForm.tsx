import {
  Alert,
  Badge,
  Button,
  Card,
  Divider,
  FileButton,
  Group,
  Modal,
  NumberInput,
  Progress,
  SimpleGrid,
  Select,
  Stack,
  Switch,
  Text,
  TextInput,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useEffect, useMemo, useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import type {
  ExtractPerformanceConfigPatch,
  ExtractStageConfig,
  JobStatus,
  JobSummary,
  SystemResources,
} from '../../types';
import { formatDateTime } from '../../utils/formatters';
import { apiClient } from '../../utils/api-client';
import { useCsvColumns } from '../anonymization/queries';
import { useMetadataIdTypes, type IdTypeInfo } from './queries';
import { IconPlayerPause, IconPlayerPlay, IconPlayerStop } from '@tabler/icons-react';
import { useUpdateJobConfig } from '../jobs/api';

const statusBadgeColor: Record<JobStatus, string> = {
  queued: 'yellow',
  running: 'blue',
  paused: 'yellow',
  completed: 'teal',
  failed: 'red',
  canceled: 'gray',
};

const TERMINAL_JOB_STATUSES: JobStatus[] = ['completed', 'failed', 'canceled'];

const ExtractProgressCard = ({
  job,
  sourcePath,
  onPause,
  onResume,
  onCancel,
  actionPending,
}: {
  job: JobSummary;
  sourcePath: string;
  onPause?: () => void;
  onResume?: () => void;
  onCancel?: () => void;
  actionPending?: boolean;
}) => {
  const metrics = job.metrics;
  const metricEntries = metrics
    ? (
        [
          { label: 'Subjects', value: metrics.subjects },
          { label: 'Studies', value: metrics.studies },
          { label: 'Series', value: metrics.series },
          { label: 'Instances', value: metrics.instances },
        ] as Array<{ label: string; value: number }>
      )
    : [];
  if (metrics && typeof metrics.safe_batch_rows === 'number') {
    metricEntries.push({ label: 'Safe batch cap', value: metrics.safe_batch_rows });
  }
  const sourceRoot = (job.config?.raw_root as string | undefined) ?? sourcePath;
  const startedLabel = job.startedAt ? formatDateTime(job.startedAt) : 'Pending';
  const { elapsedMs, etaMs, totalMs } = useMemo(() => deriveJobDurations(job), [job]);
  const elapsedLabel = formatDuration(elapsedMs);
  const etaLabel = formatDuration(etaMs);
  const isTerminalStatus = TERMINAL_JOB_STATUSES.includes(job.status);
  const totalLabel = formatDuration(totalMs ?? elapsedMs);
  let timingText: string | null = null;
  if (isTerminalStatus) {
    if (totalLabel) {
      timingText = `Total duration ${totalLabel}`;
    }
  } else {
    const parts: string[] = [];
    if (elapsedLabel) {
      parts.push(`Elapsed ${elapsedLabel}`);
    }
    if (etaLabel) {
      parts.push(`ETA ${etaLabel}`);
    }
    if (parts.length) {
      timingText = parts.join(' · ');
    }
  }

  return (
    <Card withBorder radius="md" padding="md">
      <Stack gap="sm">
        <Group justify="space-between" align="center">
          <Text fw={600}>Extraction job</Text>
          <Badge color={statusBadgeColor[job.status] ?? 'gray'}>{job.status.toUpperCase()}</Badge>
        </Group>

        <Stack gap={4}>
          <Text size="sm">
            <strong>Source:</strong> {sourceRoot}
          </Text>
          {job.cohortName && (
            <Text size="sm">
              <strong>Cohort:</strong> {job.cohortName}
            </Text>
          )}
        </Stack>

        {metrics ? (
          <SimpleGrid cols={{ base: 2, sm: 4, md: 5 }} spacing="md">
            {metricEntries.map((entry) => (
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
            Metrics will appear once ingestion begins.
          </Text>
        )}

        <Stack gap={4}>
          <Text size="xs" c="dimmed">
            Job progress
          </Text>
          <Progress value={job.progress} size="lg" radius="md" transitionDuration={200} />
          <Text size="sm" fw={600}>
            {job.progress}%
          </Text>
          {timingText && (
            <Text size="xs" c="dimmed">
              {timingText}
            </Text>
          )}
        </Stack>

        {(onPause || onResume || onCancel) && (
          <Group gap="xs">
            {job.status === 'running' && onPause && (
              <Button
                size="xs"
                variant="default"
                leftSection={<IconPlayerPause size={14} />}
                onClick={onPause}
                loading={actionPending}
              >
                Pause
              </Button>
            )}
            {job.status === 'paused' && onResume && (
              <Button
                size="xs"
                variant="filled"
                leftSection={<IconPlayerPlay size={14} />}
                onClick={onResume}
                loading={actionPending}
              >
                Resume
              </Button>
            )}
            {['queued', 'running', 'paused'].includes(job.status) && onCancel && (
              <Button
                size="xs"
                color="red"
                variant="light"
                leftSection={<IconPlayerStop size={14} />}
                onClick={onCancel}
                loading={actionPending}
              >
                Cancel
              </Button>
            )}
          </Group>
        )}
        <Group justify="space-between" align="center">
          <Text size="xs" c="dimmed">
            Started {startedLabel}
          </Text>
          <Button size="xs" variant="light" component={Link} to="/jobs">
            View all jobs
          </Button>
        </Group>
      </Stack>
    </Card>
  );
};

interface ExtractStageFormProps {
  config: ExtractStageConfig;
  sourcePath: string;
  job?: JobSummary | null;
  onChange: (config: ExtractStageConfig) => void;
  onRecommendResources?: () => void;
  recommendLoading?: boolean;
  recommendation?: SystemResources;
  onPauseJob?: () => void;
  onResumeJob?: () => void;
  onCancelJob?: () => void;
  jobActionPending?: boolean;
}

const EXTENSION_OPTIONS = [
  { value: 'all', label: 'All (.dcm/.DCM/no extension)' },
  { value: 'dcm', label: 'Lowercase .dcm only' },
  { value: 'DCM', label: 'Uppercase .DCM only' },
  { value: 'all_dcm', label: 'Any case .dcm' },
  { value: 'no_ext', label: 'Files without extension' },
];

const DUPLICATE_POLICY = [
  { value: 'skip', label: 'Skip duplicates (recommended)' },
  { value: 'append_series', label: 'Append only new series' },
  { value: 'overwrite', label: 'Overwrite metadata' },
];

interface CsvUploadResponse {
  token: string;
  filename: string;
  columns: string[];
}

const formatBytes = (value: number) => {
  if (value <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = value;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${size.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
};

const formatBytesPerSecond = (value: number) => `${formatBytes(value)}/s`;

const formatDuration = (milliseconds: number | null | undefined): string | null => {
  if (milliseconds == null || Number.isNaN(milliseconds)) {
    return null;
  }
  if (milliseconds < 1000) {
    return '<1s';
  }
  const totalSeconds = Math.floor(milliseconds / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const pad = (value: number) => value.toString().padStart(2, '0');
  if (hours > 0) {
    return `${pad(hours)}:${pad(minutes)}:${pad(seconds)}`;
  }
  return `${pad(minutes)}:${pad(seconds)}`;
};

const deriveJobDurations = (job: JobSummary) => {
  const now = Date.now();
  const startedAtMs = job.startedAt ? new Date(job.startedAt).getTime() : null;
  const finishedAtMs = job.finishedAt ? new Date(job.finishedAt).getTime() : null;

  const fallbackElapsedMs =
    startedAtMs != null ? Math.max(0, (finishedAtMs ?? now) - startedAtMs) : null;

  const elapsedMs = job.elapsedMs ?? fallbackElapsedMs ?? null;
  const etaMs =
    job.etaMs ??
    (fallbackElapsedMs != null && job.progress > 0 && job.progress < 100
      ? Math.max(0, Math.round((fallbackElapsedMs * (100 - job.progress)) / job.progress))
      : null);

  let totalMs = job.totalMs ?? null;
  if (totalMs == null) {
    if (elapsedMs != null && etaMs != null) {
      totalMs = elapsedMs + etaMs;
    } else if (startedAtMs != null && finishedAtMs != null) {
      totalMs = Math.max(0, finishedAtMs - startedAtMs);
    }
  }

  return { elapsedMs, etaMs, totalMs };
};

export const ExtractStageForm = ({
  config,
  sourcePath,
  job,
  onChange,
  onRecommendResources,
  recommendLoading,
  recommendation,
  onPauseJob,
  onResumeJob,
  onCancelJob,
  jobActionPending,
}: ExtractStageFormProps) => {
  const runningStatuses: JobStatus[] = ['queued', 'running'];
  const workerCap = recommendation?.max_workers_cap ?? 128;
  const batchCap = recommendation?.max_batch_cap ?? 5000;
  const queueCap = recommendation?.max_queue_cap ?? 500;
  const adaptiveCap = recommendation?.max_adaptive_batch_cap ?? 20000;
  const safeBatchCap = recommendation?.safe_instance_batch_rows ?? job?.metrics?.safe_batch_rows ?? null;
  const effectiveBatchCap = safeBatchCap ? Math.min(batchCap, safeBatchCap) : batchCap;
  const dbWriterPoolCap = recommendation?.max_db_writer_pool_cap ?? 16;
  const adaptiveBatchCap = safeBatchCap ? Math.min(adaptiveCap, safeBatchCap) : adaptiveCap;
  const adaptiveMinFieldCap = safeBatchCap
    ? Math.min(config.adaptiveMaxBatchSize, safeBatchCap)
    : config.adaptiveMaxBatchSize;
  const exceedsSafeBatch = safeBatchCap != null && config.batchSize > safeBatchCap;

  const queryClient = useQueryClient();
  const [csvUploadError, setCsvUploadError] = useState<string | null>(null);
  const [csvUploading, setCsvUploading] = useState(false);
  const [createdIdTypes, setCreatedIdTypes] = useState<IdTypeInfo[]>([]);
  const [idTypeError, setIdTypeError] = useState<string | null>(null);
  const [idTypeModalOpen, setIdTypeModalOpen] = useState(false);
  const [newIdTypeName, setNewIdTypeName] = useState('');
  const [newIdTypeDescription, setNewIdTypeDescription] = useState('');

  const { data: idTypesResponse } = useMetadataIdTypes();
  const createIdTypeMutation = useMutation({
    mutationFn: (body: { name: string; description?: string | null }) =>
      apiClient.post<IdTypeInfo>('/metadata/id-types', body),
  });

  const mergedIdTypes = useMemo(() => {
    const fromApi = idTypesResponse ?? [];
    if (createdIdTypes.length === 0) {
      return fromApi;
    }
    const existingIds = new Set(fromApi.map((item) => item.id));
    const merged = [...fromApi];
    createdIdTypes.forEach((item) => {
      if (!existingIds.has(item.id)) {
        merged.push(item);
      }
    });
    merged.sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: 'accent' }));
    return merged;
  }, [idTypesResponse, createdIdTypes]);

  const idTypeOptions = useMemo(
    () =>
      mergedIdTypes.map((item) => ({
        value: String(item.id),
        label: item.name,
      })),
    [mergedIdTypes],
  );

  const handleFieldChange = <K extends keyof ExtractStageConfig>(key: K, value: ExtractStageConfig[K]) => {
    if (key === 'resume') {
      const resumeValue = Boolean(value);
      onChange({
        ...config,
        resume: resumeValue,
        resumeByPath: resumeValue,
      });
      return;
    }

    onChange({
      ...config,
      [key]: value,
    });
  };

  const csvToken = config.subjectCodeCsv?.fileToken ?? null;
  const csvColumnsResponse = useCsvColumns(csvToken, { enabled: Boolean(csvToken) });
  const csvColumns = csvColumnsResponse.data?.columns ?? [];

  const resetIdTypeModal = () => {
    setNewIdTypeName('');
    setNewIdTypeDescription('');
    setIdTypeError(null);
  };

  const submitNewIdType = async () => {
    const trimmedName = newIdTypeName.trim();
    if (!trimmedName) {
      setIdTypeError('Identifier type name cannot be empty.');
      return;
    }

    const trimmedDescription = newIdTypeDescription.trim();

    try {
      const created = await createIdTypeMutation.mutateAsync({
        name: trimmedName,
        description: trimmedDescription ? trimmedDescription : undefined,
      });
      setCreatedIdTypes((prev) => {
        if (prev.some((entry) => entry.id === created.id)) {
          return prev;
        }
        return [...prev, created];
      });
      handleFieldChange('subjectIdTypeId', created.id);
      await queryClient.invalidateQueries({ queryKey: ['metadata-id-types'] });
      setIdTypeModalOpen(false);
      resetIdTypeModal();
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to create identifier type.';
      setIdTypeError(message);
    }
  };

  const handleSubjectCodeCsvChange = (
    patch: Partial<NonNullable<ExtractStageConfig['subjectCodeCsv']>> | null,
  ) => {
    if (patch === null) {
      handleFieldChange('subjectCodeCsv', null);
      return;
    }
    const current = config.subjectCodeCsv ?? {};
    handleFieldChange('subjectCodeCsv', { ...current, ...patch });
  };

  const handleSubjectCodeCsvUpload = async (file: File | null) => {
    if (!file) return;
    setCsvUploadError(null);
    setCsvUploading(true);
    try {
      const form = new FormData();
      form.append('file', file);
      const response = await apiClient.postForm<CsvUploadResponse>('/uploads/csv', form);
      handleSubjectCodeCsvChange({
        fileToken: response.token,
        fileName: response.filename,
        patientColumn: response.columns.includes(config.subjectCodeCsv?.patientColumn ?? '')
          ? config.subjectCodeCsv?.patientColumn ?? ''
          : '',
        subjectCodeColumn: response.columns.includes(config.subjectCodeCsv?.subjectCodeColumn ?? '')
          ? config.subjectCodeCsv?.subjectCodeColumn ?? ''
          : '',
      });
      await queryClient.invalidateQueries({ queryKey: ['csv-columns', response.token] });
    } catch (error) {
      setCsvUploadError((error as Error).message ?? 'Failed to upload CSV');
    } finally {
      setCsvUploading(false);
    }
  };

  useEffect(() => {
    if (!config.subjectCodeCsv || csvColumns.length === 0) {
      return;
    }
    const patch: Partial<NonNullable<ExtractStageConfig['subjectCodeCsv']>> = {};
    if (!config.subjectCodeCsv.patientColumn) {
      patch.patientColumn = csvColumns[0];
    }
    if (!config.subjectCodeCsv.subjectCodeColumn) {
      patch.subjectCodeColumn = csvColumns[Math.min(1, csvColumns.length - 1)];
    }
    if (Object.keys(patch).length > 0) {
      handleSubjectCodeCsvChange(patch);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [csvColumns]);

  const recommendationCard = useMemo(() => {
    if (!recommendation) return null;
    return (
      <Stack
        gap={2}
        p="sm"
        style={{ border: '1px solid var(--mantine-color-default-border)', borderRadius: 'var(--mantine-radius-md)' }}
      >
        <Text size="xs" c="dimmed">
          CPU cores: {recommendation.cpu_count}
        </Text>
        <Text size="xs" c="dimmed">
          RAM available: {formatBytes(recommendation.memory_available)} / total {formatBytes(recommendation.memory_total)}
        </Text>
        <Text size="xs" c="dimmed">
          Disk throughput: {formatBytesPerSecond(recommendation.disk_read_bytes_per_sec)} read ·{' '}
          {formatBytesPerSecond(recommendation.disk_write_bytes_per_sec)} write
        </Text>
        <Text size="xs" c="dimmed">
          Suggested allocation: {recommendation.recommended_workers} workers / {recommendation.recommended_processes} processes
        </Text>
        <Text size="xs" c="dimmed">
          Batch: {recommendation.recommended_batch_size} instances (adaptive {recommendation.recommended_adaptive_min_batch}–
          {recommendation.recommended_adaptive_max_batch})
        </Text>
        <Text size="xs" c="dimmed">
          Queue depth: {recommendation.recommended_queue_depth} · Series workers / subject:{' '}
          {recommendation.recommended_series_workers_per_subject} · DB writers:{' '}
          {recommendation.recommended_db_writer_pool}
        </Text>
        {(recommendation.safe_instance_batch_rows ?? null) != null && (
          <Text size="xs" c="dimmed">
            Parameter-safe batch cap: {recommendation.safe_instance_batch_rows.toLocaleString()} · DB writers ≤{' '}
            {recommendation.max_db_writer_pool_cap}
          </Text>
        )}
        <Text size="xs" c="dimmed">
          Caps → workers ≤ {recommendation.max_workers_cap}, batch ≤ {recommendation.max_batch_cap}, queue ≤{' '}
          {recommendation.max_queue_cap}, adaptive max ≤ {recommendation.max_adaptive_batch_cap}
        </Text>
      </Stack>
    );
  }, [recommendation]);

  const showMappingReminder = config.subjectIdTypeId != null && !config.subjectCodeCsv;

  if (job && runningStatuses.includes(job.status)) {
    return (
      <ExtractProgressCard
        job={job}
        sourcePath={sourcePath}
        onPause={onPauseJob}
        onResume={onResumeJob}
        onCancel={onCancelJob}
        actionPending={jobActionPending}
      />
    );
  }

  if (job?.status === 'paused') {
    return (
      <Stack gap="md">
        <ExtractProgressCard
          job={job}
          sourcePath={sourcePath}
          onPause={onPauseJob}
          onResume={onResumeJob}
          onCancel={onCancelJob}
          actionPending={jobActionPending}
        />
        <PausedPerformancePanel job={job} config={config} onPersist={onChange} />
      </Stack>
    );
  }

  return (
    <Stack gap="lg">
      <Stack gap="xs">
        <Text size="sm" fw={600}>
          Source
        </Text>
        <Text size="xs" c="dimmed">
          Metadata extraction will scan:
        </Text>
        <TextInput value={sourcePath} readOnly disabled />
      </Stack>

      <Stack gap="sm">
        <Text size="sm" fw={600}>
          File detection
        </Text>
        <Text size="xs" c="dimmed">
          Extraction scans <code>derivatives/dcm-raw</code>. Choose how to treat file extensions during discovery.
        </Text>
        <Select
          label="Extension mode"
          data={EXTENSION_OPTIONS}
          value={config.extensionMode}
          onChange={(value) => handleFieldChange('extensionMode', (value ?? 'all') as ExtractStageConfig['extensionMode'])}
        />
      </Stack>

      <Stack gap="sm">
        <Text size="sm" fw={600}>
          Performance
        </Text>
        <Group align="flex-end" gap="md">
          <NumberInput
            label="Worker processes"
            min={1}
            max={workerCap}
            value={config.maxWorkers}
            onChange={(value) => handleFieldChange('maxWorkers', Number(value ?? 1))}
          />
          <NumberInput
            label="Batch size (instances)"
            min={10}
            max={effectiveBatchCap}
            value={config.batchSize}
            onChange={(value) => handleFieldChange('batchSize', Number(value ?? 10))}
          />
          <NumberInput
            label="Queue depth"
            min={1}
            max={queueCap}
            value={config.queueSize}
            onChange={(value) => handleFieldChange('queueSize', Number(value ?? 1))}
          />
          <NumberInput
            label="Series workers / subject"
            min={1}
            max={16}
            value={config.seriesWorkersPerSubject}
            onChange={(value) => handleFieldChange('seriesWorkersPerSubject', Number(value ?? 1))}
          />
          <NumberInput
            label="DB writer pool size"
            description="Concurrent database writers (2-4 recommended for PostgreSQL)"
            min={1}
            max={dbWriterPoolCap}
            value={config.dbWriterPoolSize ?? 3}
            onChange={(value) => handleFieldChange('dbWriterPoolSize', Number(value ?? 3))}
          />
          {onRecommendResources && (
            <Button variant="light" size="sm" onClick={onRecommendResources} loading={recommendLoading}>
              Recommend
            </Button>
          )}
        </Group>
        <Switch
          label="Adaptive batching"
          checked={config.adaptiveBatchingEnabled}
          onChange={(event) => handleFieldChange('adaptiveBatchingEnabled', event.currentTarget.checked)}
        />
        {config.adaptiveBatchingEnabled && (
          <Group align="flex-end" gap="md">
            <NumberInput
              label="Target txn (ms)"
              min={50}
              max={2000}
              value={config.adaptiveTargetTxMs}
              onChange={(value) => handleFieldChange('adaptiveTargetTxMs', Number(value ?? 200))}
            />
            <NumberInput
              label="Min batch"
              min={10}
              max={adaptiveMinFieldCap}
              value={config.adaptiveMinBatchSize}
              onChange={(value) => handleFieldChange('adaptiveMinBatchSize', Number(value ?? 50))}
            />
            <NumberInput
              label="Max batch"
              min={config.adaptiveMinBatchSize}
              max={adaptiveBatchCap}
              value={config.adaptiveMaxBatchSize}
              onChange={(value) => handleFieldChange('adaptiveMaxBatchSize', Number(value ?? 1000))}
            />
          </Group>
        )}
        {safeBatchCap && (
          <Alert color={exceedsSafeBatch ? 'yellow' : 'blue'} variant="light" maw={420}>
            PostgreSQL insert limit caps effective batches at {safeBatchCap.toLocaleString()} instances.{' '}
            {exceedsSafeBatch
              ? 'Larger batches will be automatically chunked before writing.'
              : 'Values at or below this limit maximize throughput without chunking.'}
          </Alert>
        )}
        {recommendationCard}
      </Stack>

      <Stack gap="sm">
        <Text size="sm" fw={600}>
          Duplicate handling
        </Text>
        <Select
          label="Duplicate policy"
          data={DUPLICATE_POLICY}
          value={config.duplicatePolicy}
          onChange={(value) =>
            handleFieldChange('duplicatePolicy', (value ?? 'skip') as ExtractStageConfig['duplicatePolicy'])
          }
        />
        <Text size="xs" c="dimmed">
          • <b>Skip</b>: ignore existing SOP Instance UIDs and log conflicts. <br />• <b>Append series</b>: add only new
          series when studies already exist. <br />• <b>Overwrite</b>: replace stored metadata with values extracted on this
          run.
        </Text>
      </Stack>

      <Stack gap="sm">
        <Text size="sm" fw={600}>
          Resume behavior
        </Text>
        <Switch
          label="Skip already ingested data"
          description="When enabled, existing SOP Instance UIDs in the metadata database are ignored so only new data is written. Disable to overwrite stored metadata for every file (duplicate policy is forced to overwrite)."
          checked={config.resume}
          onChange={(event) => handleFieldChange('resume', event.currentTarget.checked)}
        />
      </Stack>

      <Stack gap="sm">
        <Text size="sm" fw={600}>
          Subject identification
        </Text>
        <Text size="xs" c="dimmed">
          Subject codes are derived from PatientID (falling back to StudyInstanceUID). Provide optional CSV mappings or
          identifier metadata to keep subjects aligned with downstream systems.
        </Text>
        <Group align="flex-end" gap="sm">
          <Select
            label="Subject ID type"
            placeholder="Select identifier type"
            searchable
            data={idTypeOptions}
            value={config.subjectIdTypeId != null ? String(config.subjectIdTypeId) : null}
            clearable
            style={{ flex: 1 }}
            onChange={(value) => handleFieldChange('subjectIdTypeId', value ? Number(value) : null)}
          />
          <Button
            size="xs"
            variant="light"
            onClick={() => {
              resetIdTypeModal();
              setIdTypeModalOpen(true);
            }}
          >
            New type
          </Button>
        </Group>
        <TextInput
          label="Hash seed"
          placeholder="Defaults to cohort name when left blank"
          value={config.subjectCodeSeed ?? ''}
          onChange={(event) => handleFieldChange('subjectCodeSeed', event.currentTarget.value || '')}
        />
        <Text size="xs" c="dimmed">
          When no CSV mapping is supplied, Patient IDs are hashed with the cohort name (BLAKE2b, 8-byte digest) to generate
          stable subject codes.
        </Text>
        {showMappingReminder && (
          <Alert color="yellow" radius="md" title="Hashed subject codes" variant="light">
            A subject ID type will be stored with each hashed subject code. Upload a CSV mapping if you need specific
            subject_code values instead of cohort-salted hashes.
          </Alert>
        )}
        <Stack gap="xs">
          <Group gap="sm">
            <FileButton onChange={handleSubjectCodeCsvUpload} accept=".csv">
              {(props) => (
                <Button {...props} size="sm" variant="light" loading={csvUploading}>
                  Upload subject code CSV
                </Button>
              )}
            </FileButton>
            {config.subjectCodeCsv && (
              <Button
                size="sm"
                variant="subtle"
                color="red"
                onClick={() => handleSubjectCodeCsvChange(null)}
              >
                Clear mapping
              </Button>
            )}
          </Group>
          {csvUploadError && (
            <Text size="xs" c="red">
              {csvUploadError}
            </Text>
          )}
          {config.subjectCodeCsv ? (
            <Stack gap="xs">
              <Text size="xs">
                Mapping file: <strong>{config.subjectCodeCsv.fileName ?? 'Uploaded CSV'}</strong>
              </Text>
              <Group gap="md" align="flex-end">
                <Select
                  label="Patient ID column"
                  data={csvColumns.map((column) => ({ value: column, label: column }))}
                  value={config.subjectCodeCsv.patientColumn ?? ''}
                  placeholder={csvColumns.length ? 'Select column' : 'Upload CSV to choose columns'}
                  onChange={(value) =>
                    handleSubjectCodeCsvChange({
                      patientColumn: value ?? '',
                    })
                  }
                  disabled={!csvColumns.length}
                />
                <Select
                  label="Subject code column"
                  data={csvColumns.map((column) => ({ value: column, label: column }))}
                  value={config.subjectCodeCsv.subjectCodeColumn ?? ''}
                  placeholder={csvColumns.length ? 'Select column' : 'Upload CSV to choose columns'}
                  onChange={(value) =>
                    handleSubjectCodeCsvChange({
                      subjectCodeColumn: value ?? '',
                    })
                  }
                  disabled={!csvColumns.length}
                />
              </Group>
            </Stack>
          ) : (
            <Text size="xs" c="dimmed">
              Upload a CSV containing PatientID to subject_code mappings to override hashed values.
            </Text>
          )}
          <Text size="xs" c="dimmed">
            When no mapping is provided or a PatientID is missing, subject codes are deterministically hashed using the
            seed above.
          </Text>
        </Stack>
      </Stack>

      <Modal
        opened={idTypeModalOpen}
        onClose={() => {
          setIdTypeModalOpen(false);
          resetIdTypeModal();
        }}
        title="Create identifier type"
        centered
      >
        <Stack gap="sm">
          <TextInput
            label="Identifier name"
            value={newIdTypeName}
            onChange={(event) => setNewIdTypeName(event.currentTarget.value)}
            withAsterisk
            data-autofocus
          />
          <TextInput
            label="Description (optional)"
            value={newIdTypeDescription}
            onChange={(event) => setNewIdTypeDescription(event.currentTarget.value)}
          />
          {idTypeError && (
            <Text size="xs" c="red">
              {idTypeError}
            </Text>
          )}
          <Group justify="flex-end" gap="sm">
            <Button
              variant="default"
              onClick={() => {
                setIdTypeModalOpen(false);
                resetIdTypeModal();
              }}
            >
              Cancel
            </Button>
            <Button loading={createIdTypeMutation.isPending} onClick={submitNewIdType}>
              Create
            </Button>
          </Group>
        </Stack>
      </Modal>

      <Divider />
    </Stack>
  );
};

type PerformanceKey =
  | 'maxWorkers'
  | 'batchSize'
  | 'queueSize'
  | 'seriesWorkersPerSubject'
  | 'adaptiveBatchingEnabled'
  | 'adaptiveTargetTxMs'
  | 'adaptiveMinBatchSize'
  | 'adaptiveMaxBatchSize'
  | 'useProcessPool'
  | 'processPoolWorkers'
  | 'dbWriterPoolSize';

type PerformanceNumericKey = Exclude<PerformanceKey, 'adaptiveBatchingEnabled' | 'useProcessPool'>;

type PerformanceState = Pick<ExtractStageConfig, PerformanceKey>;
type PerformancePatch = Partial<Record<PerformanceKey, ExtractStageConfig[PerformanceKey]>>;

const PERFORMANCE_BASE_FIELDS: Array<{
  key: PerformanceNumericKey;
  label: string;
  min: number;
  max: number;
  step?: number;
}> = [
  { key: 'maxWorkers', label: 'Worker processes', min: 1, max: 128, step: 1 },
  { key: 'batchSize', label: 'Batch size (instances)', min: 10, max: 5000, step: 10 },
  { key: 'queueSize', label: 'Queue depth', min: 1, max: 500, step: 1 },
  { key: 'seriesWorkersPerSubject', label: 'Series workers / subject', min: 1, max: 16, step: 1 },
  { key: 'dbWriterPoolSize', label: 'DB writer pool', min: 1, max: 16, step: 1 },
];

const PERFORMANCE_ADAPTIVE_FIELDS: Array<{
  key: PerformanceNumericKey;
  label: string;
  min: number;
  max: number;
  step?: number;
}> = [
  { key: 'adaptiveTargetTxMs', label: 'Target txn (ms)', min: 50, max: 2000, step: 10 },
  { key: 'adaptiveMinBatchSize', label: 'Min batch', min: 10, max: 10000, step: 10 },
  { key: 'adaptiveMaxBatchSize', label: 'Max batch', min: 50, max: 20000, step: 10 },
];

interface PausedPerformancePanelProps {
  job: JobSummary;
  config: ExtractStageConfig;
  onPersist: (next: ExtractStageConfig) => void;
}

const PausedPerformancePanel = ({ job, config, onPersist }: PausedPerformancePanelProps) => {
  const updateJobConfig = useUpdateJobConfig();
  const safeBatchRows = job.metrics?.safe_batch_rows ?? null;
  const dbWriterPoolCap = 16;
  const performanceDefaults = useMemo<PerformanceState>(
    () => ({
      maxWorkers: config.maxWorkers,
      batchSize: config.batchSize,
      queueSize: config.queueSize,
      seriesWorkersPerSubject: config.seriesWorkersPerSubject,
      adaptiveBatchingEnabled: config.adaptiveBatchingEnabled,
      adaptiveTargetTxMs: config.adaptiveTargetTxMs,
      adaptiveMinBatchSize: config.adaptiveMinBatchSize,
      adaptiveMaxBatchSize: config.adaptiveMaxBatchSize,
      useProcessPool: config.useProcessPool ?? true,
      processPoolWorkers: config.processPoolWorkers ?? null,
      dbWriterPoolSize: config.dbWriterPoolSize ?? 3,
    }),
    [
      config.maxWorkers,
      config.batchSize,
      config.queueSize,
      config.seriesWorkersPerSubject,
      config.adaptiveBatchingEnabled,
      config.adaptiveTargetTxMs,
      config.adaptiveMinBatchSize,
      config.adaptiveMaxBatchSize,
      config.useProcessPool,
      config.processPoolWorkers,
      config.dbWriterPoolSize,
    ],
  );

  const [draft, setDraft] = useState<PerformanceState>(performanceDefaults);

  useEffect(() => {
    setDraft(performanceDefaults);
  }, [performanceDefaults, job.id]);

  const performanceKeys: Array<keyof ExtractPerformanceConfigPatch> = [
    'maxWorkers',
    'batchSize',
    'queueSize',
    'seriesWorkersPerSubject',
    'adaptiveBatchingEnabled',
    'adaptiveTargetTxMs',
    'adaptiveMinBatchSize',
    'adaptiveMaxBatchSize',
    'useProcessPool',
    'processPoolWorkers',
    'dbWriterPoolSize',
  ];

  const dirtyFields = performanceKeys.filter((key) => draft[key] !== performanceDefaults[key]);
  const isDirty = dirtyFields.length > 0;

  const handleNumericChange = (key: PerformanceNumericKey, value: string | number | null) => {
    const fallback = performanceDefaults[key];
    if (value === '' || value === null) {
      setDraft((prev) => ({ ...prev, [key]: fallback }));
      return;
    }
    const numericValue = typeof value === 'number' ? value : Number(value);
    if (!Number.isFinite(numericValue)) {
      return;
    }
    setDraft((prev) => {
      const next = { ...prev, [key]: numericValue };
      if (key === 'adaptiveMinBatchSize' && numericValue > prev.adaptiveMaxBatchSize) {
        next.adaptiveMaxBatchSize = numericValue;
      }
      if (key === 'adaptiveMaxBatchSize' && numericValue < prev.adaptiveMinBatchSize) {
        next.adaptiveMinBatchSize = numericValue;
      }
      return next;
    });
  };

  const handleSave = () => {
    if (!isDirty) {
      return;
    }
    const payload = dirtyFields.reduce<PerformancePatch>((acc, key) => {
      acc[key] = draft[key];
      return acc;
    }, {});

    updateJobConfig.mutate(
      { jobId: job.id, payload: payload as ExtractPerformanceConfigPatch },
      {
        onSuccess: () => {
          notifications.show({ color: 'teal', message: 'Performance settings updated.' });
          onPersist({ ...config, ...draft });
        },
        onError: (error) => {
          notifications.show({ color: 'red', message: error instanceof Error ? error.message : 'Failed to update job.' });
        },
      },
    );
  };

  const mutationPending = updateJobConfig.isPending;
  const disableSave = mutationPending || !isDirty;

  return (
    <Card withBorder radius="md" padding="md">
      <Stack gap="sm">
        <Text fw={600}>Tune performance before resuming</Text>
        <Text size="sm" c="dimmed">
          Extraction checkpoints persist progress, so you can adjust concurrency for job #{job.id} while it is paused.
          Changes apply as soon as you resume.
        </Text>
        {safeBatchRows && (
          <Alert color="blue" variant="light" maw={420}>
            PostgreSQL insert limit supports {safeBatchRows.toLocaleString()} instances per write. Higher request sizes
            will be chunked automatically.
          </Alert>
        )}
        <SimpleGrid cols={{ base: 1, sm: 2 }} spacing="md">
          {PERFORMANCE_BASE_FIELDS.map((field) => {
            let dynamicMax = field.max;
            if (field.key === 'batchSize' && safeBatchRows)
              dynamicMax = Math.min(dynamicMax, safeBatchRows);
            if (field.key === 'dbWriterPoolSize')
              dynamicMax = Math.min(dynamicMax, dbWriterPoolCap);
            return (
              <NumberInput
                key={field.key}
                label={field.label}
                min={field.min}
                max={dynamicMax}
                step={field.step}
                value={draft[field.key] as number}
                onChange={(value) => handleNumericChange(field.key, value)}
              />
            );
          })}
        </SimpleGrid>
        <Switch
          label="Enable adaptive batching"
          checked={draft.adaptiveBatchingEnabled}
          onChange={(event) =>
            setDraft((prev) => ({
              ...prev,
              adaptiveBatchingEnabled: event.currentTarget.checked,
            }))
          }
        />
        {draft.adaptiveBatchingEnabled && (
          <SimpleGrid cols={{ base: 1, sm: 3 }} spacing="md">
            {PERFORMANCE_ADAPTIVE_FIELDS.map((field) => {
              let dynamicMax = field.max;
              if (
                safeBatchRows &&
                (field.key === 'adaptiveMinBatchSize' || field.key === 'adaptiveMaxBatchSize')
              ) {
                dynamicMax = Math.min(dynamicMax, safeBatchRows);
              }
              const minValue =
                field.key === 'adaptiveMaxBatchSize' ? draft.adaptiveMinBatchSize : field.min;
              return (
                <NumberInput
                  key={field.key}
                  label={field.label}
                  min={minValue}
                  max={dynamicMax}
                  step={field.step}
                  value={draft[field.key] as number}
                  onChange={(value) => handleNumericChange(field.key, value)}
                />
              );
            })}
          </SimpleGrid>
        )}
        <Group justify="flex-end" gap="sm">
          <Button variant="default" onClick={() => setDraft(performanceDefaults)} disabled={!isDirty || mutationPending}>
            Reset
          </Button>
          <Button onClick={handleSave} loading={mutationPending} disabled={disableSave}>
            Save performance changes
          </Button>
        </Group>
      </Stack>
    </Card>
  );
};
