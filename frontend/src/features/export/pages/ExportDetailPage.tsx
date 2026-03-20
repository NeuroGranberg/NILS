import {
  Anchor,
  Badge,
  Box,
  Button,
  Divider,
  Group,
  Loader,
  Stack,
  Text,
  Title,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { IconArrowLeft, IconFileExport } from '@tabler/icons-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { useExportJobQuery, useResolveManifest, useRunExport } from '../api';
import { ManifestPreview } from '../components/ManifestPreview';
import { ExportConfigForm } from '../components/ExportConfigForm';
import { ExportProgress } from '../components/ExportProgress';
import type { ExportConfig, ResolveResponse } from '../types';
import { DEFAULT_EXPORT_CONFIG } from '../types';
import { JOB_STATUS_CONFIG } from '../../../constants/status';
import { formatDateTime } from '../../../utils/formatters';

const formatDuration = (startTime: string, endTime?: string | null): string => {
  const start = new Date(startTime);
  const end = endTime ? new Date(endTime) : new Date();
  const diffMs = end.getTime() - start.getTime();
  const diffSecs = Math.floor(diffMs / 1000);
  const diffMins = Math.floor(diffSecs / 60);
  if (diffSecs < 60) return `${diffSecs}s`;
  if (diffMins < 60) return `${diffMins}m ${diffSecs % 60}s`;
  return `${Math.floor(diffMins / 60)}h ${diffMins % 60}m`;
};

const isTerminal = (status: string) =>
  ['completed', 'completed_with_warnings', 'failed', 'canceled'].includes(status);

const buildConfigFromJob = (cfg: Record<string, unknown>): ExportConfig => ({
  ...DEFAULT_EXPORT_CONFIG,
  layout: (cfg.layout as 'bids' | 'flat') || 'bids',
  outputModes: (cfg.output_modes as string[]) || ['dcm'],
  overwriteMode: (cfg.overwrite_mode as 'skip' | 'clean' | 'overwrite') || 'skip',
  bidsDcmRootName: (cfg.bids_dcm_root_name as string) || 'bids-dcm',
  bidsNiftiRootName: (cfg.bids_nifti_root_name as string) || 'bids-nifti',
  flatDcmRootName: (cfg.flat_dcm_root_name as string) || 'flat-dcm',
  flatNiftiRootName: (cfg.flat_nifti_root_name as string) || 'flat-nifti',
  copyWorkers: (cfg.copy_workers as number) || 8,
  convertWorkers: (cfg.convert_workers as number) || 8,
  subjectIdentifierSource: (cfg.subject_identifier_source as string | number) || 'subject_code',
});

export const ExportDetailPage = () => {
  const { jobId } = useParams<{ jobId: string }>();
  const navigate = useNavigate();
  const numericJobId = jobId ? parseInt(jobId, 10) : null;
  const { data: job, isLoading, isError } = useExportJobQuery(numericJobId);

  const cfg = useMemo(
    () => (job?.config as Record<string, unknown>) ?? {},
    [job?.config],
  );
  const exportName = (cfg.export_name as string) || 'Unnamed Export';
  const exportDescription = (cfg.export_description as string) || '';
  const manifestText = (cfg.manifest_text as string) || '';
  const sourceCohort = (cfg.source_cohort_name as string) || '';
  const stackCount = (cfg.stack_count as number) || 0;
  const totalSubjects = (cfg.total_subjects as number) || 0;
  const totalSessions = (cfg.total_sessions as number) || 0;
  const detectedCohorts = useMemo(
    () => (cfg.detected_cohorts as string[]) || [],
    [cfg.detected_cohorts],
  );

  // Resolve result from manifest (for preview table)
  const [resolveResult, setResolveResult] = useState<ResolveResponse | null>(null);
  const resolveMutation = useResolveManifest();

  // Config form state — initialized from job config once loaded
  const [config, setConfig] = useState<ExportConfig>({ ...DEFAULT_EXPORT_CONFIG });
  const [outputPath, setOutputPath] = useState('');

  // New job tracking (when re-running from this page)
  const [newJobId, setNewJobId] = useState<number | null>(null);
  const { data: newJob } = useExportJobQuery(newJobId);
  const runExportMutation = useRunExport();

  // Initialize config + resolve manifest once job is loaded
  const initJobIdRef = useRef<number | null>(null);
  useEffect(() => {
    if (!job || initJobIdRef.current === job.id) return;
    initJobIdRef.current = job.id;

    const jobCfg = (job.config as Record<string, unknown>) ?? {};
    setConfig(buildConfigFromJob(jobCfg));
    setOutputPath((jobCfg.output_path as string) || '');

    const manifest = (jobCfg.manifest_text as string) || '';
    if (manifest) {
      resolveMutation.mutate(manifest, {
        onSuccess: (result) => setResolveResult(result),
      });
    }
  }, [job]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleConfigChange = useCallback(
    <K extends keyof ExportConfig>(key: K, value: ExportConfig[K]) => {
      setConfig((prev) => ({ ...prev, [key]: value }));
    },
    [],
  );

  const handleRunExport = useCallback(() => {
    if (!resolveResult || !outputPath.trim()) return;

    runExportMutation.mutate(
      {
        stack_ids: resolveResult.resolved_stack_ids,
        detected_cohorts: detectedCohorts,
        output_path: outputPath,
        export_name: exportName,
        export_description: exportDescription,
        manifest_text: manifestText,
        resolve_summary: {
          total_subjects: resolveResult.total_subjects,
          total_sessions: resolveResult.total_sessions,
        },
        config,
      },
      {
        onSuccess: (result) => {
          const newId = result.job.id as number;
          setNewJobId(newId);
          notifications.show({
            title: 'Export started',
            message: 'New export job is running in the background.',
            color: 'green',
          });
        },
        onError: (error) => {
          notifications.show({
            title: 'Export failed',
            message: error.message,
            color: 'red',
          });
        },
      },
    );
  }, [resolveResult, outputPath, detectedCohorts, exportName, exportDescription, manifestText, config, runExportMutation]);

  if (isLoading) {
    return (
      <Box py="xl" style={{ display: 'flex', justifyContent: 'center', minHeight: 300 }}>
        <Loader size="md" color="var(--nils-accent-primary)" />
      </Box>
    );
  }

  if (isError || !job) {
    return (
      <Stack gap="md" p="md">
        <Anchor component={Link} to="/export" size="sm" c="var(--nils-accent-primary)">
          <Group gap={4}><IconArrowLeft size={14} /> Back to Exports</Group>
        </Anchor>
        <Text c="var(--nils-error)">Export job not found.</Text>
      </Stack>
    );
  }

  const status = JOB_STATUS_CONFIG[job.status];
  const jobIsRunning = job.status === 'running';
  const jobTerminal = isTerminal(job.status);
  const activeNewJob = newJob && !isTerminal(newJob.status);

  return (
    <Stack gap="lg" p="md">
      {/* Back link */}
      <Anchor component={Link} to="/export" size="sm" c="var(--nils-accent-primary)">
        <Group gap={4}><IconArrowLeft size={14} /> Back to Exports</Group>
      </Anchor>

      {/* Header */}
      <Group justify="space-between" align="flex-start">
        <Stack gap={4}>
          <Group gap="sm">
            <Title order={2}>{exportName}</Title>
            {sourceCohort && <Badge variant="light" color="blue">{sourceCohort}</Badge>}
            <Box
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: 4,
                padding: '2px 8px',
                borderRadius: 'var(--nils-radius-xs)',
                backgroundColor: status?.bgColor,
              }}
            >
              <Box
                style={{
                  width: 6,
                  height: 6,
                  borderRadius: '50%',
                  backgroundColor: status?.color,
                  animation: jobIsRunning ? 'pulse 2s infinite' : 'none',
                }}
              />
              <Text size="xs" fw={500} c={status?.color}>{status?.label}</Text>
            </Box>
          </Group>
          {exportDescription && (
            <Text size="sm" c="var(--nils-text-tertiary)">{exportDescription}</Text>
          )}
          <Group gap="lg">
            <Text size="xs" c="var(--nils-text-tertiary)">
              {formatDateTime(job.submittedAt)}
            </Text>
            {job.startedAt && job.finishedAt && (
              <Text size="xs" c="var(--nils-text-tertiary)">
                Took {formatDuration(job.startedAt, job.finishedAt)}
              </Text>
            )}
          </Group>
        </Stack>
        <Group gap="lg">
          <Stack gap={0} align="center">
            <Text size="xs" c="var(--nils-text-tertiary)">Stacks</Text>
            <Text size="lg" fw={600} c="var(--nils-text-primary)">{stackCount}</Text>
          </Stack>
          {totalSubjects > 0 && (
            <Stack gap={0} align="center">
              <Text size="xs" c="var(--nils-text-tertiary)">Subjects</Text>
              <Text size="lg" fw={600} c="var(--nils-text-primary)">{totalSubjects}</Text>
            </Stack>
          )}
          {totalSessions > 0 && (
            <Stack gap={0} align="center">
              <Text size="xs" c="var(--nils-text-tertiary)">Sessions</Text>
              <Text size="lg" fw={600} c="var(--nils-text-primary)">{totalSessions}</Text>
            </Stack>
          )}
        </Group>
      </Group>

      {/* Progress for original job if still running */}
      {jobIsRunning && <ExportProgress job={job} />}

      {/* Manifest Preview */}
      {resolveMutation.isPending && (
        <Group gap="xs">
          <Loader size="xs" />
          <Text size="sm" c="var(--nils-text-tertiary)">Loading manifest preview...</Text>
        </Group>
      )}
      {resolveResult && (
        <ManifestPreview
          result={resolveResult}
          subjectIdentifierSource={config.subjectIdentifierSource}
          onSubjectIdentifierChange={(value) => handleConfigChange('subjectIdentifierSource', value)}
        />
      )}

      {/* Config + Re-run (only show when original job is terminal) */}
      {jobTerminal && (
        <>
          <Divider label="Export Configuration" labelPosition="center" />

          <ExportConfigForm
            config={config}
            onChange={handleConfigChange}
            outputPath={outputPath}
            onOutputPathChange={setOutputPath}
          />

          <Group justify="flex-end" mt="md">
            <Button
              onClick={handleRunExport}
              loading={runExportMutation.isPending}
              disabled={
                !resolveResult ||
                resolveResult.total_stacks === 0 ||
                !outputPath.trim() ||
                Boolean(activeNewJob)
              }
              leftSection={<IconFileExport size={16} />}
            >
              Run Export ({resolveResult?.total_stacks ?? stackCount} stacks)
            </Button>
          </Group>
        </>
      )}

      {/* Progress for new re-run job */}
      {newJob && (
        <Stack gap="md">
          <Divider label="New Export Run" labelPosition="center" />
          <ExportProgress job={newJob} />
          {isTerminal(newJob.status) && (
            <Group justify="center">
              <Button
                variant="light"
                onClick={() => navigate(`/export/${newJob.id}`)}
              >
                Go to new export
              </Button>
            </Group>
          )}
        </Stack>
      )}
    </Stack>
  );
};
