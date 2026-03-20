import { useState } from 'react';
import {
  ActionIcon,
  Badge,
  Box,
  Collapse,
  Group,
  Loader,
  Progress,
  Stack,
  Text,
  Tooltip,
} from '@mantine/core';
import {
  IconChevronDown,
  IconChevronUp,
  IconFileExport,
  IconRefresh,
  IconTrash,
} from '@tabler/icons-react';
import { useNavigate } from 'react-router-dom';
import type { Job, JobStatus } from '../../../types';
import { JOB_STATUS_CONFIG } from '../../../constants/status';
import { useDeleteJob } from '../api';

const formatRelativeTime = (dateString: string): string => {
  const date = new Date(dateString);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMs / 3600000);
  const diffDays = Math.floor(diffMs / 86400000);

  if (diffMins < 1) return 'just now';
  if (diffMins < 60) return `${diffMins}m ago`;
  if (diffHours < 24) return `${diffHours}h ago`;
  if (diffDays < 7) return `${diffDays}d ago`;
  return date.toLocaleDateString();
};

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

const truncatePath = (path: string, maxLength = 50): string => {
  if (path.length <= maxLength) return path;
  const parts = path.split('/');
  if (parts.length <= 3) return path;
  return `.../${parts.slice(-3).join('/')}`;
};

interface ExportJobCardProps {
  jobs: Job[];
  defaultExpanded?: boolean;
}

export const ExportJobCard = ({ jobs, defaultExpanded = false }: ExportJobCardProps) => {
  const [expanded, setExpanded] = useState(defaultExpanded);
  const deleteJob = useDeleteJob();
  const navigate = useNavigate();

  const hasActiveJob = jobs.some((j) => ['running', 'queued', 'paused'].includes(j.status));
  const hasFailedJob = jobs.some((j) => j.status === 'failed');
  const runningJob = jobs.find((j) => j.status === 'running');

  const getBorderColor = () => {
    if (hasActiveJob) return 'var(--nils-accent-primary)';
    if (hasFailedJob) return 'var(--nils-error)';
    return 'var(--nils-border-subtle)';
  };

  const canDelete = (status: JobStatus) =>
    ['completed', 'completed_with_warnings', 'failed', 'canceled'].includes(status);

  const sortedJobs = [...jobs].sort((a, b) => (a.submittedAt > b.submittedAt ? -1 : 1));

  return (
    <Box
      style={{
        backgroundColor: 'var(--nils-bg-secondary)',
        borderRadius: 'var(--nils-radius-lg)',
        border: `1px solid ${getBorderColor()}`,
        overflow: 'hidden',
        transition: 'border-color 0.2s ease',
      }}
    >
      {/* Header */}
      <Box
        p="md"
        style={{ cursor: 'pointer', userSelect: 'none' }}
        onClick={() => setExpanded((prev) => !prev)}
      >
        <Group justify="space-between" wrap="nowrap">
          <Stack gap={4} style={{ minWidth: 0, flex: 1 }}>
            <Group gap="xs" wrap="nowrap">
              <IconFileExport size={20} color="var(--nils-text-secondary)" style={{ flexShrink: 0 }} />
              <Text fw={600} size="md" c="var(--nils-text-primary)">
                Subset Exports
              </Text>
              {hasActiveJob && (
                <Badge color="blue" size="xs" variant="light">Active</Badge>
              )}
              {hasFailedJob && !hasActiveJob && (
                <Badge color="red" size="xs" variant="light">Failed</Badge>
              )}
              <Badge color="gray" size="xs" variant="outline">
                {jobs.length} job{jobs.length !== 1 ? 's' : ''}
              </Badge>
            </Group>
            <Text size="xs" c="var(--nils-text-tertiary)">
              Manifest-based data exports
            </Text>
          </Stack>

          <Group gap="sm" wrap="nowrap">
            {runningJob && (
              <Box
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 6,
                  padding: '4px 10px',
                  backgroundColor: 'rgba(88, 166, 255, 0.15)',
                  borderRadius: 'var(--nils-radius-sm)',
                }}
              >
                <Box
                  style={{
                    width: 8,
                    height: 8,
                    borderRadius: '50%',
                    backgroundColor: 'var(--nils-stage-running)',
                    animation: 'pulse 2s infinite',
                  }}
                />
                <Text size="xs" fw={500} c="var(--nils-stage-running)">
                  Exporting {runningJob.progress}%
                </Text>
              </Box>
            )}
            <ActionIcon variant="subtle" size="sm">
              {expanded ? <IconChevronUp size={18} /> : <IconChevronDown size={18} />}
            </ActionIcon>
          </Group>
        </Group>
      </Box>

      {/* Collapsible Job List */}
      <Collapse in={expanded}>
        <Box p="md" pt={0} style={{ borderTop: '1px solid var(--nils-border-subtle)' }}>
          <Stack gap="xs">
            {sortedJobs.map((job) => {
              const status = JOB_STATUS_CONFIG[job.status];
              const cfg = job.config as Record<string, unknown>;
              const outputPath = (cfg?.output_path as string) ?? '';
              const stackCount = (cfg?.stack_count as number) ?? 0;
              const sourceCohort = (cfg?.source_cohort_name as string) ?? '';
              const totalSubjects = (cfg?.total_subjects as number) ?? 0;
              const totalSessions = (cfg?.total_sessions as number) ?? 0;
              const hasManifest = Boolean(cfg?.manifest_text);
              const isRunning = job.status === 'running';
              const metrics = job.metrics as unknown as Record<string, unknown> | undefined;

              return (
                <Box
                  key={job.id}
                  p="sm"
                  style={{
                    backgroundColor: isRunning ? 'rgba(88, 166, 255, 0.05)' : 'var(--nils-bg-tertiary)',
                    borderRadius: 'var(--nils-radius-md)',
                    border: isRunning
                      ? '1px solid var(--nils-accent-primary)'
                      : '1px solid var(--nils-border-subtle)',
                  }}
                >
                  <Stack gap="xs">
                    <Group justify="space-between" wrap="nowrap" align="flex-start">
                      <Group gap="sm" wrap="nowrap" style={{ flex: 1, minWidth: 0 }}>
                        {/* Icon */}
                        <Box
                          style={{
                            width: 32,
                            height: 32,
                            borderRadius: 'var(--nils-radius-sm)',
                            backgroundColor: isRunning
                              ? 'rgba(88, 166, 255, 0.15)'
                              : 'var(--nils-bg-elevated)',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            flexShrink: 0,
                          }}
                        >
                          {isRunning ? (
                            <Loader size={14} color="var(--nils-accent-primary)" />
                          ) : (
                            <IconFileExport size={16} color={status?.color} />
                          )}
                        </Box>

                        {/* Info */}
                        <Stack gap={2} style={{ flex: 1, minWidth: 0 }}>
                          <Group gap="xs" wrap="nowrap">
                            <Text size="sm" fw={500} c="var(--nils-text-primary)">
                              {`Export - ${stackCount} stacks`}
                            </Text>
                            {sourceCohort && (
                              <Badge size="xs" variant="light" color="blue">
                                {sourceCohort}
                              </Badge>
                            )}
                            <Box
                              style={{
                                display: 'inline-flex',
                                alignItems: 'center',
                                gap: 4,
                                padding: '2px 6px',
                                borderRadius: 'var(--nils-radius-xs)',
                                backgroundColor: status?.bgColor,
                              }}
                            >
                              <Box
                                style={{
                                  width: 5,
                                  height: 5,
                                  borderRadius: '50%',
                                  backgroundColor: status?.color,
                                  animation: isRunning ? 'pulse 2s infinite' : 'none',
                                }}
                              />
                              <Text size="xs" fw={500} c={status?.color}>
                                {status?.label}
                              </Text>
                            </Box>
                          </Group>

                          {outputPath && (
                            <Text size="xs" c="var(--nils-text-tertiary)" truncate>
                              {truncatePath(outputPath)}
                            </Text>
                          )}

                          <Group gap="sm">
                            <Text size="xs" c="var(--nils-text-tertiary)">
                              {stackCount} stacks
                              {totalSubjects > 0 && ` / ${totalSubjects} subjects`}
                              {totalSessions > 0 && ` / ${totalSessions} sessions`}
                            </Text>
                            <Text size="xs" c="var(--nils-text-tertiary)">
                              {formatRelativeTime(job.submittedAt)}
                            </Text>
                            {isRunning && job.startedAt && (
                              <>
                                <Text size="xs" c="var(--nils-text-tertiary)">·</Text>
                                <Text size="xs" c="var(--nils-accent-primary)" fw={500}>
                                  Running for {formatDuration(job.startedAt)}
                                </Text>
                              </>
                            )}
                            {(job.status === 'completed' || job.status === 'completed_with_warnings') &&
                              job.startedAt &&
                              job.finishedAt && (
                                <>
                                  <Text size="xs" c="var(--nils-text-tertiary)">·</Text>
                                  <Text size="xs" c="var(--nils-text-tertiary)">
                                    Took {formatDuration(job.startedAt, job.finishedAt)}
                                  </Text>
                                </>
                              )}
                          </Group>

                          {/* Metrics summary for completed jobs */}
                          {metrics && !isRunning && (
                            <Group gap="sm">
                              {metrics.exported_stacks != null && (
                                <Text size="xs" c="var(--nils-text-tertiary)">
                                  Exported: {String(metrics.exported_stacks)} stacks
                                </Text>
                              )}
                              {metrics.copied_files != null && Number(metrics.copied_files) > 0 && (
                                <Text size="xs" c="var(--nils-text-tertiary)">
                                  Copied: {String(metrics.copied_files)} files
                                </Text>
                              )}
                            </Group>
                          )}

                          {job.errorMessage && (
                            <Text size="xs" c="var(--nils-error)" truncate style={{ maxWidth: 400 }}>
                              {job.errorMessage}
                            </Text>
                          )}
                        </Stack>
                      </Group>

                      {/* Actions */}
                      <Group gap={4} wrap="nowrap">
                        {hasManifest && canDelete(job.status) && (
                          <Tooltip label="Re-export with same manifest">
                            <ActionIcon
                              size="sm"
                              variant="subtle"
                              color="blue"
                              onClick={(e) => {
                                e.stopPropagation();
                                navigate('/export', {
                                  state: { manifestText: cfg.manifest_text, config: cfg },
                                });
                              }}
                            >
                              <IconRefresh size={14} />
                            </ActionIcon>
                          </Tooltip>
                        )}
                        {canDelete(job.status) && (
                          <Tooltip label="Delete job record">
                            <ActionIcon
                              size="sm"
                              variant="subtle"
                              color="red"
                              loading={deleteJob.isPending}
                              onClick={(e) => {
                                e.stopPropagation();
                                deleteJob.mutate(job.id);
                              }}
                            >
                              <IconTrash size={14} />
                            </ActionIcon>
                          </Tooltip>
                        )}
                      </Group>
                    </Group>

                    {/* Progress bar for running jobs */}
                    {isRunning && (
                      <Progress
                        value={job.progress ?? 0}
                        size="sm"
                        radius="md"
                        transitionDuration={200}
                      />
                    )}
                  </Stack>
                </Box>
              );
            })}
          </Stack>
        </Box>
      </Collapse>
    </Box>
  );
};
