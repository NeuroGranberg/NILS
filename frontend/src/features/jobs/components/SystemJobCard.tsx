/**
 * System job card component.
 * Displays backup/restore jobs in a compact card format.
 */

import { useState } from 'react';
import {
  ActionIcon,
  Badge,
  Box,
  Collapse,
  Group,
  Loader,
  Stack,
  Text,
  Tooltip,
} from '@mantine/core';
import { IconChevronDown, IconChevronUp, IconDatabase, IconTrash, IconDownload, IconUpload } from '@tabler/icons-react';
import type { Job, JobStatus } from '../../../types';
import { useDeleteJob } from '../api';

const statusConfig: Record<JobStatus, { color: string; bgColor: string; label: string }> = {
  queued: { color: 'var(--nils-stage-pending)', bgColor: 'rgba(163, 113, 247, 0.15)', label: 'Queued' },
  running: { color: 'var(--nils-stage-running)', bgColor: 'rgba(88, 166, 255, 0.15)', label: 'Running' },
  paused: { color: 'var(--nils-stage-paused)', bgColor: 'rgba(210, 153, 34, 0.15)', label: 'Paused' },
  completed: { color: 'var(--nils-stage-completed)', bgColor: 'rgba(63, 185, 80, 0.15)', label: 'Completed' },
  failed: { color: 'var(--nils-stage-failed)', bgColor: 'rgba(248, 81, 73, 0.15)', label: 'Failed' },
  canceled: { color: 'var(--nils-stage-idle)', bgColor: 'rgba(110, 118, 129, 0.15)', label: 'Canceled' },
};

// Format relative time
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

// Format duration
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

interface SystemJobCardProps {
  jobs: Job[];
  defaultExpanded?: boolean;
}

export const SystemJobCard = ({ jobs, defaultExpanded = false }: SystemJobCardProps) => {
  const [expanded, setExpanded] = useState(defaultExpanded);
  const deleteJob = useDeleteJob();

  // Check for active jobs
  const hasActiveJob = jobs.some((j) => ['running', 'queued', 'paused'].includes(j.status));
  const hasFailedJob = jobs.some((j) => j.status === 'failed');
  const runningJob = jobs.find((j) => j.status === 'running');

  // Determine card border color based on status
  const getBorderColor = () => {
    if (hasActiveJob) return 'var(--nils-accent-primary)';
    if (hasFailedJob) return 'var(--nils-error)';
    return 'var(--nils-border-subtle)';
  };

  const canDelete = (status: JobStatus) => ['completed', 'failed', 'canceled'].includes(status);

  // Sort jobs by submittedAt descending
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
        style={{
          cursor: 'pointer',
          userSelect: 'none',
        }}
        onClick={() => setExpanded((prev) => !prev)}
      >
        <Group justify="space-between" wrap="nowrap">
          <Stack gap={4} style={{ minWidth: 0, flex: 1 }}>
            <Group gap="xs" wrap="nowrap">
              <IconDatabase size={20} color="var(--nils-text-secondary)" style={{ flexShrink: 0 }} />
              <Text fw={600} size="md" c="var(--nils-text-primary)">
                System Operations
              </Text>
              {hasActiveJob && (
                <Badge color="blue" size="xs" variant="light">
                  Active
                </Badge>
              )}
              {hasFailedJob && !hasActiveJob && (
                <Badge color="red" size="xs" variant="light">
                  Failed
                </Badge>
              )}
              <Badge color="gray" size="xs" variant="outline">
                {jobs.length} job{jobs.length !== 1 ? 's' : ''}
              </Badge>
            </Group>
            <Text size="xs" c="var(--nils-text-tertiary)">
              Database backup and restore operations
            </Text>
          </Stack>

          <Group gap="sm" wrap="nowrap">
            {/* Running job indicator */}
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
                <Text size="xs" fw={500} c="var(--nils-stage-running)" tt="capitalize">
                  {runningJob.stageId} {runningJob.progress}%
                </Text>
              </Box>
            )}

            {/* Expand/collapse toggle */}
            <ActionIcon variant="subtle" size="sm">
              {expanded ? <IconChevronUp size={18} /> : <IconChevronDown size={18} />}
            </ActionIcon>
          </Group>
        </Group>
      </Box>

      {/* Collapsible Job List */}
      <Collapse in={expanded}>
        <Box
          p="md"
          pt={0}
          style={{ borderTop: '1px solid var(--nils-border-subtle)' }}
        >
          <Stack gap="xs">
            {sortedJobs.map((job) => {
              const status = statusConfig[job.status];
              const config = job.config as Record<string, unknown>;
              const database = config?.database as string | undefined;
              const path = config?.path as string | undefined;
              const filename = path ? path.split('/').pop() : undefined;
              const isRestore = job.stageId === 'restore';
              const isRunning = job.status === 'running';

              return (
                <Box
                  key={job.id}
                  p="sm"
                  style={{
                    backgroundColor: isRunning ? 'rgba(88, 166, 255, 0.05)' : 'var(--nils-bg-tertiary)',
                    borderRadius: 'var(--nils-radius-md)',
                    border: isRunning ? '1px solid var(--nils-accent-primary)' : '1px solid var(--nils-border-subtle)',
                  }}
                >
                  <Group justify="space-between" wrap="nowrap" align="flex-start">
                    <Group gap="sm" wrap="nowrap" style={{ flex: 1, minWidth: 0 }}>
                      {/* Icon */}
                      <Box
                        style={{
                          width: 32,
                          height: 32,
                          borderRadius: 'var(--nils-radius-sm)',
                          backgroundColor: isRunning ? 'rgba(88, 166, 255, 0.15)' : 'var(--nils-bg-elevated)',
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          flexShrink: 0,
                        }}
                      >
                        {isRunning ? (
                          <Loader size={14} color="var(--nils-accent-primary)" />
                        ) : isRestore ? (
                          <IconUpload size={16} color={status.color} />
                        ) : (
                          <IconDownload size={16} color={status.color} />
                        )}
                      </Box>

                      {/* Info */}
                      <Stack gap={2} style={{ flex: 1, minWidth: 0 }}>
                        <Group gap="xs" wrap="nowrap">
                          <Text size="sm" fw={500} c="var(--nils-text-primary)" tt="capitalize">
                            {job.stageId}
                          </Text>
                          {database && (
                            <Badge size="xs" variant="light" color={database === 'metadata' ? 'blue' : 'grape'}>
                              {database}
                            </Badge>
                          )}
                          <Box
                            style={{
                              display: 'inline-flex',
                              alignItems: 'center',
                              gap: 4,
                              padding: '2px 6px',
                              borderRadius: 'var(--nils-radius-xs)',
                              backgroundColor: status.bgColor,
                            }}
                          >
                            <Box
                              style={{
                                width: 5,
                                height: 5,
                                borderRadius: '50%',
                                backgroundColor: status.color,
                                animation: isRunning ? 'pulse 2s infinite' : 'none',
                              }}
                            />
                            <Text size="xs" fw={500} c={status.color}>
                              {status.label}
                            </Text>
                          </Box>
                        </Group>

                        {filename && (
                          <Text size="xs" c="var(--nils-text-tertiary)" truncate>
                            {filename}
                          </Text>
                        )}

                        <Group gap="sm">
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
                          {job.status === 'completed' && job.startedAt && job.finishedAt && (
                            <>
                              <Text size="xs" c="var(--nils-text-tertiary)">·</Text>
                              <Text size="xs" c="var(--nils-text-tertiary)">
                                Took {formatDuration(job.startedAt, job.finishedAt)}
                              </Text>
                            </>
                          )}
                          {job.status === 'failed' && job.errorMessage && (
                            <>
                              <Text size="xs" c="var(--nils-text-tertiary)">·</Text>
                              <Text size="xs" c="var(--nils-error)" truncate style={{ maxWidth: 200 }}>
                                {job.errorMessage}
                              </Text>
                            </>
                          )}
                        </Group>
                      </Stack>
                    </Group>

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
                </Box>
              );
            })}
          </Stack>
        </Box>
      </Collapse>
    </Box>
  );
};

export default SystemJobCard;
