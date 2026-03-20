import {
  ActionIcon,
  Badge,
  Box,
  Card,
  Group,
  Progress,
  Stack,
  Text,
  Tooltip,
} from '@mantine/core';
import { IconTrash } from '@tabler/icons-react';
import { Link } from 'react-router-dom';
import type { Job } from '../../../types';
import { JOB_STATUS_CONFIG } from '../../../constants/status';
import { formatDateTime } from '../../../utils/formatters';

interface ExportCardProps {
  job: Job;
  onDelete?: (jobId: number) => void;
  deleteLoading?: boolean;
}

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

const truncatePath = (path: string, maxLength = 60): string => {
  if (path.length <= maxLength) return path;
  const parts = path.split('/');
  if (parts.length <= 3) return path;
  return `.../${parts.slice(-3).join('/')}`;
};

const isTerminal = (status: string) =>
  ['completed', 'completed_with_warnings', 'failed', 'canceled'].includes(status);

export const ExportCard = ({ job, onDelete, deleteLoading }: ExportCardProps) => {
  const cfg = job.config as Record<string, unknown>;
  const exportName = (cfg?.export_name as string) || (job.config?.name as string) || 'Unnamed Export';
  const exportDescription = (cfg?.export_description as string) || '';
  const outputPath = (cfg?.output_path as string) || '';
  const stackCount = (cfg?.stack_count as number) || 0;
  const totalSubjects = (cfg?.total_subjects as number) || 0;
  const totalSessions = (cfg?.total_sessions as number) || 0;
  const sourceCohort = (cfg?.source_cohort_name as string) || '';
  const isRunning = job.status === 'running';
  const status = JOB_STATUS_CONFIG[job.status];
  const metrics = job.metrics as unknown as Record<string, unknown> | undefined;

  const getBorderColor = () => {
    if (isRunning) return 'var(--nils-accent-primary)';
    if (job.status === 'failed') return 'var(--nils-error)';
    return 'var(--nils-border-subtle)';
  };

  return (
    <Card
      component={Link}
      to={`/export/${job.id}`}
      padding="md"
      radius="md"
      style={{
        backgroundColor: 'var(--nils-bg-secondary)',
        border: `1px solid ${getBorderColor()}`,
        textDecoration: 'none',
        transition: 'all 150ms ease',
        cursor: 'pointer',
      }}
      styles={{
        root: {
          '&:hover': {
            borderColor: 'var(--nils-border)',
            transform: 'translateY(-2px)',
          },
        },
      }}
    >
      <Stack gap="sm">
        {/* Header */}
        <Group justify="space-between" align="flex-start" wrap="nowrap">
          <Stack gap={2} style={{ flex: 1, minWidth: 0 }}>
            <Group gap="xs" wrap="nowrap">
              <Text fw={600} size="md" c="var(--nils-text-primary)" truncate>
                {exportName}
              </Text>
              {sourceCohort && (
                <Badge size="xs" variant="light" color="blue">{sourceCohort}</Badge>
              )}
              <Box
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 4,
                  padding: '2px 8px',
                  borderRadius: 'var(--nils-radius-xs)',
                  backgroundColor: status?.bgColor,
                  flexShrink: 0,
                }}
              >
                <Box
                  style={{
                    width: 6,
                    height: 6,
                    borderRadius: '50%',
                    backgroundColor: status?.color,
                    animation: isRunning ? 'pulse 2s infinite' : 'none',
                  }}
                />
                <Text size="xs" fw={500} c={status?.color}>{status?.label}</Text>
              </Box>
            </Group>
            {exportDescription && (
              <Text size="xs" c="var(--nils-text-tertiary)">{exportDescription}</Text>
            )}
          </Stack>

          {/* Delete action */}
          {isTerminal(job.status) && onDelete && (
            <Tooltip label="Delete export record">
              <ActionIcon
                size="sm"
                variant="subtle"
                color="red"
                loading={deleteLoading}
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  onDelete(job.id);
                }}
                style={{ flexShrink: 0 }}
              >
                <IconTrash size={14} />
              </ActionIcon>
            </Tooltip>
          )}
        </Group>

        {/* Progress bar for running */}
        {isRunning && (
          <Progress value={job.progress ?? 0} size="sm" radius="md" transitionDuration={200} />
        )}

        {/* Stats row */}
        <Group gap="lg">
          <Stack gap={0}>
            <Text size="xs" c="var(--nils-text-tertiary)">Stacks</Text>
            <Text size="sm" fw={500} c="var(--nils-text-primary)">{stackCount}</Text>
          </Stack>
          {totalSubjects > 0 && (
            <Stack gap={0}>
              <Text size="xs" c="var(--nils-text-tertiary)">Subjects</Text>
              <Text size="sm" fw={500} c="var(--nils-text-primary)">{totalSubjects}</Text>
            </Stack>
          )}
          {totalSessions > 0 && (
            <Stack gap={0}>
              <Text size="xs" c="var(--nils-text-tertiary)">Sessions</Text>
              <Text size="sm" fw={500} c="var(--nils-text-primary)">{totalSessions}</Text>
            </Stack>
          )}
          {metrics && !isRunning && (
            <>
              {metrics.exported_stacks != null && (
                <Stack gap={0}>
                  <Text size="xs" c="var(--nils-text-tertiary)">Exported</Text>
                  <Text size="sm" fw={500} c="var(--nils-text-primary)">{String(metrics.exported_stacks)}</Text>
                </Stack>
              )}
              {metrics.copied_files != null && Number(metrics.copied_files) > 0 && (
                <Stack gap={0}>
                  <Text size="xs" c="var(--nils-text-tertiary)">Files Copied</Text>
                  <Text size="sm" fw={500} c="var(--nils-text-primary)">{String(metrics.copied_files)}</Text>
                </Stack>
              )}
            </>
          )}
        </Group>

        {/* Footer: path + timing */}
        <Group gap="md" wrap="wrap">
          {outputPath && (
            <Text size="xs" c="var(--nils-text-tertiary)" truncate style={{ maxWidth: 400 }}>
              {truncatePath(outputPath)}
            </Text>
          )}
          <Text size="xs" c="var(--nils-text-tertiary)">
            {formatDateTime(job.submittedAt)}
          </Text>
          {isRunning && job.startedAt && (
            <Text size="xs" c="var(--nils-accent-primary)" fw={500}>
              Running for {formatDuration(job.startedAt)}
            </Text>
          )}
          {!isRunning && job.startedAt && job.finishedAt && (
            <Text size="xs" c="var(--nils-text-tertiary)">
              Took {formatDuration(job.startedAt, job.finishedAt)}
            </Text>
          )}
        </Group>

        {/* Error message */}
        {job.errorMessage && (
          <Text size="xs" c="var(--nils-error)">{job.errorMessage}</Text>
        )}
      </Stack>
    </Card>
  );
};
