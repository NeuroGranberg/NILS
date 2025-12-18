/**
 * Job history table component.
 * Displays jobs in a compact table format with status, progress, and actions.
 */

import {
  ActionIcon,
  Box,
  Button,
  Group,
  Progress,
  ScrollArea,
  Stack,
  Table,
  Text,
  Tooltip,
} from '@mantine/core';
import {
  IconPlayerPause,
  IconPlayerPlay,
  IconPlayerStop,
  IconRefresh,
  IconTrash,
} from '@tabler/icons-react';
import type { ReactNode } from 'react';
import { useJobAction, useDeleteJob } from '../api';
import type { Job, JobAction, JobStatus, JobMetrics } from '../../../types';
import { formatDateTime } from '../../../utils/formatters';

const statusConfig: Record<JobStatus, { color: string; bgColor: string; label: string }> = {
  queued: { color: 'var(--nils-stage-pending)', bgColor: 'rgba(163, 113, 247, 0.15)', label: 'Queued' },
  running: { color: 'var(--nils-stage-running)', bgColor: 'rgba(88, 166, 255, 0.15)', label: 'Running' },
  paused: { color: 'var(--nils-stage-paused)', bgColor: 'rgba(210, 153, 34, 0.15)', label: 'Paused' },
  completed: { color: 'var(--nils-stage-completed)', bgColor: 'rgba(63, 185, 80, 0.15)', label: 'Completed' },
  failed: { color: 'var(--nils-stage-failed)', bgColor: 'rgba(248, 81, 73, 0.15)', label: 'Failed' },
  canceled: { color: 'var(--nils-stage-idle)', bgColor: 'rgba(110, 118, 129, 0.15)', label: 'Canceled' },
};

const jobActions: Record<JobStatus, Array<{ action: JobAction; icon: ReactNode; label: string }>> = {
  queued: [{ action: 'cancel', icon: <IconPlayerStop size={14} />, label: 'Cancel' }],
  running: [
    { action: 'pause', icon: <IconPlayerPause size={14} />, label: 'Pause' },
    { action: 'cancel', icon: <IconPlayerStop size={14} />, label: 'Cancel' },
  ],
  paused: [
    { action: 'resume', icon: <IconPlayerPlay size={14} />, label: 'Resume' },
    { action: 'cancel', icon: <IconPlayerStop size={14} />, label: 'Cancel' },
  ],
  completed: [{ action: 'retry', icon: <IconRefresh size={14} />, label: 'Re-run' }],
  failed: [{ action: 'retry', icon: <IconRefresh size={14} />, label: 'Retry' }],
  canceled: [],
};

const canDelete = (status: JobStatus) => ['completed', 'failed', 'canceled'].includes(status);

const metricsSummary = (metrics: JobMetrics | Record<string, unknown>) => {
  // Handle extraction job metrics format
  if ('subjects' in metrics && typeof metrics.subjects === 'number') {
    const m = metrics as JobMetrics;
    return [
      `${m.subjects.toLocaleString()} subject${m.subjects === 1 ? '' : 's'}`,
      `${m.studies.toLocaleString()} stud${m.studies === 1 ? 'y' : 'ies'}`,
      `${m.series.toLocaleString()} series`,
    ].join(' · ');
  }

  // Handle sorting job metrics format
  if ('subjects_in_cohort' in metrics) {
    const subjects = (metrics.subjects_in_cohort as number) || 0;
    const studies = (metrics.total_studies as number) || 0;
    const series = (metrics.total_series as number) || 0;

    return [
      `${subjects.toLocaleString()} subject${subjects === 1 ? '' : 's'}`,
      `${studies.toLocaleString()} stud${studies === 1 ? 'y' : 'ies'}`,
      `${series.toLocaleString()} series`,
    ].join(' · ');
  }

  return null;
};

interface JobHistoryTableProps {
  jobs: Job[];
  compact?: boolean;
  showCohort?: boolean;
}

export const JobHistoryTable = ({ jobs, compact = false, showCohort = false }: JobHistoryTableProps) => {
  const jobAction = useJobAction();
  const deleteJob = useDeleteJob();

  if (jobs.length === 0) {
    return (
      <Box
        py="md"
        style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          backgroundColor: 'var(--nils-bg-tertiary)',
          borderRadius: 'var(--nils-radius-md)',
        }}
      >
        <Text size="sm" c="var(--nils-text-tertiary)">
          No jobs yet
        </Text>
        <Text size="xs" c="var(--nils-text-tertiary)">
          Start a pipeline stage to create jobs
        </Text>
      </Box>
    );
  }

  // Sort jobs by submittedAt descending
  const sortedJobs = [...jobs].sort((a, b) => (a.submittedAt > b.submittedAt ? -1 : 1));

  return (
    <ScrollArea>
      <Table 
        verticalSpacing={compact ? 'xs' : 'sm'} 
        horizontalSpacing={compact ? 'sm' : 'md'}
        styles={{
          table: {
            backgroundColor: 'var(--nils-bg-tertiary)',
            borderRadius: 'var(--nils-radius-md)',
          },
        }}
      >
        <Table.Thead>
          <Table.Tr>
            <Table.Th style={{ color: 'var(--nils-text-tertiary)', fontWeight: 600, fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              ID
            </Table.Th>
            {showCohort && (
              <Table.Th style={{ color: 'var(--nils-text-tertiary)', fontWeight: 600, fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                Cohort
              </Table.Th>
            )}
            <Table.Th style={{ color: 'var(--nils-text-tertiary)', fontWeight: 600, fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              Stage
            </Table.Th>
            <Table.Th style={{ color: 'var(--nils-text-tertiary)', fontWeight: 600, fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              Status
            </Table.Th>
            <Table.Th style={{ color: 'var(--nils-text-tertiary)', fontWeight: 600, fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.05em', minWidth: 120 }}>
              Progress
            </Table.Th>
            <Table.Th style={{ color: 'var(--nils-text-tertiary)', fontWeight: 600, fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              Submitted
            </Table.Th>
            <Table.Th style={{ color: 'var(--nils-text-tertiary)', fontWeight: 600, fontSize: '11px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              Actions
            </Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {sortedJobs.map((job) => {
            const status = statusConfig[job.status];
            return (
              <Table.Tr key={job.id} style={{ borderBottom: '1px solid var(--nils-border-subtle)' }}>
                <Table.Td>
                  <Text size="xs" c="var(--nils-text-tertiary)" ff="monospace">
                    #{job.id}
                  </Text>
                </Table.Td>
                {showCohort && (
                  <Table.Td>
                    <Text fw={500} size="sm" c="var(--nils-text-primary)">
                      {job.cohortName || '-'}
                    </Text>
                  </Table.Td>
                )}
                <Table.Td>
                  <Stack gap={2}>
                    <Text fw={500} size="sm" c="var(--nils-text-primary)" tt="capitalize">
                      {job.stageId}{job.stepId ? ` - ${job.stepId.replace(/_/g, ' ')}` : ''}
                    </Text>
                    {job.metrics && metricsSummary(job.metrics) && (
                      <Text size="xs" c="var(--nils-text-tertiary)">
                        {metricsSummary(job.metrics)}
                      </Text>
                    )}
                  </Stack>
                </Table.Td>
                <Table.Td>
                  <Box
                    style={{
                      display: 'inline-flex',
                      alignItems: 'center',
                      gap: '6px',
                      padding: '4px 8px',
                      borderRadius: 'var(--nils-radius-sm)',
                      backgroundColor: status.bgColor,
                    }}
                  >
                    <Box
                      style={{
                        width: 6,
                        height: 6,
                        borderRadius: '50%',
                        backgroundColor: status.color,
                        animation: job.status === 'running' ? 'pulse 2s infinite' : 'none',
                      }}
                    />
                    <Text size="xs" fw={500} c={status.color}>
                      {status.label}
                    </Text>
                  </Box>
                </Table.Td>
                <Table.Td>
                  <Stack gap={4}>
                    <Progress
                      value={job.progress}
                      size="sm"
                      styles={{
                        root: { backgroundColor: 'var(--nils-bg-secondary)' },
                        section: {
                          backgroundColor:
                            job.status === 'completed'
                              ? 'var(--nils-success)'
                              : job.status === 'failed'
                                ? 'var(--nils-error)'
                                : 'var(--nils-accent-primary)',
                        },
                      }}
                    />
                    <Text size="xs" fw={500} c="var(--nils-text-secondary)" ta="right">
                      {job.progress}%
                    </Text>
                  </Stack>
                </Table.Td>
                <Table.Td>
                  <Stack gap={0}>
                    <Text size="xs" c="var(--nils-text-primary)">
                      {formatDateTime(job.submittedAt)}
                    </Text>
                    {job.finishedAt && (
                      <Text size="xs" c="var(--nils-text-tertiary)">
                        Done {formatDateTime(job.finishedAt)}
                      </Text>
                    )}
                  </Stack>
                </Table.Td>
                <Table.Td>
                  <Group gap="xs">
                    {jobActions[job.status].map(({ action, icon, label }) => (
                      <Tooltip key={action} label={label}>
                        <Button
                          size="xs"
                          variant="light"
                          leftSection={icon}
                          loading={jobAction.isPending}
                          onClick={() => jobAction.mutate({ jobId: job.id, action })}
                          styles={{
                            root: {
                              backgroundColor: 'var(--nils-bg-secondary)',
                              '&:hover': {
                                backgroundColor: 'var(--nils-bg-elevated)',
                              },
                            },
                          }}
                        >
                          {compact ? '' : label}
                        </Button>
                      </Tooltip>
                    ))}
                    {canDelete(job.status) && (
                      <Tooltip label="Delete job record">
                        <ActionIcon
                          size="sm"
                          variant="subtle"
                          color="red"
                          loading={deleteJob.isPending}
                          onClick={() => deleteJob.mutate(job.id)}
                        >
                          <IconTrash size={14} />
                        </ActionIcon>
                      </Tooltip>
                    )}
                  </Group>
                </Table.Td>
              </Table.Tr>
            );
          })}
        </Table.Tbody>
      </Table>
    </ScrollArea>
  );
};
