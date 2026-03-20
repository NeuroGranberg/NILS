import { Badge, Card, Group, Progress, Stack, Text } from '@mantine/core';
import type { Job } from '../../../types';
import { JOB_STATUS_CONFIG } from '../../../constants/status';
import { formatDateTime } from '../../../utils/formatters';

interface ExportProgressProps {
  job: Job;
}

export const ExportProgress = ({ job }: ExportProgressProps) => {
  const statusConfig = JOB_STATUS_CONFIG[job.status];
  const metrics = job.metrics as unknown as Record<string, unknown> | undefined;

  return (
    <Card withBorder padding="md" radius="md">
      <Stack gap="xs">
        <Group justify="space-between" align="center">
          <Text fw={600}>Export Job</Text>
          <Badge color={statusConfig?.mantineColor ?? 'gray'}>{job.status.toUpperCase()}</Badge>
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
        {metrics && (
          <Group gap="lg" mt="xs">
            {metrics.exported_stacks != null && (
              <Text size="xs" c="dimmed">
                Exported: {String(metrics.exported_stacks)} stacks
              </Text>
            )}
            {metrics.copied_files != null && (
              <Text size="xs" c="dimmed">
                Copied: {String(metrics.copied_files)} files
              </Text>
            )}
            {metrics.skipped_files != null && Number(metrics.skipped_files) > 0 && (
              <Text size="xs" c="yellow">
                Skipped: {String(metrics.skipped_files)} files
              </Text>
            )}
          </Group>
        )}
      </Stack>
    </Card>
  );
};
