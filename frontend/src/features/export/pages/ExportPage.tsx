import {
  Box,
  Button,
  Collapse,
  Group,
  Loader,
  Stack,
  Text,
  Title,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { IconFileExport, IconFolderOff, IconPlus } from '@tabler/icons-react';
import { useCallback, useMemo, useState } from 'react';
import { useExportJobs } from '../api';
import { useDeleteJob } from '../../jobs/api';
import { ExportCard } from '../components/ExportCard';
import { NewExportFlow } from '../components/NewExportFlow';

export const ExportPage = () => {
  const [showNewExport, setShowNewExport] = useState(false);
  const { data: exportJobs, isLoading } = useExportJobs();
  const deleteJobMutation = useDeleteJob();

  const sortedJobs = useMemo(() => {
    if (!exportJobs) return [];
    return [...exportJobs].sort((a, b) => {
      const aActive = ['running', 'queued', 'paused'].includes(a.status);
      const bActive = ['running', 'queued', 'paused'].includes(b.status);
      if (aActive && !bActive) return -1;
      if (!aActive && bActive) return 1;
      return a.submittedAt > b.submittedAt ? -1 : 1;
    });
  }, [exportJobs]);

  const handleDelete = useCallback(
    (jobId: number) => {
      deleteJobMutation.mutate(jobId, {
        onSuccess: () => {
          notifications.show({ title: 'Deleted', message: 'Export record removed.', color: 'teal' });
        },
      });
    },
    [deleteJobMutation],
  );

  const handleFlowComplete = useCallback(() => {
    setShowNewExport(false);
  }, []);

  return (
    <Stack gap="lg" p="md">
      <Group justify="space-between" align="center">
        <Group gap="sm">
          <IconFileExport size={28} stroke={1.5} />
          <div>
            <Title order={2}>Export</Title>
            <Text size="sm" c="var(--nils-text-tertiary)">
              Manage subset data exports
            </Text>
          </div>
        </Group>
        {!showNewExport && (
          <Button leftSection={<IconPlus size={16} />} onClick={() => setShowNewExport(true)}>
            New Export
          </Button>
        )}
      </Group>

      <Collapse in={showNewExport}>
        <Box
          p="md"
          style={{
            backgroundColor: 'var(--nils-bg-secondary)',
            borderRadius: 'var(--nils-radius-lg)',
            border: '1px solid var(--nils-accent-primary)',
          }}
        >
          <NewExportFlow
            onComplete={handleFlowComplete}
            onCancel={() => setShowNewExport(false)}
          />
        </Box>
      </Collapse>

      {isLoading && (
        <Box
          py="xl"
          style={{
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            minHeight: '200px',
          }}
        >
          <Loader size="md" color="var(--nils-accent-primary)" />
        </Box>
      )}

      {!isLoading && sortedJobs.length === 0 && !showNewExport && (
        <Box
          py="xl"
          style={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            minHeight: '300px',
            backgroundColor: 'var(--nils-bg-secondary)',
            borderRadius: 'var(--nils-radius-lg)',
            border: '1px solid var(--nils-border-subtle)',
          }}
        >
          <Box
            mb="md"
            style={{
              width: 48,
              height: 48,
              borderRadius: 'var(--nils-radius-md)',
              backgroundColor: 'var(--nils-bg-tertiary)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <IconFolderOff size={24} color="var(--nils-text-tertiary)" />
          </Box>
          <Text fw={600} size="md" c="var(--nils-text-primary)" mb={4}>
            No exports yet
          </Text>
          <Text size="sm" c="var(--nils-text-secondary)" ta="center" maw={320}>
            Create a new export to select a subset of data using a manifest and export it in
            BIDS or flat format.
          </Text>
          <Button mt="md" leftSection={<IconPlus size={16} />} onClick={() => setShowNewExport(true)}>
            New Export
          </Button>
        </Box>
      )}

      {!isLoading && sortedJobs.length > 0 && (
        <Stack gap="sm">
          {sortedJobs.map((job) => (
            <ExportCard
              key={job.id}
              job={job}
              onDelete={handleDelete}
              deleteLoading={deleteJobMutation.isPending}
            />
          ))}
        </Stack>
      )}
    </Stack>
  );
};
