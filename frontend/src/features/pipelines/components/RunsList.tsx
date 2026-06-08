/**
 * RunsList - all runs for a pipeline, newest first, from GET /{pipelineId}/runs.
 *
 * Sourced from a single usePipelineRuns query (persists across reloads and polls
 * while any run is still progressing), not session state. onSelect(runId) is the
 * stable selection contract consumed by PipelineDetailPage.
 */
import { Loader, Stack, Table, Text } from '@mantine/core';
import { usePipelineRuns } from '../api';
import { formatDateTime } from '../../../utils/formatters';
import { RunStatusBadge } from './RunStatusBadge';

interface RunsListProps {
  pipelineId: number;
  onSelect: (runId: number) => void;
  selectedRunId?: number;
}

export const RunsList = ({ pipelineId, onSelect, selectedRunId }: RunsListProps) => {
  const { data: runs, isLoading } = usePipelineRuns(pipelineId);

  if (isLoading) {
    return (
      <Stack align="center" py="sm">
        <Loader size="sm" color="var(--nils-accent-primary)" />
      </Stack>
    );
  }

  if (!runs || runs.length === 0) {
    return (
      <Text size="sm" c="dimmed">
        No runs yet. Launch a run above to see it here.
      </Text>
    );
  }

  return (
    <Table highlightOnHover>
      <Table.Thead>
        <Table.Tr>
          <Table.Th>Run</Table.Th>
          <Table.Th>Status</Table.Th>
          <Table.Th>Input</Table.Th>
          <Table.Th>Created</Table.Th>
        </Table.Tr>
      </Table.Thead>
      <Table.Tbody>
        {runs.map((run) => (
          <Table.Tr
            key={run.id}
            onClick={() => onSelect(run.id)}
            style={{
              cursor: 'pointer',
              backgroundColor:
                run.id === selectedRunId ? 'var(--nils-bg-tertiary)' : undefined,
            }}
          >
            <Table.Td>
              <Text size="sm" ff="monospace">
                #{run.id}
              </Text>
            </Table.Td>
            <Table.Td>
              <RunStatusBadge status={run.status} />
            </Table.Td>
            <Table.Td>
              <Text size="sm" c="dimmed">
                {run.input_source}
              </Text>
            </Table.Td>
            <Table.Td>
              <Text size="sm" c="dimmed">
                {formatDateTime(run.created_at)}
              </Text>
            </Table.Td>
          </Table.Tr>
        ))}
      </Table.Tbody>
    </Table>
  );
};
