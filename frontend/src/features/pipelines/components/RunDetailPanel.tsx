/**
 * RunDetailPanel - composes the single-run view: status badge, progress,
 * polled logs, an honest cancel control, an ingest (dry-run preview / run)
 * control, and a simple per-unit results summary after the run is terminal.
 *
 * Polling: useRun drives the authoritative terminal signal; useRunState and
 * useRunLogs poll only while the run is non-terminal (gated by `enabled`).
 * Mount this with `key={runId}` so the log cursor/buffer (held in refs inside
 * useRunLogs) reset cleanly when the selected run changes.
 */
import { useState } from 'react';
import {
  Alert,
  Badge,
  Button,
  Code,
  Group,
  Loader,
  Stack,
  Table,
  Text,
  Tooltip,
} from '@mantine/core';
import { IconAlertTriangle } from '@tabler/icons-react';
import { notifications } from '@mantine/notifications';
import { SectionCard } from '../../shared/components/SectionCard';
import {
  useCancelRun,
  useIngest,
  useRun,
  useRunLogs,
  useRunResults,
  useRunState,
} from '../api';
import { isTerminalRunStatus } from '../types';
import { formatDateTime } from '../../../utils/formatters';
import { RunStatusBadge } from './RunStatusBadge';
import { RunProgress } from './RunProgress';
import { RunLogs } from './RunLogs';

interface RunDetailPanelProps {
  runId: number;
}

const CANCEL_TOOLTIP =
  'Requests cancellation; takes effect between work units. An in-flight unit on the local executor cannot be interrupted.';

export const RunDetailPanel = ({ runId }: RunDetailPanelProps) => {
  const { data: runData, isLoading } = useRun(runId);
  const run = runData?.run;
  const terminal = isTerminalRunStatus(run?.status);
  const active = run != null && !terminal;

  const { data: state } = useRunState(runId, active);
  const { data: logs } = useRunLogs(runId, active);
  const { data: results } = useRunResults(runId, terminal);

  const cancelRun = useCancelRun(runId);
  const ingest = useIngest(runId);
  const [ingestPlan, setIngestPlan] = useState<Record<string, unknown> | null>(null);

  const handleCancel = () => {
    cancelRun.mutate(undefined, {
      onSuccess: () => {
        notifications.show({ color: 'yellow', message: 'Cancellation requested' });
      },
      onError: (error) => {
        notifications.show({ color: 'red', message: (error as Error).message });
      },
    });
  };

  const handleIngest = (dryRun: boolean) => {
    ingest.mutate(
      { dryRun },
      {
        onSuccess: (resp) => {
          if (dryRun) {
            setIngestPlan(resp.plan ?? {});
            notifications.show({ color: 'blue', message: 'Ingest preview ready' });
          } else {
            notifications.show({ color: 'green', message: 'Ingest started' });
          }
        },
        onError: (error) => {
          notifications.show({ color: 'red', message: (error as Error).message });
        },
      },
    );
  };

  if (isLoading || !run) {
    return (
      <SectionCard title="Run detail" description="">
        <Group justify="center" py="md">
          <Loader size="sm" color="var(--nils-accent-primary)" />
        </Group>
      </SectionCard>
    );
  }

  return (
    <SectionCard title={`Run #${run.id}`} description="Live status, logs, and ingest controls.">
      <Stack gap="md">
        {/* Header row */}
        <Group justify="space-between" align="center">
          <Group gap="sm">
            <RunStatusBadge status={run.status} />
            <Text size="xs" c="dimmed">
              {run.input_source}
            </Text>
            <Text size="xs" c="dimmed">
              {formatDateTime(run.created_at)}
            </Text>
          </Group>
          <Tooltip label={CANCEL_TOOLTIP} multiline w={280} withArrow>
            <Button
              color="red"
              variant="light"
              size="xs"
              onClick={handleCancel}
              disabled={terminal || cancelRun.isPending}
              loading={cancelRun.isPending}
            >
              Cancel run
            </Button>
          </Tooltip>
        </Group>

        {/* Error */}
        {run.last_error && (
          <Alert
            icon={<IconAlertTriangle size={16} />}
            color="red"
            variant="light"
            title="Run error"
          >
            <Text size="sm">{run.last_error}</Text>
          </Alert>
        )}

        {/* Progress */}
        <RunProgress state={state} />

        {/* Output path */}
        {run.output_path && (
          <Text size="xs" c="dimmed" ff="monospace">
            Output: {run.output_path}
          </Text>
        )}

        {/* Logs */}
        <Stack gap="xs">
          <Text size="sm" fw={600} c="var(--nils-text-primary)">
            Logs
          </Text>
          <RunLogs lines={logs ?? []} />
        </Stack>

        {/* Per-unit results (after terminal) — from the run's results.json */}
        {terminal && (
          <Stack gap="xs">
            <Text size="sm" fw={600} c="var(--nils-text-primary)">
              Results
              {state ? ` — ${state.units_done}/${state.units_total} succeeded` : ''}
            </Text>
            {results && results.units.length > 0 ? (
              <Table withTableBorder withColumnBorders>
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th>Unit</Table.Th>
                    <Table.Th>Status</Table.Th>
                    <Table.Th>Derivatives</Table.Th>
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {results.units.map((u) => (
                    <Table.Tr key={u.unit_id}>
                      <Table.Td>
                        <Text size="sm" ff="monospace">
                          {u.unit_id}
                        </Text>
                      </Table.Td>
                      <Table.Td>
                        <Badge
                          variant="light"
                          color={
                            u.status === 'succeeded'
                              ? 'green'
                              : u.status === 'failed'
                                ? 'red'
                                : 'gray'
                          }
                        >
                          {u.status}
                        </Badge>
                      </Table.Td>
                      <Table.Td>
                        {u.derivatives.length > 0 ? (
                          <Text size="xs" ff="monospace" c="dimmed">
                            {u.derivatives.join(', ')}
                          </Text>
                        ) : (
                          <Text size="xs" c="dimmed">
                            {u.error ?? '—'}
                          </Text>
                        )}
                      </Table.Td>
                    </Table.Tr>
                  ))}
                </Table.Tbody>
              </Table>
            ) : state ? (
              <Text size="sm" c="dimmed">
                {state.units_total} units · {state.units_done} done ·{' '}
                {state.units_failed} failed (no per-unit results.json found)
              </Text>
            ) : (
              <Text size="sm" c="dimmed">
                No results.
              </Text>
            )}
          </Stack>
        )}

        {/* Ingest controls */}
        <Stack gap="xs">
          <Text size="sm" fw={600} c="var(--nils-text-primary)">
            Ingest
          </Text>
          <Group gap="sm">
            <Button
              variant="default"
              size="xs"
              onClick={() => handleIngest(true)}
              loading={ingest.isPending}
            >
              Preview ingest (dry-run)
            </Button>
            <Tooltip
              label="Requires an output path. The backend rejects ingest without one."
              disabled={!!run.output_path}
            >
              <Button
                size="xs"
                onClick={() => handleIngest(false)}
                disabled={!run.output_path || ingest.isPending}
                loading={ingest.isPending}
              >
                Run ingest
              </Button>
            </Tooltip>
          </Group>
          {ingestPlan && (
            <Code block>{JSON.stringify(ingestPlan, null, 2)}</Code>
          )}
        </Stack>
      </Stack>
    </SectionCard>
  );
};
