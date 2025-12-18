import { Box, Button, Group, Stack, Text } from '@mantine/core';
import { IconDatabase, IconFileText } from '@tabler/icons-react';
import { useEffect, useRef } from 'react';
import { notifications } from '@mantine/notifications';
import type { Step2Metrics } from '../types';

interface Step2PreviewCardProps {
  cohortId: number;
  metrics: Step2Metrics;
  onPushToDatabase: () => void;
  isPushing?: boolean;
}

/**
 * Step2PreviewCard - Preview component for Stack Fingerprint step.
 * 
 * Note: This component is currently not used as preview mode has been removed.
 * Kept for potential future use.
 */
export function Step2PreviewCard({ cohortId, metrics, onPushToDatabase, isPushing = false }: Step2PreviewCardProps) {
  const totalStacks = metrics.stacks_processed || 0;
  const totalFingerprints = metrics.total_fingerprints_created || 0;

  const tableHostRef = useRef<HTMLDivElement>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const dataTableRef = useRef<any>(null);
  const tableElementRef = useRef<HTMLTableElement | null>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const dataTableCtorRef = useRef<any>(null);

  // Cleanup function
  const cleanup = () => {
    if (dataTableRef.current) {
      try {
        dataTableRef.current.destroy(true);
      } catch (error) {
        console.error('Error destroying DataTable:', error);
      }
      dataTableRef.current = null;
    }
    if (tableElementRef.current) {
      tableElementRef.current.remove();
      tableElementRef.current = null;
    }
  };

  useEffect(() => {
    if (totalStacks === 0) return;

    let disposed = false;

    const initialize = async () => {
      try {
        // Dynamically import DataTables
        const [dataTablesFactory] = await Promise.all([
          import('datatables.net'),
          import('datatables.net-fixedcolumns'),
        ]);

        if (disposed) return;

        const DataTableCtor = dataTablesFactory.default;
        dataTableCtorRef.current = DataTableCtor;

        // Cleanup any existing table
        cleanup();

        if (!tableHostRef.current) return;

        // Create table element
        const table = document.createElement('table');
        table.className = 'display compact stripe hover';
        table.style.width = '100%';
        tableHostRef.current.appendChild(table);
        tableElementRef.current = table;

        // Define columns
        const columns = [
          { data: 'series_id', title: 'Series ID', className: 'dt-body-nowrap' },
          { data: 'stack_index', title: 'Index', className: 'dt-body-nowrap dt-body-center' },
          { data: 'stack_modality', title: 'Modality', className: 'dt-body-nowrap' },
          { data: 'stack_key', title: 'Stack Key', className: 'dt-body-nowrap' },
          { data: 'stack_n_instances', title: 'Instances', className: 'dt-body-nowrap dt-body-right' },
          { data: 'stack_inversion_time', title: 'TI (ms)', className: 'dt-body-nowrap dt-body-right' },
          { data: 'stack_echo_time', title: 'TE (ms)', className: 'dt-body-nowrap dt-body-right' },
          { data: 'stack_repetition_time', title: 'TR (ms)', className: 'dt-body-nowrap dt-body-right' },
          { data: 'stack_flip_angle', title: 'Flip Angle', className: 'dt-body-nowrap dt-body-right' },
          { data: 'stack_image_orientation', title: 'Orientation', className: 'dt-body-nowrap' },
          { data: 'stack_receive_coil_name', title: 'Coil', className: 'dt-body-nowrap' },
        ];

        // Create AJAX config (use new step ID)
        const ajax = {
          url: `/api/cohorts/${cohortId}/stages/sort/steps/stack_fingerprint/preview`,
          type: 'GET',
          dataSrc: 'data',
        };

        // Initialize DataTable
        const instance = new DataTableCtor(table, {
          serverSide: true,
          processing: true,
          deferRender: true,
          scrollX: true,
          scrollCollapse: true,
          autoWidth: false,
          layout: {
            topStart: 'pageLength',
            topEnd: 'search',
            bottomStart: 'info',
            bottomEnd: 'paging',
          },
          pageLength: 50,
          lengthMenu: [
            [25, 50, 100, 250],
            [25, 50, 100, 250],
          ],
          fixedColumns: {
            left: 2,  // Fix Series ID and Index columns
          },
          columnDefs: [
            {
              targets: '_all',
              className: 'dt-body-nowrap',
            },
          ],
          ajax,
          columns,
          order: [[0, 'asc']],  // Default sort by Series ID
        });

        dataTableRef.current = instance;
      } catch (error) {
        if (disposed) return;
        console.error('Failed to initialize preview DataTable', error);
        notifications.show({ color: 'red', message: 'Failed to render preview table.' });
      }
    };

    void initialize();

    return () => {
      disposed = true;
      cleanup();
    };
  }, [cohortId, totalStacks]);

  if (totalStacks === 0) {
    return null;  // No data available
  }

  return (
    <Box
      p="md"
      style={{
        backgroundColor: 'rgba(88, 166, 255, 0.08)',
        border: '2px solid rgba(88, 166, 255, 0.3)',
        borderRadius: 'var(--nils-radius-md)',
      }}
    >
      {/* Header */}
      <Group gap="sm" mb="md">
        <IconFileText size={20} color="var(--nils-accent-primary)" />
        <Text fw={600} c="var(--nils-accent-primary)">
          Stack Fingerprint Results
        </Text>
      </Group>

      <Stack gap="md">
        {/* Summary */}
        <Box
          p="sm"
          style={{
            backgroundColor: 'var(--nils-bg-elevated)',
            borderRadius: 'var(--nils-radius-sm)',
          }}
        >
          <Group justify="space-between">
            <Box>
              <Text size="xs" c="var(--nils-text-tertiary)">Fingerprints Created</Text>
              <Text size="lg" fw={700} c="var(--nils-accent-primary)">{totalFingerprints}</Text>
            </Box>
            <Box>
              <Text size="xs" c="var(--nils-text-tertiary)">Stacks Processed</Text>
              <Text size="lg" fw={700} c="var(--nils-success)">{totalStacks}</Text>
            </Box>
            <Box>
              <Text size="xs" c="var(--nils-text-tertiary)">Multi-Stack Series</Text>
              <Text size="lg" fw={700} c="var(--nils-warning)">{metrics.series_with_multiple_stacks}</Text>
            </Box>
          </Group>
        </Box>

        {/* Modality Breakdown */}
        {metrics.breakdown_by_modality && Object.keys(metrics.breakdown_by_modality).length > 0 && (
          <Box
            p="sm"
            style={{
              backgroundColor: 'var(--nils-bg-elevated)',
              borderRadius: 'var(--nils-radius-sm)',
            }}
          >
            <Text size="sm" fw={600} c="var(--nils-text-primary)" mb="xs">
              Breakdown by Modality
            </Text>
            <Group gap="xs">
              {Object.entries(metrics.breakdown_by_modality).map(([modality, count]) => (
                <Box
                  key={modality}
                  px="sm"
                  py={4}
                  style={{
                    backgroundColor: 'rgba(88, 166, 255, 0.15)',
                    borderRadius: 'var(--nils-radius-xs)',
                  }}
                >
                  <Text size="sm" fw={500} c="var(--nils-accent-primary)">
                    {modality}: {count}
                  </Text>
                </Box>
              ))}
            </Group>
          </Box>
        )}

        {/* DataTable Preview */}
        <Box
          p="sm"
          style={{
            backgroundColor: 'var(--nils-bg-elevated)',
            borderRadius: 'var(--nils-radius-sm)',
          }}
        >
          <Text size="sm" fw={600} c="var(--nils-text-primary)" mb="xs">
            Stack Details: {totalStacks} Stacks
          </Text>
          <div ref={tableHostRef} className="preview-table-container" />
        </Box>

        {/* Push to Database Button */}
        <Group justify="flex-end">
          <Button
            leftSection={<IconDatabase size={18} />}
            onClick={onPushToDatabase}
            loading={isPushing}
            disabled={isPushing}
            size="md"
            color="green"
          >
            Push to Database
          </Button>
        </Group>
      </Stack>
    </Box>
  );
}
