/**
 * ApplicationTableExplorer - DataTables-based table explorer for application database.
 *
 * Performance optimized: Table buttons use useCallback for stable onClick handlers.
 */
import { memo, useCallback, useEffect, useRef, useState } from 'react';
import {
  Badge,
  Button,
  Card,
  Group,
  Loader,
  ScrollArea,
  Stack,
  Tabs,
  Text,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useApplicationTables } from '../api';
import type { MetadataTableInfo } from '../../../types';
import 'datatables.net-dt/css/dataTables.dataTables.css';
import 'datatables.net-fixedcolumns-dt/css/fixedColumns.dataTables.css';

type DataTablesOrderDirection = 'asc' | 'desc';

interface DataTablesColumn {
  data: string;
  name: string;
  searchable?: boolean;
  orderable?: boolean;
  title?: string;
}

interface DataTablesRequestPayload {
  draw?: number;
  start?: number;
  length?: number;
  columns?: DataTablesColumn[];
  search?: { value: string; regex: boolean };
  order?: Array<{ column: number; dir: DataTablesOrderDirection }>;
}

interface DataTablesResponsePayload {
  draw: number;
  recordsTotal: number;
  recordsFiltered: number;
  data: unknown[];
}

interface DataTablesOptions {
  ajax?: (request: DataTablesRequestPayload, callback: (data: DataTablesResponsePayload) => void) => void | Promise<void>;
  columns?: DataTablesColumn[];
  pageLength?: number;
  lengthMenu?: number[][];
  fixedColumns?: { left?: number };
  dom?: string;
  autoWidth?: boolean;
  [key: string]: unknown;
}

interface DataTablesInstance {
  destroy: () => void;
  table: () => { container: () => HTMLElement };
}

type DataTablesCtor = new (node: HTMLTableElement, options: DataTablesOptions) => DataTablesInstance;

const formatNumber = (value: number) => value.toLocaleString();

// Memoized table button component to prevent unnecessary re-renders
interface TableButtonProps {
  table: MetadataTableInfo;
  isActive: boolean;
  onSelect: (table: MetadataTableInfo) => void;
}

const TableButton = memo(({ table, isActive, onSelect }: TableButtonProps) => {
  const hasData = table.row_count > 0;
  return (
    <Button
      variant={isActive ? 'filled' : 'light'}
      size="compact-sm"
      onClick={() => onSelect(table)}
      styles={{
        root: {
          opacity: hasData ? 1 : 0.5,
        },
      }}
    >
      <Group gap={4} align="center" wrap="nowrap">
        <Text size="sm">{table.label}</Text>
        <Badge
          size="xs"
          color={hasData ? (isActive ? 'white' : 'gray') : 'dark'}
          variant={isActive ? 'light' : 'outline'}
        >
          {formatNumber(table.row_count)}
        </Badge>
      </Group>
    </Button>
  );
});
TableButton.displayName = 'AppTableButton';

// Table categories for application database (matches backend categories)
const TABLE_CATEGORIES: Record<string, { label: string; description: string; tables: string[] }> = {
  'cohorts': {
    label: 'Cohorts & Pipeline',
    description: 'Cohort definitions and pipeline execution state',
    tables: ['cohorts', 'nils_dataset_pipeline_steps'],
  },
  'jobs': {
    label: 'Jobs',
    description: 'Job execution history and run logs',
    tables: ['jobs', 'job_runs'],
  },
  'anonymization': {
    label: 'Anonymization',
    description: 'PHI removal audit logs and processing summaries',
    tables: ['anonymize_study_audit', 'anonymize_leaf_summary'],
  },
};

export const ApplicationTableExplorer = () => {
  const tablesQuery = useApplicationTables();
  const [selectedTable, setSelectedTable] = useState<MetadataTableInfo | null>(null);
  const tableHostRef = useRef<HTMLDivElement | null>(null);
  const tableElementRef = useRef<HTMLTableElement | null>(null);
  const dataTableRef = useRef<DataTablesInstance | null>(null);
  const dataTableCtorRef = useRef<DataTablesCtor | null>(null);
  const activeRequestRef = useRef<AbortController | null>(null);

  useEffect(() => {
    if (tablesQuery.isLoading || tablesQuery.isError) {
      return;
    }
    if (!tablesQuery.data || tablesQuery.data.length === 0) {
      setSelectedTable(null);
      return;
    }
    setSelectedTable((current) => {
      if (current) {
        const updated = tablesQuery.data?.find((table) => table.name === current.name);
        if (updated) {
          return updated;
        }
      }
      return tablesQuery.data[0];
    });
  }, [tablesQuery.data, tablesQuery.isError, tablesQuery.isLoading]);

  useEffect(() => {
    return () => {
      if (activeRequestRef.current) {
        activeRequestRef.current.abort();
        activeRequestRef.current = null;
      }
      if (dataTableRef.current) {
        dataTableRef.current.destroy();
        dataTableRef.current = null;
      }
      if (tableElementRef.current && tableElementRef.current.parentNode) {
        tableElementRef.current.parentNode.removeChild(tableElementRef.current);
        tableElementRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    let disposed = false;

    const cleanup = () => {
      if (activeRequestRef.current) {
        activeRequestRef.current.abort();
        activeRequestRef.current = null;
      }
      if (dataTableRef.current) {
        dataTableRef.current.destroy();
        dataTableRef.current = null;
      }
      if (tableElementRef.current && tableElementRef.current.parentNode) {
        tableElementRef.current.parentNode.removeChild(tableElementRef.current);
        tableElementRef.current = null;
      }
    };

    if (!selectedTable) {
      cleanup();
      return () => {
        disposed = true;
        cleanup();
      };
    }

    const columns = selectedTable.columns.map((column) => ({
      data: column.name,
      name: column.name,
      title: column.label,
    }));

    const ajax = async (requestData: DataTablesRequestPayload, callback: (data: DataTablesResponsePayload) => void) => {
      if (activeRequestRef.current) {
        activeRequestRef.current.abort();
      }
      const controller = new AbortController();
      activeRequestRef.current = controller;

      const payload: DataTablesRequestPayload = {
        ...requestData,
        columns: requestData?.columns ?? columns.map((column) => ({
          data: column.data,
          name: column.name,
          searchable: true,
          orderable: true,
        })),
      };

      try {
        const response = await fetch(`/api/application/tables/${selectedTable.name}/query`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
          signal: controller.signal,
        });

        if (!response.ok) {
          const message = await response.text();
          throw new Error(message || `Failed to load ${selectedTable.label}`);
        }

        const json: DataTablesResponsePayload = await response.json();
        callback(json);
      } catch (error) {
        if (error instanceof DOMException && error.name === 'AbortError') {
          return;
        }
        const message = error instanceof Error ? error.message : 'Failed to load table data';
        notifications.show({ color: 'red', message });
        callback({
          draw: requestData?.draw ?? 0,
          recordsTotal: 0,
          recordsFiltered: 0,
          data: [],
        });
      } finally {
        if (activeRequestRef.current === controller) {
          activeRequestRef.current = null;
        }
      }
    };

    const initialize = async () => {
      try {
        if (!dataTableCtorRef.current) {
          const module = await import('datatables.net-dt');
          dataTableCtorRef.current = module.default;
          await import('datatables.net-fixedcolumns');
        }

        if (disposed || !tableHostRef.current) {
          return;
        }

        cleanup();

        const table = document.createElement('table');
        table.className = 'display compact nowrap stripe hover row-border order-column';
        table.style.width = '100%';

        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        for (const column of selectedTable.columns) {
          const th = document.createElement('th');
          th.textContent = column.label;
          headerRow.appendChild(th);
        }
        thead.appendChild(headerRow);
        table.appendChild(thead);

        tableHostRef.current.innerHTML = '';
        tableHostRef.current.appendChild(table);
        tableElementRef.current = table;

        const instance = new dataTableCtorRef.current(table, {
          legacy: false,
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
          pageLength: 10,
          lengthMenu: [
            [10, 25, 50, 100, 250],
            [10, 25, 50, 100, 250],
          ],
          fixedColumns: {
            left: 1,
          },
          columnDefs: [
            {
              targets: '_all',
              className: 'dt-body-nowrap',
            },
          ],
          ajax,
          columns,
          order: [],
        });

        dataTableRef.current = instance;
      } catch (error) {
        if (disposed) {
          return;
        }
        console.error('Failed to initialize application DataTable', error);
        notifications.show({ color: 'red', message: 'Failed to render application table.' });
      }
    };

    void initialize();

    return () => {
      disposed = true;
      cleanup();
    };
  }, [selectedTable]);

  // Memoized table selection handler to prevent re-creating callbacks
  const handleTableSelect = useCallback((table: MetadataTableInfo) => {
    setSelectedTable(table);
  }, []);

  // Memoized render function for table buttons
  const renderTableButtons = useCallback(
    (categoryKey: string) => {
      if (!tablesQuery.data) return null;
      const category = TABLE_CATEGORIES[categoryKey];
      if (!category) return null;

      const tables = tablesQuery.data.filter((t) => category.tables.includes(t.name));
      if (tables.length === 0) {
        return (
          <Stack gap="xs">
            <Text size="xs" c="dimmed">
              {category.description}
            </Text>
            <Text size="sm" c="dimmed">
              No tables available in this category.
            </Text>
          </Stack>
        );
      }
      return (
        <Stack gap="sm">
          <Text size="xs" c="dimmed">
            {category.description}
          </Text>
          <Group gap="xs" wrap="wrap">
            {tables.map((table) => (
              <TableButton
                key={table.name}
                table={table}
                isActive={selectedTable?.name === table.name}
                onSelect={handleTableSelect}
              />
            ))}
          </Group>
        </Stack>
      );
    },
    [tablesQuery.data, selectedTable?.name, handleTableSelect]
  );

  return (
    <Card withBorder radius="md" padding="lg">
      <Stack gap="md">
        <Group justify="space-between" align="flex-start">
          <Stack gap={4}>
            <Text fw={600}>Application tables</Text>
            <Text size="sm" c="dimmed">
              Explore application database records with server-side pagination.
            </Text>
          </Stack>
          {tablesQuery.isLoading && <Loader size="sm" />}
        </Group>

        {tablesQuery.isError ? (
          <Text size="sm" c="red">
            {(tablesQuery.error as Error)?.message ?? 'Failed to load application tables.'}
          </Text>
        ) : tablesQuery.data && tablesQuery.data.length ? (
          <Stack gap="md">
            <Tabs defaultValue="cohorts">
              <Tabs.List>
                <Tabs.Tab value="cohorts">Cohorts & Pipeline</Tabs.Tab>
                <Tabs.Tab value="jobs">Jobs</Tabs.Tab>
                <Tabs.Tab value="anonymization">Anonymization</Tabs.Tab>
              </Tabs.List>

              <Tabs.Panel value="cohorts" pt="md">
                {renderTableButtons('cohorts')}
              </Tabs.Panel>

              <Tabs.Panel value="jobs" pt="md">
                {renderTableButtons('jobs')}
              </Tabs.Panel>

              <Tabs.Panel value="anonymization" pt="md">
                {renderTableButtons('anonymization')}
              </Tabs.Panel>
            </Tabs>

            {selectedTable ? (
              <Stack gap="sm">
                <Text size="xs" c="dimmed">
                  Showing data for <strong>{selectedTable.label}</strong> ({formatNumber(selectedTable.row_count)} rows).
                </Text>
                <ScrollArea>
                  <div key={selectedTable.name} ref={tableHostRef} className="application-table-container" />
                </ScrollArea>
              </Stack>
            ) : (
              <Text size="sm" c="dimmed">
                Select a table to view its contents.
              </Text>
            )}
          </Stack>
        ) : (
          <Text size="sm" c="dimmed">
            No application tables available.
          </Text>
        )}
      </Stack>
    </Card>
  );
};

export default ApplicationTableExplorer;
