import {
  Alert,
  Group,
  Select,
  Stack,
  Text,
} from '@mantine/core';
import { IconAlertTriangle, IconCheck } from '@tabler/icons-react';
import { useEffect, useMemo, useRef } from 'react';
import { SectionCard } from '../../shared/components/SectionCard';
import { useMetadataIdTypes } from '../../extraction/queries';
import { useSubjectIdentifiers } from '../api';
import type { ResolveResponse } from '../types';
import 'datatables.net-dt/css/dataTables.dataTables.css';

interface ManifestPreviewProps {
  result: ResolveResponse;
  subjectIdentifierSource: string | number;
  onSubjectIdentifierChange: (value: string | number) => void;
}

interface FlatRow {
  subject_code: string;
  export_name: string;
  study_date: string;
  series_stack_id: number;
  directory_type: string;
  base: string;
  modifier: string;
  technique: string;
  _subjectIdx: number;
}

type DataTablesCtor = new (node: HTMLTableElement, options: Record<string, unknown>) => {
  destroy: () => void;
};

export const ManifestPreview = ({
  result,
  subjectIdentifierSource,
  onSubjectIdentifierChange,
}: ManifestPreviewProps) => {
  const tableHostRef = useRef<HTMLDivElement | null>(null);
  const dataTableRef = useRef<{ destroy: () => void } | null>(null);
  const dataTableCtorRef = useRef<DataTablesCtor | null>(null);

  // Fetch id_types for the selector
  const { data: idTypes } = useMetadataIdTypes();
  const subjectIdentifierOptions = useMemo(() => {
    const options = [{ value: 'subject_code', label: 'Subject Code (default)' }];
    for (const idType of idTypes ?? []) {
      options.push({ value: String(idType.id), label: idType.name });
    }
    return options;
  }, [idTypes]);

  // Get unique subject codes from resolved entries
  const uniqueSubjectCodes = useMemo(() => {
    const codes = new Set<string>();
    for (const entry of result.resolved_entries) {
      codes.add(entry.subject_code);
    }
    return Array.from(codes).sort();
  }, [result]);

  // Fetch alternative identifiers when a non-default id_type is selected
  const activeIdTypeId = typeof subjectIdentifierSource === 'number' ? subjectIdentifierSource : null;
  const { data: idMapping } = useSubjectIdentifiers(uniqueSubjectCodes, activeIdTypeId);

  // Build flat rows with subject index for coloring + export name
  const flatRows = useMemo<FlatRow[]>(() => {
    const subjectOrder = new Map<string, number>();
    let idx = 0;
    for (const entry of result.resolved_entries) {
      if (!subjectOrder.has(entry.subject_code)) {
        subjectOrder.set(entry.subject_code, idx++);
      }
    }

    return result.resolved_entries.map((e) => ({
      subject_code: e.subject_code,
      export_name: (idMapping && idMapping[e.subject_code]) || e.subject_code,
      study_date: e.study_date,
      series_stack_id: e.series_stack_id,
      directory_type: e.directory_type ?? '',
      base: e.base ?? '',
      modifier: e.modifier ?? '',
      technique: e.technique ?? '',
      _subjectIdx: subjectOrder.get(e.subject_code) ?? 0,
    }));
  }, [result, idMapping]);

  useEffect(() => {
    let disposed = false;

    const cleanup = () => {
      if (dataTableRef.current) {
        dataTableRef.current.destroy();
        dataTableRef.current = null;
      }
      if (tableHostRef.current) {
        tableHostRef.current.innerHTML = '';
      }
    };

    if (flatRows.length === 0) {
      cleanup();
      return () => { disposed = true; cleanup(); };
    }

    const initialize = async () => {
      try {
        if (!dataTableCtorRef.current) {
          const module = await import('datatables.net-dt');
          dataTableCtorRef.current = module.default;
        }

        if (disposed || !tableHostRef.current) return;

        cleanup();

        const table = document.createElement('table');
        table.className = 'display compact nowrap stripe hover row-border order-column';
        table.style.width = '100%';

        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        for (const label of ['Subject', 'Export Name', 'Session (Date)', 'Stack ID', 'Intent', 'Base', 'Modifier', 'Technique']) {
          const th = document.createElement('th');
          th.textContent = label;
          headerRow.appendChild(th);
        }
        thead.appendChild(headerRow);
        table.appendChild(thead);

        tableHostRef.current.appendChild(table);

        const instance = new dataTableCtorRef.current(table, {
          data: flatRows,
          columns: [
            { data: 'subject_code', title: 'Subject' },
            { data: 'export_name', title: 'Export Name' },
            { data: 'study_date', title: 'Session (Date)' },
            { data: 'series_stack_id', title: 'Stack ID' },
            { data: 'directory_type', title: 'Intent' },
            { data: 'base', title: 'Base' },
            { data: 'modifier', title: 'Modifier' },
            { data: 'technique', title: 'Technique' },
          ],
          pageLength: 10,
          lengthMenu: [
            [10, 25, 50, 100, -1],
            [10, 25, 50, 100, 'All'],
          ],
          layout: {
            topStart: 'pageLength',
            topEnd: 'search',
            bottomStart: 'info',
            bottomEnd: 'paging',
          },
          order: [[0, 'asc'], [2, 'asc'], [3, 'asc']],
          deferRender: true,
          autoWidth: false,
          scrollX: true,
          createdRow: (row: HTMLTableRowElement, data: FlatRow) => {
            if (data._subjectIdx % 2 === 0) {
              row.style.backgroundColor = 'var(--nils-bg-secondary)';
            } else {
              row.style.backgroundColor = 'var(--nils-bg-tertiary)';
            }
          },
        });

        dataTableRef.current = instance;
      } catch (error) {
        console.error('Failed to initialize ManifestPreview DataTable', error);
      }
    };

    void initialize();

    return () => {
      disposed = true;
      cleanup();
    };
  }, [flatRows]);

  return (
    <Stack gap="md">
      {/* Summary */}
      <SectionCard title="Resolution Summary" description="Overview of what will be exported">
        <Group gap="lg">
          <Stack gap={2} align="center">
            <Text size="xl" fw={700} c="var(--nils-accent-primary)">
              {result.total_subjects}
            </Text>
            <Text size="xs" c="var(--nils-text-tertiary)">Subjects</Text>
          </Stack>
          <Stack gap={2} align="center">
            <Text size="xl" fw={700} c="var(--nils-accent-primary)">
              {result.total_sessions}
            </Text>
            <Text size="xs" c="var(--nils-text-tertiary)">Sessions</Text>
          </Stack>
          <Stack gap={2} align="center">
            <Text size="xl" fw={700} c="var(--nils-accent-primary)">
              {result.total_stacks}
            </Text>
            <Text size="xs" c="var(--nils-text-tertiary)">Stacks</Text>
          </Stack>
        </Group>
      </SectionCard>

      {/* Warnings */}
      {result.warnings.length > 0 && (
        <Alert
          icon={<IconAlertTriangle size={16} />}
          title={`${result.warnings.length} warning(s)`}
          color="yellow"
          variant="light"
        >
          <Stack gap={4}>
            {result.warnings.map((w, i) => (
              <Text key={i} size="sm">{w}</Text>
            ))}
          </Stack>
        </Alert>
      )}

      {result.total_stacks === 0 && result.warnings.length === 0 && (
        <Alert icon={<IconAlertTriangle size={16} />} color="red" variant="light">
          No stacks resolved. Check that your manifest entries match subjects in the database.
        </Alert>
      )}

      {/* Subject Naming - above the table so user sees effect immediately */}
      {result.total_stacks > 0 && (
        <SectionCard
          title="Subject Naming"
          description="Choose which identifier to use for subject folder names (sub-*) in the export. The 'Export Name' column below reflects your choice."
        >
          <Select
            label="Subject identifier for export"
            value={String(subjectIdentifierSource)}
            data={subjectIdentifierOptions}
            onChange={(value) => {
              const parsed = value === 'subject_code' ? 'subject_code' : Number(value);
              onSubjectIdentifierChange(parsed);
            }}
          />
        </SectionCard>
      )}

      {/* DataTables preview */}
      {result.total_stacks > 0 && (
        <SectionCard
          title="Resolved Entries"
          description={`${result.total_stacks} stacks across ${result.total_subjects} subjects -- rows colored by subject`}
          collapsible
          defaultExpanded
        >
          <div ref={tableHostRef} style={{ width: '100%' }} />
        </SectionCard>
      )}

      {result.total_stacks > 0 && (
        <Group gap="xs">
          <IconCheck size={14} color="var(--nils-stage-completed)" />
          <Text size="sm" c="var(--nils-text-secondary)">
            Manifest resolved successfully. Configure export options below.
          </Text>
        </Group>
      )}
    </Stack>
  );
};
