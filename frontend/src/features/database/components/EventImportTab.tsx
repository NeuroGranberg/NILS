import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  FileButton,
  Group,
  Loader,
  ScrollArea,
  Select,
  SimpleGrid,
  Stack,
  Table,
  Text,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { IconTrash, IconUpload } from '@tabler/icons-react';

import {
  type EventImportFieldsResponse,
  type EventImportFieldDefinition,
  type EventImportPayload,
  type EventImportPreview,
  type SubjectImportFieldMapping,
  useEventImportFields,
  useEventImportPreview,
  useEventImportApply,
} from '../api';
import { apiClient } from '../../../utils/api-client';

type FieldMappingState = {
  column?: string;
  defaultValue?: string;
  parser?: string;
};

type UploadedCsvInfo = {
  token: string;
  filename: string;
  columns: string[];
};

const buildColumnsOptions = (columns: string[] | undefined) =>
  (columns ?? []).map((c) => ({ value: c, label: c }));

const buildFieldMappingPayload = (
  field: EventImportFieldDefinition,
  state: FieldMappingState | undefined,
): SubjectImportFieldMapping | undefined => {
  if (!state) return undefined;
  const payload: SubjectImportFieldMapping = {};
  if (state.column) payload.column = state.column;
  if (state.defaultValue) payload.default = state.defaultValue;
  if (state.parser) payload.parser = state.parser;
  if (!payload.column && (payload.default === undefined || payload.default === '')) return undefined;
  if (!payload.parser && field.defaultParser) payload.parser = field.defaultParser;
  return payload;
};

const EventImportTab = () => {
  const fieldsQuery = useEventImportFields();
  const previewMutation = useEventImportPreview();
  const applyMutation = useEventImportApply();

  const [selectedObsTypeId, setSelectedObsTypeId] = useState<string | null>(null);
  const [selectedIdType, setSelectedIdType] = useState<string>('subject_code');
  const [csvInfo, setCsvInfo] = useState<UploadedCsvInfo | null>(null);
  const [uploading, setUploading] = useState(false);
  const [mappings, setMappings] = useState<Record<string, FieldMappingState>>({});
  const [preview, setPreview] = useState<EventImportPreview | null>(null);

  const data = fieldsQuery.data as EventImportFieldsResponse | undefined;
  const obsTypes = useMemo(() => data?.observationTypes ?? [], [data]);
  const idTypes = useMemo(() => data?.idTypes ?? [], [data]);
  const eventFields = useMemo(() => data?.eventFields ?? [], [data]);
  const measureFields = useMemo(() => data?.measureFields ?? [], [data]);

  const selectedObsType = useMemo(
    () => obsTypes.find((o) => String(o.id) === selectedObsTypeId) ?? null,
    [obsTypes, selectedObsTypeId],
  );

  const needsValue = selectedObsType?.valueType != null && selectedObsType.valueType !== '';

  const activeFields = useMemo(() => {
    if (!needsValue) return eventFields;
    return [...eventFields, ...measureFields];
  }, [eventFields, measureFields, needsValue]);

  const obsTypeOptions = useMemo(() => {
    const groups = new Map<string, { value: string; label: string }[]>();
    obsTypes.forEach((o) => {
      const items = groups.get(o.category) ?? [];
      items.push({ value: String(o.id), label: o.name });
      groups.set(o.category, items);
    });
    return Array.from(groups.entries()).map(([group, items]) => ({ group, items }));
  }, [obsTypes]);

  const idTypeOptions = useMemo(() => {
    const options = [{ value: 'subject_code', label: 'Subject Code' }];
    idTypes.forEach((it) => options.push({ value: String(it.id), label: it.name }));
    return options;
  }, [idTypes]);

  useEffect(() => {
    setPreview(null);
    if (!activeFields.length || !csvInfo?.columns) {
      setMappings({});
      return;
    }
    const cols = csvInfo.columns;
    const next: Record<string, FieldMappingState> = {};
    activeFields.forEach((f) => {
      const auto = cols.find((c) => c.toLowerCase() === f.name.toLowerCase());
      next[f.name] = { parser: f.defaultParser, column: auto };
    });
    setMappings(next);
  }, [activeFields, csvInfo?.columns, selectedObsTypeId]);

  const columnOptions = useMemo(() => buildColumnsOptions(csvInfo?.columns), [csvInfo?.columns]);

  const handleUpload = async (file: File | null) => {
    if (!file) return;
    const form = new FormData();
    form.append('file', file);
    try {
      setUploading(true);
      const response = await apiClient.postForm<UploadedCsvInfo>('/uploads/csv', form);
      setCsvInfo(response);
      setMappings({});
      setPreview(null);
      notifications.show({ color: 'teal', message: `Uploaded ${response.filename}` });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to upload CSV';
      notifications.show({ color: 'red', message });
    } finally {
      setUploading(false);
    }
  };

  const handleClearCsv = () => {
    setCsvInfo(null);
    setMappings({});
    setPreview(null);
    notifications.show({ color: 'gray', message: 'CSV removed.' });
  };

  const updateMapping = (fieldName: string, updates: Partial<FieldMappingState>) => {
    setMappings((current) => ({ ...current, [fieldName]: { ...current[fieldName], ...updates } }));
  };

  const buildPayload = useCallback(
    (dryRun: boolean): EventImportPayload | null => {
      if (!csvInfo?.token) {
        notifications.show({ color: 'yellow', message: 'Upload a CSV file first.' });
        return null;
      }
      if (!selectedObsTypeId) {
        notifications.show({ color: 'yellow', message: 'Select an observation type.' });
        return null;
      }

      const fieldPayload: Record<string, SubjectImportFieldMapping> = {};
      const missing: string[] = [];
      activeFields.forEach((f) => {
        const mp = buildFieldMappingPayload(f, mappings[f.name]);
        if (mp) {
          fieldPayload[f.name] = mp;
        } else if (f.required) {
          missing.push(f.label);
        }
      });

      if (missing.length) {
        notifications.show({ color: 'red', message: `Map required fields: ${missing.join(', ')}` });
        return null;
      }

      return {
        fileToken: csvInfo.token,
        observationTypeId: Number(selectedObsTypeId),
        subjectIdentifierType: selectedIdType,
        fields: fieldPayload,
        options: { skipBlankUpdates: true },
        dryRun,
      };
    },
    [csvInfo, selectedObsTypeId, selectedIdType, activeFields, mappings],
  );

  const handlePreview = async () => {
    const payload = buildPayload(false);
    if (!payload) return;
    try {
      const result = await previewMutation.mutateAsync(payload);
      setPreview(result);
      notifications.show({ color: 'teal', message: 'Preview generated.' });
    } catch {
      // error handled by mutation hook
    }
  };

  const handleApply = async (dryRun: boolean) => {
    const payload = buildPayload(dryRun);
    if (!payload) return;
    try {
      await applyMutation.mutateAsync(payload);
      if (!dryRun) setPreview(null);
    } catch {
      // error handled by mutation hook
    }
  };

  const isBusy = uploading || previewMutation.isPending || applyMutation.isPending;
  const canRun = Boolean(csvInfo?.token) && Boolean(selectedObsTypeId);

  const obsTypeInfoLabel = useMemo(() => {
    if (!selectedObsType) return null;
    const parts = [selectedObsType.name];
    if (selectedObsType.valueType) {
      let detail = selectedObsType.valueType;
      if (selectedObsType.minValue != null || selectedObsType.maxValue != null) {
        const lo = selectedObsType.minValue != null ? String(selectedObsType.minValue) : '...';
        const hi = selectedObsType.maxValue != null ? String(selectedObsType.maxValue) : '...';
        detail += `, ${lo} - ${hi}`;
      }
      if (selectedObsType.unit) detail += ` ${selectedObsType.unit}`;
      parts.push(detail);
    } else {
      parts.push('date-only (no value)');
    }
    return parts.join(' — ');
  }, [selectedObsType]);

  return (
    <Stack gap="md">
      {fieldsQuery.isLoading ? (
        <Group gap="xs">
          <Loader size="sm" />
          <Text size="sm">Loading event import configuration...</Text>
        </Group>
      ) : null}

      {fieldsQuery.isError ? (
        <Alert color="red" title="Failed to load event import configuration">
          {(fieldsQuery.error as Error)?.message ?? 'Unable to load import configuration.'}
        </Alert>
      ) : null}

      {data ? (
        <Stack gap="md">
          <Stack gap={4}>
            <Text fw={600} size="sm">Event CSV Import</Text>
            <Text size="xs" c="dimmed">
              Select an observation type and subject identifier type, upload a CSV, map columns, then preview and submit.
            </Text>
          </Stack>

          <SimpleGrid cols={{ base: 1, sm: 2 }} spacing="sm">
            <Select
              label="Observation Type"
              placeholder="Select observation type"
              data={obsTypeOptions}
              value={selectedObsTypeId}
              onChange={(value) => setSelectedObsTypeId(value)}
              searchable
              withAsterisk
            />
            <Select
              label="Subject Identifier"
              placeholder="Select identifier type"
              data={idTypeOptions}
              value={selectedIdType}
              onChange={(value) => setSelectedIdType(value ?? 'subject_code')}
            />
          </SimpleGrid>

          {obsTypeInfoLabel ? (
            <Badge variant="light" color="blue" size="lg">
              {obsTypeInfoLabel}
            </Badge>
          ) : null}

          <Group justify="space-between" align="center" wrap="wrap">
            <Group gap="sm">
              <FileButton onChange={handleUpload} accept=".csv">
                {(props) => (
                  <Button leftSection={<IconUpload size={16} />} loading={uploading} {...props}>
                    Upload CSV
                  </Button>
                )}
              </FileButton>
              {csvInfo?.columns ? (
                <Text size="xs" c="dimmed">
                  Columns: {csvInfo.columns.join(', ')}
                </Text>
              ) : null}
            </Group>
            <Group gap="xs">
              {csvInfo?.filename ? <Badge size="sm" color="blue">{csvInfo.filename}</Badge> : null}
              {csvInfo ? (
                <ActionIcon size="sm" variant="light" color="red" onClick={handleClearCsv} aria-label="Remove CSV">
                  <IconTrash size={16} />
                </ActionIcon>
              ) : null}
            </Group>
          </Group>

          {csvInfo && selectedObsTypeId ? (
            <Stack gap="md">
              <Text fw={600} size="sm">Column Mapping</Text>
              <SimpleGrid cols={{ base: 1, sm: 2, md: 3 }} spacing="sm">
                {activeFields.map((field) => {
                  const state = mappings[field.name] ?? {};
                  return (
                    <Stack key={field.name} gap={6}>
                      <Select
                        label={field.label}
                        placeholder="Select column"
                        data={columnOptions}
                        value={state.column ?? null}
                        onChange={(value) => updateMapping(field.name, { column: value ?? undefined })}
                        withAsterisk={field.required}
                        clearable
                      />
                      {field.parsers.length > 1 ? (
                        <Select
                          label="Parser"
                          data={field.parsers.map((p) => ({ value: p, label: p }))}
                          value={state.parser ?? field.defaultParser}
                          onChange={(value) => updateMapping(field.name, { parser: value ?? undefined })}
                          maw={140}
                        />
                      ) : null}
                    </Stack>
                  );
                })}
              </SimpleGrid>
            </Stack>
          ) : null}

          <Group gap="sm">
            <Button variant="light" onClick={handlePreview} disabled={isBusy || !canRun} loading={previewMutation.isPending}>
              Preview
            </Button>
            <Button onClick={() => handleApply(false)} disabled={isBusy || !canRun} loading={applyMutation.isPending}>
              Submit
            </Button>
            <Button variant="outline" onClick={() => handleApply(true)} disabled={isBusy || !canRun} loading={applyMutation.isPending}>
              Dry run
            </Button>
          </Group>

          {preview ? (
            <Stack gap="sm" mt="md">
              <Group justify="space-between" align="center">
                <Text fw={600}>Preview — Events</Text>
                <Group gap="xs">
                  <Badge color="gray" variant="light">{preview.totalRows} total rows</Badge>
                  <Badge color="green" variant="light">{preview.rows.filter((r) => !r.existing).length} new</Badge>
                  <Badge color="blue" variant="light">{preview.rows.filter((r) => r.existing).length} updates</Badge>
                  <Badge color="yellow" variant="light">{preview.rows.filter((r) => !r.subjectFound).length} unresolved</Badge>
                  {preview.skippedRows > 0 ? (
                    <Badge color="red" variant="light">{preview.skippedRows} skipped</Badge>
                  ) : null}
                </Group>
              </Group>

              {preview.warnings.length ? (
                <Alert color="yellow" title="Warnings">
                  <Stack gap={4}>
                    {preview.warnings.slice(0, 20).map((w, i) => (
                      <Text key={i} size="sm">{w}</Text>
                    ))}
                    {preview.warnings.length > 20 ? (
                      <Text size="sm" c="dimmed">...and {preview.warnings.length - 20} more warnings</Text>
                    ) : null}
                  </Stack>
                </Alert>
              ) : null}

              <ScrollArea h={300} offsetScrollbars>
                <Table striped highlightOnHover horizontalSpacing="md" verticalSpacing="xs">
                  <Table.Thead>
                    <Table.Tr>
                      <Table.Th>Action</Table.Th>
                      <Table.Th>Subject ID</Table.Th>
                      <Table.Th>Resolved Code</Table.Th>
                      <Table.Th>Date</Table.Th>
                      <Table.Th>Time</Table.Th>
                      {needsValue ? <Table.Th>Value</Table.Th> : null}
                      <Table.Th>Notes</Table.Th>
                    </Table.Tr>
                  </Table.Thead>
                  <Table.Tbody>
                    {preview.rows.map((row, index) => (
                      <Table.Tr key={index}>
                        <Table.Td>
                          {!row.subjectFound ? (
                            <Badge color="red" variant="light">Not Found</Badge>
                          ) : row.existing ? (
                            <Badge color="blue" variant="light">Update</Badge>
                          ) : (
                            <Badge color="green" variant="light">Create</Badge>
                          )}
                        </Table.Td>
                        <Table.Td>{row.subjectIdentifier}</Table.Td>
                        <Table.Td>{row.resolvedSubjectCode ?? '-'}</Table.Td>
                        <Table.Td>{row.eventDate}</Table.Td>
                        <Table.Td>{row.eventTime ?? '-'}</Table.Td>
                        {needsValue ? <Table.Td>{row.value != null ? String(row.value) : '-'}</Table.Td> : null}
                        <Table.Td>{row.notes ?? '-'}</Table.Td>
                      </Table.Tr>
                    ))}
                  </Table.Tbody>
                </Table>
              </ScrollArea>
            </Stack>
          ) : null}
        </Stack>
      ) : null}
    </Stack>
  );
};

export default EventImportTab;
