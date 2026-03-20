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
  Switch,
  Table,
  Text,
  TextInput,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { IconTrash, IconUpload } from '@tabler/icons-react';

import {
  type ObservationTypeImportFieldDefinition,
  type ObservationTypeImportPayload,
  type ObservationTypeImportPreview,
  type ObservationTypeDetail,
  type SubjectImportFieldMapping,
  useObservationTypeImportFields,
  useObservationTypeImportPreview,
  useObservationTypeImportApply,
} from '../api';
import { apiClient, ApiError } from '../../../utils/api-client';

type FieldMappingState = {
  column?: string;
  defaultValue?: string;
  parser?: string;
  manualValue?: string;
};

type UploadedCsvInfo = {
  token: string;
  filename: string;
  columns: string[];
};

const escapeCsvValue = (value: string | number | boolean | null | undefined): string => {
  if (value === null || value === undefined) return '';
  const stringValue = String(value);
  if (stringValue.includes('"') || stringValue.includes(',') || stringValue.includes('\n')) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }
  return stringValue;
};

const normalizeFieldState = (
  field: ObservationTypeImportFieldDefinition,
  existing: FieldMappingState | undefined,
  columns: string[] | undefined,
  manualValues: Record<string, string>,
): FieldMappingState => {
  const next: FieldMappingState = existing ? { ...existing } : {};
  if (!next.parser) {
    next.parser = field.defaultParser;
  }
  if (!next.column && columns && columns.length) {
    const auto = columns.find((column) => column.toLowerCase() === field.name.toLowerCase());
    if (auto) {
      next.column = auto;
    }
  }
  if (!next.manualValue && manualValues[field.name] !== undefined) {
    next.manualValue = manualValues[field.name];
  }
  return next;
};

const buildColumnsOptions = (columns: string[] | undefined) =>
  (columns ?? []).map((column) => ({ value: column, label: column }));

const buildFieldMappingPayload = (
  field: ObservationTypeImportFieldDefinition,
  state: FieldMappingState | undefined,
  manualFallback?: string,
): SubjectImportFieldMapping | undefined => {
  if (!state) return undefined;
  const payload: SubjectImportFieldMapping = {};
  if (state.column) payload.column = state.column;
  if (state.defaultValue) payload.default = state.defaultValue;
  if (!state.column && manualFallback) {
    payload.default = manualFallback;
  }
  if (state.parser) payload.parser = state.parser;
  if (!payload.column && (payload.default === undefined || payload.default === '')) {
    return undefined;
  }
  if (!payload.parser && field.defaultParser) {
    payload.parser = field.defaultParser;
  }
  return payload;
};

const VALUE_TYPE_OPTIONS = [
  { value: '', label: '(none)' },
  { value: 'Numeric', label: 'Numeric' },
  { value: 'Text', label: 'Text' },
  { value: 'Boolean', label: 'Boolean' },
  { value: 'JSON', label: 'JSON' },
];

const ObservationTypeImportTab = () => {
  const fieldsQuery = useObservationTypeImportFields();
  const previewMutation = useObservationTypeImportPreview();
  const applyMutation = useObservationTypeImportApply();

  const [csvInfo, setCsvInfo] = useState<UploadedCsvInfo | null>(null);
  const [uploading, setUploading] = useState(false);
  const [mappings, setMappings] = useState<Record<string, FieldMappingState>>({});
  const [manualValues, setManualValues] = useState<Record<string, string>>({});
  const [manualActive, setManualActive] = useState(true);
  const [manualPrimary, setManualPrimary] = useState(false);
  const [preview, setPreview] = useState<ObservationTypeImportPreview | null>(null);
  const [lookupLoading, setLookupLoading] = useState(false);

  const fields = useMemo(() => fieldsQuery.data?.observationTypeFields ?? [], [fieldsQuery.data]);
  const editableFields = useMemo(
    () => fields.filter((f) => f.name !== 'is_active' && f.name !== 'is_primary'),
    [fields],
  );
  const isManualEntry = !csvInfo?.token;
  const manualId = (manualValues.observation_type_id ?? '').trim();
  const manualCategory = (manualValues.category ?? '').trim();
  const manualName = (manualValues.name ?? '').trim();
  const hasManualData = manualCategory.length > 0 && manualName.length > 0;

  useEffect(() => {
    if (!fields.length) return;
    setManualValues((current) => {
      const next = { ...current };
      let changed = false;
      fields.forEach((f) => {
        if (f.name === 'is_active' || f.name === 'is_primary') return;
        if (!(f.name in next)) {
          next[f.name] = '';
          changed = true;
        }
      });
      return changed ? next : current;
    });
  }, [fields]);

  useEffect(() => {
    if (!fields.length) return;
    setMappings((current) => {
      const next = { ...current };
      fields.forEach((f) => {
        next[f.name] = normalizeFieldState(f, next[f.name], csvInfo?.columns, manualValues);
      });
      return next;
    });
  }, [fields, csvInfo?.columns, manualValues]);

  const columnOptions = useMemo(() => buildColumnsOptions(csvInfo?.columns), [csvInfo?.columns]);

  const mapDetailToManual = useCallback(
    (id: string, detail: ObservationTypeDetail | null) => {
      setManualValues({
        observation_type_id: id,
        category: detail?.category ?? '',
        name: detail?.name ?? '',
        description: detail?.description ?? '',
        unit: detail?.unit ?? '',
        value_type: detail?.valueType ?? '',
        min_value: detail?.minValue != null ? String(detail.minValue) : '',
        max_value: detail?.maxValue != null ? String(detail.maxValue) : '',
      });
      setManualActive(detail?.isActive ?? true);
      setManualPrimary(detail?.isPrimary ?? false);
      setPreview(null);
    },
    [],
  );

  const lookupExisting = useCallback(
    async (options?: { silent?: boolean }) => {
      const silent = options?.silent ?? false;
      if (!manualId) {
        if (!silent) {
          notifications.show({ color: 'yellow', message: 'Enter an observation type ID to load existing data.' });
        }
        return;
      }
      try {
        setLookupLoading(true);
        const detail = await apiClient.get<ObservationTypeDetail>(
          `/metadata/imports/observation-types/${encodeURIComponent(manualId)}`,
        );
        mapDetailToManual(manualId, detail);
        if (!silent) {
          notifications.show({ color: 'teal', message: `Loaded observation type ${manualId}.` });
        }
      } catch (error) {
        if (error instanceof ApiError && error.status === 404) {
          mapDetailToManual(manualId, null);
          if (!silent) {
            notifications.show({ color: 'blue', message: `ID ${manualId} not found. A new record will be created.` });
          }
        } else if (!silent) {
          const message = error instanceof Error ? error.message : 'Failed to load observation type.';
          notifications.show({ color: 'red', message });
        }
      } finally {
        setLookupLoading(false);
      }
    },
    [manualId, mapDetailToManual],
  );

  const handleUpload = async (file: File | null) => {
    if (!file) return;
    const form = new FormData();
    form.append('file', file);
    try {
      setUploading(true);
      const response = await apiClient.postForm<UploadedCsvInfo>('/uploads/csv', form);
      setCsvInfo(response);
      notifications.show({ color: 'teal', message: `Uploaded ${response.filename}` });
      setPreview(null);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to upload CSV';
      notifications.show({ color: 'red', message });
    } finally {
      setUploading(false);
    }
  };

  const handleClearCsv = () => {
    setCsvInfo(null);
    setPreview(null);
    setMappings({});
    notifications.show({ color: 'gray', message: 'CSV removed.' });
  };

  const updateMapping = (fieldName: string, updates: Partial<FieldMappingState>) => {
    setMappings((current) => ({ ...current, [fieldName]: { ...current[fieldName], ...updates } }));
  };

  const updateManualValue = (fieldName: string, value: string) => {
    setManualValues((current) => ({ ...current, [fieldName]: value }));
    setPreview(null);
  };

  const prepareManualCsv = useCallback(async () => {
    if (!fields.length) throw new Error('Field metadata is still loading.');
    const headers = fields.map((f) => f.name);
    const row = fields.map((f) => {
      if (f.name === 'is_active') return escapeCsvValue(manualActive ? 1 : 0);
      if (f.name === 'is_primary') return escapeCsvValue(manualPrimary ? 1 : 0);
      return escapeCsvValue(manualValues[f.name] ?? '');
    });
    const csvContent = `${headers.join(',')}\n${row.join(',')}\n`;
    const file = new File([csvContent], 'manual-observation-type.csv', { type: 'text/csv' });
    const form = new FormData();
    form.append('file', file);
    return apiClient.postForm<UploadedCsvInfo>('/uploads/csv', form);
  }, [fields, manualActive, manualPrimary, manualValues]);

  const buildPayload = async (dryRun: boolean): Promise<ObservationTypeImportPayload | null> => {
    if (!fields.length) {
      notifications.show({ color: 'yellow', message: 'Field metadata is still loading.' });
      return null;
    }

    if (isManualEntry) {
      if (!hasManualData) {
        notifications.show({ color: 'red', message: 'Category and Name are required.' });
        return null;
      }
      try {
        const csv = await prepareManualCsv();
        const fieldPayload: Record<string, SubjectImportFieldMapping> = {};
        fields.forEach((f) => {
          fieldPayload[f.name] = {
            column: f.name,
            ...(f.defaultParser !== 'string' ? { parser: f.defaultParser } : {}),
          };
        });
        const payload: ObservationTypeImportPayload = {
          fileToken: csv.token,
          fields: fieldPayload,
          options: { skipBlankUpdates: true },
        };
        if (dryRun) payload.dryRun = true;
        return payload;
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Failed to prepare manual entry.';
        notifications.show({ color: 'red', message });
        return null;
      }
    }

    if (!csvInfo?.token) {
      notifications.show({ color: 'yellow', message: 'Upload a CSV file.' });
      return null;
    }

    const fieldPayload: Record<string, SubjectImportFieldMapping> = {};
    const missing: string[] = [];
    fields.forEach((f) => {
      const state = mappings[f.name];
      const manualValue = state?.manualValue ?? manualValues[f.name];
      const mp = buildFieldMappingPayload(f, state, manualValue);
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

    const payload: ObservationTypeImportPayload = {
      fileToken: csvInfo.token,
      fields: fieldPayload,
      options: { skipBlankUpdates: true },
    };
    if (dryRun) payload.dryRun = true;
    return payload;
  };

  const handlePreview = async () => {
    const payload = await buildPayload(false);
    if (!payload) return;
    try {
      const result = await previewMutation.mutateAsync(payload);
      setPreview(result);
      notifications.show({ color: 'teal', message: 'Preview generated.' });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Preview failed';
      notifications.show({ color: 'red', message });
    }
  };

  const handleApply = async (dryRun: boolean) => {
    const payload = await buildPayload(dryRun);
    if (!payload) return;
    try {
      const result = await applyMutation.mutateAsync(payload);
      const summary = `Observation types: inserted ${result.typesInserted}, updated ${result.typesUpdated}.`;
      notifications.show({ color: dryRun ? 'blue' : 'teal', message: dryRun ? `Dry run: ${summary}` : summary });
      if (!dryRun) {
        setPreview(null);
        if (isManualEntry && manualId) {
          void lookupExisting({ silent: true });
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Submission failed';
      notifications.show({ color: 'red', message });
    }
  };

  const isBusy = uploading || previewMutation.isPending || applyMutation.isPending || lookupLoading;
  const canRun = isManualEntry ? hasManualData : Boolean(csvInfo?.token);

  return (
    <Stack gap="md">
      {fieldsQuery.isLoading ? (
        <Group gap="xs">
          <Loader size="sm" />
          <Text size="sm">Loading field definitions...</Text>
        </Group>
      ) : null}

      {fieldsQuery.isError ? (
        <Alert color="red" title="Failed to load observation type metadata">
          {(fieldsQuery.error as Error)?.message ?? 'Unable to load import configuration.'}
        </Alert>
      ) : null}

      {fields.length ? (
        <Stack gap="md">
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

          <Stack gap={4}>
            <Text fw={600} size="sm">
              Observation Type Fields
            </Text>
            <Text size="xs" c="dimmed">
              Enter an ID to load an existing type for editing, or fill in Category and Name to create new.
              Uploading a CSV switches to column mapping mode.
            </Text>
          </Stack>

          <SimpleGrid cols={{ base: 1, sm: 2, md: 3 }} spacing="sm">
            {editableFields.map((field) => {
              const state = mappings[field.name] ?? {};
              const manualValue = state.manualValue ?? manualValues[field.name] ?? '';

              if (isManualEntry) {
                if (field.name === 'value_type') {
                  return (
                    <Select
                      key={field.name}
                      label={field.label}
                      data={VALUE_TYPE_OPTIONS}
                      value={manualValue}
                      onChange={(value) => {
                        updateMapping(field.name, { manualValue: value ?? '' });
                        updateManualValue(field.name, value ?? '');
                      }}
                      clearable
                    />
                  );
                }
                return (
                  <TextInput
                    key={field.name}
                    label={field.label}
                    placeholder={field.required ? 'Required' : 'Optional'}
                    value={manualValue}
                    withAsterisk={field.required}
                    onChange={(event) => {
                      const value = event.currentTarget.value;
                      updateMapping(field.name, { manualValue: value });
                      updateManualValue(field.name, value);
                    }}
                    onBlur={() => {
                      if (field.name === 'observation_type_id') {
                        void lookupExisting();
                      }
                    }}
                    onKeyDown={(event) => {
                      if (field.name === 'observation_type_id' && event.key === 'Enter') {
                        void lookupExisting();
                      }
                    }}
                  />
                );
              }

              return (
                <Stack key={field.name} gap={6}>
                  <Select
                    label={field.label}
                    placeholder="Select column"
                    data={columnOptions}
                    value={state.column ?? null}
                    onChange={(value) => updateMapping(field.name, { column: value ?? undefined })}
                    disabled={!csvInfo}
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

            {isManualEntry ? (
              <>
                <Switch
                  label="Active"
                  checked={manualActive}
                  onChange={(event) => setManualActive(event.currentTarget.checked)}
                />
                <Switch
                  label="Primary"
                  checked={manualPrimary}
                  onChange={(event) => setManualPrimary(event.currentTarget.checked)}
                />
              </>
            ) : null}
          </SimpleGrid>

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
                <Text fw={600}>Preview — Observation Types</Text>
                <Group gap="xs">
                  <Badge color="gray" variant="light">
                    {preview.processedRows} rows
                  </Badge>
                  <Badge color="green" variant="light">
                    {preview.rows.filter((r) => !r.existing).length} new
                  </Badge>
                  <Badge color="blue" variant="light">
                    {preview.rows.filter((r) => r.existing).length} updates
                  </Badge>
                </Group>
              </Group>
              {preview.warnings.length ? (
                <Alert color="yellow" title="Warnings">
                  <Stack gap={4}>
                    {preview.warnings.map((warning, index) => (
                      <Text key={index} size="sm">{warning}</Text>
                    ))}
                  </Stack>
                </Alert>
              ) : null}
              <ScrollArea h={240} offsetScrollbars>
                <Table striped highlightOnHover horizontalSpacing="md" verticalSpacing="xs">
                  <Table.Thead>
                    <Table.Tr>
                      <Table.Th>Action</Table.Th>
                      {fields.map((f) => (
                        <Table.Th key={f.name}>{f.label}</Table.Th>
                      ))}
                    </Table.Tr>
                  </Table.Thead>
                  <Table.Tbody>
                    {preview.rows.map((row, index) => (
                      <Table.Tr key={index}>
                        <Table.Td>
                          <Badge color={row.existing ? 'blue' : 'green'} variant="light">
                            {row.existing ? 'Update' : 'Create'}
                          </Badge>
                        </Table.Td>
                        {fields.map((f) => (
                          <Table.Td key={f.name}>
                            {(() => {
                              const value = row.observationType[f.name] ?? row.existingObservationType?.[f.name] ?? '';
                              return value === null ? '' : String(value);
                            })()}
                          </Table.Td>
                        ))}
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

export default ObservationTypeImportTab;
