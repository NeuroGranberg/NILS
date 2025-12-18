import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  FileButton,
  Group,
  Loader,
  Select,
  SimpleGrid,
  Stack,
  Table,
  Text,
  TextInput,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { IconTrash, IconUpload } from '@tabler/icons-react';

import {
  type CohortImportFieldDefinition,
  type CohortImportPayload,
  type CohortImportPreview,
  type CohortDetail,
  useCohortImportApply,
  useCohortImportFields,
  useCohortImportPreview,
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
  field: CohortImportFieldDefinition,
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
  field: CohortImportFieldDefinition,
  state: FieldMappingState | undefined,
  manualFallback?: string,
) => {
  if (!state) return undefined;
  const payload: { column?: string; default?: string; parser?: string } = {};
  if (state.column) payload.column = state.column;
  if (state.defaultValue) payload.default = state.defaultValue;
  if (!state.column && manualFallback) {
    payload.default = manualFallback;
  }
  if (state.parser) payload.parser = state.parser;
  if (!payload.column && (payload.default === undefined || payload.default === '')) {
    if (field.required) {
      return undefined;
    }
    return undefined;
  }
  if (!payload.parser && field.defaultParser) {
    payload.parser = field.defaultParser;
  }
  return payload;
};

const INITIAL_MANUAL_VALUES: Record<string, string> = {};

export const CohortImportTab = () => {
  const fieldsQuery = useCohortImportFields();
  const previewMutation = useCohortImportPreview();
  const applyMutation = useCohortImportApply();

  const [csvInfo, setCsvInfo] = useState<UploadedCsvInfo | null>(null);
  const [uploading, setUploading] = useState(false);
  const [mappings, setMappings] = useState<Record<string, FieldMappingState>>({});
  const [manualValues, setManualValues] = useState<Record<string, string>>(INITIAL_MANUAL_VALUES);
  const [preview, setPreview] = useState<CohortImportPreview | null>(null);
  const [manualLookupLoading, setManualLookupLoading] = useState(false);

  const fields = useMemo(() => fieldsQuery.data?.cohortFields ?? [], [fieldsQuery.data]);
  const manualFields = useMemo(
    () => fields.filter((definition) => definition.name !== 'cohort_id'),
    [fields],
  );
  const isManualEntry = !csvInfo?.token;
  const manualName = (manualValues.name ?? '').trim();
  const normalizedManualName = manualName.toLowerCase();
  const manualNameValid = normalizedManualName.length > 0;
  const missingManualRequired = useMemo(() => {
    if (!isManualEntry) return [] as string[];
    return manualFields
      .filter((definition) => definition.required)
      .filter((definition) => {
        const value = (manualValues[definition.name] ?? '').trim();
        return value.length === 0;
      })
      .map((definition) => definition.label);
  }, [isManualEntry, manualFields, manualValues]);
  const hasManualData = isManualEntry && missingManualRequired.length === 0 && manualNameValid;
  const hasCsvData = Boolean(csvInfo?.token);
  const canRun = hasCsvData || hasManualData;

  useEffect(() => {
    if (!manualFields.length) return;
    setManualValues((current) => {
      const next = { ...current };
      let changed = false;
      manualFields.forEach((definition) => {
        if (!(definition.name in next)) {
          next[definition.name] = '';
          changed = true;
        }
      });
      return changed ? next : current;
    });
  }, [manualFields]);

  useEffect(() => {
    if (!fields.length) return;
    setMappings((current) => {
      const next = { ...current };
      fields.forEach((definition) => {
        next[definition.name] = normalizeFieldState(definition, next[definition.name], csvInfo?.columns, manualValues);
      });
      return next;
    });
  }, [fields, csvInfo?.columns, manualValues]);

  const columnOptions = useMemo(() => buildColumnsOptions(csvInfo?.columns), [csvInfo?.columns]);

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
    notifications.show({ color: 'gray', message: 'Cohort CSV removed.' });
  };

  const updateMapping = (fieldName: string, updates: Partial<FieldMappingState>) => {
    setMappings((current) => ({ ...current, [fieldName]: { ...current[fieldName], ...updates } }));
  };

  const updateManualValue = (fieldName: string, value: string) => {
    const normalizedValue = fieldName === 'name' ? value.toLowerCase() : value;
    setManualValues((current) => ({ ...current, [fieldName]: normalizedValue }));
    setPreview(null);
  };

  const lookupExistingCohort = useCallback(
    async (options?: { silent?: boolean }) => {
      const silent = options?.silent ?? false;
      const trimmed = (manualValues.name ?? '').trim();
      if (!trimmed) {
        if (!silent) {
          notifications.show({ color: 'yellow', message: 'Enter a cohort name to load existing data.' });
        }
        return false;
      }
      const normalized = trimmed.toLowerCase();
      try {
        setManualLookupLoading(true);
        const detail = await apiClient.get<CohortDetail>(
          `/metadata/cohorts/by-name/${encodeURIComponent(normalized)}`,
        );
        setManualValues({
          name: detail.name?.toLowerCase() ?? normalized,
          owner: detail.owner ?? '',
          path: detail.path ?? '',
          description: detail.description ?? '',
        });
        setPreview(null);
        if (!silent) {
          notifications.show({ color: 'teal', message: `Loaded existing cohort “${detail.name}”.` });
        }
        return true;
      } catch (error) {
        if (error instanceof ApiError && error.status === 404) {
          setPreview(null);
          if (!silent) {
            notifications.show({
              color: 'blue',
              message: `Cohort “${normalized}” not found. A new record will be created.`,
            });
          }
          return false;
        }
        if (!silent) {
          const message = error instanceof Error ? error.message : 'Failed to load cohort data.';
          notifications.show({ color: 'red', message });
        }
        return false;
      } finally {
        setManualLookupLoading(false);
      }
    },
    [manualValues.name],
  );

  const prepareManualCohortCsv = useCallback(async () => {
    if (!manualFields.length) {
      throw new Error('Field metadata is still loading.');
    }
    const normalizedName = (manualValues.name ?? '').trim().toLowerCase();
    if (!normalizedName) {
      throw new Error('Cohort name is required.');
    }
    const missingLabels = manualFields
      .filter((definition) => definition.required)
      .filter((definition) => (manualValues[definition.name] ?? '').trim().length === 0)
      .map((definition) => definition.label);
    if (missingLabels.length) {
      throw new Error(`Provide values for required fields: ${missingLabels.join(', ')}`);
    }
    const headers = manualFields.map((definition) => definition.name);
    const row = manualFields.map((definition) => {
      const raw = (manualValues[definition.name] ?? '').trim();
      const value = definition.name === 'name' ? raw.toLowerCase() : raw;
      return escapeCsvValue(value);
    });
    const csvContent = `${headers.join(',')}
${row.join(',')}
`;
    const file = new File([csvContent], 'manual-cohort-entry.csv', { type: 'text/csv' });
    const form = new FormData();
    form.append('file', file);
    return apiClient.postForm<UploadedCsvInfo>('/uploads/csv', form);
  }, [manualFields, manualValues]);

  const buildPayload = async (dryRun: boolean): Promise<CohortImportPayload | null> => {
    if (!fields.length) {
      notifications.show({ color: 'yellow', message: 'Field metadata is still loading.' });
      return null;
    }

    if (isManualEntry) {
      if (!manualNameValid) {
        notifications.show({ color: 'red', message: 'Cohort name is required before submitting.' });
        return null;
      }
      if (missingManualRequired.length) {
        notifications.show({ color: 'red', message: `Provide required cohort fields: ${missingManualRequired.join(', ')}` });
        return null;
      }
      try {
        const csv = await prepareManualCohortCsv();
        const cohortFields: Record<string, { column: string; parser: string }> = {};
        manualFields.forEach((definition) => {
          cohortFields[definition.name] = {
            column: definition.name,
            parser: definition.defaultParser,
          };
        });
        const payload: CohortImportPayload = {
          fileToken: csv.token,
          cohortFields,
          options: { skipBlankUpdates: true },
        };
        if (dryRun) {
          payload.dryRun = true;
        }
        return payload;
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Failed to prepare manual cohort entry.';
        notifications.show({ color: 'red', message });
        return null;
      }
    }

    if (!csvInfo?.token) {
      notifications.show({ color: 'yellow', message: 'Upload a CSV file for the Cohort tab.' });
      return null;
    }

    const cohortFields: Record<string, { column?: string; default?: string; parser?: string }> = {};
    const missing: string[] = [];
    fields.forEach((definition) => {
      const state = mappings[definition.name];
      const manualValue = state?.manualValue ?? manualValues[definition.name];
      const mappingPayload = buildFieldMappingPayload(definition, state, manualValue);
      if (mappingPayload) {
        cohortFields[definition.name] = mappingPayload;
      } else if (definition.required) {
        missing.push(definition.label);
      }
    });

    if (missing.length) {
      notifications.show({ color: 'red', message: `Map required cohort fields: ${missing.join(', ')}` });
      return null;
    }

    const payload: CohortImportPayload = {
      fileToken: csvInfo.token,
      cohortFields,
      options: { skipBlankUpdates: true },
    };
    if (dryRun) {
      payload.dryRun = true;
    }
    return payload;
  };

  const handlePreview = async () => {
    const payload = await buildPayload(false);
    if (!payload) return;
    try {
      const result = await previewMutation.mutateAsync(payload);
      setPreview(result);
      notifications.show({ color: 'teal', message: 'Cohort preview generated.' });
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
      const summary = `Cohort submission: inserted ${result.cohortsInserted} cohorts, updated ${result.cohortsUpdated}.`;
      notifications.show({ color: dryRun ? 'blue' : 'teal', message: summary });
      if (!dryRun) {
        setPreview(null);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Submission failed';
      notifications.show({ color: 'red', message });
    }
  };

  const isBusy = uploading || previewMutation.isPending || applyMutation.isPending || manualLookupLoading;

  return (
    <Stack gap="md">
      {fieldsQuery.isLoading ? (
        <Group gap="xs">
          <Loader size="sm" />
          <Text size="sm">Loading cohort field definitions…</Text>
        </Group>
      ) : null}

      {fieldsQuery.isError ? (
        <Alert color="red" title="Failed to load cohort metadata">
          {(fieldsQuery.error as Error)?.message ?? 'Unable to load cohort import configuration.'}
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
                <ActionIcon size="sm" variant="light" color="red" onClick={handleClearCsv} aria-label="Remove cohort CSV">
                  <IconTrash size={16} />
                </ActionIcon>
              ) : null}
            </Group>
          </Group>

          <Stack gap={4}>
            <Text fw={600} size="sm">
              Cohort Fields
            </Text>
            <Text size="xs" c="dimmed">
              Provide a cohort name (lowercase) and optional metadata to create or update a cohort. Uploading a CSV switches fields to column mapping.
            </Text>
          </Stack>

          <SimpleGrid cols={{ base: 1, sm: 2, md: 3 }} spacing="sm">
              {(isManualEntry ? manualFields : fields).map((definition) => {
                const state = mappings[definition.name] ?? {};
                const manualValue = state.manualValue ?? manualValues[definition.name] ?? '';
                if (isManualEntry) {
                  const isNameField = definition.name === 'name';
                  return (
                    <TextInput
                      key={definition.name}
                      label={definition.label}
                      placeholder={definition.required ? 'Required' : 'Optional'}
                      value={manualValue}
                      withAsterisk={definition.required}
                      onChange={(event) => {
                        const value = event.currentTarget.value;
                        updateMapping(definition.name, { manualValue: value });
                        updateManualValue(definition.name, value);
                      }}
                      onBlur={() => {
                        if (isNameField) {
                          void lookupExistingCohort({ silent: true });
                        }
                      }}
                      onKeyDown={(event) => {
                        if (isNameField && event.key === 'Enter') {
                          event.preventDefault();
                          void lookupExistingCohort();
                        }
                      }}
                    />
                  );
                }
                return (
                  <Stack gap={6} key={definition.name}>
                    <Select
                      label={definition.label}
                      placeholder="Select column"
                      data={columnOptions}
                      value={state.column ?? null}
                      onChange={(value) =>
                        updateMapping(definition.name, {
                          column: value ?? undefined,
                        })
                      }
                      clearable
                    />
                  </Stack>
                );
              })}
            </SimpleGrid>

          <Group gap="sm">
            <Button disabled={isBusy || !canRun} onClick={() => handlePreview()}>
              Preview
            </Button>
            <Button color="teal" disabled={isBusy || !canRun} onClick={() => handleApply(false)}>
              Submit
            </Button>
            <Button variant="light" color="gray" disabled={isBusy || !canRun} onClick={() => handleApply(true)}>
              Dry Run
            </Button>
          </Group>

          {preview ? (
            <Stack gap="sm">
              <Text fw={600} size="sm">
                Preview — Cohort
              </Text>
              <Table striped highlightOnHover captionSide="top">
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th>Status</Table.Th>
                    {fields.map((definition) => (
                      <Table.Th key={definition.name}>{definition.label}</Table.Th>
                    ))}
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {preview.rows.map((row, index) => (
                    <Table.Tr key={`cohort-preview-${index}`}>
                      <Table.Td>
                        {row.existing ? (
                          <Badge color="yellow">Update</Badge>
                        ) : (
                          <Badge color="green">Create</Badge>
                        )}
                      </Table.Td>
                      {fields.map((definition) => {
                        const value = row.cohort[definition.name];
                        return <Table.Td key={`${index}-${definition.name}`}>{value === null ? '' : String(value ?? '')}</Table.Td>;
                      })}
                    </Table.Tr>
                  ))}
                </Table.Tbody>
              </Table>
              {preview.warnings.length ? (
                <Alert color="yellow" title="Warnings">
                  <Stack gap={4}>
                    {preview.warnings.map((warning, idx) => (
                      <Text key={`cohort-warning-${idx}`} size="sm">
                        {warning}
                      </Text>
                    ))}
                  </Stack>
                </Alert>
              ) : null}
            </Stack>
          ) : null}
        </Stack>
      ) : null}
    </Stack>
  );
};

export default CohortImportTab;
