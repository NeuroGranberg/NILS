import { useCallback, useEffect, useMemo, useState, type Dispatch, type SetStateAction } from 'react';
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Card,
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
  type SubjectImportFieldDefinition,
  type SubjectImportFieldMapping,
  type SubjectImportPayload,
  type SubjectImportPreview,
  type SubjectDetail,
  useSubjectImportApply,
  useSubjectImportFields,
  useSubjectImportPreview,
} from '../api';
import { apiClient, ApiError } from '../../../utils/api-client';
import CohortImportTab from './CohortImportTab';
import SubjectCohortImportTab from './SubjectCohortImportTab';
import IdentifierTypeManagerTab from './IdentifierTypeManagerTab';
import SubjectOtherIdentifierImportTab from './SubjectOtherIdentifierImportTab';

type FieldMappingState = {
  column?: string;
  defaultValue?: string;
  parser?: string;
  manualValue?: string;
};

type IdentifierMappingState = {
  key: string;
  idTypeId?: string;
  column?: string;
  defaultValue?: string;
};

type UploadedCsvInfo = {
  token: string;
  filename: string;
  columns: string[];
};

type ImportTab =
  | 'subject'
  | 'cohort'
  | 'subject_cohorts'
  | 'identifiers'
  | 'subject_other_identifiers'
  | 'id_types';
type SubjectImportMode = 'subject' | 'identifiers';

const escapeCsvValue = (value: string | number | boolean | null | undefined): string => {
  if (value === null || value === undefined) return '';
  const stringValue = String(value);
  if (stringValue.includes('"') || stringValue.includes(',') || stringValue.includes('\n')) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }
  return stringValue;
};

const buildFieldMappingPayload = (
  field: SubjectImportFieldDefinition,
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

const normalizeFieldState = (
  field: SubjectImportFieldDefinition,
  existing: FieldMappingState | undefined,
  columns: string[] | undefined,
  manualValues: Record<string, string>,
): FieldMappingState => {
  const next: FieldMappingState = existing ? { ...existing } : {};
  if (!next.parser) {
    next.parser = field.defaultParser;
  }
  if (!next.column && columns && columns.length) {
    const autoColumn = columns.find((column) => column.toLowerCase() === field.name.toLowerCase());
    if (autoColumn) {
      next.column = autoColumn;
    }
  }
  if (!next.manualValue && manualValues[field.name] !== undefined) {
    next.manualValue = manualValues[field.name];
  }
  return next;
};

const buildColumnsOptions = (columns: string[] | undefined) =>
  (columns ?? []).map((column) => ({ value: column, label: column }));

const ensureFieldMapping = (
  mode: SubjectImportMode,
  definitions: SubjectImportFieldDefinition[] | undefined,
  setState: Dispatch<SetStateAction<Record<SubjectImportMode, Record<string, FieldMappingState>>>>,
  columns: string[] | undefined,
  manualValues: Record<string, string>,
) => {
  if (!definitions) return;
  setState((current) => {
    const next = { ...current };
    const existing = { ...(next[mode] ?? {}) };
    definitions.forEach((definition) => {
      existing[definition.name] = normalizeFieldState(definition, existing[definition.name], columns, manualValues);
    });
    next[mode] = existing;
    return next;
  });
};

interface SubjectImportPanelProps {
  tableName: string;
}

export const SubjectImportPanel = ({ tableName }: SubjectImportPanelProps) => {
  const fieldsQuery = useSubjectImportFields();
  const previewMutation = useSubjectImportPreview();
  const applyMutation = useSubjectImportApply();

  const [csvByMode, setCsvByMode] = useState<Record<SubjectImportMode, UploadedCsvInfo | null>>({
    subject: null,
    identifiers: null,
  });
  const [uploadingMode, setUploadingMode] = useState<SubjectImportMode | null>(null);
  const [subjectMappingsByMode, setSubjectMappingsByMode] = useState<Record<SubjectImportMode, Record<string, FieldMappingState>>>(
    {
      subject: {},
      identifiers: {},
    },
  );
  const [identifierMappings, setIdentifierMappings] = useState<IdentifierMappingState[]>([]);
  const [previewByMode, setPreviewByMode] = useState<Record<SubjectImportMode, SubjectImportPreview | null>>({
    subject: null,
    identifiers: null,
  });
  const [manualSubjectValues, setManualSubjectValues] = useState<Record<string, string>>({});
  const [manualSubjectActive, setManualSubjectActive] = useState(true);
  const [manualLookupLoading, setManualLookupLoading] = useState(false);

  const fieldsData = fieldsQuery.data;

  // Determine active tab based on table name
  const activeTab: ImportTab = (() => {
    switch (tableName) {
      case 'subject':
        return 'subject';
      case 'cohort':
        return 'cohort';
      case 'subject_cohorts':
        return 'subject_cohorts';
      case 'subject_other_identifiers':
        return 'subject_other_identifiers';
      case 'id_types':
        return 'id_types';
      default:
        return 'subject';
    }
  })();

  useEffect(() => {
    const subjectDefinitions = fieldsData?.subjectFields;
    const subjectCodeDefinitions = subjectDefinitions?.filter((definition) => definition.name === 'subject_code');
    ensureFieldMapping('subject', subjectDefinitions, setSubjectMappingsByMode, csvByMode.subject?.columns, manualSubjectValues);
    ensureFieldMapping('identifiers', subjectCodeDefinitions, setSubjectMappingsByMode, csvByMode.identifiers?.columns, manualSubjectValues);
  }, [fieldsData?.subjectFields, csvByMode.subject?.columns, csvByMode.identifiers?.columns, manualSubjectValues]);

  useEffect(() => {
    if (!fieldsData?.subjectFields) return;
    setManualSubjectValues((current) => {
      const next = { ...current };
      let changed = false;
      fieldsData.subjectFields.forEach((definition) => {
        if (definition.name === 'is_active') return;
        if (!(definition.name in next)) {
          next[definition.name] = '';
          changed = true;
        }
      });
      if (!('subject_code' in next)) {
        next.subject_code = '';
        changed = true;
      }
      return changed ? next : current;
    });
  }, [fieldsData?.subjectFields]);

  const subjectModeLabels = useMemo<Record<SubjectImportMode, string>>(
    () => ({
      subject: 'Subject',
      identifiers: 'Subject Identifiers',
    }),
    [],
  );

  const handleClearCsv = useCallback(
    (mode: SubjectImportMode) => {
      setCsvByMode((current) => ({ ...current, [mode]: null }));
      setPreviewByMode((current) => ({ ...current, [mode]: null }));
      setSubjectMappingsByMode((current) => ({ ...current, [mode]: {} }));
      if (mode === 'identifiers') {
        setIdentifierMappings([]);
      }
      notifications.show({ color: 'gray', message: `${subjectModeLabels[mode]} CSV removed.` });
    },
    [subjectModeLabels],
  );

  const columnOptionsByMode = useMemo(
    () => ({
      subject: buildColumnsOptions(csvByMode.subject?.columns),
      identifiers: buildColumnsOptions(csvByMode.identifiers?.columns),
    }),
    [csvByMode.subject?.columns, csvByMode.identifiers?.columns],
  );

  const handleUpload = async (mode: SubjectImportMode, file: File | null) => {
    if (!file) return;
    const form = new FormData();
    form.append('file', file);
    try {
      setUploadingMode(mode);
      const response = await apiClient.postForm<UploadedCsvInfo>('/uploads/csv', form);
      setCsvByMode((current) => ({ ...current, [mode]: response }));
      notifications.show({ color: 'teal', message: `Uploaded ${response.filename}` });
      setPreviewByMode((current) => ({ ...current, [mode]: null }));
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to upload CSV';
      notifications.show({ color: 'red', message });
    } finally {
      setUploadingMode((current) => (current === mode ? null : current));
    }
  };

  const updateSubjectMapping = (mode: SubjectImportMode, fieldName: string, updates: Partial<FieldMappingState>) => {
    setSubjectMappingsByMode((current) => ({
      ...current,
      [mode]: { ...current[mode], [fieldName]: { ...current[mode][fieldName], ...updates } },
    }));
    if (mode === 'subject' && updates.manualValue !== undefined) {
      setManualSubjectValues((current) => ({ ...current, [fieldName]: updates.manualValue ?? '' }));
    }
  };

  const manualSubjectCode = (manualSubjectValues.subject_code ?? '').trim();
  const isManualSubjectEntry = !csvByMode.subject?.token;

  const updateManualValue = useCallback((field: string, value: string) => {
    setManualSubjectValues((current) => ({ ...current, [field]: value }));
  }, []);

  const mapDetailToManualValues = useCallback(
    (subjectCode: string, detail: SubjectDetail | null) => {
      setManualSubjectValues((current) => {
        const next: Record<string, string> = {};
        (fieldsData?.subjectFields ?? []).forEach((definition) => {
          if (definition.name === 'is_active') return;
          switch (definition.name) {
            case 'subject_code':
              next[definition.name] = subjectCode;
              break;
            case 'patient_name':
              next[definition.name] = detail?.patientName ?? '';
              break;
            case 'patient_birth_date':
              next[definition.name] = detail?.patientBirthDate ?? '';
              break;
            case 'patient_sex':
              next[definition.name] = detail?.patientSex ?? '';
              break;
            case 'ethnic_group':
              next[definition.name] = detail?.ethnicGroup ?? '';
              break;
            case 'occupation':
              next[definition.name] = detail?.occupation ?? '';
              break;
            case 'additional_patient_history':
              next[definition.name] = detail?.additionalPatientHistory ?? '';
              break;
            default:
              next[definition.name] = current[definition.name] ?? '';
              break;
          }
        });
        if (!('subject_code' in next)) {
          next.subject_code = subjectCode;
        }
        return next;
      });
      const isActiveValue = detail?.isActive;
      setManualSubjectActive(isActiveValue === null || isActiveValue === undefined ? true : Boolean(isActiveValue));
    },
    [fieldsData?.subjectFields],
  );

  const lookupExistingSubject = useCallback(
    async (options?: { silent?: boolean }) => {
      const silent = options?.silent ?? false;
      if (!manualSubjectCode) {
        if (!silent) {
          notifications.show({ color: 'yellow', message: 'Enter a subject code to load existing data.' });
        }
        return false;
      }
      try {
        setManualLookupLoading(true);
        const detail = await apiClient.get<SubjectDetail>(`/metadata/subjects/${encodeURIComponent(manualSubjectCode)}`);
        mapDetailToManualValues(manualSubjectCode, detail);
        setPreviewByMode((current) => ({ ...current, subject: null }));
        if (!silent) {
          notifications.show({ color: 'teal', message: `Loaded existing subject ${manualSubjectCode}.` });
        }
        return true;
      } catch (error) {
        if (error instanceof ApiError && error.status === 404) {
          mapDetailToManualValues(manualSubjectCode, null);
          setPreviewByMode((current) => ({ ...current, subject: null }));
          if (!silent) {
            notifications.show({
              color: 'blue',
              message: `Subject ${manualSubjectCode} not found. A new record will be created.`,
            });
          }
          return false;
        }
        if (!silent) {
          const message = error instanceof Error ? error.message : 'Failed to load subject data.';
          notifications.show({ color: 'red', message });
        }
        return false;
      } finally {
        setManualLookupLoading(false);
      }
    },
    [manualSubjectCode, mapDetailToManualValues],
  );

  const prepareManualSubjectCsv = useCallback(async () => {
    if (!fieldsData) {
      throw new Error('Field metadata is still loading.');
    }
    const definitions = fieldsData.subjectFields;
    const headers = definitions.map((definition) => definition.name);
    const row = definitions.map((definition) => {
      if (definition.name === 'is_active') {
        return escapeCsvValue(manualSubjectActive ? 1 : 0);
      }
      if (definition.name === 'subject_code') {
        return escapeCsvValue(manualSubjectCode);
      }
      const value =
        subjectMappingsByMode.subject[definition.name]?.manualValue ?? manualSubjectValues[definition.name];
      return escapeCsvValue(value ?? '');
    });
    const csvContent = `${headers.join(',')}
${row.join(',')}
`;
    const file = new File([csvContent], 'manual-subject-entry.csv', { type: 'text/csv' });
    const form = new FormData();
    form.append('file', file);
    return apiClient.postForm<UploadedCsvInfo>('/uploads/csv', form);
  }, [fieldsData, manualSubjectActive, manualSubjectCode, manualSubjectValues, subjectMappingsByMode.subject]);

  const hasDataForMode = (mode: SubjectImportMode) => {
    if (mode === 'subject' && isManualSubjectEntry) {
      return manualSubjectCode.length > 0;
    }
    return Boolean(csvByMode[mode]?.token);
  };

  const buildSubjectFieldsPayload = (mode: SubjectImportMode, requireAll: boolean) => {
    if (!fieldsData) {
      return { payload: {} as Record<string, SubjectImportFieldMapping>, missing: ['Field metadata is unavailable.'] };
    }

    const definitions =
      mode === 'subject'
        ? fieldsData.subjectFields
        : fieldsData.subjectFields.filter((definition) => definition.name === 'subject_code');

    const payload: Record<string, SubjectImportFieldMapping> = {};
    let subjectCodeLabel = 'Subject Code';
    const mappings = subjectMappingsByMode[mode] ?? {};

    definitions.forEach((definition) => {
      if (definition.name === 'subject_code') {
        subjectCodeLabel = definition.label;
      }
      const manualValue = mappings[definition.name]?.manualValue ?? manualSubjectValues[definition.name];
      const mappingPayload = buildFieldMappingPayload(definition, mappings[definition.name], manualValue);
      if (mappingPayload) {
        const cleaned: SubjectImportFieldMapping = { ...mappingPayload };
        if (mode === 'subject') {
          delete cleaned.default;
        }
        payload[definition.name] = cleaned;
      }
    });

    if (requireAll) {
      const missing = definitions
        .filter((definition) => definition.required)
        .filter((definition) => !payload[definition.name])
        .map((definition) => definition.label);
      return { payload, missing };
    }

    if (!payload.subject_code) {
      return { payload, missing: [subjectCodeLabel] };
    }

    return { payload, missing: [] };
  };

  const buildIdentifiersPayload = () => {
    const identifiersPayload = identifierMappings
      .map((entry) => {
        if (!entry.idTypeId) return null;
        const mapping: SubjectImportFieldMapping = {};
        if (entry.column) mapping.column = entry.column;
        if (entry.defaultValue) mapping.default = entry.defaultValue;
        if (!mapping.column && !mapping.default) return null;
        return {
          idTypeId: Number(entry.idTypeId),
          value: mapping,
        };
      })
      .filter((item): item is NonNullable<typeof item> => Boolean(item));

    return identifiersPayload;
  };

  const buildPayload = async (mode: SubjectImportMode, dryRun: boolean): Promise<SubjectImportPayload | null> => {
    if (!fieldsData) {
      notifications.show({ color: 'yellow', message: 'Field metadata is still loading.' });
      return null;
    }

    if (mode === 'subject' && isManualSubjectEntry) {
      if (!manualSubjectCode) {
        notifications.show({ color: 'red', message: 'Enter a subject code before submitting.' });
        return null;
      }
      try {
        const csvInfo = await prepareManualSubjectCsv();
        const subjectFieldsPayload: Record<string, SubjectImportFieldMapping> = {};
        fieldsData.subjectFields.forEach((definition) => {
          subjectFieldsPayload[definition.name] = {
            column: definition.name,
            ...(definition.defaultParser ? { parser: definition.defaultParser } : {}),
          };
        });
        const payload: SubjectImportPayload = {
          fileToken: csvInfo.token,
          subjectFields: subjectFieldsPayload,
          options: { skipBlankUpdates: true },
        };
        if (dryRun) {
          payload.dryRun = true;
        }
        return payload;
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Failed to prepare manual subject entry.';
        notifications.show({ color: 'red', message });
        return null;
      }
    }

    const csvInfo = csvByMode[mode];
    if (!csvInfo?.token) {
      if (mode === 'subject' && isManualSubjectEntry) {
        notifications.show({ color: 'red', message: 'Enter a subject code before submitting.' });
        return null;
      }
      notifications.show({ color: 'yellow', message: `Upload a CSV file for the ${subjectModeLabels[mode]} tab.` });
      return null;
    }

    const { payload: subjectFieldsPayload, missing: missingSubjectFields } = buildSubjectFieldsPayload(
      mode,
      mode === 'subject',
    );
    if (missingSubjectFields.length) {
      notifications.show({
        color: 'red',
        message: `Map required subject fields: ${missingSubjectFields.join(', ')}`,
      });
      return null;
    }

    const subjectCodeMapping = subjectFieldsPayload.subject_code;
    if (!subjectCodeMapping) {
      const subjectCodeLabel =
        fieldsData.subjectFields.find((definition) => definition.name === 'subject_code')?.label ?? 'Subject Code';
      notifications.show({ color: 'red', message: `Map required subject field: ${subjectCodeLabel}` });
      return null;
    }

    const payload: SubjectImportPayload = {
      fileToken: csvInfo.token,
      subjectFields: subjectFieldsPayload,
      options: { skipBlankUpdates: true },
    };

    if (mode === 'identifiers') {
      const identifiersPayload = buildIdentifiersPayload();
      if (!identifiersPayload.length) {
        notifications.show({
          color: 'red',
          message: 'Add at least one identifier mapping with a column or default value.',
        });
        return null;
      }
      payload.identifiers = identifiersPayload;
    }

    if (dryRun) {
      payload.dryRun = true;
    }

    return payload;
  };

  const handlePreview = async (mode: SubjectImportMode) => {
    const payload = await buildPayload(mode, false);
    if (!payload) return;
    try {
      const result = await previewMutation.mutateAsync(payload);
      setPreviewByMode((current) => ({ ...current, [mode]: result }));
      notifications.show({ color: 'teal', message: `${subjectModeLabels[mode]} preview generated.` });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Preview failed';
      notifications.show({ color: 'red', message });
    }
  };

  const handleApply = async (mode: SubjectImportMode, dryRun: boolean) => {
    const payload = await buildPayload(mode, dryRun);
    if (!payload) return;
    try {
      const result = await applyMutation.mutateAsync(payload);
      const summaryMessage = `${subjectModeLabels[mode]} submission: inserted ${result.subjectsInserted} subjects, updated ${result.subjectsUpdated}.`;
      notifications.show({ color: 'teal', message: dryRun ? `Dry run success. ${summaryMessage}` : summaryMessage });
      if (!dryRun) {
        setPreviewByMode((current) => ({ ...current, [mode]: null }));
        if (mode === 'subject' && isManualSubjectEntry) {
          void lookupExistingSubject({ silent: true });
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Submit failed';
      notifications.show({ color: 'red', message });
    }
  };

  const isBusy =
    uploadingMode !== null ||
    previewMutation.isPending ||
    applyMutation.isPending ||
    manualLookupLoading;

  const renderRunButtons = (mode: SubjectImportMode) => (
    <Group gap="sm">
      <Button
        variant="light"
        onClick={() => handlePreview(mode)}
        disabled={isBusy || !hasDataForMode(mode)}
        loading={previewMutation.isPending}
      >
        Preview
      </Button>
      <Button
        onClick={() => handleApply(mode, false)}
        disabled={isBusy || !hasDataForMode(mode)}
        loading={applyMutation.isPending}
      >
        Submit
      </Button>
      <Button
        variant="outline"
        onClick={() => handleApply(mode, true)}
        disabled={isBusy || !hasDataForMode(mode)}
        loading={applyMutation.isPending}
      >
        Dry run
      </Button>
    </Group>
  );

  const renderPreview = (mode: SubjectImportMode) => {
    const preview = previewByMode[mode];
    if (!preview || !fieldsData) return null;
    const insertCount = preview.rows.filter((row) => !row.existing).length;
    const updateCount = preview.rows.length - insertCount;
    return (
      <Stack gap="sm" mt="md">
        <Group justify="space-between" align="center">
          <Text fw={600}>Preview — {subjectModeLabels[mode]}</Text>
          <Group gap="xs">
            <Badge color="gray" variant="light">
              {preview.processedRows} rows
            </Badge>
            <Badge color="green" variant="light">
              {insertCount} new
            </Badge>
            <Badge color="blue" variant="light">
              {updateCount} updates
            </Badge>
          </Group>
        </Group>
        {preview.warnings.length ? (
          <Alert color="yellow" title="Warnings">
            <Stack gap={4}>
              {preview.warnings.map((warning, index) => (
                <Text key={index} size="sm">
                  {warning}
                </Text>
              ))}
            </Stack>
          </Alert>
        ) : null}
        <ScrollArea h={240} offsetScrollbars>
          <Table striped highlightOnHover withColumnBorders={false} horizontalSpacing="md" verticalSpacing="xs">
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Action</Table.Th>
                {fieldsData.subjectFields.map((definition) => (
                  <Table.Th key={definition.name}>{definition.label}</Table.Th>
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
                  {fieldsData.subjectFields.map((definition) => (
                    <Table.Td key={definition.name}>
                      {(() => {
                        const value = row.subject[definition.name] ?? row.existingSubject?.[definition.name] ?? '';
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
    );
  };

  return (
    <Card withBorder radius="md" padding="lg">
      <Stack gap="md">
        {fieldsQuery.isLoading ? (
          <Group gap="xs">
            <Loader size="sm" />
            <Text size="sm">Loading field definitions…</Text>
          </Group>
        ) : null}

        {fieldsQuery.isError ? (
          <Alert color="red" title="Failed to load import metadata">
            {(fieldsQuery.error as Error)?.message ?? 'Unable to load subject import configuration.'}
          </Alert>
        ) : null}

        {fieldsData && activeTab === 'subject' ? (
          <Stack gap="md">
            <Group justify="space-between" align="center" wrap="wrap">
              <Group gap="sm">
                <FileButton onChange={(file) => handleUpload('subject', file)} accept=".csv">
                  {(props) => (
                    <Button leftSection={<IconUpload size={16} />} loading={uploadingMode === 'subject'} {...props}>
                      Upload CSV
                    </Button>
                  )}
                </FileButton>
                {csvByMode.subject?.columns ? (
                  <Text size="xs" c="dimmed">
                    Columns: {csvByMode.subject.columns.join(', ')}
                  </Text>
                ) : null}
              </Group>
              <Group gap="xs">
                {csvByMode.subject?.filename ? <Badge size="sm" color="blue">{csvByMode.subject.filename}</Badge> : null}
                {csvByMode.subject ? (
                  <ActionIcon
                    size="sm"
                    variant="light"
                    color="red"
                    onClick={() => handleClearCsv('subject')}
                    aria-label="Remove subject CSV"
                  >
                    <IconTrash size={16} />
                  </ActionIcon>
                ) : null}
              </Group>
            </Group>

            <Stack gap={4}>
              <Text fw={600} size="sm">
                Subject Fields
              </Text>
              <Text size="xs" c="dimmed">
                Provide a subject code and optional details to create or update a record. Uploading a CSV switches fields to column mapping.
              </Text>
            </Stack>

            <SimpleGrid cols={{ base: 1, sm: 2, md: 3 }} spacing="sm">
              {fieldsData.subjectFields.map((definition) => {
                const state = subjectMappingsByMode.subject[definition.name] ?? {};
                const manualValue = state.manualValue ?? manualSubjectValues[definition.name] ?? '';
                if (definition.name === 'is_active') {
                  return (
                    <Switch
                      key={definition.name}
                      label={definition.label}
                      checked={manualSubjectActive}
                      onChange={(event) => setManualSubjectActive(event.currentTarget.checked)}
                    />
                  );
                }
                if (isManualSubjectEntry) {
                  return (
                    <TextInput
                      key={definition.name}
                      label={definition.label}
                      placeholder={definition.required ? 'Required' : 'Optional'}
                      value={manualValue}
                      withAsterisk={definition.required}
                      onChange={(event) => {
                        const value = event.currentTarget.value;
                        updateSubjectMapping('subject', definition.name, { manualValue: value });
                        updateManualValue(definition.name, value);
                      }}
                      onBlur={() => {
                        if (definition.name === 'subject_code') {
                          void lookupExistingSubject();
                        }
                      }}
                      onKeyDown={(event) => {
                        if (definition.name === 'subject_code' && event.key === 'Enter') {
                          void lookupExistingSubject();
                        }
                      }}
                    />
                  );
                }
                return (
                  <Stack key={definition.name} gap={6}>
                    <Select
                      label={definition.label}
                      placeholder="Select column"
                      data={columnOptionsByMode.subject}
                      value={state.column ?? null}
                      onChange={(value) =>
                        updateSubjectMapping('subject', definition.name, { column: value ?? undefined })
                      }
                      disabled={!csvByMode.subject}
                      withAsterisk={definition.required}
                      clearable
                    />
                    {definition.parsers.length > 1 ? (
                      <Select
                        label="Parser"
                        data={definition.parsers.map((parser) => ({ value: parser, label: parser }))}
                        value={state.parser ?? definition.defaultParser}
                        onChange={(value) =>
                          updateSubjectMapping('subject', definition.name, { parser: value ?? undefined })
                        }
                        maw={140}
                      />
                    ) : null}
                  </Stack>
                );
              })}
            </SimpleGrid>

            {renderRunButtons('subject')}
            {renderPreview('subject')}
          </Stack>
        ) : null}

        {fieldsData && activeTab === 'cohort' ? (
          <CohortImportTab />
        ) : null}

        {fieldsData && activeTab === 'subject_cohorts' ? <SubjectCohortImportTab /> : null}

        {activeTab === 'subject_other_identifiers' ? <SubjectOtherIdentifierImportTab /> : null}

        {activeTab === 'id_types' ? <IdentifierTypeManagerTab /> : null}

        {!['subject', 'cohort', 'subject_other_identifiers', 'subject_cohorts', 'id_types'].includes(tableName) ? (
          <Alert color="blue" title="Import form not yet available">
            <Text size="sm">
              Import functionality for this table is coming soon. For now, you can view the data in the table below.
            </Text>
          </Alert>
        ) : null}
      </Stack>
    </Card>
  );
};

export default SubjectImportPanel;
