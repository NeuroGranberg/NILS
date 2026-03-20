import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Card,
  Divider,
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
  type SubjectDiseaseFieldsResponse,
  type SubjectDiseaseImportPayload,
  type SubjectDiseaseImportPreview,
  type SubjectImportFieldMapping,
  type CohortAssignPayload,
  useSubjectDiseaseFields,
  useSubjectDiseaseImportPreview,
  useSubjectDiseaseImportApply,
  useCohortDiseaseAssign,
} from '../api';
import { apiClient } from '../../../utils/api-client';

type FieldMappingState = {
  column?: string;
  parser?: string;
};

type UploadedCsvInfo = {
  token: string;
  filename: string;
  columns: string[];
};

const buildColumnsOptions = (columns: string[] | undefined) =>
  (columns ?? []).map((c) => ({ value: c, label: c }));

const SubjectDiseaseImportTab = () => {
  const fieldsQuery = useSubjectDiseaseFields();
  const previewMutation = useSubjectDiseaseImportPreview();
  const applyMutation = useSubjectDiseaseImportApply();
  const cohortAssignMutation = useCohortDiseaseAssign();

  const [selectedDiseaseId, setSelectedDiseaseId] = useState<string | null>(null);
  const [selectedIdType, setSelectedIdType] = useState<string>('subject_code');
  const [csvInfo, setCsvInfo] = useState<UploadedCsvInfo | null>(null);
  const [uploading, setUploading] = useState(false);
  const [mappings, setMappings] = useState<Record<string, FieldMappingState>>({});
  const [preview, setPreview] = useState<SubjectDiseaseImportPreview | null>(null);

  // Cohort assign state
  const [selectedCohortId, setSelectedCohortId] = useState<string | null>(null);
  const [cohortDiseaseId, setCohortDiseaseId] = useState<string | null>(null);

  const data = fieldsQuery.data as SubjectDiseaseFieldsResponse | undefined;
  const fields = useMemo(() => data?.fields ?? [], [data]);
  const diseases = useMemo(() => data?.diseases ?? [], [data]);
  const cohorts = useMemo(() => data?.cohorts ?? [], [data]);
  const idTypes = useMemo(() => data?.idTypes ?? [], [data]);

  const diseaseOptions = useMemo(
    () => diseases.map((d) => ({
      value: String(d.id),
      label: d.code ? `${d.name} (${d.code})` : d.name,
    })),
    [diseases],
  );

  const idTypeOptions = useMemo(() => {
    const options = [{ value: 'subject_code', label: 'Subject Code' }];
    idTypes.forEach((it) => options.push({ value: String(it.id), label: it.name }));
    return options;
  }, [idTypes]);

  const cohortOptions = useMemo(
    () => cohorts.map((c) => ({
      value: String(c.id),
      label: `${c.name} (${c.subjectCount} subjects)`,
    })),
    [cohorts],
  );

  useEffect(() => {
    setPreview(null);
    if (!fields.length || !csvInfo?.columns) {
      setMappings({});
      return;
    }
    const cols = csvInfo.columns;
    const next: Record<string, FieldMappingState> = {};
    fields.forEach((f) => {
      const auto = cols.find((c) => c.toLowerCase() === f.name.toLowerCase());
      next[f.name] = { parser: f.defaultParser, column: auto };
    });
    setMappings(next);
  }, [fields, csvInfo?.columns, selectedDiseaseId]);

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
    (dryRun: boolean): SubjectDiseaseImportPayload | null => {
      if (!csvInfo?.token) {
        notifications.show({ color: 'yellow', message: 'Upload a CSV file first.' });
        return null;
      }
      if (!selectedDiseaseId) {
        notifications.show({ color: 'yellow', message: 'Select a disease.' });
        return null;
      }

      const fieldPayload: Record<string, SubjectImportFieldMapping> = {};
      const missing: string[] = [];
      fields.forEach((f) => {
        const state = mappings[f.name];
        if (state?.column) {
          const mp: SubjectImportFieldMapping = { column: state.column };
          if (state.parser) mp.parser = state.parser;
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
        diseaseId: Number(selectedDiseaseId),
        subjectIdentifierType: selectedIdType,
        fields: fieldPayload,
        options: { skipBlankUpdates: true },
        dryRun,
      };
    },
    [csvInfo, selectedDiseaseId, selectedIdType, fields, mappings],
  );

  const handlePreview = async () => {
    const payload = buildPayload(false);
    if (!payload) return;
    try {
      const result = await previewMutation.mutateAsync(payload);
      setPreview(result);
      notifications.show({ color: 'teal', message: 'Preview generated.' });
    } catch {
      // handled by mutation hook
    }
  };

  const handleApply = async (dryRun: boolean) => {
    const payload = buildPayload(dryRun);
    if (!payload) return;
    try {
      await applyMutation.mutateAsync(payload);
      if (!dryRun) setPreview(null);
    } catch {
      // handled by mutation hook
    }
  };

  const handleCohortAssign = async (dryRun: boolean) => {
    if (!cohortDiseaseId || !selectedCohortId) {
      notifications.show({ color: 'yellow', message: 'Select both a disease and a cohort.' });
      return;
    }
    const payload: CohortAssignPayload = {
      diseaseId: Number(cohortDiseaseId),
      cohortId: Number(selectedCohortId),
      ...(dryRun ? { dryRun: true } : {}),
    };
    try {
      const result = await cohortAssignMutation.mutateAsync(payload);
      const msg = `${result.inserted} assigned, ${result.skipped} existed (${result.totalSubjects} total)`;
      notifications.show({
        color: dryRun ? 'blue' : 'teal',
        message: dryRun ? `Dry run: ${msg}` : msg,
      });
    } catch {
      // handled by mutation hook
    }
  };

  const isBusy = uploading || previewMutation.isPending || applyMutation.isPending || cohortAssignMutation.isPending;
  const canRunCsv = Boolean(csvInfo?.token) && Boolean(selectedDiseaseId);
  const canRunCohort = Boolean(cohortDiseaseId) && Boolean(selectedCohortId);

  return (
    <Stack gap="md">
      {fieldsQuery.isLoading ? (
        <Group gap="xs">
          <Loader size="sm" />
          <Text size="sm">Loading subject disease configuration...</Text>
        </Group>
      ) : null}

      {fieldsQuery.isError ? (
        <Alert color="red" title="Failed to load configuration">
          {(fieldsQuery.error as Error)?.message ?? 'Unable to load import configuration.'}
        </Alert>
      ) : null}

      {data ? (
        <Stack gap="lg">
          {/* ---- Cohort Bulk Assign ---- */}
          <Card withBorder padding="md" radius="sm">
            <Stack gap="md">
              <Stack gap={4}>
                <Text fw={600} size="sm">Cohort Bulk Assign</Text>
                <Text size="xs" c="dimmed">
                  Assign all subjects of a cohort to a disease. Diagnosis and onset events are auto-linked if they exist.
                </Text>
              </Stack>
              <SimpleGrid cols={{ base: 1, sm: 2 }} spacing="sm">
                <Select
                  label="Disease"
                  placeholder="Select disease"
                  data={diseaseOptions}
                  value={cohortDiseaseId}
                  onChange={(v) => setCohortDiseaseId(v)}
                  searchable
                  withAsterisk
                />
                <Select
                  label="Cohort"
                  placeholder="Select cohort"
                  data={cohortOptions}
                  value={selectedCohortId}
                  onChange={(v) => setSelectedCohortId(v)}
                  searchable
                  withAsterisk
                />
              </SimpleGrid>
              <Group gap="sm">
                <Button onClick={() => handleCohortAssign(false)} disabled={isBusy || !canRunCohort} loading={cohortAssignMutation.isPending}>
                  Assign
                </Button>
                <Button variant="outline" onClick={() => handleCohortAssign(true)} disabled={isBusy || !canRunCohort} loading={cohortAssignMutation.isPending}>
                  Dry run
                </Button>
              </Group>
            </Stack>
          </Card>

          <Divider label="OR" labelPosition="center" />

          {/* ---- CSV Import ---- */}
          <Card withBorder padding="md" radius="sm">
            <Stack gap="md">
              <Stack gap={4}>
                <Text fw={600} size="sm">CSV Import</Text>
                <Text size="xs" c="dimmed">
                  Upload a CSV with subject identifiers to assign them to a disease. Optional columns: diagnosis notes, family history, active status.
                  Diagnosis and onset events are auto-linked if they exist.
                </Text>
              </Stack>

              <SimpleGrid cols={{ base: 1, sm: 2 }} spacing="sm">
                <Select
                  label="Disease"
                  placeholder="Select disease"
                  data={diseaseOptions}
                  value={selectedDiseaseId}
                  onChange={(v) => setSelectedDiseaseId(v)}
                  searchable
                  withAsterisk
                />
                <Select
                  label="Subject Identifier"
                  placeholder="Select identifier type"
                  data={idTypeOptions}
                  value={selectedIdType}
                  onChange={(v) => setSelectedIdType(v ?? 'subject_code')}
                />
              </SimpleGrid>

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
                    <Text size="xs" c="dimmed">Columns: {csvInfo.columns.join(', ')}</Text>
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

              {csvInfo && selectedDiseaseId ? (
                <Stack gap="md">
                  <Text fw={600} size="sm">Column Mapping</Text>
                  <SimpleGrid cols={{ base: 1, sm: 2, md: 3 }} spacing="sm">
                    {fields.map((field) => {
                      const state = mappings[field.name] ?? {};
                      return (
                        <Stack key={field.name} gap={6}>
                          <Select
                            label={field.label}
                            placeholder="Select column"
                            data={columnOptions}
                            value={state.column ?? null}
                            onChange={(v) => updateMapping(field.name, { column: v ?? undefined })}
                            withAsterisk={field.required}
                            clearable
                          />
                        </Stack>
                      );
                    })}
                  </SimpleGrid>
                </Stack>
              ) : null}

              <Group gap="sm">
                <Button variant="light" onClick={handlePreview} disabled={isBusy || !canRunCsv} loading={previewMutation.isPending}>
                  Preview
                </Button>
                <Button onClick={() => handleApply(false)} disabled={isBusy || !canRunCsv} loading={applyMutation.isPending}>
                  Submit
                </Button>
                <Button variant="outline" onClick={() => handleApply(true)} disabled={isBusy || !canRunCsv} loading={applyMutation.isPending}>
                  Dry run
                </Button>
              </Group>

              {preview ? (
                <Stack gap="sm" mt="md">
                  <Group justify="space-between" align="center">
                    <Text fw={600}>Preview — Subject Diseases</Text>
                    <Group gap="xs">
                      <Badge color="gray" variant="light">{preview.totalRows} total</Badge>
                      <Badge color="green" variant="light">{preview.rows.filter((r) => !r.existing && r.subjectFound).length} new</Badge>
                      <Badge color="blue" variant="light">{preview.rows.filter((r) => r.existing).length} existing</Badge>
                      <Badge color="yellow" variant="light">{preview.rows.filter((r) => !r.subjectFound).length} unresolved</Badge>
                    </Group>
                  </Group>

                  {preview.warnings.length ? (
                    <Alert color="yellow" title="Warnings">
                      <Stack gap={4}>
                        {preview.warnings.slice(0, 20).map((w, i) => (
                          <Text key={i} size="sm">{w}</Text>
                        ))}
                        {preview.warnings.length > 20 ? (
                          <Text size="sm" c="dimmed">...and {preview.warnings.length - 20} more</Text>
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
                          <Table.Th>Diagnosis Event</Table.Th>
                          <Table.Th>Onset Event</Table.Th>
                          <Table.Th>Notes</Table.Th>
                          <Table.Th>Family History</Table.Th>
                        </Table.Tr>
                      </Table.Thead>
                      <Table.Tbody>
                        {preview.rows.map((row, index) => (
                          <Table.Tr key={index}>
                            <Table.Td>
                              {!row.subjectFound ? (
                                <Badge color="red" variant="light">Not Found</Badge>
                              ) : row.existing ? (
                                <Badge color="blue" variant="light">Exists</Badge>
                              ) : (
                                <Badge color="green" variant="light">New</Badge>
                              )}
                            </Table.Td>
                            <Table.Td>{row.subjectIdentifier}</Table.Td>
                            <Table.Td>{row.resolvedSubjectCode ?? '-'}</Table.Td>
                            <Table.Td>
                              <Badge size="xs" color={row.hasDiagnosisEvent ? 'teal' : 'gray'} variant="light">
                                {row.hasDiagnosisEvent ? 'Yes' : 'No'}
                              </Badge>
                            </Table.Td>
                            <Table.Td>
                              <Badge size="xs" color={row.hasOnsetEvent ? 'teal' : 'gray'} variant="light">
                                {row.hasOnsetEvent ? 'Yes' : 'No'}
                              </Badge>
                            </Table.Td>
                            <Table.Td>{row.diagnosisNotes ?? '-'}</Table.Td>
                            <Table.Td>{row.familyHistory ?? '-'}</Table.Td>
                          </Table.Tr>
                        ))}
                      </Table.Tbody>
                    </Table>
                  </ScrollArea>
                </Stack>
              ) : null}
            </Stack>
          </Card>
        </Stack>
      ) : null}
    </Stack>
  );
};

export default SubjectDiseaseImportTab;
