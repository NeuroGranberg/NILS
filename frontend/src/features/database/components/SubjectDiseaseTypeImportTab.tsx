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
  NumberInput,
  ScrollArea,
  Select,
  SimpleGrid,
  Stack,
  Table,
  Text,
  TextInput,
  Tooltip,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { IconTrash, IconUpload } from '@tabler/icons-react';

import {
  type SDTFieldsResponse,
  type SDTImportPayload,
  type SDTImportPreview,
  type SDTManualPayload,
  type SubjectImportFieldMapping,
  useSDTFields,
  useSDTImportPreview,
  useSDTImportApply,
  useSDTManualUpsert,
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

const SubjectDiseaseTypeImportTab = () => {
  const fieldsQuery = useSDTFields();
  const previewMutation = useSDTImportPreview();
  const applyMutation = useSDTImportApply();
  const manualMutation = useSDTManualUpsert();

  // Shared state
  const [selectedDiseaseId, setSelectedDiseaseId] = useState<string | null>(null);
  const [selectedIdType, setSelectedIdType] = useState<string>('subject_code');

  // CSV state
  const [csvInfo, setCsvInfo] = useState<UploadedCsvInfo | null>(null);
  const [uploading, setUploading] = useState(false);
  const [mappings, setMappings] = useState<Record<string, FieldMappingState>>({});
  const [preview, setPreview] = useState<SDTImportPreview | null>(null);

  // Manual state
  const [manualSubject, setManualSubject] = useState('');
  const [manualDiseaseTypeId, setManualDiseaseTypeId] = useState<string | null>(null);
  const [manualDate, setManualDate] = useState('');
  const [manualNotes, setManualNotes] = useState('');
  const [manualId, setManualId] = useState('');

  const data = fieldsQuery.data as SDTFieldsResponse | undefined;
  const fields = useMemo(() => data?.fields ?? [], [data]);
  const diseases = useMemo(() => data?.diseases ?? [], [data]);
  const allDiseaseTypes = useMemo(() => data?.diseaseTypes ?? [], [data]);
  const idTypes = useMemo(() => data?.idTypes ?? [], [data]);

  const diseaseOptions = useMemo(
    () => diseases.map((d) => ({
      value: String(d.id),
      label: d.code ? `${d.name} (${d.code})` : d.name,
    })),
    [diseases],
  );

  const filteredDiseaseTypes = useMemo(
    () => selectedDiseaseId
      ? allDiseaseTypes.filter((dt) => dt.diseaseId === Number(selectedDiseaseId))
      : [],
    [allDiseaseTypes, selectedDiseaseId],
  );

  const diseaseTypeOptions = useMemo(
    () => filteredDiseaseTypes.map((dt) => ({
      value: String(dt.id),
      label: dt.name,
      description: dt.aliases.length > 0 ? `Aliases: ${dt.aliases.slice(0, 5).join(', ')}` : undefined,
    })),
    [filteredDiseaseTypes],
  );

  const idTypeOptions = useMemo(() => {
    const options = [{ value: 'subject_code', label: 'Subject Code' }];
    idTypes.forEach((it) => options.push({ value: String(it.id), label: it.name }));
    return options;
  }, [idTypes]);

  // Reset disease type selection when disease changes
  useEffect(() => {
    setManualDiseaseTypeId(null);
  }, [selectedDiseaseId]);

  // CSV mapping auto-init
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

  const columnOptions = useMemo(
    () => (csvInfo?.columns ?? []).map((c) => ({ value: c, label: c })),
    [csvInfo?.columns],
  );

  // ---------- CSV handlers ----------

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
      const msg = error instanceof Error ? error.message : 'Failed to upload CSV';
      notifications.show({ color: 'red', message: msg });
    } finally {
      setUploading(false);
    }
  };

  const handleClearCsv = () => {
    setCsvInfo(null);
    setMappings({});
    setPreview(null);
  };

  const updateMapping = (fieldName: string, updates: Partial<FieldMappingState>) => {
    setMappings((cur) => ({ ...cur, [fieldName]: { ...cur[fieldName], ...updates } }));
  };

  const buildCsvPayload = useCallback(
    (dryRun: boolean): SDTImportPayload | null => {
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
        dryRun,
      };
    },
    [csvInfo, selectedDiseaseId, selectedIdType, fields, mappings],
  );

  const handlePreview = async () => {
    const payload = buildCsvPayload(false);
    if (!payload) return;
    try {
      const result = await previewMutation.mutateAsync(payload);
      setPreview(result);
    } catch {
      // handled by mutation hook
    }
  };

  const handleCsvApply = async (dryRun: boolean) => {
    const payload = buildCsvPayload(dryRun);
    if (!payload) return;
    try {
      await applyMutation.mutateAsync(payload);
      if (!dryRun) setPreview(null);
    } catch {
      // handled by mutation hook
    }
  };

  // ---------- Manual handlers ----------

  const handleManualSubmit = async (dryRun: boolean) => {
    if (!selectedDiseaseId || !manualDiseaseTypeId || !manualSubject.trim()) {
      notifications.show({ color: 'yellow', message: 'Fill required fields: subject and disease type.' });
      return;
    }
    const payload: SDTManualPayload = {
      subjectIdentifier: manualSubject.trim(),
      subjectIdentifierType: selectedIdType,
      diseaseId: Number(selectedDiseaseId),
      diseaseTypeId: Number(manualDiseaseTypeId),
      ...(manualDate.trim() ? { assignmentDate: manualDate.trim() } : {}),
      ...(manualNotes.trim() ? { notes: manualNotes.trim() } : {}),
      ...(manualId.trim() ? { subjectDiseaseTypeId: Number(manualId) } : {}),
      ...(dryRun ? { dryRun: true } : {}),
    };
    try {
      await manualMutation.mutateAsync(payload);
    } catch {
      // handled by mutation hook
    }
  };

  const isBusy = uploading || previewMutation.isPending || applyMutation.isPending || manualMutation.isPending;
  const canCsv = Boolean(csvInfo?.token) && Boolean(selectedDiseaseId);

  return (
    <Stack gap="md">
      {fieldsQuery.isLoading ? (
        <Group gap="xs">
          <Loader size="sm" />
          <Text size="sm">Loading configuration...</Text>
        </Group>
      ) : null}

      {fieldsQuery.isError ? (
        <Alert color="red" title="Failed to load configuration">
          {(fieldsQuery.error as Error)?.message ?? 'Unable to load.'}
        </Alert>
      ) : null}

      {data ? (
        <Stack gap="lg">
          {/* Shared selectors */}
          <SimpleGrid cols={{ base: 1, sm: 3 }} spacing="sm">
            <Select
              label="Disease"
              placeholder="Select disease (with subtypes)"
              data={diseaseOptions}
              value={selectedDiseaseId}
              onChange={(v) => setSelectedDiseaseId(v)}
              searchable
              withAsterisk
            />
            <Select
              label="Subject Identifier"
              placeholder="Identifier type"
              data={idTypeOptions}
              value={selectedIdType}
              onChange={(v) => setSelectedIdType(v ?? 'subject_code')}
            />
            {selectedDiseaseId && filteredDiseaseTypes.length > 0 ? (
              <Stack gap={4}>
                <Text size="xs" fw={500}>Available subtypes</Text>
                <Group gap={4} wrap="wrap">
                  {filteredDiseaseTypes.map((dt) => (
                    <Tooltip
                      key={dt.id}
                      label={dt.aliases.length ? `Fuzzy aliases: ${dt.aliases.slice(0, 6).join(', ')}` : dt.description ?? ''}
                      multiline
                      w={260}
                    >
                      <Badge size="sm" variant="light" color="blue">{dt.name}</Badge>
                    </Tooltip>
                  ))}
                </Group>
              </Stack>
            ) : null}
          </SimpleGrid>

          {/* Manual Entry */}
          <Card withBorder padding="md" radius="sm">
            <Stack gap="md">
              <Stack gap={4}>
                <Text fw={600} size="sm">Manual Entry</Text>
                <Text size="xs" c="dimmed">
                  Assign a single subject to a disease subtype. The subject must already have the disease assigned.
                </Text>
              </Stack>
              <SimpleGrid cols={{ base: 1, sm: 2, md: 3 }} spacing="sm">
                <TextInput
                  label="ID Lookup"
                  placeholder="subject_disease_type_id"
                  value={manualId}
                  onChange={(e) => setManualId(e.currentTarget.value)}
                  description="Optional: load existing record"
                />
                <TextInput
                  label="Subject"
                  placeholder="Subject identifier"
                  value={manualSubject}
                  onChange={(e) => setManualSubject(e.currentTarget.value)}
                  withAsterisk
                />
                <Select
                  label="Disease Type"
                  placeholder="Select subtype"
                  data={diseaseTypeOptions}
                  value={manualDiseaseTypeId}
                  onChange={(v) => setManualDiseaseTypeId(v)}
                  searchable
                  withAsterisk
                  disabled={!selectedDiseaseId}
                />
                <TextInput
                  label="Assignment Date"
                  placeholder="YYYY-MM-DD or year (optional)"
                  value={manualDate}
                  onChange={(e) => setManualDate(e.currentTarget.value)}
                />
                <TextInput
                  label="Notes"
                  placeholder="Optional notes"
                  value={manualNotes}
                  onChange={(e) => setManualNotes(e.currentTarget.value)}
                />
              </SimpleGrid>
              <Group gap="sm">
                <Button onClick={() => handleManualSubmit(false)} disabled={isBusy} loading={manualMutation.isPending}>
                  Submit
                </Button>
                <Button variant="outline" onClick={() => handleManualSubmit(true)} disabled={isBusy}>
                  Dry run
                </Button>
              </Group>
            </Stack>
          </Card>

          <Divider label="OR" labelPosition="center" />

          {/* CSV Import */}
          <Card withBorder padding="md" radius="sm">
            <Stack gap="md">
              <Stack gap={4}>
                <Text fw={600} size="sm">CSV Import</Text>
                <Text size="xs" c="dimmed">
                  Upload a CSV with subject identifiers, disease type (fuzzy matched), and assignment date.
                  Subjects must already have the disease assigned via Subject Diseases tab.
                  SP Transition events are auto-linked when assigning SPMS subtype.
                </Text>
              </Stack>

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
                        <Select
                          key={field.name}
                          label={field.label}
                          placeholder="Select column"
                          data={columnOptions}
                          value={state.column ?? null}
                          onChange={(v) => updateMapping(field.name, { column: v ?? undefined })}
                          withAsterisk={field.required}
                          clearable
                        />
                      );
                    })}
                  </SimpleGrid>
                </Stack>
              ) : null}

              <Group gap="sm">
                <Button variant="light" onClick={handlePreview} disabled={isBusy || !canCsv} loading={previewMutation.isPending}>
                  Preview
                </Button>
                <Button onClick={() => handleCsvApply(false)} disabled={isBusy || !canCsv} loading={applyMutation.isPending}>
                  Submit
                </Button>
                <Button variant="outline" onClick={() => handleCsvApply(true)} disabled={isBusy || !canCsv}>
                  Dry run
                </Button>
              </Group>

              {preview ? (
                <Stack gap="sm" mt="md">
                  <Group justify="space-between" align="center">
                    <Text fw={600}>Preview -- Subject Disease Types</Text>
                    <Group gap="xs">
                      <Badge color="gray" variant="light">{preview.totalRows} total</Badge>
                      <Badge color="green" variant="light">{preview.rows.filter((r) => !r.existing && r.subjectFound && r.diseaseTypeFound).length} new</Badge>
                      <Badge color="blue" variant="light">{preview.rows.filter((r) => r.existing).length} existing</Badge>
                      <Badge color="yellow" variant="light">{preview.rows.filter((r) => !r.subjectFound).length} subject missing</Badge>
                      <Badge color="red" variant="light">{preview.rows.filter((r) => !r.diseaseTypeFound).length} type unresolved</Badge>
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
                          <Table.Th>Subject</Table.Th>
                          <Table.Th>Resolved Code</Table.Th>
                          <Table.Th>Type Input</Table.Th>
                          <Table.Th>Resolved Type</Table.Th>
                          <Table.Th>Date</Table.Th>
                          <Table.Th>Transition</Table.Th>
                          <Table.Th>Notes</Table.Th>
                        </Table.Tr>
                      </Table.Thead>
                      <Table.Tbody>
                        {preview.rows.map((row, index) => (
                          <Table.Tr key={index}>
                            <Table.Td>
                              {!row.subjectFound ? (
                                <Badge color="red" variant="light" size="xs">No Subject</Badge>
                              ) : !row.diseaseTypeFound ? (
                                <Badge color="orange" variant="light" size="xs">No Type</Badge>
                              ) : row.existing ? (
                                <Badge color="blue" variant="light" size="xs">Exists</Badge>
                              ) : (
                                <Badge color="green" variant="light" size="xs">New</Badge>
                              )}
                            </Table.Td>
                            <Table.Td>{row.subjectIdentifier}</Table.Td>
                            <Table.Td>{row.resolvedSubjectCode ?? '-'}</Table.Td>
                            <Table.Td>{row.diseaseTypeInput}</Table.Td>
                            <Table.Td>
                              {row.resolvedDiseaseType ? (
                                <Badge size="xs" color="teal" variant="light">{row.resolvedDiseaseType}</Badge>
                              ) : (
                                <Text size="xs" c="red">?</Text>
                              )}
                            </Table.Td>
                            <Table.Td>{row.assignmentDate ?? '-'}</Table.Td>
                            <Table.Td>
                              <Badge size="xs" color={row.hasTransitionEvent ? 'teal' : 'gray'} variant="light">
                                {row.hasTransitionEvent ? 'Yes' : 'No'}
                              </Badge>
                            </Table.Td>
                            <Table.Td>{row.notes ?? '-'}</Table.Td>
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

export default SubjectDiseaseTypeImportTab;
