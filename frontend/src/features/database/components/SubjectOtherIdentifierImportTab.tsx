import { useCallback, useEffect, useMemo, useState, type FormEvent } from 'react';
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  FileButton,
  Group,
  Loader,
  Modal,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { IconPencil, IconTrash, IconUpload } from '@tabler/icons-react';

import {
  type SubjectImportFieldMapping,
  type SubjectIdentifierImportPreview,
  type SubjectIdentifierImportPayload,
  useSubjectIdentifierImportFields,
  useSubjectIdentifierImportPreview,
  useSubjectIdentifierImportApply,
  useSubjectIdentifierDetail,
  useUpsertSubjectIdentifier,
  useDeleteSubjectIdentifier,
} from '../api';
import { apiClient } from '../../../utils/api-client';

type UploadedCsvInfo = {
  token: string;
  filename: string;
  columns: string[];
};

type FieldMappingState = SubjectImportFieldMapping;

const importModeOptions = [
  { value: 'append', label: 'Append (keep existing)' },
  { value: 'replace', label: 'Replace (overwrite per subject)' },
];

const buildColumnOptions = (columns: string[]): { value: string; label: string }[] =>
  columns.map((column) => ({ value: column, label: column }));

const SubjectOtherIdentifierImportTab = () => {
  const fieldsQuery = useSubjectIdentifierImportFields();
  const previewMutation = useSubjectIdentifierImportPreview();
  const applyMutation = useSubjectIdentifierImportApply();
  const upsertMutation = useUpsertSubjectIdentifier();
  const deleteMutation = useDeleteSubjectIdentifier();

  const [csvInfo, setCsvInfo] = useState<UploadedCsvInfo | null>(null);
  const [uploading, setUploading] = useState(false);
  const [subjectField, setSubjectField] = useState<FieldMappingState>({});
  const [identifierField, setIdentifierField] = useState<FieldMappingState>({});
  const [importMode, setImportMode] = useState<'append' | 'replace'>('append');
  const [preview, setPreview] = useState<SubjectIdentifierImportPreview | null>(null);

  const [subjectInput, setSubjectInput] = useState('');
  const [lookupSubject, setLookupSubject] = useState<string | null>(null);
  const trimmedSubject = lookupSubject?.trim() ?? '';
  const isCsvMode = Boolean(csvInfo?.token);

  const subjectDetailQuery = useSubjectIdentifierDetail(!isCsvMode && trimmedSubject.length ? trimmedSubject : null);

  const [selectedIdTypeId, setSelectedIdTypeId] = useState<number | null>(null);
  const [manualIdentifierValue, setManualIdentifierValue] = useState('');
  const [manualDirty, setManualDirty] = useState(false);
  const [showDeleteModal, setShowDeleteModal] = useState(false);

  const fields = fieldsQuery.data;
  const idTypeOptions = useMemo(
    () =>
      (fields?.idTypes ?? [])
        .map((entry) => ({ value: String(entry.id), label: entry.name, description: entry.description ?? undefined }))
        .sort((a, b) => a.label.localeCompare(b.label)),
    [fields?.idTypes],
  );

  useEffect(() => {
    if (!fields?.idTypes?.length) return;
    if (selectedIdTypeId !== null) return;
    setSelectedIdTypeId(fields.idTypes[0].id);
  }, [fields?.idTypes, selectedIdTypeId]);

  const columnOptions = useMemo(
    () => buildColumnOptions(csvInfo?.columns ?? []),
    [csvInfo?.columns],
  );


  useEffect(() => {
    if (!csvInfo?.columns?.length) return;
    setSubjectField((current) => {
      if (current.column) return current;
      const auto = csvInfo.columns.find((column) => column.toLowerCase() === 'subject_code');
      return auto ? { ...current, column: auto } : current;
    });
    setIdentifierField((current) => {
      if (current.column) return current;
      const auto = csvInfo.columns.find((column) => column.toLowerCase().includes('identifier'));
      return auto ? { ...current, column: auto } : current;
    });
  }, [csvInfo?.columns]);

  useEffect(() => {
    if (isCsvMode) return;
    if (manualDirty) return;
    if (!subjectDetailQuery.data) {
      setManualIdentifierValue('');
      return;
    }
    if (selectedIdTypeId === null) return;
    const target = subjectDetailQuery.data.identifiers.find((item) => item.idTypeId === selectedIdTypeId);
    setManualIdentifierValue(target?.identifierValue ?? '');
  }, [isCsvMode, manualDirty, selectedIdTypeId, subjectDetailQuery.data]);

  const handleUpload = useCallback(async (file: File | null) => {
    if (!file) return;
    try {
      setUploading(true);
      const form = new FormData();
      form.append('file', file);
      const response = await apiClient.postForm<UploadedCsvInfo>('/uploads/csv', form);
      setCsvInfo(response);
      setPreview(null);
      notifications.show({ color: 'teal', message: `Uploaded ${response.filename}` });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to upload CSV.';
      notifications.show({ color: 'red', message });
    } finally {
      setUploading(false);
    }
  }, []);

  const handleClearCsv = useCallback(() => {
    setCsvInfo(null);
    setSubjectField({});
    setIdentifierField({});
    setImportMode('append');
    setPreview(null);
  }, []);

  const buildPayload = useCallback(
    (dryRun: boolean): SubjectIdentifierImportPayload | null => {
      if (!csvInfo?.token && !csvInfo?.filename) {
        notifications.show({ color: 'yellow', message: 'Upload a CSV file first.' });
        return null;
      }
      if (!subjectField.column) {
        notifications.show({ color: 'red', message: 'Select the subject code column.' });
        return null;
      }
      if (!identifierField.column) {
        notifications.show({ color: 'red', message: 'Select the identifier value column.' });
        return null;
      }
      if (selectedIdTypeId === null) {
        notifications.show({ color: 'red', message: 'Select an identifier type.' });
        return null;
      }
      const idTypeId = selectedIdTypeId;
      const payload: SubjectIdentifierImportPayload = {
        fileToken: csvInfo?.token ?? undefined,
        subjectField: { column: subjectField.column },
        identifierField: { column: identifierField.column },
        staticIdTypeId: idTypeId,
        options: { mode: importMode },
      };
      if (dryRun) payload.dryRun = true;
      return payload;
    },
    [csvInfo, subjectField.column, identifierField.column, selectedIdTypeId, importMode],
  );

  const handlePreview = useCallback(async () => {
    const payload = buildPayload(false);
    if (!payload) return;
    try {
      const result = await previewMutation.mutateAsync(payload);
      setPreview(result);
      notifications.show({ color: 'teal', message: 'Preview generated.' });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Preview failed.';
      notifications.show({ color: 'red', message });
    }
  }, [buildPayload, previewMutation]);

  const handleApply = useCallback(
    async (dryRun: boolean) => {
      const payload = buildPayload(dryRun);
      if (!payload) return;
      try {
        const result = await applyMutation.mutateAsync(payload);
        const summary = `Inserted ${result.identifiersInserted}, updated ${result.identifiersUpdated}, skipped ${result.identifiersSkipped}.`;
        notifications.show({ color: dryRun ? 'blue' : 'teal', message: dryRun ? `Dry run: ${summary}` : summary });
        if (!dryRun) {
          setPreview(null);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Import failed.';
        notifications.show({ color: 'red', message });
      }
    },
    [applyMutation, buildPayload],
  );

  const handleManualSubmit = useCallback(
    async (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      if (!trimmedSubject.length || selectedIdTypeId === null) {
        notifications.show({ color: 'red', message: 'Select subject and identifier type.' });
        return;
      }
      const value = manualIdentifierValue.trim();
      if (!value.length) {
        notifications.show({ color: 'red', message: 'Enter an identifier value.' });
        return;
      }
      try {
        await upsertMutation.mutateAsync({
          subjectCode: trimmedSubject,
          idTypeId: selectedIdTypeId,
          identifierValue: value,
        });
        notifications.show({ color: 'teal', message: 'Identifier saved.' });
        setManualDirty(false);
        setManualIdentifierValue('');
        void subjectDetailQuery.refetch();
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Failed to save identifier.';
        notifications.show({ color: 'red', message });
      }
    },
    [manualIdentifierValue, selectedIdTypeId, trimmedSubject, upsertMutation, subjectDetailQuery],
  );

  const handleDelete = useCallback(async () => {
    if (!trimmedSubject.length || selectedIdTypeId === null) return;
    try {
      await deleteMutation.mutateAsync({ subjectCode: trimmedSubject, idTypeId: selectedIdTypeId });
      notifications.show({ color: 'teal', message: 'Identifier removed.' });
      setManualDirty(false);
      setManualIdentifierValue('');
      void subjectDetailQuery.refetch();
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to delete identifier.';
      notifications.show({ color: 'red', message });
    } finally {
      setShowDeleteModal(false);
    }
  }, [deleteMutation, selectedIdTypeId, subjectDetailQuery, trimmedSubject]);

  const manualDetails = subjectDetailQuery.data?.identifiers ?? [];
  const selectedDetail = manualDetails.find((detail) => detail.idTypeId === selectedIdTypeId);
  const canDelete = Boolean(selectedDetail?.identifierValue);
  const subjectExists = subjectDetailQuery.data?.subjectExists ?? false;
  const isLoadingDetail = subjectDetailQuery.isFetching || subjectDetailQuery.isLoading;

  const isCsvReady = Boolean(
    csvInfo?.token &&
      subjectField.column &&
      identifierField.column &&
      selectedIdTypeId !== null &&
      !previewMutation.isPending &&
      !applyMutation.isPending,
  );

  const manualDisabled = isCsvMode;

  return (
    <Stack gap="md">
      <Stack gap={4}>
        <Text fw={600} size="sm">
          Subject Identifier Import
        </Text>
        <Text size="xs" c="dimmed">
          Upload a CSV to batch update identifier values or use the same controls for manual edits when no file is selected.
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

      <Stack gap="sm">
        {isCsvMode ? (
          <Select
            label="Subject Code Column"
            placeholder="Select column"
            data={columnOptions}
            value={subjectField.column ?? null}
            onChange={(value) => setSubjectField({ column: value ?? undefined })}
            withAsterisk
            clearable
            disabled={!csvInfo}
          />
        ) : (
          <TextInput
            label="Subject Code"
            placeholder="Enter subject code"
            value={subjectInput}
            onChange={(event) => setSubjectInput(event.currentTarget.value)}
            onBlur={(event) => {
              const normalized = event.currentTarget.value.trim();
              if (!normalized.length) {
                setSubjectInput('');
                setLookupSubject(null);
                setManualDirty(false);
                setManualIdentifierValue('');
                return;
              }
              setSubjectInput(normalized);
              setLookupSubject(normalized);
              setManualDirty(false);
            }}
            description="Leave blank to start a new identifier entry. Move focus away to load existing values."
          />
        )}

        {isCsvMode ? (
          <Select
            label="Identifier Value Column"
            placeholder="Select column"
            data={columnOptions}
            value={identifierField.column ?? null}
            onChange={(value) => setIdentifierField({ column: value ?? undefined })}
            withAsterisk
            clearable
            disabled={!csvInfo}
          />
        ) : (
          <TextInput
            label="Identifier Value"
            placeholder={trimmedSubject.length ? 'Enter identifier value' : 'Lookup a subject first'}
            value={manualIdentifierValue}
            onChange={(event) => {
              setManualDirty(true);
              setManualIdentifierValue(event.currentTarget.value);
            }}
            disabled={manualDisabled || !trimmedSubject.length || selectedIdTypeId === null}
          />
        )}

        <Select
          label="Identifier Type"
          placeholder={idTypeOptions.length ? 'Select identifier type' : 'No identifier types available'}
          data={idTypeOptions}
          value={selectedIdTypeId !== null ? String(selectedIdTypeId) : null}
          onChange={(value) => {
            const nextId = value ? Number(value) : null;
            if (nextId === null) return;
            setSelectedIdTypeId(nextId);
            setManualDirty(false);
            if (!isCsvMode) {
              const match = manualDetails.find((detail) => detail.idTypeId === nextId);
              setManualIdentifierValue(match?.identifierValue ?? '');
            }
          }}
          disabled={idTypeOptions.length === 0}
          searchable
          withAsterisk
        />

        <Select
          label="Import Mode"
          data={importModeOptions}
          value={importMode}
          onChange={(value) => setImportMode((value as 'append' | 'replace') ?? 'append')}
          disabled={!isCsvMode}
        />
      </Stack>

      <Group gap="sm">
        <Button disabled={!isCsvReady} loading={previewMutation.isPending} onClick={handlePreview}>
          Preview
        </Button>
        <Button
          color="teal"
          disabled={!isCsvReady}
          loading={applyMutation.isPending}
          onClick={() => handleApply(false)}
        >
          Apply
        </Button>
        <Button
          variant="light"
          color="gray"
          disabled={!isCsvReady}
          loading={applyMutation.isPending}
          onClick={() => handleApply(true)}
        >
          Dry Run
        </Button>
      </Group>

      {preview ? (
        <Stack gap="sm">
          <Text fw={600} size="sm">
            Preview — Subject Identifiers
          </Text>
              <Table striped highlightOnHover>
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th>Subject Code</Table.Th>
                    <Table.Th>Identifier Type</Table.Th>
                    <Table.Th>Value</Table.Th>
                    <Table.Th>Subject Exists</Table.Th>
                    <Table.Th>Already Matches</Table.Th>
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {preview.rows.map((row, idx) => (
                    <Table.Tr key={`identifier-preview-${idx}`}>
                      <Table.Td>{row.subjectCode}</Table.Td>
                      <Table.Td>{row.idTypeName ?? row.idTypeId ?? '—'}</Table.Td>
                      <Table.Td>{row.identifierValue ?? '—'}</Table.Td>
                      <Table.Td>{row.subjectExists ? 'Yes' : 'No'}</Table.Td>
                      <Table.Td>{row.existingValue ? 'Yes' : 'No'}</Table.Td>
                    </Table.Tr>
                  ))}
                </Table.Tbody>
              </Table>
              {preview.warnings.length ? (
                <Alert color="yellow" title="Warnings">
                  <Stack gap={4}>
                    {preview.warnings.map((warning, idx) => (
                      <Text key={`identifier-warning-${idx}`} size="sm">
                        {warning}
                      </Text>
                    ))}
                  </Stack>
                </Alert>
              ) : null}
        </Stack>
      ) : null}

      {!isCsvMode ? (
        <Stack gap="md">
          {isLoadingDetail ? (
            <Group gap="xs">
              <Loader size="sm" />
              <Text size="sm">Loading subject identifiers…</Text>
            </Group>
          ) : null}

          {trimmedSubject.length ? (
            <Stack gap="sm">
              {!subjectExists ? (
                <Alert color="yellow" title="Subject not found">
                  <Text size="sm">
                    The subject does not exist yet. Saving will create identifier records once the subject is added.
                  </Text>
                </Alert>
              ) : null}
              <Table striped highlightOnHover>
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th>Identifier Type</Table.Th>
                    <Table.Th>Value</Table.Th>
                    <Table.Th>Updated</Table.Th>
                    <Table.Th style={{ width: 100 }}>Actions</Table.Th>
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {manualDetails.map((detail) => (
                    <Table.Tr
                      key={detail.idTypeId}
                      style={{ backgroundColor: detail.idTypeId === selectedIdTypeId ? 'var(--mantine-color-blue-light)' : undefined }}
                    >
                      <Table.Td>{detail.idTypeName}</Table.Td>
                      <Table.Td>{detail.identifierValue ?? <Text c="dimmed" size="sm">—</Text>}</Table.Td>
                      <Table.Td>{detail.updatedAt ?? '—'}</Table.Td>
                      <Table.Td>
                        <Group gap="xs">
                          <ActionIcon
                            variant="subtle"
                            color="blue"
                            onClick={() => {
                              setSelectedIdTypeId(detail.idTypeId);
                              setManualDirty(false);
                              setManualIdentifierValue(detail.identifierValue ?? '');
                            }}
                            aria-label="Edit"
                          >
                            <IconPencil size={16} />
                          </ActionIcon>
                          <ActionIcon
                            variant="subtle"
                            color="red"
                            onClick={() => {
                              setSelectedIdTypeId(detail.idTypeId);
                              setManualDirty(false);
                              setManualIdentifierValue(detail.identifierValue ?? '');
                              setShowDeleteModal(true);
                            }}
                            aria-label="Delete"
                            disabled={!detail.identifierValue}
                          >
                            <IconTrash size={16} />
                          </ActionIcon>
                        </Group>
                      </Table.Td>
                    </Table.Tr>
                  ))}
                </Table.Tbody>
              </Table>

              <form onSubmit={handleManualSubmit}>
                <Stack gap="sm" maw={420}>
                  <Select
                    label="Identifier Type"
                    placeholder="Select identifier type"
                    data={manualDetails.map((detail) => ({ value: String(detail.idTypeId), label: detail.idTypeName }))}
                    value={selectedIdTypeId !== null ? String(selectedIdTypeId) : null}
                    onChange={(value) => {
                      const nextId = value ? Number(value) : null;
                      if (nextId === null) return;
                      setSelectedIdTypeId(nextId);
                      setManualDirty(false);
                      const match = manualDetails.find((detail) => detail.idTypeId === nextId);
                      setManualIdentifierValue(match?.identifierValue ?? '');
                    }}
                    searchable
                  />
                  <TextInput
                    label="Identifier Value"
                    placeholder="Enter identifier value"
                    value={manualIdentifierValue}
                    onChange={(event) => {
                      setManualDirty(true);
                      setManualIdentifierValue(event.currentTarget.value);
                    }}
                    disabled={selectedIdTypeId === null}
                  />
                  <Group gap="sm">
                    <Button
                      type="submit"
                      disabled={selectedIdTypeId === null || !manualIdentifierValue.trim().length}
                      loading={upsertMutation.isPending}
                    >
                      Save Identifier
                    </Button>
                    <Button
                      type="button"
                      variant="light"
                      color="red"
                      disabled={!canDelete}
                      onClick={() => setShowDeleteModal(true)}
                    >
                      Remove
                    </Button>
                  </Group>
                </Stack>
              </form>
            </Stack>
          ) : null}
        </Stack>
      ) : null}

      <Modal opened={showDeleteModal} onClose={() => setShowDeleteModal(false)} title="Delete identifier" centered>
        <Stack gap="md">
          <Text size="sm">This will remove the identifier value for the selected type. Continue?</Text>
          <Group gap="sm" justify="flex-end">
            <Button variant="light" color="gray" onClick={() => setShowDeleteModal(false)} disabled={deleteMutation.isPending}>
              Cancel
            </Button>
            <Button color="red" onClick={handleDelete} loading={deleteMutation.isPending}>
              Delete
            </Button>
          </Group>
        </Stack>
      </Modal>
    </Stack>
  );
};

export default SubjectOtherIdentifierImportTab;
