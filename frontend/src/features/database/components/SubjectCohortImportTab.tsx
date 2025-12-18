import { useCallback, useEffect, useMemo, useState } from 'react';
import { ActionIcon, Alert, Badge, Button, FileButton, Group, Loader, Select, Stack, Table, Text, TextInput } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { IconPlus, IconTrash, IconUpload } from '@tabler/icons-react';

import {
  type SubjectCohortImportPayload,
  type SubjectImportFieldMapping,
  type SubjectCohortImportPreview,
  useSubjectCohortImportPreview,
  useSubjectCohortImportApply,
  useSubjectCohortMemberships,
  useDeleteSubjectCohortMembership,
  useMetadataCohorts,
} from '../api';
import { apiClient } from '../../../utils/api-client';

type UploadedCsvInfo = {
  token: string;
  filename: string;
  columns: string[];
};

const membershipModeOptions = [
  { value: 'append', label: 'Append (keep existing)' },
  { value: 'replace', label: 'Replace (overwrite listed subjects)' },
];

const escapeCsvValue = (value: string | number | boolean | null | undefined): string => {
  if (value === null || value === undefined) return '';
  const stringValue = String(value);
  if (stringValue.includes('"') || stringValue.includes(',') || stringValue.includes('\n')) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }
  return stringValue;
};

const SubjectCohortImportTab = () => {
  const [csvInfo, setCsvInfo] = useState<UploadedCsvInfo | null>(null);
  const [uploading, setUploading] = useState(false);
  const [subjectField, setSubjectField] = useState<SubjectImportFieldMapping>({});
  const [subjectInput, setSubjectInput] = useState('');
  const [csvCohortName, setCsvCohortName] = useState<string | null>(null);
  const [membershipMode, setMembershipMode] = useState<'append' | 'replace'>('append');
  const [preview, setPreview] = useState<SubjectCohortImportPreview | null>(null);
  const [manualSelectedCohort, setManualSelectedCohort] = useState<string | null>(null);
  const [manualSubmitting, setManualSubmitting] = useState(false);

  const previewMutation = useSubjectCohortImportPreview();
  const applyMutation = useSubjectCohortImportApply();
  const cohortsQuery = useMetadataCohorts();

  const isCsvMode = Boolean(csvInfo?.token);
  const trimmedSubject = (isCsvMode ? '' : subjectInput).trim();
  const membershipsQuery = useSubjectCohortMemberships(!isCsvMode && trimmedSubject.length ? trimmedSubject : null);
  const deleteMembership = useDeleteSubjectCohortMembership();

  const columnOptions = useMemo(
    () => (csvInfo?.columns ?? []).map((column) => ({ value: column, label: column })),
    [csvInfo?.columns],
  );

  const cohortOptions = useMemo(
    () =>
      (cohortsQuery.data ?? [])
        .map((cohort) => ({ value: cohort.name, label: cohort.name }))
        .sort((a, b) => a.label.localeCompare(b.label)),
    [cohortsQuery.data],
  );
  const cohortsError = cohortsQuery.isError
    ? cohortsQuery.error instanceof Error
      ? cohortsQuery.error.message
      : 'Failed to load cohorts.'
    : null;

  useEffect(() => {
    if (!csvInfo?.columns?.length) return;
    setSubjectField((current) => {
      if (current.column) return current;
      const auto = csvInfo.columns.find((column) => column.toLowerCase() === 'subject_code');
      return auto ? { ...current, column: auto } : current;
    });
  }, [csvInfo?.columns]);

  const handleUpload = useCallback(async (file: File | null) => {
    if (!file) return;
    try {
      setUploading(true);
      const form = new FormData();
      form.append('file', file);
      const response = await apiClient.postForm<UploadedCsvInfo>('/uploads/csv', form);
      setCsvInfo(response);
      setPreview(null);
    setSubjectField((current) => ({ ...current, column: undefined }));
    setSubjectInput('');
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
    setCsvCohortName(null);
    setPreview(null);
    setSubjectInput('');
  }, []);

  const buildPayload = useCallback(
    (dryRun: boolean): SubjectCohortImportPayload | null => {
      if (!csvInfo?.token) {
        notifications.show({ color: 'yellow', message: 'Upload a CSV file first.' });
        return null;
      }
      if (!subjectField.column) {
        notifications.show({ color: 'red', message: 'Map the subject code column.' });
        return null;
      }
      if (!csvCohortName) {
        notifications.show({ color: 'red', message: 'Select a cohort to assign.' });
        return null;
      }
      const payload: SubjectCohortImportPayload = {
        fileToken: csvInfo.token,
        subjectField: { column: subjectField.column },
        staticCohortName: csvCohortName,
        options: { membershipMode },
      };
      if (dryRun) payload.dryRun = true;
      return payload;
    },
    [csvInfo?.token, subjectField.column, csvCohortName, membershipMode],
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
        const summary = `Subject/cohort submission inserted ${result.membershipsInserted} memberships, ${result.membershipsExisting} already existed.`;
        notifications.show({ color: dryRun ? 'blue' : 'teal', message: dryRun ? `Dry run: ${summary}` : summary });
        if (!dryRun) {
          setPreview(null);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Submission failed.';
        notifications.show({ color: 'red', message });
      }
    },
    [applyMutation, buildPayload],
  );

  const preparedManualCsv = useCallback(async (subjectCode: string) => {
    const content = `subject_code\n${escapeCsvValue(subjectCode)}\n`;
    const file = new File([content], 'manual-subject-cohort.csv', { type: 'text/csv' });
    const form = new FormData();
    form.append('file', file);
    return apiClient.postForm<UploadedCsvInfo>('/uploads/csv', form);
  }, []);

  const handleManualAdd = useCallback(
    async (dryRun: boolean) => {
      const subjectCode = trimmedSubject;
      const cohortName = manualSelectedCohort?.trim() ?? '';
      if (!subjectCode) {
        notifications.show({ color: 'red', message: 'Enter a subject code first.' });
        return;
      }
      if (!cohortName) {
        notifications.show({ color: 'red', message: 'Select a cohort to assign.' });
        return;
      }
      try {
        setManualSubmitting(true);
        const uploaded = await preparedManualCsv(subjectCode);
        const payload: SubjectCohortImportPayload = {
          fileToken: uploaded.token,
          subjectField: { column: 'subject_code' },
          staticCohortName: cohortName,
          options: { membershipMode: 'append' },
        };
        if (dryRun) payload.dryRun = true;
        const result = await applyMutation.mutateAsync(payload);
        const summary = `Membership inserted ${result.membershipsInserted}, ${result.membershipsExisting} already existed.`;
        notifications.show({ color: dryRun ? 'blue' : 'teal', message: dryRun ? `Dry run: ${summary}` : summary });
        if (!dryRun) {
          setManualSelectedCohort(null);
          void membershipsQuery.refetch();
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Failed to add membership.';
        notifications.show({ color: 'red', message });
      } finally {
        setManualSubmitting(false);
      }
    },
    [applyMutation, manualSelectedCohort, membershipsQuery, preparedManualCsv, trimmedSubject],
  );

  const handleDeleteMembership = useCallback(
    async (cohortId: number, cohortName: string) => {
      if (!trimmedSubject) return;
      try {
        await deleteMembership.mutateAsync({ subjectCode: trimmedSubject, cohortId, cohortName });
        notifications.show({ color: 'teal', message: `Removed ${cohortName} from ${trimmedSubject}.` });
        void membershipsQuery.refetch();
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Failed to remove membership.';
        notifications.show({ color: 'red', message });
      }
    },
    [deleteMembership, membershipsQuery, trimmedSubject],
  );

  const isCsvReady = Boolean(
    csvInfo?.token && subjectField.column && csvCohortName && !previewMutation.isPending && !applyMutation.isPending,
  );

  const manualMemberships = membershipsQuery.data?.memberships ?? [];
  const manualActionDisabled =
    manualSubmitting ||
    !manualSelectedCohort ||
    !trimmedSubject ||
    isCsvMode ||
    cohortOptions.length === 0 ||
    Boolean(cohortsError);

  return (
    <Stack gap="md">
      <Stack gap={4}>
        <Text fw={600} size="sm">
          Subject Cohort Import
        </Text>
        <Text size="xs" c="dimmed">
          Upload a CSV and map columns, or leave CSV empty to manage a single subject's cohorts.
        </Text>
      </Stack>

      {cohortsError ? (
        <Alert color="red" title="Unable to load cohorts">
          <Text size="sm">{cohortsError}</Text>
        </Alert>
      ) : null}
      {!cohortsError && !cohortsQuery.isLoading && cohortOptions.length === 0 ? (
        <Alert color="yellow" title="No cohorts available">
          <Text size="sm">Create a cohort before importing subject memberships.</Text>
        </Alert>
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

      <Stack gap={4}>
        <Text fw={600} size="sm">
          Subject & Cohort Selection
        </Text>
        <Text size="xs" c="dimmed">
          {isCsvMode
            ? 'Choose the subject code column, cohort, and membership mode for this CSV.'
            : 'Lookup a subject to review memberships, or add/remove cohorts.'}
        </Text>
      </Stack>

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
                return;
              }
              setSubjectInput(normalized);
              void membershipsQuery.refetch();
            }}
            description="Leave blank to start a new membership entry. When a subject exists, current cohorts will load below."
          />
        )}
        <Group gap="sm" align="flex-end" wrap="wrap">
          <Select
            label={isCsvMode ? 'Target Cohort' : 'Add Cohort'}
            placeholder={cohortOptions.length ? 'Select cohort' : cohortsError ? 'Retry after resolving error' : 'No cohorts available'}
            data={cohortOptions}
            value={isCsvMode ? csvCohortName : manualSelectedCohort}
            onChange={isCsvMode ? setCsvCohortName : setManualSelectedCohort}
            searchable
            nothingFoundMessage="No cohorts"
            disabled={cohortOptions.length === 0 || Boolean(cohortsError)}
          />
          <Select
            label="Membership Mode"
            data={membershipModeOptions}
            value={membershipMode}
            onChange={(value) => setMembershipMode((value as 'append' | 'replace') ?? 'append')}
            disabled={!isCsvMode}
          />
        </Group>
      </Stack>

      <Group gap="sm">
        <Button disabled={!isCsvReady} onClick={() => handlePreview()} loading={previewMutation.isPending}>
          Preview CSV
        </Button>
        <Button
          color="teal"
          disabled={!isCsvReady}
          onClick={() => handleApply(false)}
          loading={applyMutation.isPending}
        >
          Submit CSV
        </Button>
        <Button
          variant="light"
          color="gray"
          disabled={!isCsvReady}
          onClick={() => handleApply(true)}
          loading={applyMutation.isPending}
        >
          CSV Dry Run
        </Button>
      </Group>

      {preview ? (
        <Stack gap="sm">
          <Text fw={600} size="sm">
            Preview — Subject Cohorts
          </Text>
              <Table striped highlightOnHover captionSide="top">
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th>Subject Code</Table.Th>
                    <Table.Th>Cohort</Table.Th>
                    <Table.Th>Subject Exists</Table.Th>
                    <Table.Th>Cohort Exists</Table.Th>
                    <Table.Th>Already Member</Table.Th>
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {preview.rows.map((row, index) => (
                    <Table.Tr key={`subject-cohort-preview-${index}`}>
                      <Table.Td>{row.subjectCode}</Table.Td>
                      <Table.Td>{row.cohortName}</Table.Td>
                      <Table.Td>{row.subjectExists ? 'Yes' : 'No'}</Table.Td>
                      <Table.Td>{row.cohortExists ? 'Yes' : 'No'}</Table.Td>
                      <Table.Td>{row.alreadyMember ? 'Yes' : 'No'}</Table.Td>
                    </Table.Tr>
                  ))}
                </Table.Tbody>
              </Table>
              {preview.warnings.length ? (
                <Alert color="yellow" title="Warnings">
                  <Stack gap={4}>
                    {preview.warnings.map((warning, idx) => (
                      <Text key={`subject-cohort-warning-${idx}`} size="sm">
                        {warning}
                      </Text>
                    ))}
                  </Stack>
                </Alert>
              ) : null}
        </Stack>
      ) : null}

      {isCsvMode ? null : (
        <Stack gap="md">
          <Stack gap={4}>
            <Group justify="space-between" align="center">
              <Text fw={600} size="sm">Current Cohorts</Text>
              {membershipsQuery.isLoading || membershipsQuery.isRefetching ? <Loader size="sm" /> : null}
            </Group>
          </Stack>

          {membershipsQuery.isError ? (
            <Alert color="red" title="Unable to load memberships">
              <Text size="sm">
                {membershipsQuery.error instanceof Error
                  ? membershipsQuery.error.message
                  : 'Failed to fetch cohort memberships.'}
              </Text>
            </Alert>
          ) : null}
          {!membershipsQuery.isError && trimmedSubject.length === 0 ? (
            <Alert color="blue" title="Manual update">
              <Text size="sm">Enter a subject code to manage cohort memberships individually.</Text>
            </Alert>
          ) : null}
          {!membershipsQuery.isError && trimmedSubject.length > 0 ? (
            manualMemberships.length === 0 ? (
              <Text size="sm" c="dimmed">
                No cohorts linked to this subject.
              </Text>
            ) : (
              <Group gap="xs">
                {manualMemberships.map((membership) => (
                  <Badge
                    key={membership.cohortId}
                    rightSection={
                      <ActionIcon
                        size="xs"
                        color="red"
                        variant="subtle"
                        onClick={() => handleDeleteMembership(membership.cohortId, membership.cohortName)}
                        aria-label={`Remove ${membership.cohortName}`}
                      >
                        <IconTrash size={12} />
                      </ActionIcon>
                    }
                  >
                    {membership.cohortName}
                  </Badge>
                ))}
              </Group>
            )
          ) : null}

          <Group gap="sm">
            <Button
              leftSection={<IconPlus size={16} />}
              disabled={manualActionDisabled}
              onClick={() => handleManualAdd(false)}
              loading={manualSubmitting}
            >
              Add Cohort
            </Button>
            <Button
              variant="light"
              color="gray"
              disabled={manualActionDisabled}
              onClick={() => handleManualAdd(true)}
              loading={manualSubmitting}
            >
              Manual Dry Run
            </Button>
          </Group>
        </Stack>
      )}
    </Stack>
  );
};

export default SubjectCohortImportTab;
