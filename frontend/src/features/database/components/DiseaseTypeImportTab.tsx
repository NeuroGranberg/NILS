import { useCallback, useState } from 'react';
import {
  Alert,
  Button,
  Group,
  Loader,
  NumberInput,
  Select,
  Stack,
  Text,
  TextInput,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';

import {
  type DiseaseTypeDetail,
  type DiseaseTypeUpsertPayload,
  useDiseaseTypeFields,
  useDiseaseTypeUpsert,
} from '../api';
import { apiClient, ApiError } from '../../../utils/api-client';

const DiseaseTypeImportTab = () => {
  const fieldsQuery = useDiseaseTypeFields();
  const upsertMutation = useDiseaseTypeUpsert();

  const [lookupId, setLookupId] = useState('');
  const [lookupLoading, setLookupLoading] = useState(false);
  const [diseaseId, setDiseaseId] = useState<string | null>(null);
  const [typeName, setTypeName] = useState('');
  const [description, setDescription] = useState('');
  const [sortOrder, setSortOrder] = useState<number | ''>('');

  const diseases = fieldsQuery.data?.diseases ?? [];
  const diseaseOptions = diseases.map((d) => ({
    value: String(d.id),
    label: d.code ? `${d.name} (${d.code})` : d.name,
  }));

  const hasData = diseaseId !== null && typeName.trim().length > 0;

  const resetForm = useCallback(() => {
    setLookupId('');
    setDiseaseId(null);
    setTypeName('');
    setDescription('');
    setSortOrder('');
  }, []);

  const mapDetailToForm = useCallback((detail: DiseaseTypeDetail | null) => {
    if (detail) {
      setDiseaseId(String(detail.diseaseId));
      setTypeName(detail.typeName);
      setDescription(detail.description ?? '');
      setSortOrder(detail.sortOrder ?? '');
    } else {
      setDiseaseId(null);
      setTypeName('');
      setDescription('');
      setSortOrder('');
    }
  }, []);

  const lookupExisting = useCallback(
    async (options?: { silent?: boolean }) => {
      const silent = options?.silent ?? false;
      const id = lookupId.trim();
      if (!id) {
        if (!silent) notifications.show({ color: 'yellow', message: 'Enter a disease type ID to load.' });
        return;
      }
      try {
        setLookupLoading(true);
        const detail = await apiClient.get<DiseaseTypeDetail>(
          `/metadata/imports/disease-types/${encodeURIComponent(id)}`,
        );
        mapDetailToForm(detail);
        if (!silent) notifications.show({ color: 'teal', message: `Loaded disease type ${id}.` });
      } catch (error) {
        if (error instanceof ApiError && error.status === 404) {
          mapDetailToForm(null);
          if (!silent) notifications.show({ color: 'blue', message: `ID ${id} not found. A new record will be created.` });
        } else if (!silent) {
          const message = error instanceof Error ? error.message : 'Failed to load disease type.';
          notifications.show({ color: 'red', message });
        }
      } finally {
        setLookupLoading(false);
      }
    },
    [lookupId, mapDetailToForm],
  );

  const handleSubmit = async (dryRun: boolean) => {
    if (!hasData) {
      notifications.show({ color: 'red', message: 'Select a disease and enter a type name.' });
      return;
    }
    const payload: DiseaseTypeUpsertPayload = {
      diseaseId: Number(diseaseId),
      typeName: typeName.trim(),
      ...(lookupId.trim() ? { diseaseTypeId: Number(lookupId.trim()) } : {}),
      ...(description.trim() ? { description: description.trim() } : {}),
      ...(sortOrder !== '' ? { sortOrder: Number(sortOrder) } : {}),
      ...(dryRun ? { dryRun: true } : {}),
    };
    try {
      const result = await upsertMutation.mutateAsync(payload);
      const action = result.inserted ? 'Created' : result.updated ? 'Updated' : 'No changes to';
      const msg = `${action} disease type (ID: ${result.diseaseTypeId})`;
      notifications.show({ color: dryRun ? 'blue' : 'teal', message: dryRun ? `Dry run: ${msg}` : msg });
      if (!dryRun && result.inserted) {
        setLookupId(String(result.diseaseTypeId));
      }
    } catch {
      // handled by mutation hook
    }
  };

  const isBusy = lookupLoading || upsertMutation.isPending;

  return (
    <Stack gap="md">
      {fieldsQuery.isLoading ? (
        <Group gap="xs">
          <Loader size="sm" />
          <Text size="sm">Loading disease type configuration...</Text>
        </Group>
      ) : null}

      {fieldsQuery.isError ? (
        <Alert color="red" title="Failed to load disease type metadata">
          {(fieldsQuery.error as Error)?.message ?? 'Unable to load configuration.'}
        </Alert>
      ) : null}

      {fieldsQuery.data ? (
        <Stack gap="md">
          <Stack gap={4}>
            <Text fw={600} size="sm">Disease Type Fields</Text>
            <Text size="xs" c="dimmed">
              Enter an ID to load an existing disease type for editing, or select a disease and type name to create new.
            </Text>
          </Stack>

          <Group gap="sm" align="flex-end">
            <TextInput
              label="Disease Type ID"
              placeholder="Enter ID to load"
              value={lookupId}
              onChange={(e) => setLookupId(e.currentTarget.value)}
              onBlur={() => { if (lookupId.trim()) void lookupExisting(); }}
              onKeyDown={(e) => { if (e.key === 'Enter' && lookupId.trim()) void lookupExisting(); }}
              w={160}
            />
            <Button variant="subtle" size="xs" onClick={resetForm} disabled={isBusy}>
              Clear
            </Button>
          </Group>

          <Select
            label="Disease"
            placeholder="Select disease"
            data={diseaseOptions}
            value={diseaseId}
            onChange={(value) => setDiseaseId(value)}
            searchable
            withAsterisk
          />

          <TextInput
            label="Type Name"
            placeholder="e.g. RRMS"
            value={typeName}
            onChange={(e) => setTypeName(e.currentTarget.value)}
            withAsterisk
          />

          <TextInput
            label="Description"
            placeholder="Optional description"
            value={description}
            onChange={(e) => setDescription(e.currentTarget.value)}
          />

          <NumberInput
            label="Sort Order"
            placeholder="Optional"
            value={sortOrder}
            onChange={(value) => setSortOrder(typeof value === 'number' ? value : '')}
            min={0}
            w={160}
          />

          <Group gap="sm">
            <Button onClick={() => handleSubmit(false)} disabled={isBusy || !hasData} loading={upsertMutation.isPending}>
              Submit
            </Button>
            <Button variant="outline" onClick={() => handleSubmit(true)} disabled={isBusy || !hasData} loading={upsertMutation.isPending}>
              Dry run
            </Button>
          </Group>
        </Stack>
      ) : null}
    </Stack>
  );
};

export default DiseaseTypeImportTab;
