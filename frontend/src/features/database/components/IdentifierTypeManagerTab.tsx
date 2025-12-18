import { useState, type FormEvent } from 'react';
import {
  ActionIcon,
  Button,
  Group,
  Loader,
  Modal,
  Stack,
  Table,
  Text,
  TextInput,
  Tooltip,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { IconCheck, IconPencil, IconRefresh, IconTrash, IconX } from '@tabler/icons-react';

import {
  type IdTypeInfo,
  useIdTypes,
  useCreateIdType,
  useUpdateIdType,
  useDeleteIdType,
} from '../api';

type DeleteDialogState = {
  id: number;
  name: string;
} | null;

const toNullable = (value: string) => {
  const trimmed = value.trim();
  return trimmed.length ? trimmed : null;
};

const IdentifierTypeManagerTab = () => {
  const idTypesQuery = useIdTypes();
  const createMutation = useCreateIdType();
  const updateMutation = useUpdateIdType();
  const deleteMutation = useDeleteIdType();

  const [editingId, setEditingId] = useState<number | null>(null);
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [deleteDialog, setDeleteDialog] = useState<DeleteDialogState>(null);

  const items = idTypesQuery.data?.items ?? [];
  const isSubmitting = createMutation.isPending || updateMutation.isPending;
  const isDeleting = deleteMutation.isPending;

  const formModeLabel = editingId ? 'Update Identifier Type' : 'Add Identifier Type';

  const sortedItems = [...items].sort((a, b) => a.name.localeCompare(b.name));

  const resetForm = () => {
    setEditingId(null);
    setName('');
    setDescription('');
  };

  const handleEdit = (item: IdTypeInfo) => {
    setEditingId(item.id);
    setName(item.name);
    setDescription(item.description ?? '');
  };

  const handleSubmit = async (event: FormEvent) => {
    event.preventDefault();
    const trimmedName = name.trim();
    if (!trimmedName.length) {
      notifications.show({ color: 'red', message: 'Provide a name for the identifier type.' });
      return;
    }

    const payload = {
      name: trimmedName,
      description: toNullable(description),
    };

    try {
      if (editingId === null) {
        const created = await createMutation.mutateAsync(payload);
        notifications.show({ color: 'teal', message: `Created identifier type “${created.name}”.` });
      } else {
        const updated = await updateMutation.mutateAsync({ id: editingId, ...payload });
        notifications.show({ color: 'teal', message: `Updated identifier type “${updated.name}”.` });
      }
      resetForm();
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to save identifier type.';
      notifications.show({ color: 'red', message });
    }
  };

  const handleDelete = async () => {
    if (!deleteDialog) return;
    try {
      const result = await deleteMutation.mutateAsync({ id: deleteDialog.id });
      const suffix = result.identifiersDeleted
        ? ` Removed ${result.identifiersDeleted} linked identifier${result.identifiersDeleted === 1 ? '' : 's'}.`
        : '';
      notifications.show({ color: 'teal', message: `Deleted identifier type “${result.name}”.${suffix}` });
      if (editingId === deleteDialog.id) {
        resetForm();
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to delete identifier type.';
      notifications.show({ color: 'red', message });
    } finally {
      setDeleteDialog(null);
    }
  };

  return (
    <Stack gap="md">
      <form onSubmit={handleSubmit}>
        <Stack gap="sm">
          <Text fw={600} size="sm">
            {formModeLabel}
          </Text>
          <Group align="flex-end" gap="sm" wrap="wrap">
            <TextInput
              label="Name"
              placeholder="Identifier type name"
              value={name}
              onChange={(event) => setName(event.currentTarget.value)}
              required
              maw={320}
            />
            <TextInput
              label="Description"
              placeholder="Optional description"
              value={description}
              onChange={(event) => setDescription(event.currentTarget.value)}
              maw={360}
            />
            <Group gap="sm">
              <Button type="submit" leftSection={<IconCheck size={16} />} loading={isSubmitting}>
                {editingId === null ? 'Create' : 'Save'}
              </Button>
              <Button
                type="button"
                variant="light"
                color="gray"
                leftSection={<IconRefresh size={16} />}
                onClick={resetForm}
                disabled={isSubmitting && editingId === null}
              >
                Reset
              </Button>
            </Group>
          </Group>
          {editingId !== null ? (
            <Text size="xs" c="dimmed">
              Editing identifier type #{editingId}. Use Reset to switch back to creation mode.
            </Text>
          ) : null}
        </Stack>
      </form>

      <Stack gap={4}>
        <Group align="center" justify="space-between">
          <Text fw={600} size="sm">Existing Identifier Types</Text>
          {idTypesQuery.isRefetching ? <Loader size="sm" /> : null}
        </Group>
      </Stack>

      {idTypesQuery.isLoading ? (
        <Group gap="xs">
          <Loader size="sm" />
          <Text size="sm">Loading identifier types…</Text>
        </Group>
      ) : sortedItems.length === 0 ? (
        <Text size="sm" c="dimmed">
          No identifier types yet. Create one above to get started.
        </Text>
      ) : (
        <Table striped highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Name</Table.Th>
                <Table.Th>Description</Table.Th>
                <Table.Th style={{ width: 160 }}>Linked Identifiers</Table.Th>
                <Table.Th style={{ width: 120 }}>Actions</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {sortedItems.map((item) => (
                <Table.Tr key={item.id}>
                  <Table.Td>{item.name}</Table.Td>
                  <Table.Td>{item.description ?? <Text c="dimmed" size="sm">—</Text>}</Table.Td>
                  <Table.Td>{item.identifiersCount}</Table.Td>
                  <Table.Td>
                    <Group gap="xs">
                      <Tooltip label="Edit" withArrow>
                        <ActionIcon variant="subtle" color="blue" onClick={() => handleEdit(item)}>
                          <IconPencil size={16} />
                        </ActionIcon>
                      </Tooltip>
                      <Tooltip label="Delete" withArrow>
                        <ActionIcon
                          variant="subtle"
                          color="red"
                          onClick={() => setDeleteDialog({ id: item.id, name: item.name })}
                        >
                          <IconTrash size={16} />
                        </ActionIcon>
                      </Tooltip>
                    </Group>
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
        </Table>
      )}

      <Modal
        opened={deleteDialog !== null}
        onClose={() => setDeleteDialog(null)}
        title={`Delete identifier type${deleteDialog ? ` “${deleteDialog.name}”` : ''}`}
        centered
      >
        <Stack gap="md">
          <Text size="sm">
            This will remove the identifier type{deleteDialog ? ` “${deleteDialog.name}”` : ''} and cascade delete all
            linked subject identifiers. This action cannot be undone.
          </Text>
          <Group gap="sm" justify="flex-end">
            <Button
              variant="light"
              color="gray"
              leftSection={<IconX size={16} />}
              onClick={() => setDeleteDialog(null)}
              disabled={isDeleting}
            >
              Cancel
            </Button>
            <Button
              color="red"
              leftSection={<IconTrash size={16} />}
              onClick={handleDelete}
              loading={isDeleting}
            >
              Delete
            </Button>
          </Group>
        </Stack>
      </Modal>
    </Stack>
  );
};

export default IdentifierTypeManagerTab;
