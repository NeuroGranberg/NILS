import { useMemo, useState } from 'react';
import {
  ActionIcon,
  Badge,
  Button,
  Card,
  Checkbox,
  Group,
  Loader,
  Modal,
  ScrollArea,
  Stack,
  Table,
  Text,
  TextInput,
} from '@mantine/core';
import { useDisclosure } from '@mantine/hooks';
import { notifications } from '@mantine/notifications';
import { IconDatabaseImport, IconDatabasePlus, IconRestore, IconTrash } from '@tabler/icons-react';
import {
  useCreateDatabaseBackup,
  useDatabaseBackupsQuery,
  useRestoreDatabaseBackup,
  useDeleteDatabaseBackup,
  type DatabaseBackup,
  type DatabaseKey,
} from '../api';

const formatBytes = (value: number) => {
  if (value < 1024) return `${value} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let size = value / 1024;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(1)} ${units[unitIndex]}`;
};

const formatTimestamp = (value: string) => new Date(value).toLocaleString();

const DATABASE_DEFAULT_DIRECTORIES: Record<DatabaseKey, string> = {
  metadata: 'resource/backups/metadata',
  application: 'resource/backups/application',
};

export const BackupRestoreTab = () => {
  const backupsQuery = useDatabaseBackupsQuery();
  const createBackup = useCreateDatabaseBackup();
  const restoreBackup = useRestoreDatabaseBackup();
  const deleteBackup = useDeleteDatabaseBackup();
  
  const [modalOpened, { open: openModal, close: closeModal }] = useDisclosure(false);
  const [selectedDatabases, setSelectedDatabases] = useState<DatabaseKey[]>(['metadata']);
  const [directory, setDirectory] = useState('');
  const [note, setNote] = useState('');
  const [activeRestores, setActiveRestores] = useState<string[]>([]);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<DatabaseBackup | null>(null);
  const [activeDeletes, setActiveDeletes] = useState<string[]>([]);

  const primaryDatabase = selectedDatabases[0] ?? 'metadata';

  const sortedBackups = useMemo<DatabaseBackup[]>(() => {
    return [...(backupsQuery.data ?? [])].sort((a, b) => (a.created_at < b.created_at ? 1 : -1));
  }, [backupsQuery.data]);

  const resetModal = () => {
    setSelectedDatabases(['metadata']);
    setDirectory('');
    setNote('');
  };

  const handleOpenModal = () => {
    resetModal();
    openModal();
  };

  const toggleDatabase = (database: DatabaseKey) => {
    setSelectedDatabases((current) => {
      if (current.includes(database)) {
        const next = current.filter((value) => value !== database);
        return next.length === 0 ? current : next;
      }
      return [...current, database];
    });
  };

  const handleCreate = async () => {
    if (!selectedDatabases.length) {
      notifications.show({ color: 'yellow', message: 'Select at least one database to back up.' });
      return;
    }

    const trimmedDirectory = directory.trim() || undefined;
    const payloads = selectedDatabases.map((database) => ({
      database,
      directory: trimmedDirectory,
      note: note.trim() || undefined,
    }));

    try {
      setIsSubmitting(true);
      for (const payload of payloads) {
        await createBackup.mutateAsync(payload);
      }
      const label = payloads.length > 1 ? 'selected databases' : payloads[0].database;
      notifications.show({ color: 'teal', message: `Backup created for ${label}.` });
      resetModal();
      closeModal();
    } catch (error) {
      notifications.show({ color: 'red', message: (error as Error).message });
    } finally {
      setIsSubmitting(false);
    }
  };

  const restoreKey = (database: DatabaseKey, path?: string) => `${database}:${path ?? 'latest'}`;
  const deleteKey = (database: DatabaseKey, path: string) => `${database}:${path}`;

  const handleRestore = async (database: DatabaseKey, path?: string) => {
    const key = restoreKey(database, path);
    setActiveRestores((current) => (current.includes(key) ? current : [...current, key]));
    try {
      const result = await restoreBackup.mutateAsync({ database, path });
      notifications.show({
        color: 'teal',
        message: `Restore job started for ${result.backup.database_label} backup ${result.backup.filename}`,
      });
    } catch (error) {
      notifications.show({ color: 'red', message: (error as Error).message });
    } finally {
      setActiveRestores((current) => current.filter((value) => value !== key));
    }
  };

  const restoreIsActive = (backup: DatabaseBackup) => activeRestores.includes(restoreKey(backup.database, backup.path));
  const restoreLatestIsActive = (database: DatabaseKey) => activeRestores.includes(restoreKey(database));
  const deleteIsActive = (backup: DatabaseBackup) => activeDeletes.includes(deleteKey(backup.database, backup.path));

  const handleDeleteRequest = (backup: DatabaseBackup) => {
    setDeleteTarget(backup);
  };

  const handleDeleteConfirm = async () => {
    if (!deleteTarget) return;
    const target = deleteTarget;
    const key = deleteKey(target.database, target.path);
    setActiveDeletes((current) => (current.includes(key) ? current : [...current, key]));
    try {
      await deleteBackup.mutateAsync({ database: target.database, path: target.path });
      notifications.show({
        color: 'teal',
        message: `Backup deleted for ${target.database_label} database (${target.filename}).`,
      });
      setDeleteTarget(null);
    } catch (error) {
      notifications.show({ color: 'red', message: (error as Error).message });
    } finally {
      setActiveDeletes((current) => current.filter((value) => value !== key));
    }
  };

  const handleDeleteCancel = () => {
    if (!deleteTarget) return;
    const key = deleteKey(deleteTarget.database, deleteTarget.path);
    setActiveDeletes((current) => current.filter((value) => value !== key));
    setDeleteTarget(null);
  };

  const deleteLoading = deleteTarget ? deleteIsActive(deleteTarget) : false;

  return (
    <Stack gap="lg">
      <Card withBorder radius="md" padding="lg">
        <Group justify="space-between" align="flex-start" mb="md">
          <Stack gap={4}>
            <Text fw={600}>Database backups</Text>
            <Text size="sm" c="dimmed">
              Create snapshots of the metadata or application databases and restore them when needed.
            </Text>
          </Stack>
          <Button
            leftSection={<IconDatabasePlus size={16} />}
            onClick={handleOpenModal}
          >
            Create backup
          </Button>
        </Group>

        {backupsQuery.isLoading ? (
          <Group justify="center" py="xl">
            <Loader size="sm" />
          </Group>
        ) : sortedBackups.length === 0 ? (
          <Stack gap={4} align="center" py="xl">
            <IconDatabaseImport size={32} />
            <Text c="dimmed" size="sm">
              No backups found yet. Create one to get started.
            </Text>
          </Stack>
        ) : (
          <ScrollArea>
            <Table striped highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Database</Table.Th>
                  <Table.Th>Name</Table.Th>
                  <Table.Th>Created</Table.Th>
                  <Table.Th>Size</Table.Th>
                  <Table.Th>Note</Table.Th>
                  <Table.Th ta="right">Actions</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {sortedBackups.map((backup) => (
                  <Table.Tr key={`${backup.database}-${backup.path}`}>
                    <Table.Td>
                      <Badge color={backup.database === 'metadata' ? 'blue' : 'grape'} variant="light">
                        {backup.database_label}
                      </Badge>
                    </Table.Td>
                    <Table.Td>{backup.filename}</Table.Td>
                    <Table.Td>{formatTimestamp(backup.created_at)}</Table.Td>
                    <Table.Td>{formatBytes(backup.size_bytes)}</Table.Td>
                    <Table.Td>{backup.note?.trim() || '-'}</Table.Td>
                    <Table.Td>
                      <Group justify="flex-end">
                        <ActionIcon
                          variant="light"
                          color="blue"
                          onClick={() => handleRestore(backup.database, backup.path)}
                          disabled={restoreIsActive(backup) || deleteIsActive(backup)}
                          aria-label={`Restore backup ${backup.filename}`}
                        >
                          {restoreIsActive(backup) ? <Loader size="xs" /> : <IconRestore size={16} />}
                        </ActionIcon>
                        <ActionIcon
                          variant="light"
                          color="red"
                          onClick={() => handleDeleteRequest(backup)}
                          disabled={deleteIsActive(backup) || restoreIsActive(backup)}
                          aria-label={`Delete backup ${backup.filename}`}
                        >
                          {deleteIsActive(backup) ? <Loader size="xs" /> : <IconTrash size={16} />}
                        </ActionIcon>
                      </Group>
                    </Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </ScrollArea>
        )}

        <Group justify="flex-end" mt="md">
          <Button
            variant="subtle"
            onClick={() => handleRestore('metadata')}
            disabled={
              sortedBackups.filter((entry) => entry.database === 'metadata').length === 0 ||
              restoreLatestIsActive('metadata')
            }
            leftSection={<IconRestore size={14} />}
          >
            Restore latest metadata backup
          </Button>
          <Button
            variant="subtle"
            onClick={() => handleRestore('application')}
            disabled={
              sortedBackups.filter((entry) => entry.database === 'application').length === 0 ||
              restoreLatestIsActive('application')
            }
            leftSection={<IconRestore size={14} />}
          >
            Restore latest application backup
          </Button>
        </Group>
      </Card>

      {/* Create Backup Modal */}
      <Modal opened={modalOpened} onClose={closeModal} title="Create backup" size="lg">
        <Stack gap="md">
          <Stack gap={4}>
            <Text fw={600}>Select databases</Text>
            <Group>
              <Checkbox
                label="Metadata"
                checked={selectedDatabases.includes('metadata')}
                onChange={() => toggleDatabase('metadata')}
              />
              <Checkbox
                label="Application"
                checked={selectedDatabases.includes('application')}
                onChange={() => toggleDatabase('application')}
              />
            </Group>
            <Text size="xs" c="dimmed">
              Choose one or both databases to snapshot. Each backup is saved with an automatic timestamped filename
              per database.
            </Text>
          </Stack>

          <TextInput
            label="Backup directory"
            placeholder={DATABASE_DEFAULT_DIRECTORIES[primaryDatabase]}
            value={directory}
            onChange={(event) => setDirectory(event.currentTarget.value)}
            description="Leave blank to use the default backup directory. Paths must stay inside the configured backup folder."
          />
          <TextInput
            label="Note (optional)"
            placeholder="Brief description for this backup"
            value={note}
            onChange={(event) => setNote(event.currentTarget.value)}
            description="Add a short note to remember why this snapshot was created."
          />

          <Group justify="flex-end">
            <Button variant="default" onClick={closeModal} disabled={isSubmitting}>
              Cancel
            </Button>
            <Button onClick={handleCreate} loading={isSubmitting}>
              Create backup
            </Button>
          </Group>
        </Stack>
      </Modal>

      {/* Delete Confirmation Modal */}
      <Modal
        opened={deleteTarget !== null}
        onClose={() => {
          if (!deleteLoading) handleDeleteCancel();
        }}
        title="Delete backup"
        size="sm"
        closeOnClickOutside={!deleteLoading}
        closeOnEscape={!deleteLoading}
      >
        <Stack gap="md">
          <Text size="sm">
            {deleteTarget ? (
              <>
                Permanently delete backup <strong>{deleteTarget.filename}</strong> from the{' '}
                <strong>{deleteTarget.database_label}</strong> database?
              </>
            ) : (
              'Select a backup to delete.'
            )}
          </Text>
          <Text size="xs" c="dimmed">
            This action removes the dump file and its metadata sidecar from disk.
          </Text>
          <Group justify="flex-end">
            <Button variant="default" onClick={handleDeleteCancel} disabled={deleteLoading}>
              Cancel
            </Button>
            <Button color="red" onClick={handleDeleteConfirm} loading={deleteLoading} disabled={!deleteTarget}>
              Delete
            </Button>
          </Group>
        </Stack>
      </Modal>
    </Stack>
  );
};

export default BackupRestoreTab;
