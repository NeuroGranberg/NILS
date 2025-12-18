/**
 * Database Management Page
 *
 * Performance optimized: keepMounted={true} preserves DataTable state
 * between tab switches, avoiding expensive re-initialization.
 */
import { Stack, Tabs, Title } from '@mantine/core';
import { IconDatabase, IconServer, IconCloudDownload } from '@tabler/icons-react';
import MetadataDatabaseTab from '../components/MetadataDatabaseTab';
import ApplicationDatabaseTab from '../components/ApplicationDatabaseTab';
import BackupRestoreTab from '../components/BackupRestoreTab';

export const DatabaseManagementPage = () => {
  return (
    <Stack gap="lg" p="md">
      <Title order={2}>Database management</Title>

      <Tabs defaultValue="metadata" keepMounted={true}>
        <Tabs.List>
          <Tabs.Tab value="metadata" leftSection={<IconDatabase size={16} />}>
            Metadata Database
          </Tabs.Tab>
          <Tabs.Tab value="application" leftSection={<IconServer size={16} />}>
            Application Database
          </Tabs.Tab>
          <Tabs.Tab value="backups" leftSection={<IconCloudDownload size={16} />}>
            Backup & Restore
          </Tabs.Tab>
        </Tabs.List>

        <Tabs.Panel value="metadata" pt="md">
          <MetadataDatabaseTab />
        </Tabs.Panel>

        <Tabs.Panel value="application" pt="md">
          <ApplicationDatabaseTab />
        </Tabs.Panel>

        <Tabs.Panel value="backups" pt="md">
          <BackupRestoreTab />
        </Tabs.Panel>
      </Tabs>
    </Stack>
  );
};

export default DatabaseManagementPage;
