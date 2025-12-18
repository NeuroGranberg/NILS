import { Box, Button, Group, Loader, SimpleGrid, Stack, Text, Title } from '@mantine/core';
import { IconPlus, IconStack2 } from '@tabler/icons-react';
import { useDisclosure } from '@mantine/hooks';
import { CohortCard } from '../components/CohortCard';
import { CohortCreateModal } from '../components/CohortCreateModal';
import { useCohortsQuery } from '../api';

export const CohortsListPage = () => {
  const { data: cohorts, isLoading } = useCohortsQuery();
  const [createOpened, { open, close }] = useDisclosure(false);

  return (
    <Stack gap="lg" p="md">
      {/* Page Header */}
      <Group justify="space-between" align="flex-start">
        <Stack gap={4}>
          <Title order={2} fw={600} c="var(--nils-text-primary)">
            Cohorts
          </Title>
          <Text size="sm" c="var(--nils-text-secondary)">
            Manage neuroimaging datasets and monitor pipeline progress
          </Text>
        </Stack>
        <Button 
          leftSection={<IconPlus size={16} />} 
          onClick={open}
          style={{
            backgroundColor: 'var(--nils-accent-primary)',
            color: 'var(--nils-bg-primary)',
          }}
        >
          New cohort
        </Button>
      </Group>

      {/* Loading State */}
      {isLoading && (
        <Box
          py="xl"
          style={{
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            minHeight: '200px',
          }}
        >
          <Loader size="md" color="var(--nils-accent-primary)" />
        </Box>
      )}

      {/* Empty State */}
      {!isLoading && cohorts && cohorts.length === 0 && (
        <Box
          py="xl"
          style={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            minHeight: '300px',
            backgroundColor: 'var(--nils-bg-secondary)',
            borderRadius: 'var(--nils-radius-lg)',
            border: '1px solid var(--nils-border-subtle)',
          }}
        >
          <Box
            mb="md"
            style={{
              width: 48,
              height: 48,
              borderRadius: 'var(--nils-radius-md)',
              backgroundColor: 'var(--nils-bg-tertiary)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <IconStack2 size={24} color="var(--nils-text-tertiary)" />
          </Box>
          <Text fw={600} size="md" c="var(--nils-text-primary)" mb={4}>
            No cohorts yet
          </Text>
          <Text size="sm" c="var(--nils-text-secondary)" ta="center" maw={320} mb="md">
            Start by creating a cohort to organize your neuroimaging data and run processing pipelines.
          </Text>
          <Button
            variant="light"
            leftSection={<IconPlus size={16} />}
            onClick={open}
          >
            Create your first cohort
          </Button>
        </Box>
      )}

      {/* Cohort Grid */}
      {!isLoading && cohorts && cohorts.length > 0 && (
        <SimpleGrid cols={{ base: 1, sm: 2, lg: 3 }} spacing="md">
          {cohorts.map((cohort) => (
            <CohortCard key={cohort.id} cohort={cohort} />
          ))}
        </SimpleGrid>
      )}

      <CohortCreateModal opened={createOpened} onClose={close} />
    </Stack>
  );
};
