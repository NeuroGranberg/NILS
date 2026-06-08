/**
 * PipelinesPage (Surface 1) - the analysis-pipeline inventory.
 *
 * Shows registered repositories (with refresh/remove) and a grid of pipeline
 * cards. A "Register repository" button opens AddPipelineModal.
 */
import { Box, Button, Group, Loader, SimpleGrid, Stack, Text, Title } from '@mantine/core';
import { IconBinaryTree2, IconPlus } from '@tabler/icons-react';
import { useDisclosure } from '@mantine/hooks';
import { SectionCard } from '../../shared/components/SectionCard';
import { PipelineCard } from '../components/PipelineCard';
import { AddPipelineModal } from '../components/AddPipelineModal';
import { RepoList } from '../components/RepoList';
import { usePipelines, useRepos } from '../api';

export const PipelinesPage = () => {
  const { data: repos, isLoading: reposLoading } = useRepos();
  const { data: pipelines, isLoading: pipelinesLoading } = usePipelines();
  const [addOpened, { open, close }] = useDisclosure(false);

  const isLoading = reposLoading || pipelinesLoading;
  const hasRepos = (repos?.length ?? 0) > 0;
  const hasPipelines = (pipelines?.length ?? 0) > 0;

  return (
    <Stack gap="lg" p="md">
      {/* Page header */}
      <Group justify="space-between" align="flex-start">
        <Stack gap={4}>
          <Title order={2} fw={600} c="var(--nils-text-primary)">
            Analysis Pipelines
          </Title>
          <Text size="sm" c="var(--nils-text-secondary)">
            Register pipeline repositories and launch containerized analyses
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
          Register repository
        </Button>
      </Group>

      {/* Loading */}
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

      {/* Repositories section */}
      {!isLoading && hasRepos && repos && (
        <SectionCard title="Repositories" description="Registered pipeline sources">
          <RepoList repos={repos} />
        </SectionCard>
      )}

      {/* Empty state: no repos registered */}
      {!isLoading && !hasRepos && (
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
            <IconBinaryTree2 size={24} color="var(--nils-text-tertiary)" />
          </Box>
          <Text fw={600} size="md" c="var(--nils-text-primary)" mb={4}>
            No pipeline repositories
          </Text>
          <Text size="sm" c="var(--nils-text-secondary)" ta="center" maw={360} mb="md">
            Register a git repository containing Boutiques pipeline descriptors to make its
            analyses available here.
          </Text>
          <Button variant="light" leftSection={<IconPlus size={16} />} onClick={open}>
            Register your first repository
          </Button>
        </Box>
      )}

      {/* Pipelines grid */}
      {!isLoading && hasRepos && hasPipelines && pipelines && (
        <SimpleGrid cols={{ base: 1, sm: 2, lg: 3 }} spacing="md">
          {pipelines.map((pipeline) => (
            <PipelineCard key={pipeline.id} pipeline={pipeline} />
          ))}
        </SimpleGrid>
      )}

      {/* Repos but no pipelines discovered */}
      {!isLoading && hasRepos && !hasPipelines && (
        <Text size="sm" c="var(--nils-text-tertiary)" ta="center" py="md">
          No pipelines discovered in the registered repositories yet. Try refreshing a repository.
        </Text>
      )}

      <AddPipelineModal opened={addOpened} onClose={close} />
    </Stack>
  );
};
