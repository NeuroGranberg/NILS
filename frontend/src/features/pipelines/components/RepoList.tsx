/**
 * RepoList - lists registered pipeline repositories with refresh/remove actions.
 *
 * Refresh re-syncs pipelines from the repo's default branch; Remove deletes the
 * repo (confirmed) and surfaces a 409 "has runs" conflict via a red notification.
 */
import { useState } from 'react';
import { ActionIcon, Badge, Group, Stack, Text, Tooltip } from '@mantine/core';
import { IconRefresh, IconTrash } from '@tabler/icons-react';
import { notifications } from '@mantine/notifications';
import type { RepoDTO } from '../types';
import { useRefreshRepo, useRemoveRepo } from '../api';

interface RepoListProps {
  repos: RepoDTO[];
}

interface RepoRowProps {
  repo: RepoDTO;
}

const RepoRow = ({ repo }: RepoRowProps) => {
  const refreshRepo = useRefreshRepo();
  const removeRepo = useRemoveRepo();
  const [confirming, setConfirming] = useState(false);

  const handleRefresh = () => {
    refreshRepo.mutate(
      { id: repo.id },
      {
        onSuccess: (data) => {
          notifications.show({
            color: 'green',
            message: `Refreshed ${data.pipelines.length} pipeline(s)`,
          });
        },
        onError: (error) => {
          notifications.show({ color: 'red', message: (error as Error).message });
        },
      },
    );
  };

  const handleRemove = () => {
    if (!confirming) {
      setConfirming(true);
      return;
    }
    removeRepo.mutate(repo.id, {
      onSuccess: () => {
        notifications.show({ color: 'green', message: 'Repository removed' });
      },
      onError: (error) => {
        // Surface 409 "has runs" (or any) conflict message.
        notifications.show({ color: 'red', message: (error as Error).message });
        setConfirming(false);
      },
    });
  };

  return (
    <Group
      justify="space-between"
      wrap="nowrap"
      py="xs"
      px="sm"
      style={{
        backgroundColor: 'var(--nils-bg-tertiary)',
        borderRadius: 'var(--nils-radius-sm)',
        border: '1px solid var(--nils-border-subtle)',
      }}
    >
      <Stack gap={2} style={{ minWidth: 0, flex: 1 }}>
        <Text size="sm" c="var(--nils-text-primary)" truncate>
          {repo.url}
        </Text>
        <Group gap={8}>
          <Text size="xs" c="var(--nils-text-tertiary)" ff="monospace">
            {repo.sha ? repo.sha.slice(0, 7) : '—'}
          </Text>
          {repo.default_branch && (
            <Badge variant="light" size="xs" color="gray">
              {repo.default_branch}
            </Badge>
          )}
          <Text size="xs" c="var(--nils-text-tertiary)">
            v{repo.version}
          </Text>
        </Group>
      </Stack>
      <Group gap={4} wrap="nowrap">
        <Tooltip label="Refresh pipelines">
          <ActionIcon
            variant="subtle"
            color="blue"
            onClick={handleRefresh}
            loading={refreshRepo.isPending}
            aria-label="Refresh repository"
          >
            <IconRefresh size={16} />
          </ActionIcon>
        </Tooltip>
        <Tooltip label={confirming ? 'Click again to confirm removal' : 'Remove repository'}>
          <ActionIcon
            variant={confirming ? 'filled' : 'subtle'}
            color="red"
            onClick={handleRemove}
            loading={removeRepo.isPending}
            aria-label="Remove repository"
          >
            <IconTrash size={16} />
          </ActionIcon>
        </Tooltip>
      </Group>
    </Group>
  );
};

export const RepoList = ({ repos }: RepoListProps) => {
  return (
    <Stack gap="xs">
      {repos.map((repo) => (
        <RepoRow key={repo.id} repo={repo} />
      ))}
    </Stack>
  );
};
