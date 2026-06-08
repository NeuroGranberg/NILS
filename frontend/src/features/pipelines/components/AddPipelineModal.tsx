/**
 * AddPipelineModal - register a pipeline git repository.
 *
 * Submits { url, ref? } via useRegisterRepo. On success surfaces the count of
 * discovered pipelines; on error surfaces the ApiError message.
 */
import { useEffect, useState } from 'react';
import { Button, Group, Modal, Stack, TextInput } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { useRegisterRepo } from '../api';

interface AddPipelineModalProps {
  opened: boolean;
  onClose: () => void;
}

export const AddPipelineModal = ({ opened, onClose }: AddPipelineModalProps) => {
  const [url, setUrl] = useState('');
  const [ref, setRef] = useState('');
  const registerRepo = useRegisterRepo();

  useEffect(() => {
    if (opened) {
      setUrl('');
      setRef('');
    }
  }, [opened]);

  const handleSubmit = () => {
    const trimmedUrl = url.trim();
    if (!trimmedUrl) {
      notifications.show({ color: 'red', message: 'Please provide a repository URL.' });
      return;
    }

    registerRepo.mutate(
      { url: trimmedUrl, ref: ref.trim() || undefined },
      {
        onSuccess: (data) => {
          notifications.show({
            color: 'green',
            message: `Registered ${data.pipelines.length} pipeline(s)`,
          });
          onClose();
        },
        onError: (error) => {
          notifications.show({ color: 'red', message: (error as Error).message });
        },
      },
    );
  };

  return (
    <Modal opened={opened} onClose={onClose} title="Register repository" size="md">
      <Stack>
        <TextInput
          label="Repository URL"
          placeholder="https://github.com/org/pipelines.git"
          value={url}
          onChange={(event) => setUrl(event.currentTarget.value)}
          required
          autoComplete="off"
        />
        <TextInput
          label="Ref"
          description="Optional branch, tag, or commit SHA. Defaults to the repository's default branch."
          placeholder="main"
          value={ref}
          onChange={(event) => setRef(event.currentTarget.value)}
          autoComplete="off"
        />
        <Group justify="flex-end">
          <Button variant="default" onClick={onClose} disabled={registerRepo.isPending}>
            Cancel
          </Button>
          <Button onClick={handleSubmit} loading={registerRepo.isPending}>
            Register
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
};
