/**
 * Restore-previous button — single-level undo.
 */
import { Button, Group, Modal, Stack, Text } from '@mantine/core';
import { IconArrowBackUp } from '@tabler/icons-react';
import { useState } from 'react';

import { useRestorePreviousMainQCMutation } from './api';

interface RestorePreviousButtonProps {
  cohortId: number;
  hasPrevious: boolean;
  previousRunAt: string | null;
}

export const RestorePreviousButton = ({
  cohortId,
  hasPrevious,
  previousRunAt,
}: RestorePreviousButtonProps) => {
  const [open, setOpen] = useState(false);
  const mutation = useRestorePreviousMainQCMutation(cohortId);

  const handleConfirm = async () => {
    await mutation.mutateAsync();
    setOpen(false);
  };

  if (!hasPrevious) return null;

  const formattedDate = previousRunAt
    ? new Date(previousRunAt).toLocaleString()
    : '(unknown)';

  return (
    <>
      <Button
        variant="default"
        leftSection={<IconArrowBackUp size={16} />}
        onClick={() => setOpen(true)}
        data-testid="main-qc-restore-button"
      >
        Restore previous
      </Button>
      <Modal
        opened={open}
        onClose={() => setOpen(false)}
        title="Restore previous Main QC run?"
        size="md"
      >
        <Stack gap="md">
          <Text size="sm">
            This will swap the current run for the one from <strong>{formattedDate}</strong>.
            The current run will be discarded — this is a single-level undo.
          </Text>
          <Group justify="flex-end" gap="xs">
            <Button variant="subtle" onClick={() => setOpen(false)}>Cancel</Button>
            <Button onClick={handleConfirm} loading={mutation.isPending}>
              Restore
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
};
