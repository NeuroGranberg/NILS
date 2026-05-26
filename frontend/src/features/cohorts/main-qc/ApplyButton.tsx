/**
 * Apply button with explicit confirmation dialog explaining the wipe behaviour.
 */
import { Button, Group, Modal, Stack, Text } from '@mantine/core';
import { IconAlertTriangle, IconPlayerPlay } from '@tabler/icons-react';
import { useState } from 'react';

import { useApplyMainQCMutation } from './api';

interface ApplyButtonProps {
  cohortId: number;
  hasCurrent: boolean;
}

export const ApplyButton = ({ cohortId, hasCurrent }: ApplyButtonProps) => {
  const [open, setOpen] = useState(false);
  const mutation = useApplyMainQCMutation(cohortId);

  const handleConfirm = async () => {
    await mutation.mutateAsync();
    setOpen(false);
  };

  return (
    <>
      <Button
        leftSection={<IconPlayerPlay size={16} />}
        onClick={() => setOpen(true)}
        loading={mutation.isPending}
        data-testid="main-qc-apply-button"
      >
        {hasCurrent ? 'Re-run auto-pick' : 'Run auto-pick'}
      </Button>

      <Modal
        opened={open}
        onClose={() => setOpen(false)}
        title="Apply cohort-wide Main Acquisition QC?"
        size="md"
      >
        <Stack gap="md">
          <Stack gap={4} bg="orange.0" p="sm" style={{ borderRadius: 4 }}>
            <Group gap={4}>
              <IconAlertTriangle size={16} color="var(--mantine-color-orange-7)" />
              <Text size="sm" fw={600} c="orange.8">This will overwrite manual choices</Text>
            </Group>
            <Text size="xs" c="orange.8">
              All <strong>main_acquisition</strong> tags in this cohort will be cleared and replaced by
              the algorithm's picks — including any selections you've made in the per-session
              Main Acquisition QC view.
            </Text>
            <Text size="xs" c="orange.8">
              Don't worry: the previous run will remain available via <strong>Restore previous</strong>.
            </Text>
          </Stack>
          <Group justify="flex-end" gap="xs">
            <Button variant="subtle" onClick={() => setOpen(false)}>Cancel</Button>
            <Button color="orange" onClick={handleConfirm} loading={mutation.isPending}>
              Apply
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
};
