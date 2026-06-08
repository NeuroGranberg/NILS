/**
 * RunProgress - renders the bridge RunState as a progress bar with a
 * done/total caption and a red failed-count when units have failed.
 *
 * Mirrors the layout idiom of features/export/components/ExportProgress.
 */
import { Group, Progress, Stack, Text } from '@mantine/core';
import type { RunState } from '../types';

interface RunProgressProps {
  state: RunState | undefined;
}

export const RunProgress = ({ state }: RunProgressProps) => {
  const total = state?.units_total ?? 0;
  const done = state?.units_done ?? 0;
  const failed = state?.units_failed ?? 0;
  const value = total > 0 ? (done / total) * 100 : 0;

  return (
    <Stack gap="xs">
      <Progress value={value} size="lg" radius="md" transitionDuration={200} />
      <Group justify="space-between">
        <Text size="xs" c="dimmed">
          {done}/{total} units
        </Text>
        {failed > 0 && (
          <Text size="xs" c="red">
            {failed} failed
          </Text>
        )}
      </Group>
    </Stack>
  );
};
