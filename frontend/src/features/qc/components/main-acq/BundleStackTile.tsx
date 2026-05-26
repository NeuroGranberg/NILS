/**
 * One stack tile inside a bundle (per-session MASQC view).
 *
 * - Renders the current slice via the shared StackImagePane (server PNG,
 *   native wheel scroll, ±5 prefetch, multi-frame DICOM).
 * - Adds three role-toggle buttons under the image: MAIN | PRE | POST.
 * - Slice progress is shared across the bundle so all tiles scroll in sync.
 */

import { useEffect, useState } from 'react';
import { Button, Group, Stack } from '@mantine/core';
import type { BundleStack } from '../../api/main-acq';
import { useSetStackRole } from '../../api/main-acq';
import { StackImagePane } from './StackImagePane';

interface BundleStackTileProps {
  stack: BundleStack;
  progress: number; // 0..1, shared across the bundle
  onProgressChange: (next: number) => void;
}

export const BundleStackTile = ({ stack, progress, onProgressChange }: BundleStackTileProps) => {
  const setRole = useSetStackRole();

  // Local optimistic state for the three toggle buttons.
  const [isMain, setIsMain] = useState<boolean>(stack.is_main);
  const [postContrast, setPostContrast] = useState<number | null>(stack.post_contrast);

  useEffect(() => setIsMain(stack.is_main), [stack.is_main]);
  useEffect(() => setPostContrast(stack.post_contrast), [stack.post_contrast]);

  const togglePre = () => {
    const turningOn = postContrast !== 0;
    setPostContrast(turningOn ? 0 : null);
    setRole.mutate(
      { seriesStackId: stack.series_stack_id, role: 'pre', value: turningOn },
      { onError: () => setPostContrast(stack.post_contrast) },
    );
  };

  const togglePost = () => {
    const turningOn = postContrast !== 1;
    setPostContrast(turningOn ? 1 : null);
    setRole.mutate(
      { seriesStackId: stack.series_stack_id, role: 'post', value: turningOn },
      { onError: () => setPostContrast(stack.post_contrast) },
    );
  };

  const toggleMain = () => {
    const turningOn = !isMain;
    setIsMain(turningOn);
    setRole.mutate(
      { seriesStackId: stack.series_stack_id, role: 'main', value: turningOn },
      { onError: () => setIsMain(stack.is_main) },
    );
  };

  return (
    <Stack gap={4}>
      <StackImagePane
        seriesInstanceUid={stack.series_instance_uid}
        stackIndex={stack.stack_index}
        progress={progress}
        onProgressChange={onProgressChange}
        topRightOverlay={stack.series_time}
        altLabel={`stack ${stack.series_stack_id}`}
      />
      <Group gap="xs" justify="center">
        <Button
          size="compact-xs"
          variant={isMain ? 'filled' : 'outline'}
          color="green"
          onClick={toggleMain}
        >
          MAIN
        </Button>
        <Button.Group>
          <Button
            size="compact-xs"
            variant={postContrast === 0 ? 'filled' : 'outline'}
            color="blue"
            onClick={togglePre}
          >
            PRE
          </Button>
          <Button
            size="compact-xs"
            variant={postContrast === 1 ? 'filled' : 'outline'}
            color="blue"
            onClick={togglePost}
          >
            POST
          </Button>
        </Button.Group>
      </Group>
    </Stack>
  );
};
