/**
 * Single Stack QC Modal
 * 
 * A fullscreen modal for quick QC of a single stack.
 * Reuses HUD components from AxesQCShared.
 */

import { useMemo } from 'react';
import {
  Box,
  Group,
  Stack,
  Text,
  Badge,
  Button,
  Loader,
  Center,
  Modal,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { IconCheck, IconX } from '@tabler/icons-react';
import { DicomViewer } from './DicomViewer';
import {
  useAxesQCItem,
  useAxisOptions,
  useUpdateAxisValue,
  useAxesQCSession,
  useConfirmAxesChanges,
  useDiscardAxesChanges,
} from '../api';
import type { AxisType, AxisFlagType, AxisOptions } from '../types';
import {
  filterNonNull,
  AXIS_TO_COLUMN,
  hudContainerStyle,
  AcquisitionHUD,
  SequenceHUD,
  ClassificationHUD,
  FovHUD,
  FlaggedAxisRow,
} from './AxesQCShared';

interface SingleStackQCModalProps {
  cohortId: number;
  stackId: number;
  seriesUid: string;
  stackIndex: number;
  stackName: string;
  opened: boolean;
  onClose: () => void;
}

export const SingleStackQCModal = ({
  cohortId,
  stackId,
  seriesUid,
  stackIndex,
  stackName,
  opened,
  onClose,
}: SingleStackQCModalProps) => {
  // Fetch QC data for this specific stack
  const { data: qcItem, isLoading, error } = useAxesQCItem(opened ? stackId : null);
  const { data: axisOptions } = useAxisOptions();
  const { data: session } = useAxesQCSession(opened ? cohortId : null);
  const updateAxisMutation = useUpdateAxisValue();
  const confirmMutation = useConfirmAxesChanges();
  const discardMutation = useDiscardAxesChanges();

  const draftChangeCount = session?.draft_change_count ?? 0;

  // Get flagged axes for current item
  const flaggedAxes = useMemo(() => {
    if (!qcItem) return [];
    return (['base', 'technique', 'modifier', 'provenance', 'construct'] as AxisType[])
      .filter((axis) => qcItem.flags[axis])
      .map((axis) => ({
        axis,
        flag: qcItem.flags[axis]!,
      }));
  }, [qcItem]);

  // Always show base, modifier, technique, construct + any additional flagged axes
  const displayAxes = useMemo(() => {
    const requiredAxes: AxisType[] = ['base', 'modifier', 'technique', 'construct'];
    const required: { axis: AxisType; flag?: AxisFlagType | null }[] = requiredAxes.map((axis) => ({
      axis,
      flag: (qcItem?.flags ?? {})[axis] ?? null,
    }));
    const extra = flaggedAxes.filter((fa) => !requiredAxes.includes(fa.axis));
    return [...required, ...extra];
  }, [qcItem, flaggedAxes]);

  // Handle axis value selection
  const handleAxisSelect = (axis: AxisType, value: string | null) => {
    if (!qcItem) return;
    updateAxisMutation.mutate({
      cohortId,
      stackId: qcItem.stack_id,
      axis,
      value,
    });
  };

  // Confirm changes
  const handleConfirm = () => {
    confirmMutation.mutate(cohortId, {
      onSuccess: (data) => {
        notifications.show({
          title: 'Saved',
          message: `${data.confirmed_changes} changes saved.`,
          color: 'green',
          icon: <IconCheck size={16} />,
        });
        onClose();
      },
    });
  };

  // Discard changes
  const handleDiscard = () => {
    discardMutation.mutate(cohortId, {
      onSuccess: (data) => {
        notifications.show({
          title: 'Discarded',
          message: `${data.discarded_changes} changes discarded.`,
          color: 'yellow',
        });
      },
    });
  };

  return (
    <Modal
      opened={opened}
      onClose={onClose}
      fullScreen
      title={
        <Group gap="sm">
          <Text fw={600}>{stackName}</Text>
          {qcItem?.subject_code && (
            <Badge size="sm" color="cyan" variant="light">
              {qcItem.subject_code}
            </Badge>
          )}
          {qcItem?.study_date && (
            <Text size="sm" c="dimmed">{qcItem.study_date}</Text>
          )}
        </Group>
      }
      styles={{
        body: { 
          height: 'calc(100vh - 60px)',
          padding: 0,
          display: 'flex',
          flexDirection: 'column',
          backgroundColor: '#000',
        },
        header: {
          backgroundColor: 'var(--mantine-color-dark-7)',
          borderBottom: '1px solid rgba(255,255,255,0.1)',
        },
        title: {
          width: '100%',
        },
      }}
    >
      {/* Loading state */}
      {isLoading && (
        <Center h="100%">
          <Loader color="white" size="lg" />
        </Center>
      )}

      {/* Error state */}
      {error && (
        <Center h="100%">
          <Stack align="center">
            <Text c="red" size="lg">Error loading QC data</Text>
            <Button variant="light" onClick={onClose}>Close</Button>
          </Stack>
        </Center>
      )}

      {/* Main content */}
      {qcItem && !isLoading && (
        <>
          {/* DICOM Viewer with HUD overlays */}
          <Box
            style={{
              flex: 1,
              position: 'relative',
              minHeight: 0,
              display: 'flex',
              flexDirection: 'column',
            }}
          >
            <Box style={{ flex: 1, width: '100%', overflow: 'hidden' }}>
              <DicomViewer
                seriesUid={seriesUid}
                stackIndex={stackIndex}
                compact
                maxHeight="100%"
              />
            </Box>

            {/* Top-Left HUD: Acquisition + Sequence info */}
            <Box style={{ ...hudContainerStyle, top: 8, left: 8, alignItems: 'flex-start' }}>
              <AcquisitionHUD params={qcItem.params} tags={qcItem.tags} />
              <SequenceHUD tags={qcItem.tags} />
            </Box>

            {/* Top-Right HUD: Classification */}
            <Box style={{ ...hudContainerStyle, top: 8, right: 8, alignItems: 'flex-end' }}>
              <ClassificationHUD
                current={qcItem.current}
                intent={qcItem.intent}
                draftChanges={qcItem.draft_changes}
              />
            </Box>

            {/* Bottom-Right HUD: FOV */}
            <Box style={{ ...hudContainerStyle, bottom: 22, right: 8, alignItems: 'flex-end' }}>
              <FovHUD params={qcItem.params} />
            </Box>
          </Box>

          {/* Footer: Axis selection + actions */}
          <Stack
            gap={0}
            style={{
              flexShrink: 0,
              borderTop: '1px solid rgba(255,255,255,0.1)',
              backgroundColor: 'rgba(0,0,0,0.9)',
            }}
          >
            <Box px="md" py="sm">
              <Stack gap="xs">
                {displayAxes.map((fa) => {
                  const opts = axisOptions ? filterNonNull((axisOptions as AxisOptions)[fa.axis] ?? []) : [];
                  const column = AXIS_TO_COLUMN[fa.axis];
                  const hasDraft = !!(qcItem?.draft_changes && column in qcItem.draft_changes);
                  const draftValue = hasDraft ? qcItem?.draft_changes?.[column] : undefined;

                  return (
                    <FlaggedAxisRow
                      key={fa.axis}
                      axis={fa.axis}
                      flag={fa.flag}
                      currentValue={qcItem?.current[fa.axis] ?? null}
                      draftValue={draftValue}
                      hasDraft={hasDraft}
                      options={opts}
                      metadata={fa.axis === 'technique' ? axisOptions?.technique_metadata : undefined}
                      onSelect={(value) => handleAxisSelect(fa.axis, value)}
                      isUpdating={updateAxisMutation.isPending}
                    />
                  );
                })}
              </Stack>
            </Box>

            {/* Action buttons */}
            <Box
              px="md"
              py="sm"
              style={{
                borderTop: '1px solid rgba(255,255,255,0.1)',
                display: 'flex',
                justifyContent: 'flex-end',
                gap: 8,
              }}
            >
              {draftChangeCount > 0 && (
                <Badge color="green" variant="light" size="lg" mr="auto">
                  {draftChangeCount} pending changes
                </Badge>
              )}
              <Button
                size="sm"
                variant="subtle"
                color="red"
                leftSection={<IconX size={14} />}
                onClick={handleDiscard}
                disabled={draftChangeCount === 0 || discardMutation.isPending}
                loading={discardMutation.isPending}
              >
                Discard
              </Button>
              <Button
                size="sm"
                variant="filled"
                color="green"
                leftSection={<IconCheck size={14} />}
                onClick={handleConfirm}
                disabled={draftChangeCount === 0 || confirmMutation.isPending}
                loading={confirmMutation.isPending}
              >
                Save Changes
              </Button>
            </Box>
          </Stack>
        </>
      )}
    </Modal>
  );
};

export default SingleStackQCModal;

