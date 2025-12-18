/**
 * Axes QC Page - "Dashboard Cockpit" Design
 *
 * Design Philosophy: Image-centric with HUD overlays
 * - 80% of screen is DICOM viewer
 * - Metadata displayed as semi-transparent overlays ON the image
 * - Only flagged axes shown in compact footer
 * - Keyboard-first with visible shortcuts
 * - Zero cognitive context-switching
 */

import { useState, useCallback, useMemo } from 'react';
import {
  Box,
  Group,
  Stack,
  Text,
  ActionIcon,
  Badge,
  Button,
  Loader,
  Center,
  Select,
} from '@mantine/core';
import { useHotkeys } from '@mantine/hooks';
import { notifications } from '@mantine/notifications';
import {
  IconChevronLeft,
  IconChevronRight,
  IconCheck,
  IconX,
} from '@tabler/icons-react';
import { DicomViewer } from '../components/DicomViewer';
import {
  useAxesQCItems,
  useAxisOptions,
  useUpdateAxisValue,
  useAxesQCSession,
  useConfirmAxesChanges,
  useDiscardAxesChanges,
  useAvailableFilters,
  type AxesQCFilters,
} from '../api';
import type { AxisType, AxisFlagType, AxisOptions } from '../types';
import type { Cohort } from '../../../types';
import {
  filterNonNull,
  AXIS_TO_COLUMN,
  hudContainerStyle,
  AcquisitionHUD,
  SequenceHUD,
  ClassificationHUD,
  FovHUD,
  FlaggedAxisRow,
} from '../components/AxesQCShared';

// Keyboard shortcuts for option selection
interface AxesQCPageProps {
  cohort: Cohort;
  onBack: () => void;
}

// ============================================================================
// Main Component
// ============================================================================

export const AxesQCPage = ({ cohort, onBack }: AxesQCPageProps) => {
  const [currentIndex, setCurrentIndex] = useState(0);
  const [axisFilter, setAxisFilter] = useState<string | null>(null);
  const [flagFilter, setFlagFilter] = useState<string | null>(null);

  // Build filters
  const filters: AxesQCFilters = useMemo(() => ({
    axis: axisFilter || null,
    flagType: flagFilter || null,
  }), [axisFilter, flagFilter]);


  // Reset index when filters change
  const handleFilterChange = (setter: (v: string | null) => void) => (value: string | null) => {
    setter(value);
    setCurrentIndex(0);
  };

  // Queries
  const { data: itemsData, isLoading, error } = useAxesQCItems(cohort.id, 0, 500, filters);
  const { data: axisOptions } = useAxisOptions();
  const { data: session } = useAxesQCSession(cohort.id);
  const { data: availableFilters } = useAvailableFilters(cohort.id);
  const updateAxisMutation = useUpdateAxisValue();
  const confirmMutation = useConfirmAxesChanges();
  const discardMutation = useDiscardAxesChanges();

  const items = itemsData?.items ?? [];

  // Build dynamic filter options based on available data
  const axisFilterOptions = useMemo(() => {
    const base = [{ value: '', label: 'All Axes' }];
    if (!availableFilters?.available_axes?.length) return base;
    
    const axisLabels: Record<string, string> = {
      base: 'Base',
      technique: 'Technique',
      modifier: 'Modifier',
      provenance: 'Provenance',
      construct: 'Construct',
    };
    
    return [
      ...base,
      ...availableFilters.available_axes.map(axis => ({
        value: axis,
        label: axisLabels[axis] || axis,
      })),
    ];
  }, [availableFilters?.available_axes]);

  const flagFilterOptions = useMemo(() => {
    const base = [{ value: '', label: 'All Flags' }];
    if (!availableFilters?.available_flags?.length) return base;
    
    const flagLabels: Record<string, string> = {
      missing: 'Missing',
      conflict: 'Conflict',
      low_confidence: 'Low Conf',
      ambiguous: 'Ambiguous',
      review: 'Review',
    };
    
    return [
      ...base,
      ...availableFilters.available_flags.map(flag => ({
        value: flag,
        label: flagLabels[flag] || flag,
      })),
    ];
  }, [availableFilters?.available_flags]);
  const total = itemsData?.total ?? 0;
  const currentItem = items[currentIndex];
  const draftChangeCount = session?.draft_change_count ?? 0;

  // Get flagged axes for current item
  const flaggedAxes = useMemo(() => {
    if (!currentItem) return [];
    return (['base', 'technique', 'modifier', 'provenance', 'construct'] as AxisType[])
      .filter((axis) => currentItem.flags[axis])
      .map((axis) => ({
        axis,
        flag: currentItem.flags[axis]!,
      }));
  }, [currentItem]);

  const displayAxes = useMemo(() => {
    const requiredAxes: AxisType[] = ['base', 'modifier', 'technique', 'construct'];
    const required: { axis: AxisType; flag?: AxisFlagType | null }[] = requiredAxes.map((axis) => ({
      axis,
      flag: (currentItem?.flags ?? {})[axis] ?? null,
    }));
    const extra = flaggedAxes.filter((fa) => !requiredAxes.includes(fa.axis));
    return [...required, ...extra];
  }, [currentItem, flaggedAxes]);

  // Navigation
  const goNext = useCallback(() => {
    if (currentIndex < items.length - 1) {
      setCurrentIndex((i) => i + 1);
    }
  }, [currentIndex, items.length]);

  const goPrev = useCallback(() => {
    if (currentIndex > 0) {
      setCurrentIndex((i) => i - 1);
    }
  }, [currentIndex]);

  // Handle option selection via keyboard
  const handleKeySelect = useCallback((rowIndex: number, keyIndex: number) => {
    if (!currentItem || !axisOptions || flaggedAxes.length === 0) return;
    if (rowIndex >= flaggedAxes.length) return;

    const targetAxis = flaggedAxes[rowIndex];
    const opts = filterNonNull((axisOptions as AxisOptions)[targetAxis.axis] ?? []);

    if (keyIndex < opts.length) {
      updateAxisMutation.mutate({
        cohortId: cohort.id,
        stackId: currentItem.stack_id,
        axis: targetAxis.axis,
        value: opts[keyIndex],
      });
    }
  }, [currentItem, axisOptions, flaggedAxes, updateAxisMutation, cohort.id]);

  // Handle axis value selection
  const handleAxisSelect = (axis: AxisType, value: string | null) => {
    if (!currentItem) return;
    updateAxisMutation.mutate({
      cohortId: cohort.id,
      stackId: currentItem.stack_id,
      axis,
      value,
    });
  };

  // Confirm/Discard
  const handleConfirm = () => {
    confirmMutation.mutate(cohort.id, {
      onSuccess: (data) => {
        notifications.show({
          title: 'Saved',
          message: `${data.confirmed_changes} changes saved.`,
          color: 'green',
          icon: <IconCheck size={16} />,
        });
      },
    });
  };

  const handleDiscard = () => {
    discardMutation.mutate(cohort.id, {
      onSuccess: (data) => {
        notifications.show({
          title: 'Discarded',
          message: `${data.discarded_changes} changes discarded.`,
          color: 'yellow',
        });
      },
    });
  };

  // Keyboard shortcuts
  useHotkeys([
    // Navigation
    ['ArrowRight', goNext],
    ['ArrowLeft', goPrev],
    ['l', goNext],
    ['h', goPrev],
    ['Enter', goNext],
    // Row 1: number keys (1-0)
    ['1', () => handleKeySelect(0, 0)],
    ['2', () => handleKeySelect(0, 1)],
    ['3', () => handleKeySelect(0, 2)],
    ['4', () => handleKeySelect(0, 3)],
    ['5', () => handleKeySelect(0, 4)],
    ['6', () => handleKeySelect(0, 5)],
    ['7', () => handleKeySelect(0, 6)],
    ['8', () => handleKeySelect(0, 7)],
    ['9', () => handleKeySelect(0, 8)],
    ['0', () => handleKeySelect(0, 9)],
    // Row 2: letter keys (q-p)
    ['q', () => handleKeySelect(1, 0)],
    ['w', () => handleKeySelect(1, 1)],
    ['e', () => handleKeySelect(1, 2)],
    ['r', () => handleKeySelect(1, 3)],
    ['t', () => handleKeySelect(1, 4)],
    ['y', () => handleKeySelect(1, 5)],
    ['u', () => handleKeySelect(1, 6)],
    ['i', () => handleKeySelect(1, 7)],
    ['o', () => handleKeySelect(1, 8)],
    ['p', () => handleKeySelect(1, 9)],
  ]);

  // Loading
  if (isLoading) {
    return (
      <Center h="100vh" bg="black">
        <Loader color="white" size="lg" />
      </Center>
    );
  }

  // Error
  if (error) {
    return (
      <Center h="100vh" bg="black">
        <Stack align="center">
          <Text c="red" size="lg">Error loading QC items</Text>
          <Button variant="light" onClick={onBack}>Back</Button>
        </Stack>
      </Center>
    );
  }

  // Empty
  if (items.length === 0) {
    return (
      <Center h="100vh" bg="var(--mantine-color-dark-9)">
        <Stack align="center">
          <IconCheck size={64} color="var(--mantine-color-green-5)" />
          <Text size="xl" fw={600} c="white">All Done!</Text>
          <Text c="dimmed">No items need QC review.</Text>
          <Button variant="light" onClick={onBack} mt="md">Back to Dashboard</Button>
        </Stack>
      </Center>
    );
  }

  // Calculate viewer height based on number of flagged axes
  const headerHeight = 48;

  return (
    <Box
      style={{
        height: 'calc(100vh - 100px)', // Fit to viewport, accounting for AppShell header/padding
        display: 'flex',
        flexDirection: 'column',
        backgroundColor: '#000',
        overflow: 'hidden',
      }}
    >
      {/* ===== HEADER: Navigation + Identity/Scanner + Actions ===== */}
      <Box
        px="sm"
        py="xs"
        style={{
          minHeight: headerHeight,
          flexShrink: 0,
          borderBottom: '1px solid rgba(255,255,255,0.1)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 8,
          flexWrap: 'nowrap',
        }}
      >
        {/* Left: Navigation + Filters - compact */}
        <Group gap={6} wrap="nowrap" style={{ flexShrink: 0 }}>
          <Group gap={2} wrap="nowrap">
            <ActionIcon
              variant="subtle"
              color="gray"
              size="sm"
              onClick={goPrev}
              disabled={currentIndex === 0}
            >
              <IconChevronLeft size={16} />
            </ActionIcon>
            <Text size="xs" fw={600} c="white" style={{ minWidth: 50, textAlign: 'center' }}>
              {currentIndex + 1}/{total}
            </Text>
            <ActionIcon
              variant="subtle"
              color="gray"
              size="sm"
              onClick={goNext}
              disabled={currentIndex >= items.length - 1}
            >
              <IconChevronRight size={16} />
            </ActionIcon>
          </Group>

          {/* Compact Filters - only show options that have QC items */}
          <Group gap={4} wrap="nowrap">
            <Select
              size="xs"
              data={axisFilterOptions}
              value={axisFilter ?? ''}
              onChange={handleFilterChange(setAxisFilter)}
              w={100}
              styles={{ 
                input: { 
                  fontSize: 11, 
                  backgroundColor: 'rgba(255,255,255,0.05)',
                  borderColor: 'rgba(255,255,255,0.1)',
                  color: 'white',
                  height: 26,
                  minHeight: 26,
                  paddingLeft: 8,
                  paddingRight: 22,
                } 
              }}
            />
            <Select
              size="xs"
              data={flagFilterOptions}
              value={flagFilter ?? ''}
              onChange={handleFilterChange(setFlagFilter)}
              w={100}
              styles={{ 
                input: { 
                  fontSize: 11, 
                  backgroundColor: 'rgba(255,255,255,0.05)',
                  borderColor: 'rgba(255,255,255,0.1)',
                  color: 'white',
                  height: 26,
                  minHeight: 26,
                  paddingLeft: 8,
                  paddingRight: 22,
                } 
              }}
            />
          </Group>
        </Group>

        {/* Center: Subject/Session/Stack + Scanner badge */}
        <Group gap={8} wrap="nowrap" style={{ flex: 1, justifyContent: 'center', minWidth: 0, overflow: 'hidden' }}>
          {/* Scanner info badge */}
          {(currentItem?.scanner?.manufacturer || currentItem?.scanner?.model || currentItem?.scanner?.field_strength) && (
            <Badge 
              size="sm" 
              variant="light" 
              color="violet"
              styles={{ root: { fontWeight: 500, textTransform: 'none', flexShrink: 0 } }}
            >
              {[
                currentItem.scanner.manufacturer,
                currentItem.scanner.model,
                currentItem.scanner.field_strength ? `${currentItem.scanner.field_strength}T` : null,
              ].filter(Boolean).join(' · ')}
            </Badge>
          )}
          
          {/* Subject/Session/Stack info */}
          <Stack gap={0} align="center" style={{ minWidth: 0 }}>
            <Text size="xs" fw={700} c="cyan" truncate style={{ maxWidth: 140 }}>
              {currentItem?.subject_code ?? '—'}
            </Text>
            <Group gap={4} wrap="nowrap">
              <Text size="xs" c="dimmed" truncate>
                {currentItem?.study_date ?? '—'}
              </Text>
              <Text size="10px" c="rgba(255,255,255,0.4)" ff="monospace">
                #{currentItem?.stack_id}
              </Text>
              {currentItem?.has_draft && (
                <Badge size="xs" color="green" variant="filled" style={{ flexShrink: 0 }}>
                  ✓
                </Badge>
              )}
            </Group>
          </Stack>
        </Group>

        {/* Right: Draft controls - compact */}
        <Group gap={4} wrap="nowrap" style={{ flexShrink: 0 }}>
          {draftChangeCount > 0 && (
            <Badge color="green" variant="light" size="sm">
              {draftChangeCount}
            </Badge>
          )}
          <Button
            size="compact-xs"
            variant="subtle"
            color="red"
            leftSection={<IconX size={12} />}
            onClick={handleDiscard}
            disabled={draftChangeCount === 0 || discardMutation.isPending}
            loading={discardMutation.isPending}
            styles={{ root: { padding: '4px 8px' } }}
          >
            Discard
          </Button>
          <Button
            size="compact-xs"
            variant="filled"
            color="green"
            leftSection={<IconCheck size={12} />}
            onClick={handleConfirm}
            disabled={draftChangeCount === 0 || confirmMutation.isPending}
            loading={confirmMutation.isPending}
            styles={{ root: { padding: '4px 8px' } }}
          >
            Submit
          </Button>
        </Group>
      </Box>

      {/* ===== MAIN: DICOM Viewer with HUD overlays ===== */}
      <Box
        style={{
          flex: 1,
          position: 'relative',
          minHeight: 0,
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        {currentItem && (
          <>
            {/* DICOM Viewer - fills the space */}
            <Box style={{ flex: 1, width: '100%', overflow: 'hidden' }}>
              <DicomViewer
                seriesUid={currentItem.series_uid}
                stackIndex={currentItem.stack_index}
                compact
                maxHeight="100%"
              />
            </Box>

            {/* Top-Left HUD: Acquisition + Sequence info */}
            <Box style={{ ...hudContainerStyle, top: 8, left: 8, alignItems: 'flex-start' }}>
              <AcquisitionHUD params={currentItem.params} tags={currentItem.tags} />
              <SequenceHUD tags={currentItem.tags} />
            </Box>

            {/* Top-Right HUD: Classification */}
            <Box style={{ ...hudContainerStyle, top: 8, right: 8, alignItems: 'flex-end' }}>
              <ClassificationHUD
                current={currentItem.current}
                intent={currentItem.intent}
                draftChanges={currentItem.draft_changes}
              />
            </Box>

            {/* Bottom-Right HUD: FOV - positioned to align with controls hint */}
            <Box style={{ ...hudContainerStyle, bottom: 22, right: 8, alignItems: 'flex-end' }}>
              <FovHUD params={currentItem.params} />
            </Box>
          </>
        )}
      </Box>

      {/* ===== FOOTER: Flagged axes with inline selection ===== */}
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
              const hasDraft = !!(currentItem?.draft_changes && column in currentItem.draft_changes);
              const draftValue = hasDraft ? currentItem?.draft_changes?.[column] : undefined;

              return (
                <FlaggedAxisRow
                  key={fa.axis}
                  axis={fa.axis}
                  flag={fa.flag}
                  currentValue={currentItem?.current[fa.axis] ?? null}
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
      </Stack>
    </Box>
  );
};

export default AxesQCPage;
