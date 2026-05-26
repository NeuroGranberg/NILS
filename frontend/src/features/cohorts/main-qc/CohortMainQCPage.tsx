/**
 * Main T1w / T2w-FLAIR auto-pick QC — rev 4 layout.
 *
 * - Continuous score color gradient (no high/low wording).
 * - Content legend with chips for technique / dim / family / resolution / status.
 * - Side-by-side axes (T1w + FLAIR) by default; selectable to a single axis.
 * - SubjectStrip cards: each subject sized to its own visit count, no fillers,
 *   internal multi-row when visits exceed innerCols.
 * - Density picker and identifier picker (synced via localStorage with the
 *   per-session QC viewer).
 */
import { ActionIcon, Alert, Group, Loader, Stack, Text, Title } from '@mantine/core';
import { useLocalStorage } from '@mantine/hooks';
import { IconArrowLeft, IconInfoCircle } from '@tabler/icons-react';
import { startTransition, useCallback, useEffect, useMemo, useState } from 'react';

import type { Cohort } from '../../../types/cohort';

import { ApplyButton } from './ApplyButton';
import { AxisHeatmap } from './AxisHeatmap';
import { AxisModePicker } from './AxisModePicker';
import { ContentLegend } from './ContentLegend';
import { DensityPicker } from './DensityPicker';
import { HeatmapTooltipProvider } from './HeatmapTooltip';
import { IdentifierPicker } from './IdentifierPicker';
import { RestorePreviousButton } from './RestorePreviousButton';
import { SessionPickModal } from './SessionPickModal';
import { cellKey } from './SubjectStrip';
import { useMainQCStateQuery } from './api';
import type {
  AxisDisplayMode,
  ContentFilter,
  Density,
  MainQCSessionPick,
} from './types';
import { EMPTY_FILTER } from './types';
import { DENSITY_PRESETS, isFilterActive, pickMatchesFilter } from './utils';

interface CohortMainQCPageProps {
  cohort: Cohort;
  onBack: () => void;
}

export const CohortMainQCPage = ({ cohort, onBack }: CohortMainQCPageProps) => {
  const cohortId = cohort.id;
  const { data, isLoading, isError, error } = useMainQCStateQuery(cohortId);

  const [axisMode, setAxisMode] = useLocalStorage<AxisDisplayMode>({
    key: 'cohort-main-qc-axis-mode',
    defaultValue: 'both',
  });
  const [density, setDensity] = useLocalStorage<Density>({
    key: 'cohort-main-qc-density',
    defaultValue: 'compact',
  });
  const [displayIdType, setDisplayIdType] = useLocalStorage<string>({
    key: 'qc-viewer-display-id-type',
    defaultValue: 'code',
  });

  const [filter, setFilter] = useState<ContentFilter>(EMPTY_FILTER);
  const [activePick, setActivePick] = useState<MainQCSessionPick | null>(null);
  const [modalOpen, setModalOpen] = useState(false);

  const picks = data?.picks ?? [];
  const summary = data?.summary ?? {};
  const availableIdTypes = data?.available_id_types ?? ['code'];

  // If localStorage holds a now-unavailable id type (e.g. user switched cohorts),
  // fall back to "code".
  useEffect(() => {
    if (availableIdTypes.length > 0 && !availableIdTypes.includes(displayIdType)) {
      setDisplayIdType('code');
    }
  }, [availableIdTypes, displayIdType, setDisplayIdType]);

  const densityPreset = useMemo(() => DENSITY_PRESETS[density], [density]);

  // When a fresh state arrives, refresh the open modal's pick.
  useEffect(() => {
    if (!modalOpen || !activePick || !data) return;
    const next = data.picks.find(
      (p) =>
        p.subject_id === activePick.subject_id
        && p.session_date === activePick.session_date
        && p.axis === activePick.axis,
    );
    if (next && next !== activePick) setActivePick(next);
  }, [data, modalOpen, activePick]);

  const handleCellClick = useCallback((pick: MainQCSessionPick) => {
    setActivePick(pick);
    setModalOpen(true);
  }, []);

  // Precompute the set of cells that are dimmed by the current filter, once
  // per (picks, filter) change. Cells then read this in O(1) instead of running
  // pickMatchesFilter on every render.
  const filterActive = useMemo(() => isFilterActive(filter), [filter]);
  const dimmedKeys = useMemo(() => {
    if (!filterActive) return new Set<string>();
    const out = new Set<string>();
    for (const p of picks) {
      if (!pickMatchesFilter(p, filter)) out.add(cellKey(p));
    }
    return out;
  }, [picks, filter, filterActive]);

  // Chip toggles update the filter as a non-urgent transition: the chip flips
  // immediately, the heatmap re-render can yield to input.
  const handleFilterChange = useCallback((next: typeof filter) => {
    startTransition(() => setFilter(next));
  }, []);

  if (isLoading) {
    return (
      <Stack align="center" py="xl">
        <Loader />
      </Stack>
    );
  }

  if (isError) {
    return (
      <Alert color="red" icon={<IconInfoCircle size={16} />}>
        Failed to load Main QC state: {(error as Error).message}
      </Alert>
    );
  }

  const showT1w = axisMode === 't1w' || axisMode === 'both';
  const showFlair = axisMode === 'flair' || axisMode === 'both';

  return (
    <HeatmapTooltipProvider>
    <Stack gap="md" data-testid="cohort-main-qc-page">
      {/* Header */}
      <Group justify="space-between" align="flex-start">
        <Group gap="md" align="flex-start">
          <ActionIcon variant="subtle" size="lg" onClick={onBack} aria-label="Back">
            <IconArrowLeft size={24} />
          </ActionIcon>
          <Stack gap={2}>
            <Title order={2} c="var(--nils-text-primary)">
              Main T1w / T2w-FLAIR — {cohort.name}
            </Title>
            <Text size="sm" c="var(--nils-text-tertiary)">
              Algorithmic main-T1w and main-FLAIR pick per session, written as
              the <code>main_acquisition</code> tag. Click a cell to override.
            </Text>
            {data?.current_run_at && (
              <Text size="xs" c="dimmed">
                Last run: {new Date(data.current_run_at).toLocaleString()}
              </Text>
            )}
          </Stack>
        </Group>
        <Group gap="xs">
          <RestorePreviousButton
            cohortId={cohortId}
            hasPrevious={!!data?.has_previous}
            previousRunAt={data?.previous_run_at ?? null}
          />
          <ApplyButton cohortId={cohortId} hasCurrent={!!data?.has_current} />
        </Group>
      </Group>

      {!data?.has_current ? (
        <Alert color="blue" icon={<IconInfoCircle size={16} />}>
          No auto-pick has been run yet for this cohort. Click <strong>Run auto-pick</strong>{' '}
          to compute a main acquisition for every session.
        </Alert>
      ) : (
        <>
          {/* Controls row */}
          <Group justify="space-between" wrap="wrap" gap="md">
            <Group gap="md" align="center" wrap="wrap">
              <Group gap={6} align="center">
                <Text size="xs" c="dimmed">Show as</Text>
                <IdentifierPicker
                  value={displayIdType}
                  options={availableIdTypes}
                  onChange={setDisplayIdType}
                />
              </Group>
              <Group gap={6} align="center">
                <Text size="xs" c="dimmed">Density</Text>
                <DensityPicker value={density} onChange={setDensity} />
              </Group>
            </Group>
            <Group gap={6} align="center">
              <Text size="xs" c="dimmed">Show</Text>
              <AxisModePicker value={axisMode} onChange={setAxisMode} />
            </Group>
          </Group>

          {/* Legend */}
          <ContentLegend
            picks={picks}
            axisMode={axisMode}
            filter={filter}
            onFilterChange={handleFilterChange}
          />

          {/* Heatmaps */}
          <Group
            align="flex-start"
            gap="lg"
            wrap={axisMode === 'both' ? 'nowrap' : 'wrap'}
            style={{ minWidth: 0 }}
          >
            {showT1w && (
              <Stack gap="xs" style={{ flex: 1, minWidth: 0 }}>
                <AxisHeatmap
                  axis="t1w"
                  picks={picks}
                  density={densityPreset}
                  dimmedKeys={dimmedKeys}
                  filterActive={filterActive}
                  displayIdType={displayIdType}
                  summary={summary.t1w}
                  onCellClick={handleCellClick}
                />
              </Stack>
            )}
            {showFlair && (
              <Stack gap="xs" style={{ flex: 1, minWidth: 0 }}>
                <AxisHeatmap
                  axis="flair"
                  picks={picks}
                  density={densityPreset}
                  dimmedKeys={dimmedKeys}
                  filterActive={filterActive}
                  displayIdType={displayIdType}
                  summary={summary.flair}
                  onCellClick={handleCellClick}
                />
              </Stack>
            )}
          </Group>
        </>
      )}

      <SessionPickModal
        cohortId={cohortId}
        pick={activePick}
        opened={modalOpen}
        onClose={() => setModalOpen(false)}
        displayIdType={displayIdType}
      />
    </Stack>
    </HeatmapTooltipProvider>
  );
};
