/**
 * AxisHeatmap — one axis (T1w or T2w-FLAIR) rendered as a flow-wrapped grid
 * of SubjectStrip cards. Each subject's strip is sized to that subject's own
 * visit count, so there are no empty filler cells.
 */
import { Box, Group, Stack, Text } from '@mantine/core';
import { memo, useMemo } from 'react';

import { SubjectStrip } from './SubjectStrip';
import type { MainQCAxis, MainQCSessionPick, MainQCSummary } from './types';
import {
  type DensityPreset,
  groupPicksBySubject,
} from './utils';

interface AxisHeatmapProps {
  axis: MainQCAxis;
  picks: MainQCSessionPick[];
  density: DensityPreset;
  /** Precomputed (study_id_axis) keys that are dimmed by the active filter. */
  dimmedKeys: Set<string>;
  filterActive: boolean;
  displayIdType: string;
  summary?: MainQCSummary;
  onCellClick: (pick: MainQCSessionPick) => void;
}

export const AxisHeatmap = memo(function AxisHeatmap({
  axis,
  picks,
  density,
  dimmedKeys,
  filterActive,
  displayIdType,
  summary,
  onCellClick,
}: AxisHeatmapProps) {
  const { rows } = useMemo(
    () => groupPicksBySubject(picks, axis, displayIdType),
    [picks, axis, displayIdType],
  );

  const headerLabel =
    axis === 't1w' ? 'Main T1w' : 'Main T2w-FLAIR';

  return (
    <Stack gap="xs" data-testid={`axis-heatmap-${axis}`}>
      <Group gap="md" align="baseline">
        <Text fw={600} size="sm">{headerLabel}</Text>
        {summary ? (
          <Text size="xs" c="dimmed">
            {rows.length} subjects · {summary.total_sessions} sessions ·{' '}
            <span style={{ color: 'var(--mantine-color-green-7)' }}>
              {summary.green} good
            </span>{' · '}
            <span style={{ color: 'var(--mantine-color-yellow-7)' }}>
              {summary.amber} mid
            </span>{' · '}
            <span style={{ color: 'var(--mantine-color-red-6)' }}>
              {summary.red} low
            </span>
            {summary.needs_check > 0 && (
              <>
                {' · '}
                <span style={{ color: 'var(--mantine-color-orange-7)' }}>
                  {summary.needs_check} need review
                </span>
              </>
            )}
          </Text>
        ) : (
          <Text size="xs" c="dimmed">{rows.length} subjects</Text>
        )}
      </Group>

      {rows.length === 0 ? (
        <Box p="xl" ta="center">
          <Text c="dimmed">No sessions in this axis.</Text>
        </Box>
      ) : (
        <Box
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: 6,
            alignContent: 'flex-start',
            // No hard scroll cap — the page layout handles overflow.
          }}
        >
          {rows.map((row) => (
            <SubjectStrip
              key={row.subject_id}
              visits={row.visits}
              density={density}
              dimmedKeys={dimmedKeys}
              filterActive={filterActive}
              displayIdType={displayIdType}
              onCellClick={onCellClick}
            />
          ))}
        </Box>
      )}
    </Stack>
  );
});
