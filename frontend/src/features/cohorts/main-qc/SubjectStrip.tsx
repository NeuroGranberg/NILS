/**
 * One subject's visit strip — a small bordered card sized to that subject's
 * own visit count. Inner cells flow left-to-right and wrap onto another row
 * inside the SAME card if the visit count exceeds `innerCols`. No empty
 * filler cells are rendered.
 *
 * Performance notes:
 * - Cells use a single shared deferred tooltip (HeatmapTooltipProvider) instead
 *   of one Mantine Tooltip per cell — the latter was the dominant cost on big
 *   cohorts (thousands of Floating UI handlers).
 * - SubjectStrip and Cell are React.memo-wrapped so chip-toggles only re-render
 *   the strips/cells that actually change.
 * - The "dimmed by filter" bit is precomputed at the page level via a shared
 *   `dimmedKeys` Set, so cells don't run filter evaluation themselves.
 */
import { Box, Stack, Text } from '@mantine/core';
import { memo, useMemo } from 'react';

import type { MainQCSessionPick } from './types';
import { CELL_BORDER_COLORS, EMPTY_CELL_FILL } from './types';
import {
  type DensityPreset,
  formatScore,
  reasonsLabel,
  scoreToColor,
  subjectDisplayLabel,
} from './utils';
import { useHeatmapTooltip } from './HeatmapTooltip';

interface SubjectStripProps {
  /** Subject's chronologically-ordered picks for ONE axis. */
  visits: MainQCSessionPick[];
  density: DensityPreset;
  /** Precomputed set of "study_id_axis" cell keys that are dimmed by the filter. */
  dimmedKeys: Set<string>;
  /** True iff any filter chip is currently active. */
  filterActive: boolean;
  displayIdType: string;
  onCellClick: (pick: MainQCSessionPick) => void;
}

/**
 * Stable cell identity for the heatmap.
 *
 * A "cell" in the heatmap is one (subject, date, axis) — *not* one study —
 * because a single calendar visit can span multiple StudyInstanceUIDs.
 */
export const cellKey = (p: MainQCSessionPick) =>
  `${p.subject_id}_${p.session_date ?? ''}_${p.axis}`;

export const SubjectStrip = memo(function SubjectStrip({
  visits,
  density,
  dimmedKeys,
  filterActive,
  displayIdType,
  onCellClick,
}: SubjectStripProps) {
  if (visits.length === 0) return null;

  const label = subjectDisplayLabel(visits[0], displayIdType);
  const titleAttr =
    displayIdType !== 'code' && visits[0]?.subject_code
      ? `Code: ${visits[0].subject_code}`
      : undefined;

  const cols = Math.min(density.innerCols, visits.length);
  const cellW = density.cellSize;
  const cellsBlockWidth = cols * cellW + Math.max(0, cols - 1) * density.gap;

  // Make sure the strip is at least as wide as the subject label so we never
  // truncate (single-session subjects would otherwise be ~10px wide).
  const labelWidth = Math.ceil(label.length * density.labelFontSize * 0.62) + 2;
  const contentWidth = Math.max(cellsBlockWidth, labelWidth);

  return (
    <Stack
      gap={2}
      data-testid="subject-strip"
      data-subject-id={visits[0].subject_id}
      style={{
        border: '1px solid var(--nils-border)',
        borderRadius: 4,
        padding: 4,
        flex: '0 0 auto',
        background: 'var(--nils-bg-secondary)',
      }}
    >
      <Text
        size="xs"
        title={titleAttr ?? label}
        style={{
          fontFamily: 'monospace',
          fontSize: density.labelFontSize,
          width: contentWidth,
          color: 'var(--nils-text-primary)',
          whiteSpace: 'nowrap',
          lineHeight: 1.1,
        }}
      >
        {label}
      </Text>
      <Box
        style={{
          display: 'grid',
          gridTemplateColumns: `repeat(${cols}, ${cellW}px)`,
          gap: density.gap,
          width: contentWidth,
        }}
      >
        {visits.map((pick) => (
          <Cell
            key={cellKey(pick)}
            pick={pick}
            size={cellW}
            dimmed={filterActive && dimmedKeys.has(cellKey(pick))}
            onClick={onCellClick}
          />
        ))}
      </Box>
    </Stack>
  );
});

interface CellProps {
  pick: MainQCSessionPick;
  size: number;
  dimmed: boolean;
  onClick: (pick: MainQCSessionPick) => void;
}

const Cell = memo(function Cell({ pick, size, dimmed, onClick }: CellProps) {
  const tooltip = useHeatmapTooltip();

  const hasPick = !!(pick.winning_stack_ids && pick.winning_stack_ids.length > 0);
  const fill = hasPick ? scoreToColor(pick.score) : EMPTY_CELL_FILL;

  let border = '1px solid rgba(0,0,0,0.10)';
  const isManual = pick.needs_check_reasons?.includes('manual_override');
  if (isManual) {
    border = `2px solid ${CELL_BORDER_COLORS['manual-pick']}`;
  } else if (pick.needs_check) {
    border = `2px solid ${CELL_BORDER_COLORS['needs-check']}`;
  }

  // Memoise the inline style so React reuses the same object across renders
  // when nothing visual changed.
  const cellStyle = useMemo<React.CSSProperties>(
    () => ({
      width: size,
      height: size,
      background: fill,
      border,
      borderRadius: 2,
      cursor: 'pointer',
      opacity: dimmed ? 0.18 : 1,
      transition: 'opacity 80ms ease',
    }),
    [size, fill, border, dimmed],
  );

  const handleEnter = (e: React.MouseEvent<HTMLDivElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    tooltip.show(<TooltipContent pick={pick} />, rect);
  };
  const handleLeave = () => tooltip.hide();
  const handleClick = () => onClick(pick);

  return (
    <div
      data-testid="main-qc-cell"
      data-has-pick={hasPick}
      data-needs-check={pick.needs_check}
      data-dimmed={dimmed}
      onClick={handleClick}
      onMouseEnter={handleEnter}
      onMouseLeave={handleLeave}
      style={cellStyle}
    />
  );
});

const TooltipContent = ({ pick }: { pick: MainQCSessionPick }) => (
  <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
    <div style={{ fontWeight: 600 }}>{pick.subject_code}</div>
    <div style={{ opacity: 0.7 }}>{pick.session_date ?? '(no date)'}</div>
    <div>Score: {formatScore(pick.score)}</div>
    {pick.content?.technique && (
      <div style={{ opacity: 0.7 }}>
        {pick.content.technique}
        {pick.content.dim ? ` · ${pick.content.dim}` : ''}
        {pick.content.slices ? ` · ${pick.content.slices} sl` : ''}
      </div>
    )}
    {pick.needs_check_reasons.length > 0 && (
      <div style={{ color: '#fd7e14' }}>
        {pick.needs_check_reasons.map(reasonsLabel).join('; ')}
      </div>
    )}
  </div>
);
