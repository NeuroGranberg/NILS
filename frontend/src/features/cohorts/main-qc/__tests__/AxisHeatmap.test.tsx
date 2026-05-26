/**
 * AxisHeatmap:
 *   - renders one strip per subject in the chosen axis
 *   - subjects with different visit counts each render their own card sized
 *     to that count — no global grid
 *   - displayIdType drives sort order
 */
import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MantineProvider } from '@mantine/core';

import { AxisHeatmap } from '../AxisHeatmap';
import type { MainQCSessionPick } from '../types';
import { DENSITY_PRESETS } from '../utils';

const visit = (
  overrides: Partial<MainQCSessionPick> & { study_id?: number } = {},
): MainQCSessionPick => {
  const { study_id, ...rest } = overrides as Partial<MainQCSessionPick> & { study_id?: number };
  const primary = rest.primary_study_id ?? study_id ?? 1;
  // Keep visits unique by giving each (study_id-derived) a unique date so the
  // (subject_id, session_date) cell key is distinct.
  const defaultDate = `2024-${String((primary % 12) + 1).padStart(2, '0')}-${String((primary % 28) + 1).padStart(2, '0')}`;
  return {
    subject_id: 1,
    session_date: rest.session_date ?? defaultDate,
    subject_code: 'S001',
    axis: 't1w',
    study_ids: rest.study_ids ?? [primary],
    primary_study_id: primary,
    winning_stack_ids: [10],
    score: 0.8,
    needs_check: false,
    needs_check_reasons: [],
    candidate_summary: [],
    content: {
      technique: 'MPRAGE',
      dim: '3D',
      family: 'plain',
      slice_bucket: 'hi',
      slices: 192,
    },
    subject_other_ids: {},
    ...rest,
  };
};

const renderAxis = (picks: MainQCSessionPick[]) =>
  render(
    <MantineProvider>
      <AxisHeatmap
        axis="t1w"
        picks={picks}
        density={DENSITY_PRESETS.compact}
        dimmedKeys={new Set()}
        filterActive={false}
        displayIdType="code"
        onCellClick={vi.fn()}
      />
    </MantineProvider>,
  );

describe('AxisHeatmap', () => {
  it('renders one SubjectStrip card per subject', () => {
    const picks = [
      visit({ subject_id: 1, subject_code: 'A', study_id: 1 }),
      visit({ subject_id: 1, subject_code: 'A', study_id: 2 }),
      visit({ subject_id: 2, subject_code: 'B', study_id: 3 }),
    ];
    renderAxis(picks);
    expect(screen.getAllByTestId('subject-strip')).toHaveLength(2);
  });

  it('only renders picks for the selected axis', () => {
    const picks = [
      visit({ subject_id: 1, axis: 't1w' }),
      visit({ subject_id: 2, subject_code: 'B', axis: 'flair' }),
    ];
    renderAxis(picks);
    expect(screen.getAllByTestId('subject-strip')).toHaveLength(1);
  });

  it('does NOT add filler cells (subject A has 2 visits, B has 5)', () => {
    const picks = [
      visit({ subject_id: 1, subject_code: 'A', study_id: 1 }),
      visit({ subject_id: 1, subject_code: 'A', study_id: 2 }),
      ...Array.from({ length: 5 }, (_, i) =>
        visit({ subject_id: 2, subject_code: 'B', study_id: 100 + i }),
      ),
    ];
    renderAxis(picks);
    // Total cells = 2 + 5 = 7 (rev 3 would have padded A to 5 cells = 10 total).
    expect(screen.getAllByTestId('main-qc-cell')).toHaveLength(7);
  });

  it('shows an empty-state message when no picks match the axis', () => {
    const picks = [visit({ subject_id: 1, axis: 'flair' })];
    renderAxis(picks);
    expect(screen.getByText(/no sessions in this axis/i)).toBeInTheDocument();
  });
});
