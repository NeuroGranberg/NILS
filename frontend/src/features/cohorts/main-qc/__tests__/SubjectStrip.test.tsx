/**
 * SubjectStrip:
 *   - renders exactly N visit cells (no fillers)
 *   - subject with > innerCols visits gets a multi-row inner grid (still ONE strip)
 *   - cell click invokes the handler
 *   - subject label respects displayIdType
 */
import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MantineProvider } from '@mantine/core';

import { SubjectStrip, cellKey } from '../SubjectStrip';
import type { MainQCSessionPick } from '../types';
import { DENSITY_PRESETS } from '../utils';

// Test helper: a "visit" used to take `study_id`; with the session-keying
// change that became `primary_study_id` (+ the implicit `study_ids: [n]`).
// We accept either field for backward-compat across the existing test bodies.
const visit = (
  overrides: Partial<MainQCSessionPick> & { study_id?: number } = {},
): MainQCSessionPick => {
  const { study_id, ...rest } = overrides as Partial<MainQCSessionPick> & { study_id?: number };
  const primary = rest.primary_study_id ?? study_id ?? 1;
  // Default each visit to a unique date so the subject row reflects N
  // distinct sessions even when only `study_id` is varied in the test.
  // Encode primary across (year, month, day) so up to 28*12*N visits stay
  // unique without collisions.
  const yyyy = 2020 + Math.floor((primary - 1) / (12 * 28));
  const mm = String(Math.floor(((primary - 1) % (12 * 28)) / 28) + 1).padStart(2, '0');
  const dd = String(((primary - 1) % 28) + 1).padStart(2, '0');
  const defaultDate = `${yyyy}-${mm}-${dd}`;
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

const renderStrip = (visits: MainQCSessionPick[], extra?: Partial<React.ComponentProps<typeof SubjectStrip>>) =>
  render(
    <MantineProvider>
      <SubjectStrip
        visits={visits}
        density={DENSITY_PRESETS.compact}
        dimmedKeys={new Set()}
        filterActive={false}
        displayIdType="code"
        onCellClick={extra?.onCellClick ?? vi.fn()}
        {...extra}
      />
    </MantineProvider>,
  );

describe('SubjectStrip', () => {
  it('renders exactly one cell per visit (no filler)', () => {
    const visits = [
      visit({ study_id: 1 }),
      visit({ study_id: 2 }),
      visit({ study_id: 3 }),
    ];
    renderStrip(visits);
    const cells = screen.getAllByTestId('main-qc-cell');
    expect(cells).toHaveLength(3);
  });

  it('uses subject_code label by default', () => {
    renderStrip([visit({ subject_code: 'X42' })]);
    expect(screen.getByText('X42')).toBeInTheDocument();
  });

  it('uses other id when displayIdType is set and the id exists', () => {
    renderStrip(
      [visit({ subject_code: 'X42', subject_other_ids: { MRN: 'M-9001' } })],
      { displayIdType: 'MRN' },
    );
    expect(screen.getByText('M-9001')).toBeInTheDocument();
  });

  it('falls back to subject_code when chosen id is missing', () => {
    renderStrip([visit({ subject_code: 'X42', subject_other_ids: {} })], { displayIdType: 'MRN' });
    expect(screen.getByText('X42')).toBeInTheDocument();
  });

  it('invokes onCellClick when a cell is clicked', () => {
    const onCellClick = vi.fn();
    renderStrip([visit({ study_id: 7 })], { onCellClick });
    screen.getAllByTestId('main-qc-cell')[0].click();
    expect(onCellClick).toHaveBeenCalledWith(
      expect.objectContaining({ primary_study_id: 7 }),
    );
  });

  it('renders 32 cells in a single strip when visits exceed innerCols', () => {
    const visits = Array.from({ length: 32 }, (_, i) => visit({ study_id: i + 1 }));
    renderStrip(visits);  // compact density: innerCols=8 → 4 inner rows, ONE strip
    expect(screen.getAllByTestId('main-qc-cell')).toHaveLength(32);
    expect(screen.getAllByTestId('subject-strip')).toHaveLength(1);
  });

  it('marks cells dimmed when filter is active and the cell key is in dimmedKeys', () => {
    const v = visit({
      content: { technique: 'TSE', dim: '2D', family: 'plain', slice_bucket: 'std', slices: 100 },
    });
    renderStrip([v], {
      dimmedKeys: new Set([cellKey(v)]),
      filterActive: true,
    });
    const cells = screen.getAllByTestId('main-qc-cell');
    expect(cells[0].getAttribute('data-dimmed')).toBe('true');
  });
});
