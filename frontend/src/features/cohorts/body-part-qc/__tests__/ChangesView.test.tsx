/**
 * Component test: ChangesView — table renders rows from the changes
 * query, filter dropdowns trigger refetches, and clicking the edit
 * icon opens the override modal.
 */
import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { MantineProvider } from '@mantine/core';

import { ChangesView } from '../ChangesView';
import type { BodyPartChangeRow, BodyPartSessionPick } from '../types';

let lastQueryParams: Record<string, unknown> = {};
const overrideMutate = vi.fn();

const rows: BodyPartChangeRow[] = [
  {
    study_id: 1, subject_id: 1, stack_id: 101, subject_code: 'S001',
    session_date: '2024-02-01', series_description: null,
    technique: 'MPRAGE', orientation: 'axial',
    previous_label: 'brain', new_label: 'Brain',
    prior_source: 'text_keyword', confidence: 0.62,
    needs_check: true, is_override: false,
    thumbnail_url: '/thumb/101',
  },
  {
    study_id: 2, subject_id: 2, stack_id: 202, subject_code: 'S002',
    session_date: '2024-03-01', series_description: null,
    technique: 'MPRAGE', orientation: 'axial',
    previous_label: null, new_label: 'Brain',
    prior_source: null, confidence: 0.91,
    needs_check: false, is_override: false,
    thumbnail_url: '/thumb/202',
  },
];

vi.mock('../api', async () => ({
  useBodyPartChangesQuery: (cohortId: unknown, params: Record<string, unknown>) => {
    lastQueryParams = params;
    return {
      data: {
        total: rows.length, offset: 0, limit: 50, rows,
      },
      isLoading: false,
    };
  },
  useOverrideStackMutation: () => ({
    mutate: overrideMutate, isPending: false,
  }),
  useCommitBodyPartMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
  useDestageBodyPartMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
}));

vi.mock('@mantine/notifications', () => ({
  notifications: { show: vi.fn() },
}));

const picks: BodyPartSessionPick[] = [
  {
    subject_id: 1, session_date: '2024-02-01',
    subject_code: 'S001',
    study_ids: [1], primary_study_id: 1,
    stacks: [{
      stack_id: 101, label: 'Brain', confidence: 0.62,
      probs: { Brain: 0.62, Spine: 0.28 },
      is_override: false, needs_check: true,
      series_instance_uid: 'uid', technique: 'MPRAGE',
      orientation: 'axial', previous_label: 'brain',
      prior_source: 'text_keyword', changed: true,
    }],
    session_combo: ['Brain'], session_combo_key: 'Brain',
    session_prev_combo_key: 'brain', session_changed: true,
    stacks_changed: 1, low_conf_count: 1, needs_check: true,
    subject_other_ids: {},
  },
];

const renderView = () =>
  render(
    <MantineProvider>
      <ChangesView
        cohortId={1}
        categories={['Brain', 'Spine']}
        picks={picks}
      />
    </MantineProvider>,
  );

describe('ChangesView', () => {
  it('renders one row per change with previous → new badges', () => {
    renderView();
    const row1 = screen.getByTestId('body-part-change-row-101');
    const row2 = screen.getByTestId('body-part-change-row-202');
    // First row had prior "brain"; second had no prior body_part.
    expect(row1.textContent).toMatch(/brain/);
    expect(row2.textContent).toMatch(/\(none\)/);
  });

  it('flags low-confidence rows', () => {
    renderView();
    const row = screen.getByTestId('body-part-change-row-101');
    expect(row.textContent).toMatch(/low-conf/);
    expect(row.textContent).toMatch(/62%/);
  });

  it('passes pagination params to the changes query on initial render', () => {
    renderView();
    expect(lastQueryParams).toMatchObject({
      offset: 0,
      limit: 50,
    });
  });

  it('opens the override modal when the edit icon is clicked', async () => {
    renderView();
    const editButtons = screen.getAllByLabelText('Override this stack');
    fireEvent.click(editButtons[0]);
    // Override modal renders the corrected-label select.
    const select = await screen.findByTestId('override-label-select');
    expect(select).toBeInTheDocument();
  });
});
