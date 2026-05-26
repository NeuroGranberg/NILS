/**
 * Component test: OverrideModal — preselects the new_label, lets the
 * user pick a correction, and submits a single
 * ``BodyPartOverridePayload`` via the override mutation.
 */
import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { MantineProvider } from '@mantine/core';

import { OverrideModal } from '../OverrideModal';
import type { BodyPartChangeRow } from '../types';

const overrideMutate = vi.fn();

vi.mock('../api', () => ({
  useOverrideStackMutation: () => ({
    mutate: overrideMutate, isPending: false,
  }),
}));

vi.mock('@mantine/notifications', () => ({
  notifications: { show: vi.fn() },
}));

const sampleRow: BodyPartChangeRow = {
  study_id: 1, subject_id: 1, stack_id: 101, subject_code: 'S001',
  session_date: '2024-02-01', series_description: null,
  technique: 'MPRAGE', orientation: 'axial',
  previous_label: 'brain', new_label: 'Brain',
  prior_source: 'text_keyword', confidence: 0.62,
  needs_check: true, is_override: false,
  thumbnail_url: '/thumb/101',
};

const renderModal = (props: Partial<Parameters<typeof OverrideModal>[0]> = {}) =>
  render(
    <MantineProvider>
      <OverrideModal
        cohortId={1}
        categories={['Brain', 'Brain-Neck', 'Spine']}
        probs={{ Brain: 0.62, 'Brain-Neck': 0.20, Spine: 0.18 }}
        row={sampleRow}
        opened
        onClose={() => {}}
        {...props}
      />
    </MantineProvider>,
  );

describe('OverrideModal', () => {
  it('renders the prior → new diff and the per-class probabilities', () => {
    renderModal();
    expect(screen.getByText('Was:')).toBeInTheDocument();
    // Prior label "brain" appears as a badge.
    expect(screen.getByText('brain')).toBeInTheDocument();
    // Probability bars list every class label as a row — at least one
    // match is enough (Mantine renders the same labels in both the
    // probability bars and the Select dropdown options).
    expect(screen.getAllByText('Brain-Neck').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Spine').length).toBeGreaterThan(0);
  });

  it('disables Save when the label is unchanged AND no note', () => {
    renderModal();
    const save = screen.getByRole('button', { name: /save override/i });
    expect(save).toBeDisabled();
  });

  it('submits an override after a note is provided (label unchanged)', () => {
    overrideMutate.mockClear();
    renderModal();
    // Adding a note is enough to enable the submit button (audit
    // trail; we don't require a label change).
    const note = screen.getByLabelText('Note (optional)');
    fireEvent.change(note, { target: { value: 'manual fix' } });
    fireEvent.click(
      screen.getByRole('button', { name: /save override/i }),
    );
    expect(overrideMutate).toHaveBeenCalledTimes(1);
    const arg = overrideMutate.mock.calls[0][0];
    expect(arg.subject_id).toBe(1);
    expect(arg.session_date).toBe('2024-02-01');
    expect(arg.stack_id).toBe(101);
    expect(arg.note).toBe('manual fix');
    // Default label is the row's new_label.
    expect(arg.label).toBe('Brain');
  });
});
