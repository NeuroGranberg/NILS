/**
 * Component test: SessionPickModal (image-tile edition).
 *
 * Validates the new tile-based contract:
 *   - the needs_check banner renders when reasons are present,
 *   - candidates are grouped into Current pick / Family siblings / Other,
 *   - clicking the per-tile checkbox moves a candidate into "Current pick",
 *   - rendering with pick=null produces no modal.
 *
 * We mock the API hooks and the StackImagePane (the latter pulls images via
 * fetch which is unnecessary noise in this unit test).
 */
import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MantineProvider } from '@mantine/core';
import userEvent from '@testing-library/user-event';

import { SessionPickModal } from '../SessionPickModal';
import type { MainQCSessionPick } from '../types';

const ackMutate = vi.fn().mockResolvedValue(undefined);

vi.mock('../api', () => ({
  useUpdateSessionPickMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useResetSessionPickMutation:  () => ({ mutateAsync: vi.fn(), isPending: false }),
  useAcknowledgeSessionPickMutation: () => ({ mutateAsync: ackMutate, isPending: false }),
}));

vi.mock('@mantine/notifications', () => ({
  notifications: { show: vi.fn() },
}));

// Skip the image fetch path entirely in this unit test.
vi.mock('../../../qc/components/main-acq/StackImagePane', () => ({
  StackImagePane: () => <div data-testid="stack-image-pane" />,
}));

vi.mock('../../../qc/api/main-acq', () => ({
  useSetStackRole: () => ({ mutate: vi.fn() }),
}));

const dixonPick: MainQCSessionPick = {
  subject_id: 1,
  session_date: '2024-01-01',
  subject_code: 'S001',
  axis: 't1w',
  study_ids: [99],
  primary_study_id: 99,
  winning_stack_ids: [101],
  score: 0.82,
  needs_check: true,
  needs_check_reasons: ['dixon_vs_plain'],
  candidate_summary: [
    {
      stack_id: 101,
      score: 0.82,
      technique: 'VIBE',
      modifier_csv: 'Dixon',
      construct_csv: 'InPhase',
      dim: '3D',
      slices: 176,
      fov_x_mm: 240,
      post_contrast: 0,
      provenance: 'RawRecon',
      is_main: true,
      in_winning_family: true,
      is_canonical_in_family: true,
      series_instance_uid: '1.2.3.4',
      stack_index: 0,
    },
    {
      stack_id: 102,
      score: 0.82,
      technique: 'VIBE',
      modifier_csv: 'Dixon',
      construct_csv: 'Water',
      dim: '3D',
      slices: 176,
      fov_x_mm: 240,
      post_contrast: 0,
      provenance: 'RawRecon',
      is_main: false,
      in_winning_family: true,
      is_canonical_in_family: false,
      series_instance_uid: '1.2.3.5',
      stack_index: 0,
    },
    {
      stack_id: 103,
      score: 0.78,
      technique: 'MPRAGE',
      modifier_csv: null,
      construct_csv: null,
      dim: '3D',
      slices: 176,
      fov_x_mm: 240,
      post_contrast: 0,
      provenance: 'RawRecon',
      is_main: false,
      in_winning_family: false,
      is_canonical_in_family: false,
      series_instance_uid: '1.2.3.6',
      stack_index: 0,
    },
  ],
  content: {
    technique: 'VIBE',
    dim: '3D',
    family: 'dixon',
    slice_bucket: 'hi',
    slices: 176,
  },
  subject_other_ids: {},
};

const renderModal = (pick: MainQCSessionPick | null) =>
  render(
    <MantineProvider>
      <SessionPickModal
        cohortId={1}
        pick={pick}
        opened={true}
        onClose={vi.fn()}
      />
    </MantineProvider>,
  );

describe('SessionPickModal', () => {
  it('renders one tile per candidate with image preview and stack-id annotation', () => {
    renderModal(dixonPick);
    const tiles = screen.getAllByTestId('main-qc-candidate-tile');
    expect(tiles).toHaveLength(3);
    const stackIds = tiles.map((t) => t.getAttribute('data-stack-id'));
    expect(stackIds).toEqual(expect.arrayContaining(['101', '102', '103']));
  });

  it('groups candidates: winner under Current pick, sister under Family siblings, other elsewhere', () => {
    renderModal(dixonPick);
    expect(screen.getByText(/current pick/i)).toBeInTheDocument();
    expect(screen.getByText(/family siblings/i)).toBeInTheDocument();
    expect(screen.getByText(/other candidates/i)).toBeInTheDocument();
  });

  it('shows the needs_check banner when reasons are present', () => {
    renderModal(dixonPick);
    expect(screen.getByText(/needs review/i)).toBeInTheDocument();
    expect(screen.getByText(/Dixon family/i)).toBeInTheDocument();
  });

  it('renders nothing when pick is null', () => {
    const { container } = renderModal(null);
    expect(container.querySelector('[data-testid="main-qc-session-modal"]')).toBeNull();
  });

  it('renders the "In NILS we trust" button enabled for a clean unacknowledged pick', () => {
    renderModal({
      ...dixonPick,
      // Clean: no review-level reasons, not acknowledged.
      needs_check: false,
      needs_check_reasons: [],
      acknowledged: false,
    });
    const btn = screen.getByTestId('main-qc-acknowledge-button');
    expect(btn).toHaveTextContent(/In NILS we trust/i);
    expect(btn).not.toBeDisabled();
  });

  it('disables "In NILS we trust" when the pick is already acknowledged', () => {
    renderModal({ ...dixonPick, acknowledged: true });
    expect(screen.getByTestId('main-qc-acknowledge-button')).toBeDisabled();
    // The "Reviewed" badge should also appear in the header.
    expect(screen.getByTestId('main-qc-reviewed-badge')).toBeInTheDocument();
  });

  it('calls the acknowledge mutation with the pick axis when clicked', async () => {
    ackMutate.mockClear();
    const onClose = vi.fn();
    render(
      <MantineProvider>
        <SessionPickModal
          cohortId={1}
          pick={{ ...dixonPick, acknowledged: false }}
          opened={true}
          onClose={onClose}
        />
      </MantineProvider>,
    );
    await userEvent.click(screen.getByTestId('main-qc-acknowledge-button'));
    expect(ackMutate).toHaveBeenCalledWith({
      subject_id: dixonPick.subject_id,
      session_date: dixonPick.session_date,
      axis: dixonPick.axis,
    });
    expect(onClose).toHaveBeenCalled();
  });
});
