/**
 * M10 — Keyword editor protection banner.
 *
 * When a cohort has an active Body Part QC run AND the user is editing
 * the body_part axis, the editor must show a banner explaining that
 * QC-decided stacks are protected from keyword overwrites during sort
 * step 3. The badge in the left rail is shown regardless of which axis
 * is currently active.
 */
import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { MantineProvider } from '@mantine/core';

import { KeywordSettingsTab } from '../KeywordSettingsTab';
import type { CohortKeywordConfig } from '../types';
import type { BodyPartState } from '../../body-part-qc/types';

const buildConfig = (): CohortKeywordConfig => ({
  cohort_id: 1,
  axes: [
    {
      axis: 'technique', label: 'Technique', description: null,
      buckets: [{
        axis: 'technique', bucket_path: 'a',
        display_name: 'A', group_label: null, description: null,
        defaults: ['mprage'], added: [], removed: [], effective: ['mprage'],
      }],
    },
    {
      axis: 'body_part', label: 'Body part', description: null,
      buckets: [{
        axis: 'body_part', bucket_path: 'brain',
        display_name: 'Brain', group_label: null, description: null,
        defaults: ['brain'], added: [], removed: [], effective: ['brain'],
      }],
    },
  ],
});

const buildBodyPartState = (
  overrides: Partial<BodyPartState> = {},
): BodyPartState => ({
  cohort_id: 1, has_current: true, has_previous: false,
  current_run_at: '2024-04-01', previous_run_at: null,
  categories: ['Brain', 'Spine'],
  training_summary: {},
  classifier_meta: { accuracy: 0.92 },
  summary: {
    total_sessions: 10, by_combo: {}, needs_check: 0,
    total_stacks: 42, stacks_changed: 5, sessions_changed: 3,
    change_matrix: {},
  },
  picks: [], profile: {}, available_id_types: ['code'],
  ...overrides,
});

vi.mock('../api', () => ({
  useCohortKeywordConfigQuery: () => ({
    data: buildConfig(), isLoading: false, isError: false, error: null,
    refetch: vi.fn(), isFetching: false,
  }),
  useUpdateKeywordBucketMutation: () => ({
    mutate: vi.fn(), isPending: false, variables: undefined,
  }),
  useResetKeywordBucketMutation: () => ({
    mutate: vi.fn(), isPending: false, variables: undefined,
  }),
  useResetAllKeywordsMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
}));

let mockedBodyPartState: BodyPartState | null = buildBodyPartState();
vi.mock('../../body-part-qc/api', () => ({
  useBodyPartQCStateQuery: () => ({
    data: mockedBodyPartState,
    isLoading: false,
  }),
}));

vi.mock('@mantine/notifications', () => ({
  notifications: { show: vi.fn() },
}));

const renderTab = () =>
  render(
    <MantineProvider>
      <KeywordSettingsTab cohortId={1} />
    </MantineProvider>,
  );

describe('KeywordSettingsTab — Body Part QC protection', () => {
  it('shows the QC badge in the left rail when QC is active', () => {
    mockedBodyPartState = buildBodyPartState();
    renderTab();
    expect(screen.getByTestId('body-part-axis-qc-badge'))
      .toBeInTheDocument();
  });

  it('hides the badge when no QC run has been applied', () => {
    mockedBodyPartState = buildBodyPartState({
      has_current: false,
      summary: {
        total_sessions: 0, by_combo: {}, needs_check: 0,
        total_stacks: 0, stacks_changed: 0, sessions_changed: 0,
        change_matrix: {},
      },
    });
    renderTab();
    expect(screen.queryByTestId('body-part-axis-qc-badge'))
      .not.toBeInTheDocument();
    expect(screen.queryByTestId('body-part-qc-protection-banner'))
      .not.toBeInTheDocument();
  });

  it('renders the protection banner only on the body_part axis', () => {
    mockedBodyPartState = buildBodyPartState();
    renderTab();
    // First-loaded axis is "Technique" — banner should be hidden.
    expect(screen.queryByTestId('body-part-qc-protection-banner'))
      .not.toBeInTheDocument();

    // Switch to body_part.
    fireEvent.click(screen.getByText('Body part'));
    expect(screen.getByTestId('body-part-qc-protection-banner'))
      .toBeInTheDocument();
    // Mentions the protected count.
    expect(screen.getByTestId('body-part-qc-protection-banner').textContent)
      .toMatch(/42 stacks/);
  });

  it('does not show the banner when QC has run but produced 0 stacks', () => {
    mockedBodyPartState = buildBodyPartState({
      summary: {
        total_sessions: 0, by_combo: {}, needs_check: 0,
        total_stacks: 0, stacks_changed: 0, sessions_changed: 0,
        change_matrix: {},
      },
    });
    renderTab();
    fireEvent.click(screen.getByText('Body part'));
    expect(screen.queryByTestId('body-part-qc-protection-banner'))
      .not.toBeInTheDocument();
  });
});
