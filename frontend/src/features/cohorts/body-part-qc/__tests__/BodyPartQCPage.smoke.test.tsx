/**
 * M11 — frontend smoke test for the cohort Body Part QC page.
 *
 * Mounts ``BodyPartQCPage`` with a mocked state covering every UI
 * branch (categories, training summary, classifier metadata, current
 * run summary) and walks each tab to make sure no panel crashes when
 * fed realistic data. This is a render-time smoke test — heavier
 * interaction logic lives in per-component tests.
 */
import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { MantineProvider } from '@mantine/core';

import { BodyPartQCPage } from '../BodyPartQCPage';
import type { BodyPartState, BodyPartTrainingSample } from '../types';
import type { Cohort } from '../../../../types/cohort';

const cohort: Cohort = {
  id: 1,
  name: 'alpha-cohort',
  source_path: '/tmp/alpha',
  created_at: '2024-04-01T00:00:00Z',
  updated_at: '2024-04-26T00:00:00Z',
  anonymization_enabled: false,
  tags: [],
  status: 'idle' as Cohort['status'],
  total_subjects: 10,
  total_sessions: 30,
  total_series: 200,
  completion_percentage: 0,
  stages: [],
};

const state: BodyPartState = {
  cohort_id: 1,
  has_current: true,
  has_previous: false,
  current_run_at: '2024-04-26T10:00:00Z',
  previous_run_at: null,
  categories: ['Brain', 'Spine'],
  training_summary: {
    Brain: { axial: 5, sagittal: 0, coronal: 0, total: 5 },
    Spine: { axial: 5, sagittal: 0, coronal: 0, total: 5 },
  },
  classifier_meta: {
    accuracy: 0.94,
    n_samples: 10,
    classes: ['Brain', 'Spine'],
    encoder_name: 'biomedclip+siglip2',
    n_features: 4,
    trained_at: '2024-04-26T09:30:00Z',
  },
  summary: {
    total_sessions: 3, by_combo: { Brain: 2, Spine: 1 },
    needs_check: 1, total_stacks: 4,
    stacks_changed: 2, sessions_changed: 1,
    change_matrix: { brain: { Brain: 2 }, spine: { Brain: 1 } },
    override_conflicts_count: 0,
  },
  picks: [
    {
      subject_id: 1, session_date: '2024-01-01',
      subject_code: 'sub-001',
      study_ids: [1], primary_study_id: 1,
      stacks: [{
        stack_id: 101, label: 'Brain', confidence: 0.92,
        probs: { Brain: 0.92, Spine: 0.08 },
        is_override: false, needs_check: false,
        series_instance_uid: '1.1.1', technique: 'MPRAGE',
        orientation: 'axial', previous_label: 'brain',
        prior_source: 'text_keyword', changed: true,
      }],
      session_combo: ['Brain'], session_combo_key: 'Brain',
      session_prev_combo_key: 'brain', session_changed: true,
      stacks_changed: 1, low_conf_count: 0, needs_check: false,
      subject_other_ids: {},
    },
  ],
  profile: {}, available_id_types: ['code'],
  stage_status: 'staged',
  last_committed_at: null,
  pending_changes_count: 2,
  selected_model_id: null,
  selected_model_name: null,
};

const samples: BodyPartTrainingSample[] = [
  {
    stack_id: 1, slice_index: 10, label: 'Brain',
    orientation: 'axial', approved_at: '2024-04-25T10:00:00Z',
    thumbnail_url: '/thumb/1', subject_code: 'sub-001',
  },
];

vi.mock('../api', () => ({
  useBodyPartQCStateQuery: () => ({
    data: state, isLoading: false, isError: false, error: null,
  }),
  useBodyPartSamplesQuery: () => ({
    data: samples, isLoading: false,
  }),
  useBodyPartChangesQuery: () => ({
    data: {
      total: 1, offset: 0, limit: 50,
      rows: [{
        study_id: 1, subject_id: 1, stack_id: 101, subject_code: 'sub-001',
        session_date: '2024-01-01', series_description: null,
        technique: 'MPRAGE', orientation: 'axial',
        previous_label: 'brain', new_label: 'Brain',
        prior_source: 'text_keyword', confidence: 0.92,
        needs_check: false, is_override: false,
        thumbnail_url: '/thumb/101',
      }],
    },
    isLoading: false,
  }),
  useUpdateCategoriesMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
  useSeedCandidatesMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
  useUpdateSamplesMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
  useApplyBodyPartMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
  useCommitBodyPartMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
  useRestorePreviousBodyPartMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
  useOverrideStackMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
  useResetSessionMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
  useDestageBodyPartMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
  useResetBodyPartMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
  useSelectModelMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
}));

vi.mock('../globalApi', () => ({
  usePoolSummaryQuery: () => ({
    data: { by_label: { Brain: 183, 'Brain-Neck': 231, Spine: 563 }, total: 977 },
    isLoading: false,
  }),
  useModelsQuery: () => ({
    data: [],
    isLoading: false,
  }),
  usePushToPoolMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
  useTrainModelMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
  useSetDefaultModelMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
  useDeleteModelMutation: () => ({
    mutate: vi.fn(), isPending: false,
  }),
}));

vi.mock('@mantine/notifications', () => ({
  notifications: { show: vi.fn() },
}));

const renderPage = () =>
  render(
    <MantineProvider>
      <BodyPartQCPage cohort={cohort} onBack={() => {}} />
    </MantineProvider>,
  );

describe('BodyPartQCPage — smoke', () => {
  it('renders the header, categories, and all four tabs', () => {
    renderPage();
    expect(screen.getByTestId('cohort-body-part-qc-page'))
      .toBeInTheDocument();
    expect(screen.getByText(/Body Part QC — alpha-cohort/))
      .toBeInTheDocument();
    expect(screen.getByText('Label queue')).toBeInTheDocument();
    expect(screen.getByText('Approved samples')).toBeInTheDocument();
    expect(screen.getByText('Models')).toBeInTheDocument();
    expect(screen.getByText('Apply')).toBeInTheDocument();
    expect(screen.getByText(/Changes \(2\)/)).toBeInTheDocument();
  });

  it('shows the apply summary numbers in the header', () => {
    renderPage();
    expect(screen.getByText(/Last apply:/)).toBeInTheDocument();
  });

  it('switches to the Approved samples tab and renders the sample card', () => {
    renderPage();
    fireEvent.click(screen.getByText('Approved samples'));
    expect(screen.getByTestId('sample-card-1-10')).toBeInTheDocument();
  });

  it('switches to the Apply tab and shows the change matrix', () => {
    renderPage();
    fireEvent.click(screen.getByText('Apply'));
    expect(screen.getByTestId('body-part-change-matrix'))
      .toBeInTheDocument();
  });

  it('switches to the Changes tab and renders the diff row', () => {
    renderPage();
    fireEvent.click(screen.getByText(/Changes/));
    expect(screen.getByTestId('body-part-change-row-101'))
      .toBeInTheDocument();
  });
});
