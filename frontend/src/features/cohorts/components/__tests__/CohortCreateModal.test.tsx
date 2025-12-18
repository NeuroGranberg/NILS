import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { MantineProvider } from '@mantine/core';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';
import userEvent from '@testing-library/user-event';

import type { Cohort } from '../../../../types/cohort';
import type { SubjectCohortMetadataCohort } from '../../../database/api';

let mutateAsyncMock: ReturnType<typeof vi.fn>;
let metadataMutateAsyncMock: ReturnType<typeof vi.fn>;
let cohortsMock: Cohort[] | undefined;
let metadataCohortsMock: SubjectCohortMetadataCohort[] | undefined;
const dataRootsMock = ['/data'];
const apiClientMock = {
  get: vi.fn(),
  post: vi.fn(),
  put: vi.fn(),
  delete: vi.fn(),
};

vi.mock('@mantine/notifications', () => ({
  notifications: {
    show: vi.fn(),
  },
}));

vi.mock('../../api', () => ({
  __esModule: true,
  useCreateCohortMutation: () => ({
    mutateAsync: mutateAsyncMock,
    mutate: mutateAsyncMock,
    isPending: false,
  }),
  useCohortsQuery: () => ({
    data: cohortsMock,
  }),
}));

vi.mock('../../../database/api', () => ({
  __esModule: true,
  useMetadataCohorts: () => ({
    data: metadataCohortsMock,
  }),
  useUpsertMetadataCohort: () => ({
    mutateAsync: metadataMutateAsyncMock,
    isPending: false,
  }),
}));

vi.mock('../../../files/api', () => ({
  useDirectoryQuery: () => ({ data: [], isFetching: false }),
  useDataRootsQuery: () => ({ data: dataRootsMock }),
}));

vi.mock('../../../anonymization/defaults', () => ({
  buildDefaultAnonymizeConfig: vi.fn(() => ({ anonymize: true })),
}));

vi.mock('../../../../utils/api-client', () => ({
  __esModule: true,
  apiClient: apiClientMock,
}));

import { notifications } from '@mantine/notifications';
import { CohortCreateModal } from '../CohortCreateModal';

const renderModal = (props: Partial<{ onClose: () => void; opened: boolean }> = {}) => {
  const combinedProps = { opened: true, onClose: vi.fn(), ...props };
  const queryClient = new QueryClient();
  render(
    <MemoryRouter>
      <QueryClientProvider client={queryClient}>
        <MantineProvider>
          <CohortCreateModal {...combinedProps} />
        </MantineProvider>
      </QueryClientProvider>
    </MemoryRouter>,
  );
  return combinedProps;
};

beforeEach(() => {
  vi.clearAllMocks();
  mutateAsyncMock = vi.fn().mockResolvedValue({});
  metadataMutateAsyncMock = vi.fn().mockResolvedValue({});
  cohortsMock = [];
  metadataCohortsMock = [];
  apiClientMock.post.mockResolvedValue({});
  apiClientMock.get.mockResolvedValue([]);
});

beforeAll(() => {
  class ResizeObserverMock {
    observe() {}
    unobserve() {}
    disconnect() {}
  }

  (globalThis as unknown as { ResizeObserver: typeof ResizeObserver }).ResizeObserver = ResizeObserverMock as unknown as typeof ResizeObserver;
});

describe('CohortCreateModal', () => {
  it('submits a normalized cohort name for new cohorts', async () => {
    const user = userEvent.setup();
    const onCloseMock = vi.fn();
    renderModal({ onClose: onCloseMock });

    const nameInput = screen.getByLabelText(/Cohort name/i);
    await user.click(nameInput);
    await user.type(nameInput, 'MyNewCohort');
    expect(nameInput).toHaveValue('MyNewCohort');

    const ownerInput = screen.getByLabelText(/Owner/i);
    await user.type(ownerInput, 'Clinical Ops');

    const submitButton = screen.getByRole('button', { name: /Create draft cohort/i });
    await user.click(submitButton);

    await waitFor(() => expect(metadataMutateAsyncMock).toHaveBeenCalledTimes(1));
    expect(metadataMutateAsyncMock).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'mynewcohort',
        owner: 'Clinical Ops',
        path: '/data',
        description: null,
        isActive: true,
      }),
    );

    await waitFor(() => expect(notifications.show).toHaveBeenCalled());
    expect(notifications.show).toHaveBeenLastCalledWith(
      expect.objectContaining({ message: 'Cohort metadata and draft created successfully.' }),
    );
    await waitFor(() => expect(mutateAsyncMock).toHaveBeenCalledTimes(1));
    expect(mutateAsyncMock).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'mynewcohort', tags: ['demo'] }),
    );

    await waitFor(() => expect(onCloseMock).toHaveBeenCalled());
  });

  it('prefills existing cohort details and updates them on submit', async () => {
    const existingCohort: Cohort = {
      id: 1,
      name: 'existing',
      description: 'Existing description',
      source_path: '/data/existing',
      anonymization_enabled: true,
      tags: ['tag1'],
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      status: 'idle',
      total_subjects: 0,
      total_sessions: 0,
      total_series: 0,
      completion_percentage: 0,
      stages: [],
      anonymize_job: null,
      anonymize_history: [],
      extract_job: null,
      extract_history: [],
    };
    cohortsMock = [existingCohort];
    metadataCohortsMock = [
      {
        cohortId: 101,
        name: 'existing',
        owner: 'Existing Owner',
        path: '/data/existing',
        description: 'Existing metadata description',
        isActive: true,
      },
    ];

    const onCloseMock = vi.fn();
    renderModal({ onClose: onCloseMock });

    const user = userEvent.setup();
    const nameInput = screen.getByLabelText(/Cohort name/i);
    await user.click(nameInput);
    await user.clear(nameInput);
    await user.type(nameInput, 'Existing');

    await waitFor(() =>
      expect(screen.getByText(/Metadata cohort/i, { selector: 'span' })).toBeInTheDocument(),
    );
    await waitFor(() =>
      expect(screen.getByText(/Pipeline draft/i, { selector: 'span' })).toBeInTheDocument(),
    );

    const ownerField = screen.getByLabelText(/Owner/i) as HTMLInputElement;
    await waitFor(() => expect(ownerField.value).toBe('Existing Owner'));
    await user.clear(ownerField);
    await user.type(ownerField, 'Updated Owner');

    const descriptionField = screen.getByLabelText(/Description/i) as HTMLTextAreaElement;
    await waitFor(() => expect(descriptionField.value).toBe('Existing metadata description'));
    await user.clear(descriptionField);
    await user.type(descriptionField, 'Updated description');

    const anonymizeSwitch = screen.getByLabelText(/Add pseudo-anonymization stage/i) as HTMLInputElement;
    await waitFor(() => expect(anonymizeSwitch).toBeChecked());
    await user.click(anonymizeSwitch);

    const submitButton = screen.getByRole('button', { name: /Create draft cohort/i });
    await user.click(submitButton);

    await waitFor(() => expect(metadataMutateAsyncMock).toHaveBeenCalledTimes(1));
    expect(metadataMutateAsyncMock).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'existing',
        owner: 'Updated Owner',
        path: '/data/existing',
        description: 'Updated description',
        isActive: true,
      }),
    );

    await waitFor(() => expect(notifications.show).toHaveBeenCalled());
    expect(notifications.show).toHaveBeenLastCalledWith(
      expect.objectContaining({ message: 'Cohort metadata and draft updated successfully.' }),
    );
    await waitFor(() => expect(mutateAsyncMock).toHaveBeenCalledTimes(1));
    expect(mutateAsyncMock).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'existing',
        description: 'Updated description',
        anonymization_enabled: false,
        tags: ['tag1'],
      }),
    );

    expect(notifications.show).toHaveBeenCalledWith(
      expect.objectContaining({ message: 'Cohort metadata and draft updated successfully.' }),
    );

    await waitFor(() => expect(onCloseMock).toHaveBeenCalled());
  });
});
