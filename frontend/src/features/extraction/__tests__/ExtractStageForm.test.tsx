import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import type { ReactNode } from 'react';
import { MantineProvider } from '@mantine/core';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { ExtractStageForm } from '../ExtractStageForm';
import { buildDefaultExtractConfig } from '../defaults';
import type { JobSummary } from '../../../types';

vi.mock('../queries', () => ({
  useMetadataIdTypes: () => ({ data: [], isLoading: false }),
}));

vi.mock('../../anonymization/queries', () => ({
  useCsvColumns: () => ({ data: { columns: [] }, isLoading: false }),
}));

const renderForm = (ui: ReactNode) => {
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });

  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <MantineProvider>{ui}</MantineProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  );
};

describe('ExtractStageForm', () => {
  it('renders the extraction progress card when a job is active', () => {
    const config = buildDefaultExtractConfig();
    const job: JobSummary = {
      id: 1,
      stageId: 'extract',
      status: 'running',
      progress: 42,
      submittedAt: new Date().toISOString(),
      startedAt: new Date().toISOString(),
      finishedAt: null,
      errorMessage: null,
      cohortId: 7,
      cohortName: 'ALS',
      config: { raw_root: '/data/raw' },
      metrics: { subjects: 1, studies: 2, series: 3, instances: 4 },
    };

    renderForm(
      <ExtractStageForm
        sourcePath="/data/raw"
        config={config}
        job={job}
        onChange={vi.fn()}
      />,
    );

    expect(screen.getByText(/Extraction job/i)).toBeInTheDocument();
    expect(screen.getByText(/Subjects/i)).toBeInTheDocument();
    expect(screen.getByText(/42%/)).toBeInTheDocument();
  });
});
