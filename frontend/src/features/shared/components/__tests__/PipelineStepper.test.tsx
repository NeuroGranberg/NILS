import { MantineProvider } from '@mantine/core';
import { renderToString } from 'react-dom/server';
import { PipelineStepper } from '../PipelineStepper';
import type { StageSummary } from '../../../../types';

const mockStages: StageSummary[] = [
  {
    id: 'anonymize',
    title: 'Anonymization',
    description: 'Mock stage',
    status: 'completed',
    progress: 100,
    lastRunAt: new Date().toISOString(),
    runs: [],
  },
  {
    id: 'extract',
    title: 'Metadata Extraction',
    description: 'Mock stage',
    status: 'running',
    progress: 40,
    runs: [],
  },
];

describe('PipelineStepper', () => {
  it('renders stage titles', () => {
    const markup = renderToString(
      <MantineProvider>
        <PipelineStepper stages={mockStages} />
      </MantineProvider>,
    );

    expect(markup).toContain('Anonymization');
    expect(markup).toContain('Metadata Extraction');
  });
});
