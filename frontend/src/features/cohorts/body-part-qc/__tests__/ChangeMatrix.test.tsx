/**
 * Component test: ChangeMatrix — renders the prior → new count grid
 * including the synthetic "(none)" row for stacks that previously had
 * no body_part value.
 */
import { describe, expect, it } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MantineProvider } from '@mantine/core';

import { ChangeMatrix } from '../ChangeMatrix';

const renderMatrix = (
  matrix: Record<string, Record<string, number>>,
  categories: string[],
) =>
  render(
    <MantineProvider>
      <ChangeMatrix matrix={matrix} categories={categories} />
    </MantineProvider>,
  );

describe('ChangeMatrix', () => {
  it('renders header columns and prior rows including (none)', () => {
    renderMatrix(
      {
        '(none)': { Brain: 3, Spine: 1 },
        brain: { Brain: 5, 'Brain-Neck': 2 },
      },
      ['Brain', 'Brain-Neck', 'Spine'],
    );
    expect(screen.getByTestId('body-part-change-matrix')).toBeInTheDocument();
    // (none) is pinned to the first row.
    expect(screen.getByText('(none)')).toBeInTheDocument();
    expect(screen.getByText('brain')).toBeInTheDocument();
    // Counts.
    expect(screen.getByText('3')).toBeInTheDocument();
    expect(screen.getByText('5')).toBeInTheDocument();
  });

  it('renders an empty-state when the matrix has no priors', () => {
    renderMatrix({}, ['Brain', 'Spine']);
    expect(screen.getByText(/no relabels/i)).toBeInTheDocument();
  });
});
