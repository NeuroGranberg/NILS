/**
 * Component test: CategoriesEditor — local-buffered draft, dedupe,
 * remove confirmation when discarding samples.
 */
import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { MantineProvider } from '@mantine/core';

import { CategoriesEditor } from '../CategoriesEditor';

const mutate = vi.fn();

vi.mock('../api', () => ({
  useUpdateCategoriesMutation: () => ({
    mutate: (...args: unknown[]) => mutate(...args),
    isPending: false,
  }),
}));

vi.mock('@mantine/notifications', () => ({
  notifications: { show: vi.fn() },
}));

const renderEditor = (props: Parameters<typeof CategoriesEditor>[0]) =>
  render(
    <MantineProvider>
      <CategoriesEditor {...props} />
    </MantineProvider>,
  );

describe('CategoriesEditor', () => {
  it('renders existing categories as pills', () => {
    renderEditor({
      cohortId: 1,
      categories: ['Brain', 'Spine'],
      trainingSummary: {
        Brain: { axial: 5, sagittal: 0, coronal: 0, total: 5 },
      },
    });
    expect(screen.getByTestId('category-pill-Brain')).toBeInTheDocument();
    expect(screen.getByTestId('category-pill-Spine')).toBeInTheDocument();
  });

  it('disables Save when draft is identical to persisted', () => {
    renderEditor({
      cohortId: 1,
      categories: ['Brain'],
      trainingSummary: {},
    });
    const saveBtn = screen.getByRole('button', { name: /save categories/i });
    expect(saveBtn).toBeDisabled();
  });

  it('adds a new category via Enter and enables Save', () => {
    renderEditor({
      cohortId: 1,
      categories: ['Brain'],
      trainingSummary: {},
    });
    const input = screen.getByLabelText('New body-part category');
    fireEvent.change(input, { target: { value: 'Spine' } });
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(screen.getByTestId('category-pill-Spine')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /save categories/i }))
      .not.toBeDisabled();
  });

  it('dedupes case-insensitively', () => {
    renderEditor({
      cohortId: 1,
      categories: ['Brain'],
      trainingSummary: {},
    });
    const input = screen.getByLabelText('New body-part category');
    fireEvent.change(input, { target: { value: 'brain' } });
    fireEvent.keyDown(input, { key: 'Enter' });
    // Only one Brain-like pill exists.
    expect(screen.queryAllByTestId(/^category-pill-/)).toHaveLength(1);
  });

  it('saves immediately when no removed category had samples', () => {
    mutate.mockClear();
    renderEditor({
      cohortId: 1,
      categories: ['Brain', 'Spine'],
      trainingSummary: {},
    });
    // Remove "Spine" via its X.
    fireEvent.click(screen.getByLabelText('Remove Spine'));
    fireEvent.click(screen.getByRole('button', { name: /save categories/i }));
    expect(mutate).toHaveBeenCalledWith({ categories: ['Brain'] });
  });

  it('shows confirmation modal when removing a category with samples', async () => {
    mutate.mockClear();
    renderEditor({
      cohortId: 1,
      categories: ['Brain', 'Spine'],
      trainingSummary: {
        Spine: { axial: 3, sagittal: 0, coronal: 0, total: 3 },
      },
    });
    fireEvent.click(screen.getByLabelText('Remove Spine'));
    fireEvent.click(screen.getByRole('button', { name: /save categories/i }));
    expect(mutate).not.toHaveBeenCalled();
    // Modal renders into a portal; wait for the "Remove and save"
    // confirm button to appear.
    const confirm = await screen.findByRole(
      'button', { name: /remove and save/i },
    );
    fireEvent.click(confirm);
    expect(mutate).toHaveBeenCalledWith({ categories: ['Brain'] });
  });
});
