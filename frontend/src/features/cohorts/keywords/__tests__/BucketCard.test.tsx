/**
 * Integration-level tests for BucketCard interactions.
 */
import { describe, expect, it, vi } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MantineProvider } from '@mantine/core';

import { BucketCard } from '../BucketCard';
import type { KeywordBucketView } from '../types';

vi.mock('@mantine/notifications', () => ({
  notifications: { show: vi.fn() },
}));

const BUCKET: KeywordBucketView = {
  axis: 'contrast',
  bucket_path: 'negative_keywords',
  display_name: 'Negative (no contrast)',
  group_label: null,
  description: 'Keywords that indicate no contrast agent',
  defaults: ['utan gd', 'ohne km'],
  added: [],
  removed: [],
  effective: ['utan gd', 'ohne km'],
};

const renderCard = (
  overrides: Partial<KeywordBucketView> = {},
  onSave = vi.fn(),
  onReset = vi.fn(),
) => {
  const bucket = { ...BUCKET, ...overrides };
  render(
    <MantineProvider>
      <BucketCard
        bucket={bucket}
        isSaving={false}
        isResetting={false}
        onSave={onSave}
        onReset={onReset}
        defaultOpen
      />
    </MantineProvider>,
  );
  return { bucket, onSave, onReset };
};

describe('BucketCard', () => {
  it('renders defaults as chips', async () => {
    renderCard();
    expect(screen.getByText('utan gd')).toBeInTheDocument();
    expect(screen.getByText('ohne km')).toBeInTheDocument();
  });

  it('Save is disabled when no local changes', () => {
    renderCard();
    const save = screen.getByRole('button', { name: 'Save' });
    expect(save).toBeDisabled();
  });

  it('adding a new keyword enables Save and sends correct delta', async () => {
    const user = userEvent.setup();
    const { onSave } = renderCard();

    await user.type(
      screen.getByPlaceholderText('Add keyword…'),
      'sem contraste',
    );
    await user.click(screen.getByLabelText('Add keyword'));

    const save = screen.getByRole('button', { name: 'Save' });
    expect(save).toBeEnabled();
    await user.click(save);

    expect(onSave).toHaveBeenCalledWith({
      axis: 'contrast',
      bucket_path: 'negative_keywords',
      added: ['sem contraste'],
      removed: [],
    });
  });

  it('removing a default marks it as removed', async () => {
    const user = userEvent.setup();
    const { onSave } = renderCard();

    await user.click(screen.getByLabelText('Remove utan gd'));
    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(onSave).toHaveBeenCalledWith({
      axis: 'contrast',
      bucket_path: 'negative_keywords',
      added: [],
      removed: ['utan gd'],
    });
  });

  it('restoring a removed default clears the local change', async () => {
    const user = userEvent.setup();
    const { onSave } = renderCard();

    // Remove then restore the same keyword.
    await user.click(screen.getByLabelText('Remove utan gd'));
    await user.click(await screen.findByLabelText('Restore utan gd'));

    // Save button should now be disabled (back to clean state).
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled();
    expect(onSave).not.toHaveBeenCalled();
  });

  it('Reset button is disabled when bucket has no overrides', () => {
    renderCard();
    expect(screen.getByRole('button', { name: /reset to defaults/i })).toBeDisabled();
  });

  it('Reset fires onReset when overrides exist', async () => {
    const user = userEvent.setup();
    const { onReset } = renderCard({ added: ['custom'], removed: [] });
    await user.click(screen.getByRole('button', { name: /reset to defaults/i }));
    expect(onReset).toHaveBeenCalledWith({
      axis: 'contrast',
      bucket_path: 'negative_keywords',
    });
  });
});
