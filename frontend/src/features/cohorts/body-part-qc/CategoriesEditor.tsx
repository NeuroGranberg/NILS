/**
 * CategoriesEditor — list / add / remove the body-part categories
 * the cohort will train and predict over.
 *
 * Edits are buffered locally until the user clicks "Save"; the diff
 * vs. the persisted list is what gets sent to the PUT endpoint, so
 * removing a category that already has training samples will trigger
 * a confirmation dialog (training samples for dropped categories are
 * cleared on the server — the dialog warns the user).
 */
import {
  ActionIcon,
  Badge,
  Button,
  Group,
  Modal,
  Stack,
  Text,
  TextInput,
  Tooltip,
} from '@mantine/core';
import { IconPlus, IconTrash } from '@tabler/icons-react';
import { useEffect, useState } from 'react';

import { useUpdateCategoriesMutation } from './api';
import type { BodyPartTrainingSummaryEntry } from './types';

interface CategoriesEditorProps {
  cohortId: number;
  categories: string[];
  trainingSummary: Record<string, BodyPartTrainingSummaryEntry>;
}

export const CategoriesEditor = ({
  cohortId,
  categories,
  trainingSummary,
}: CategoriesEditorProps) => {
  const [draft, setDraft] = useState<string[]>(categories);
  const [newCat, setNewCat] = useState('');
  const [confirmRemoved, setConfirmRemoved] = useState<string[] | null>(null);
  const mutation = useUpdateCategoriesMutation(cohortId);

  // Re-sync if the server-side list changes (e.g. another tab edited).
  useEffect(() => setDraft(categories), [categories]);

  const dirty =
    draft.length !== categories.length ||
    draft.some((c, i) => c !== categories[i]);

  const handleAdd = () => {
    const v = newCat.trim();
    if (!v) return;
    if (
      draft.some((c) => c.toLowerCase() === v.toLowerCase())
    ) {
      return;
    }
    setDraft([...draft, v]);
    setNewCat('');
  };

  const handleRemove = (cat: string) => {
    setDraft(draft.filter((c) => c !== cat));
  };

  const handleSave = () => {
    const removed = categories.filter((c) => !draft.includes(c));
    const removedWithSamples = removed.filter(
      (c) => (trainingSummary[c]?.total ?? 0) > 0,
    );
    if (removedWithSamples.length > 0) {
      setConfirmRemoved(removedWithSamples);
      return;
    }
    mutation.mutate({ categories: draft });
  };

  const confirmAndSave = () => {
    setConfirmRemoved(null);
    mutation.mutate({ categories: draft });
  };

  return (
    <Stack gap="sm">
      <Group gap="xs" wrap="wrap">
        {draft.map((cat) => (
          <Badge
            key={cat}
            variant="light"
            color="violet"
            size="lg"
            data-testid={`category-pill-${cat}`}
            rightSection={
              <Tooltip label={`Remove ${cat}`}>
                <ActionIcon
                  size="xs"
                  variant="transparent"
                  color="violet"
                  onClick={() => handleRemove(cat)}
                  aria-label={`Remove ${cat}`}
                >
                  <IconTrash size={12} />
                </ActionIcon>
              </Tooltip>
            }
          >
            {cat}
            {trainingSummary[cat]?.total
              ? ` · ${trainingSummary[cat].total}`
              : ''}
          </Badge>
        ))}
        {draft.length === 0 && (
          <Text size="sm" c="dimmed">
            No categories yet — add at least two before training.
          </Text>
        )}
      </Group>

      <Group gap="xs">
        <TextInput
          placeholder="New category…"
          value={newCat}
          onChange={(e) => setNewCat(e.currentTarget.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') {
              e.preventDefault();
              handleAdd();
            }
          }}
          style={{ flex: 1, maxWidth: 320 }}
          aria-label="New body-part category"
        />
        <Button
          leftSection={<IconPlus size={14} />}
          variant="default"
          onClick={handleAdd}
          disabled={!newCat.trim()}
        >
          Add
        </Button>
        <Button
          variant="filled"
          color="violet"
          onClick={handleSave}
          disabled={!dirty || draft.length === 0 || mutation.isPending}
          loading={mutation.isPending}
        >
          Save categories
        </Button>
      </Group>

      <Modal
        opened={confirmRemoved !== null}
        onClose={() => setConfirmRemoved(null)}
        title="Remove categories with training samples?"
        size="md"
      >
        <Stack gap="md">
          <Text size="sm">
            The following categories have approved training samples that will
            be discarded:
          </Text>
          <Stack gap="xs">
            {(confirmRemoved ?? []).map((c) => (
              <Text key={c} size="sm" fw={600}>
                {c}{' '}
                <Text span c="dimmed">
                  ({trainingSummary[c]?.total ?? 0} samples)
                </Text>
              </Text>
            ))}
          </Stack>
          <Group justify="flex-end" gap="xs">
            <Button variant="default" onClick={() => setConfirmRemoved(null)}>
              Cancel
            </Button>
            <Button color="red" onClick={confirmAndSave}>
              Remove and save
            </Button>
          </Group>
        </Stack>
      </Modal>
    </Stack>
  );
};
