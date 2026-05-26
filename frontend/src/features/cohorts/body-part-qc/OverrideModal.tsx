/**
 * OverrideModal — relabel a single stack from the Changes view.
 *
 * Shows the prior label / new label / per-class probability bars and
 * lets the user pick a corrected category. Submits a single
 * ``BodyPartOverridePayload`` via :func:`useOverrideStackMutation`.
 */
import {
  Badge,
  Button,
  Group,
  Modal,
  Progress,
  Select,
  Stack,
  Text,
  Textarea,
} from '@mantine/core';
import { IconArrowRight } from '@tabler/icons-react';
import { useEffect, useState } from 'react';

import { useOverrideStackMutation } from './api';
import type { BodyPartChangeRow } from './types';

interface OverrideModalProps {
  cohortId: number;
  categories: string[];
  /** Probability map for the row; pulled from the cached state.picks if available. */
  probs?: Record<string, number>;
  row: BodyPartChangeRow | null;
  opened: boolean;
  onClose: () => void;
}

export const OverrideModal = ({
  cohortId,
  categories,
  probs,
  row,
  opened,
  onClose,
}: OverrideModalProps) => {
  const overrideMutation = useOverrideStackMutation(cohortId);
  const [label, setLabel] = useState<string | null>(null);
  const [note, setNote] = useState('');

  // Reset the form whenever a different row is opened.
  useEffect(() => {
    if (row) {
      setLabel(row.new_label);
      setNote('');
    }
  }, [row]);

  if (!row) return null;

  const submit = () => {
    if (!label) return;
    overrideMutation.mutate(
      {
        subject_id: row.subject_id,
        session_date: row.session_date ?? '',
        stack_id: row.stack_id,
        label,
        note: note.trim() || null,
      },
      { onSuccess: () => onClose() },
    );
  };

  const sortedProbs = Object.entries(probs ?? {}).sort(
    (a, b) => b[1] - a[1],
  );

  return (
    <Modal
      opened={opened}
      onClose={onClose}
      title={`Override stack ${row.stack_id}`}
      size="lg"
      data-testid="body-part-override-modal"
    >
      <Stack gap="md">
        <Group gap="xs">
          <Badge variant="light" color="gray">
            {row.subject_code}
          </Badge>
          {row.session_date && (
            <Badge variant="light" color="gray">
              {row.session_date}
            </Badge>
          )}
          {row.technique && (
            <Badge variant="light" color="gray">
              {row.technique}
            </Badge>
          )}
          {row.orientation && (
            <Badge variant="light" color="gray">
              {row.orientation}
            </Badge>
          )}
        </Group>

        <Group gap="xs" align="center">
          <Text size="sm" c="dimmed">Was:</Text>
          <Badge color="gray" variant="light">
            {row.previous_label ?? '(none)'}
          </Badge>
          <IconArrowRight size={14} />
          <Text size="sm" c="dimmed">New:</Text>
          <Badge color={row.needs_check ? 'yellow' : 'violet'} variant="filled">
            {row.new_label}
          </Badge>
          <Text size="sm" c="dimmed">
            confidence {(row.confidence * 100).toFixed(1)}%
          </Text>
        </Group>

        {row.thumbnail_url && (
          <img
            src={row.thumbnail_url}
            alt={`stack ${row.stack_id}`}
            style={{
              width: '100%',
              maxHeight: 220,
              objectFit: 'contain',
              background: '#000',
              borderRadius: 6,
            }}
            loading="lazy"
          />
        )}

        {sortedProbs.length > 0 && (
          <Stack gap={6}>
            <Text size="xs" fw={600} c="dimmed">
              Per-class probability
            </Text>
            {sortedProbs.map(([cls, p]) => (
              <Group key={cls} gap="xs" wrap="nowrap">
                <Text size="xs" w={120} truncate>
                  {cls}
                </Text>
                <Progress
                  value={p * 100}
                  size="sm"
                  color={cls === row.new_label ? 'violet' : 'gray'}
                  style={{ flex: 1 }}
                />
                <Text size="xs" c="dimmed" w={40} ta="right">
                  {(p * 100).toFixed(0)}%
                </Text>
              </Group>
            ))}
          </Stack>
        )}

        <Select
          label="Corrected label"
          data={categories}
          value={label}
          onChange={setLabel}
          allowDeselect={false}
          data-testid="override-label-select"
        />

        <Textarea
          label="Note (optional)"
          placeholder="e.g. brain-only — neck excluded by FOV"
          value={note}
          onChange={(e) => setNote(e.currentTarget.value)}
          autosize
          minRows={2}
          maxRows={4}
        />

        <Group justify="flex-end" gap="xs">
          <Button variant="default" onClick={onClose}>
            Cancel
          </Button>
          <Button
            color="violet"
            onClick={submit}
            disabled={!label || label === row.new_label && !note.trim()}
            loading={overrideMutation.isPending}
          >
            Save override
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
};
