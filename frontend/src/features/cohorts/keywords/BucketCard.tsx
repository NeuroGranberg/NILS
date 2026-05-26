/**
 * One collapsible card per editable keyword bucket.
 *
 * Users edit locally (state = ordered chip list). "Save" commits the
 * computed delta to the backend; "Reset" wipes the cohort override and
 * reverts to global defaults.
 *
 * Saving does NOT trigger a sort run (spec section 2, principle 5).
 */
import {
  Badge,
  Button,
  Card,
  Collapse,
  Group,
  Stack,
  Text,
  Tooltip,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { IconChevronDown, IconChevronRight, IconRefresh } from '@tabler/icons-react';
import { memo, useEffect, useMemo, useState } from 'react';

import { AddKeywordInput } from './AddKeywordInput';
import { KeywordChip } from './KeywordChip';
import type { KeywordBucketView } from './types';
import { buildChips, chipsContain, chipsToDelta, deltasEqual, type Chip } from './utils';

interface BucketCardProps {
  bucket: KeywordBucketView;
  isSaving: boolean;
  isResetting: boolean;
  onSave: (payload: {
    axis: string;
    bucket_path: string;
    added: string[];
    removed: string[];
  }) => void;
  onReset: (payload: { axis: string; bucket_path: string }) => void;
  defaultOpen?: boolean;
}

const BucketCardInner = ({
  bucket,
  isSaving,
  isResetting,
  onSave,
  onReset,
  defaultOpen,
}: BucketCardProps) => {
  const [open, setOpen] = useState(Boolean(defaultOpen));
  const [chips, setChips] = useState<Chip[]>(() => buildChips(bucket));

  // When the server returns a fresh bucket snapshot (after save/reset or a
  // global YAML change flowed in), resync local state.
  useEffect(() => {
    setChips(buildChips(bucket));
  }, [bucket]);

  const serverDelta = useMemo(
    () => ({ added: bucket.added, removed: bucket.removed }),
    [bucket.added, bucket.removed],
  );
  const localDelta = useMemo(
    () => chipsToDelta(chips, bucket.defaults),
    [chips, bucket.defaults],
  );

  const isDirty = !deltasEqual(serverDelta, localDelta);
  const editedCount = bucket.added.length + bucket.removed.length;

  const removeChip = (keyword: string) => {
    setChips((prev) =>
      prev.flatMap((c) => {
        if (c.keyword !== keyword) return [c];
        if (c.state === 'added') return []; // remove entirely
        return [{ ...c, state: 'removed' }]; // mark for removal
      }),
    );
  };

  const restoreChip = (keyword: string) => {
    setChips((prev) =>
      prev.flatMap((c) => {
        if (c.keyword !== keyword) return [c];
        if (c.state !== 'removed') return [c];
        // If the chip came from defaults, restore to 'default'. Otherwise
        // (pure orphan removed), drop it.
        const inDefaults = bucket.defaults.some(
          (d) => d.trim().toLowerCase() === keyword.trim().toLowerCase(),
        );
        return inDefaults ? [{ ...c, state: 'default' as const }] : [];
      }),
    );
  };

  const addKeyword = (keyword: string) => {
    if (chipsContain(chips, keyword)) {
      notifications.show({
        color: 'yellow',
        title: 'Already present',
        message: `"${keyword.trim()}" is already in this bucket.`,
      });
      return;
    }
    setChips((prev) => [...prev, { keyword, state: 'added' }]);
  };

  const handleSave = () => {
    onSave({
      axis: bucket.axis,
      bucket_path: bucket.bucket_path,
      added: localDelta.added,
      removed: localDelta.removed,
    });
  };

  const handleReset = () => {
    onReset({ axis: bucket.axis, bucket_path: bucket.bucket_path });
  };

  const handleRevertLocal = () => {
    setChips(buildChips(bucket));
  };

  return (
    <Card
      withBorder
      radius="md"
      padding="sm"
      style={{
        backgroundColor: 'var(--nils-bg-secondary)',
        borderColor: 'var(--nils-border-subtle)',
      }}
    >
      <Stack gap="xs">
        <Group
          justify="space-between"
          wrap="nowrap"
          onClick={() => setOpen((v) => !v)}
          style={{ cursor: 'pointer' }}
        >
          <Group gap="sm" wrap="nowrap" style={{ minWidth: 0 }}>
            {open ? <IconChevronDown size={16} /> : <IconChevronRight size={16} />}
            <Stack gap={0} style={{ minWidth: 0 }}>
              <Text fw={600} size="sm" truncate>
                {bucket.display_name}
              </Text>
              <Text size="xs" c="dimmed" truncate>
                {bucket.bucket_path}
              </Text>
            </Stack>
          </Group>
          <Group gap="xs" wrap="nowrap">
            {editedCount > 0 && (
              <Tooltip
                label={`${bucket.added.length} added · ${bucket.removed.length} removed`}
              >
                <Badge size="sm" color="green" variant="light">
                  {editedCount} edited
                </Badge>
              </Tooltip>
            )}
            <Badge size="sm" variant="default">
              {bucket.effective.length} keywords
            </Badge>
          </Group>
        </Group>

        <Collapse in={open}>
          <Stack gap="sm" pt="xs">
            {bucket.description && (
              <Text size="xs" c="dimmed">
                {bucket.description}
              </Text>
            )}

            {chips.length === 0 ? (
              <Text size="sm" c="dimmed" fs="italic">
                No keywords yet — add one below.
              </Text>
            ) : (
              <Group gap={6}>
                {chips.map((chip) => (
                  <KeywordChip
                    key={`${chip.keyword}-${chip.state}`}
                    keyword={chip.keyword}
                    state={chip.state}
                    onRemove={() => removeChip(chip.keyword)}
                    onRestore={() => restoreChip(chip.keyword)}
                  />
                ))}
              </Group>
            )}

            <AddKeywordInput onAdd={addKeyword} disabled={isSaving} />

            <Group justify="space-between" pt={4}>
              <Tooltip label="Remove cohort overrides; revert to global defaults">
                <Button
                  variant="subtle"
                  color="gray"
                  size="xs"
                  leftSection={<IconRefresh size={14} />}
                  onClick={handleReset}
                  loading={isResetting}
                  disabled={editedCount === 0 || isSaving}
                >
                  Reset to defaults
                </Button>
              </Tooltip>

              <Group gap="xs">
                {isDirty && (
                  <Button
                    variant="subtle"
                    color="gray"
                    size="xs"
                    onClick={handleRevertLocal}
                    disabled={isSaving}
                  >
                    Discard changes
                  </Button>
                )}
                <Tooltip
                  label="Saved changes apply on the next sort run. Save does not start a sort."
                  withArrow
                  position="top"
                >
                  <Button
                    size="xs"
                    onClick={handleSave}
                    loading={isSaving}
                    disabled={!isDirty}
                  >
                    Save
                  </Button>
                </Tooltip>
              </Group>
            </Group>
          </Stack>
        </Collapse>
      </Stack>
    </Card>
  );
};

export const BucketCard = memo(BucketCardInner);
BucketCard.displayName = 'BucketCard';
