/**
 * SamplesGrid — view, relabel and remove approved training samples.
 *
 * Sticky filter row of category chips at the top filters the grid to
 * one category. Each card supports:
 *   - Move (relabel) via a category menu  → BodyPartSampleOp "move"
 *   - Remove                              → BodyPartSampleOp "remove"
 *
 * Counts come from ``state.training_summary``; the actual sample list
 * comes from a dedicated ``GET .../body-part-qc/samples`` endpoint that
 * hydrates each entry with a thumbnail URL.
 */
import {
  ActionIcon,
  Badge,
  Button,
  Card,
  Chip,
  Group,
  Loader,
  Menu,
  SimpleGrid,
  Stack,
  Text,
} from '@mantine/core';
import {
  IconChevronDown,
  IconTrash,
} from '@tabler/icons-react';
import { useState } from 'react';

import {
  useBodyPartSamplesQuery,
  useUpdateSamplesMutation,
} from './api';
import type { BodyPartTrainingSummaryEntry } from './types';

interface SamplesGridProps {
  cohortId: number;
  categories: string[];
  trainingSummary: Record<string, BodyPartTrainingSummaryEntry>;
}

export const SamplesGrid = ({
  cohortId,
  categories,
  trainingSummary,
}: SamplesGridProps) => {
  const [active, setActive] = useState<string | null>(
    categories.length > 0 ? categories[0] : null,
  );
  const samplesQuery = useBodyPartSamplesQuery(cohortId, active ?? undefined);
  const samplesMutation = useUpdateSamplesMutation(cohortId);

  const total = Object.values(trainingSummary).reduce(
    (s, e) => s + (e?.total ?? 0),
    0,
  );

  const remove = (stack_id: number, slice_index: number) =>
    samplesMutation.mutate({
      ops: [{ op: 'remove', stack_id, slice_index }],
    });

  const move = (stack_id: number, slice_index: number, new_label: string) =>
    samplesMutation.mutate({
      ops: [{ op: 'move', stack_id, slice_index, new_label }],
    });

  if (categories.length === 0) {
    return (
      <Text size="sm" c="dimmed">
        Add categories before reviewing approved samples.
      </Text>
    );
  }

  if (total === 0) {
    return (
      <Text size="sm" c="dimmed">
        No approved samples yet. Use the "Label queue" tab to seed and
        approve at least 5 per category before training.
      </Text>
    );
  }

  return (
    <Stack gap="md" data-testid="body-part-samples-grid">
      <Chip.Group
        multiple={false}
        value={active}
        onChange={(v) => setActive((v as string) || null)}
      >
        <Group gap="xs" wrap="wrap">
          {categories.map((c) => (
            <Chip
              key={c}
              value={c}
              variant="light"
              color="violet"
            >
              {c} · {trainingSummary[c]?.total ?? 0}
            </Chip>
          ))}
        </Group>
      </Chip.Group>

      {samplesQuery.isLoading && (
        <Group justify="center" py="md">
          <Loader />
        </Group>
      )}

      {samplesQuery.data && samplesQuery.data.length === 0 && (
        <Text size="sm" c="dimmed">
          No samples for "{active}". Re-seed the queue to add some.
        </Text>
      )}

      {samplesQuery.data && samplesQuery.data.length > 0 && (
        <SimpleGrid cols={{ base: 2, sm: 3, md: 4, lg: 6 }} spacing="sm">
          {samplesQuery.data.map((s) => (
            <Card
              key={`${s.stack_id}:${s.slice_index}`}
              withBorder
              padding="xs"
              radius="md"
              data-testid={`sample-card-${s.stack_id}-${s.slice_index}`}
            >
              <Card.Section>
                {s.thumbnail_url ? (
                  <img
                    src={s.thumbnail_url}
                    alt={`stack ${s.stack_id} slice ${s.slice_index}`}
                    style={{
                      width: '100%',
                      height: 110,
                      objectFit: 'contain',
                      background: '#000',
                    }}
                    loading="lazy"
                  />
                ) : (
                  <div
                    style={{
                      width: '100%',
                      height: 110,
                      background: '#000',
                    }}
                  />
                )}
              </Card.Section>
              <Stack gap={4} mt="xs">
                <Group gap={4} wrap="nowrap">
                  <Badge size="xs" color="violet" variant="light">
                    {s.label}
                  </Badge>
                  <Badge size="xs" color="gray" variant="light">
                    {s.orientation}
                  </Badge>
                </Group>
                <Group gap={4} wrap="nowrap">
                  <Menu position="bottom-start" withinPortal>
                    <Menu.Target>
                      <Button
                        variant="default"
                        size="compact-xs"
                        rightSection={<IconChevronDown size={12} />}
                        disabled={samplesMutation.isPending}
                      >
                        Move
                      </Button>
                    </Menu.Target>
                    <Menu.Dropdown>
                      <Menu.Label>Move to…</Menu.Label>
                      {categories
                        .filter((c) => c !== s.label)
                        .map((c) => (
                          <Menu.Item
                            key={c}
                            onClick={() => move(s.stack_id, s.slice_index, c)}
                          >
                            {c}
                          </Menu.Item>
                        ))}
                      {categories.length <= 1 && (
                        <Menu.Item disabled>(no other categories)</Menu.Item>
                      )}
                    </Menu.Dropdown>
                  </Menu>
                  <ActionIcon
                    color="red"
                    variant="subtle"
                    onClick={() => remove(s.stack_id, s.slice_index)}
                    disabled={samplesMutation.isPending}
                    aria-label="Remove sample"
                  >
                    <IconTrash size={14} />
                  </ActionIcon>
                </Group>
              </Stack>
            </Card>
          ))}
        </SimpleGrid>
      )}
    </Stack>
  );
};
