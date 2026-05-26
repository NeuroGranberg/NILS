/**
 * SeedQueue — labeling queue powered by zero-shot BiomedCLIP scoring.
 *
 * Flow:
 *   1. User picks a target category and clicks "Generate queue".
 *   2. Backend returns up to N (default 100) ephemeral candidates ranked
 *      by margin × farthest-point spread.
 *   3. For each card the user can:
 *        ✓ Approve (label = target category)
 *        ↪ Reassign (label = some other category)
 *        ⤫ Skip (drop from local queue, no server call)
 *
 *   Approve / reassign send a single ``BodyPartSampleOp`` of kind
 *   "approve". The server upserts the sample into the cohort's training
 *   set; the cached state is replaced with the response so the
 *   `training_summary` counts in the page header update live.
 */
import {
  ActionIcon,
  Badge,
  Button,
  Card,
  Group,
  Loader,
  Menu,
  Select,
  SimpleGrid,
  Stack,
  Text,
  TextInput,
} from '@mantine/core';
import {
  IconCheck,
  IconChevronDown,
  IconRefresh,
  IconX,
} from '@tabler/icons-react';
import { useState } from 'react';

import { useSeedCandidatesMutation, useUpdateSamplesMutation } from './api';
import type { BodyPartCandidate } from './types';

interface SeedQueueProps {
  cohortId: number;
  categories: string[];
}

export const SeedQueue = ({ cohortId, categories }: SeedQueueProps) => {
  const [target, setTarget] = useState<string | null>(
    categories.length > 0 ? categories[0] : null,
  );
  const [nTarget, setNTarget] = useState<string>('100');
  const [queue, setQueue] = useState<BodyPartCandidate[]>([]);

  const seedMutation = useSeedCandidatesMutation(cohortId);
  const samplesMutation = useUpdateSamplesMutation(cohortId);

  const generate = () => {
    if (!target) return;
    const n = Math.max(1, Math.min(500, parseInt(nTarget, 10) || 100));
    seedMutation.mutate(
      { category: target, n_target: n },
      { onSuccess: (cands) => setQueue(cands) },
    );
  };

  const removeFromQueue = (stack_id: number, slice_index: number) =>
    setQueue((q) =>
      q.filter(
        (c) => !(c.stack_id === stack_id && c.slice_index === slice_index),
      ),
    );

  const approve = (cand: BodyPartCandidate, label: string) => {
    samplesMutation.mutate(
      {
        ops: [
          {
            op: 'approve',
            stack_id: cand.stack_id,
            slice_index: cand.slice_index,
            label,
          },
        ],
      },
      {
        onSuccess: () => removeFromQueue(cand.stack_id, cand.slice_index),
      },
    );
  };

  const skip = (cand: BodyPartCandidate) =>
    removeFromQueue(cand.stack_id, cand.slice_index);

  const disabledControls = categories.length === 0;

  return (
    <Stack gap="md" data-testid="body-part-seed-queue">
      <Group gap="xs" align="flex-end" wrap="wrap">
        <Select
          label="Seed for category"
          data={categories}
          value={target}
          onChange={setTarget}
          placeholder={
            disabledControls ? 'Add categories first' : 'Pick category'
          }
          disabled={disabledControls}
          style={{ minWidth: 200 }}
          allowDeselect={false}
        />
        <TextInput
          label="How many?"
          value={nTarget}
          onChange={(e) => setNTarget(e.currentTarget.value)}
          style={{ width: 100 }}
          disabled={disabledControls}
        />
        <Button
          leftSection={<IconRefresh size={14} />}
          onClick={generate}
          loading={seedMutation.isPending}
          disabled={!target || disabledControls}
        >
          Generate queue
        </Button>
        {queue.length > 0 && (
          <Text size="sm" c="dimmed" ml="md">
            {queue.length} candidates remaining
          </Text>
        )}
      </Group>

      {seedMutation.isPending && (
        <Group justify="center" py="md">
          <Loader />
          <Text size="sm" c="dimmed">
            Scoring candidate slices…
          </Text>
        </Group>
      )}

      {!seedMutation.isPending && queue.length === 0 && (
        <Text size="sm" c="dimmed">
          {disabledControls
            ? 'Add at least one category before seeding the queue.'
            : 'No candidates loaded. Click "Generate queue" to score the cohort.'}
        </Text>
      )}

      {queue.length > 0 && (
        <SimpleGrid cols={{ base: 2, sm: 3, md: 4, lg: 5 }} spacing="sm">
          {queue.map((cand) => (
            <Card
              key={`${cand.stack_id}:${cand.slice_index}`}
              withBorder
              padding="xs"
              radius="md"
              data-testid={`seed-card-${cand.stack_id}-${cand.slice_index}`}
            >
              <Card.Section>
                <img
                  src={cand.thumbnail_url}
                  alt={`stack ${cand.stack_id} slice ${cand.slice_index}`}
                  style={{
                    width: '100%',
                    height: 140,
                    objectFit: 'contain',
                    background: '#000',
                  }}
                  loading="lazy"
                />
              </Card.Section>
              <Stack gap={4} mt="xs">
                <Text size="xs" fw={600} truncate>
                  {cand.subject_code}
                </Text>
                <Group gap={4} wrap="nowrap">
                  <Badge size="xs" color="gray" variant="light">
                    {cand.orientation}
                  </Badge>
                  <Badge size="xs" color="violet" variant="light">
                    p={cand.zs_prob.toFixed(2)}
                  </Badge>
                </Group>
                <Group gap={4} mt={4} wrap="nowrap">
                  <ActionIcon
                    color="green"
                    variant="filled"
                    onClick={() => target && approve(cand, target)}
                    disabled={!target || samplesMutation.isPending}
                    aria-label={`Approve as ${target ?? ''}`}
                  >
                    <IconCheck size={14} />
                  </ActionIcon>
                  <Menu position="bottom-end" withinPortal>
                    <Menu.Target>
                      <ActionIcon
                        color="violet"
                        variant="default"
                        aria-label="Reassign"
                        disabled={samplesMutation.isPending}
                      >
                        <IconChevronDown size={14} />
                      </ActionIcon>
                    </Menu.Target>
                    <Menu.Dropdown>
                      <Menu.Label>Reassign to…</Menu.Label>
                      {categories
                        .filter((c) => c !== target)
                        .map((c) => (
                          <Menu.Item
                            key={c}
                            onClick={() => approve(cand, c)}
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
                    color="gray"
                    variant="default"
                    onClick={() => skip(cand)}
                    aria-label="Skip"
                  >
                    <IconX size={14} />
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
