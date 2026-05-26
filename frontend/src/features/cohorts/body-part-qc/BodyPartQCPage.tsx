/**
 * Cohort Body Part QC — page shell.
 *
 * Layout:
 *   ┌─ Header (back / title / restore / apply)
 *   ├─ Categories editor
 *   ├─ Tabs:
 *   │     ① Label queue   — zero-shot seeded candidates
 *   │     ② Samples       — approved training set
 *   │     ③ Models        — global model registry + training
 *   │     ④ Apply         — apply model + commit
 *   │     ⑤ Changes       — paginated diff vs. prior body_part labels
 *   └─ —
 */
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Group,
  Loader,
  Modal,
  Paper,
  Stack,
  Tabs,
  Text,
  Title,
} from '@mantine/core';
import {
  IconAlertTriangle,
  IconArrowLeft,
  IconBrain,
  IconCheck,
  IconChartBar,
  IconCloudUpload,
  IconHistory,
  IconInfoCircle,
  IconList,
  IconDatabase,
  IconPlayerPlay,
  IconRefresh,
  IconTrash,
} from '@tabler/icons-react';
import { useState } from 'react';

import type { Cohort } from '../../../types/cohort';

import { CategoriesEditor } from './CategoriesEditor';
import { ChangeMatrix } from './ChangeMatrix';
import { ChangesView } from './ChangesView';
import { ModelsTab } from './ModelsTab';
import { SamplesGrid } from './SamplesGrid';
import { SeedQueue } from './SeedQueue';
import {
  useApplyBodyPartMutation,
  useBodyPartQCStateQuery,
  useCommitBodyPartMutation,
  useResetBodyPartMutation,
  useRestorePreviousBodyPartMutation,
} from './api';
import type { BodyPartStageStatus } from './types';

interface BodyPartQCPageProps {
  cohort: Cohort;
  onBack: () => void;
}

const STAGE_BADGE: Record<
  BodyPartStageStatus,
  { color: string; label: string; tooltip: string }
> = {
  none: {
    color: 'gray',
    label: 'no run yet',
    tooltip: 'Apply has not been run for this cohort.',
  },
  staged: {
    color: 'orange',
    label: 'staged',
    tooltip:
      "Apply produced picks but they haven't been written to the metadata DB. Press Commit to write through.",
  },
  committed: {
    color: 'green',
    label: 'committed',
    tooltip:
      'The current picks match the most recent commit. Edits flip to "dirty".',
  },
  dirty: {
    color: 'red',
    label: 'dirty',
    tooltip:
      'You have edits made on top of a previously-committed snapshot. Press Commit again to capture them in a fresh signature.',
  },
};

const StageStatusBadge = ({
  status,
  pendingCount,
  lastCommittedAt,
}: {
  status: BodyPartStageStatus;
  pendingCount: number;
  lastCommittedAt: string | null;
}) => {
  const meta = STAGE_BADGE[status];
  const suffix =
    pendingCount > 0 && (status === 'staged' || status === 'dirty')
      ? ` · ${pendingCount} pending`
      : status === 'committed' && lastCommittedAt
        ? ` · ${new Date(lastCommittedAt).toLocaleString()}`
        : '';
  return (
    <Badge
      color={meta.color}
      variant="light"
      title={meta.tooltip}
      data-testid={`body-part-stage-${status}`}
    >
      {meta.label}
      {suffix}
    </Badge>
  );
};

export const BodyPartQCPage = ({ cohort, onBack }: BodyPartQCPageProps) => {
  const cohortId = cohort.id;
  const { data, isLoading, isError, error } = useBodyPartQCStateQuery(cohortId);

  const applyMutation = useApplyBodyPartMutation(cohortId);
  const commitMutation = useCommitBodyPartMutation(cohortId);
  const restoreMutation = useRestorePreviousBodyPartMutation(cohortId);
  const resetMutation = useResetBodyPartMutation(cohortId);

  const [resetModalOpen, setResetModalOpen] = useState(false);

  const stageStatus: BodyPartStageStatus = data?.stage_status ?? 'none';
  const pendingCount = data?.pending_changes_count ?? 0;
  const overrideConflicts = data?.summary.override_conflicts_count ?? 0;

  const categories = data?.categories ?? [];
  const trainingSummary = data?.training_summary ?? {};

  const hasModel = data?.selected_model_id != null;

  if (isLoading) {
    return (
      <Stack align="center" py="xl">
        <Loader />
      </Stack>
    );
  }

  if (isError) {
    return (
      <Alert color="red" icon={<IconInfoCircle size={16} />}>
        Failed to load Body Part QC state: {(error as Error).message}
      </Alert>
    );
  }

  return (
    <Stack gap="md" data-testid="cohort-body-part-qc-page">
      {/* Header */}
      <Group justify="space-between" align="flex-start">
        <Group gap="md" align="flex-start">
          <ActionIcon variant="subtle" size="lg" onClick={onBack} aria-label="Back">
            <IconArrowLeft size={24} />
          </ActionIcon>
          <Stack gap={2}>
            <Title order={2} c="var(--nils-text-primary)">
              Body Part QC — {cohort.name}
            </Title>
            <Text size="sm" c="var(--nils-text-tertiary)">
              Train a per-cohort body-part classifier on a few labeled
              examples, then write the result to each stack's{' '}
              <code>body_part</code> column.
            </Text>
            {data?.current_run_at && (
              <Group gap="xs" align="center">
                <Text size="xs" c="dimmed">
                  Last apply: {new Date(data.current_run_at).toLocaleString()}
                </Text>
                <StageStatusBadge
                  status={stageStatus}
                  pendingCount={pendingCount}
                  lastCommittedAt={data.last_committed_at}
                />
                {overrideConflicts > 0 && (
                  <Badge
                    color="red"
                    variant="light"
                    leftSection={<IconAlertTriangle size={12} />}
                    title="Stacks where the user override conflicts with the latest model prediction. Review under the Changes tab."
                  >
                    {overrideConflicts} override conflict
                    {overrideConflicts === 1 ? '' : 's'}
                  </Badge>
                )}
                {data.selected_model_name && (
                  <Badge
                    color="violet"
                    variant="light"
                    leftSection={<IconDatabase size={12} />}
                  >
                    model: {data.selected_model_name}
                  </Badge>
                )}
              </Group>
            )}
          </Stack>
        </Group>
        <Group gap="xs">
          <Button
            variant="default"
            leftSection={<IconRefresh size={14} />}
            onClick={() => restoreMutation.mutate()}
            loading={restoreMutation.isPending}
            disabled={!data?.has_previous}
          >
            Restore previous
          </Button>
          <Button
            variant="subtle"
            color="red"
            leftSection={<IconTrash size={14} />}
            onClick={() => setResetModalOpen(true)}
            disabled={
              !data?.has_current && !data?.has_previous && !hasModel
            }
          >
            Reset QC
          </Button>
          <Button
            variant="filled"
            color="violet"
            leftSection={<IconPlayerPlay size={14} />}
            onClick={() => applyMutation.mutate()}
            loading={applyMutation.isPending}
            disabled={!hasModel}
          >
            Apply to cohort
          </Button>
          <Button
            variant="filled"
            color="green"
            leftSection={<IconCloudUpload size={14} />}
            onClick={() => commitMutation.mutate()}
            loading={commitMutation.isPending}
            disabled={
              !data?.has_current ||
              stageStatus === 'committed' ||
              stageStatus === 'none'
            }
            title={
              stageStatus === 'staged'
                ? 'Write staged picks to the metadata DB.'
                : stageStatus === 'dirty'
                  ? 'Capture latest edits in a fresh commit signature.'
                  : 'Nothing to commit.'
            }
          >
            Commit
          </Button>
        </Group>
      </Group>

      {/* Categories */}
      <Paper withBorder p="md" radius="md">
        <Stack gap="xs">
          <Group justify="space-between" align="center">
            <Text fw={600}>Categories</Text>
            {hasModel && data?.selected_model_name && (
              <Badge color="green" variant="light" leftSection={<IconCheck size={12} />}>
                model: {data.selected_model_name}
              </Badge>
            )}
          </Group>
          <CategoriesEditor
            cohortId={cohortId}
            categories={categories}
            trainingSummary={trainingSummary}
          />
        </Stack>
      </Paper>

      {/* Tabs */}
      <Tabs defaultValue="queue" keepMounted={false}>
        <Tabs.List>
          <Tabs.Tab value="queue" leftSection={<IconList size={14} />}>
            Label queue
          </Tabs.Tab>
          <Tabs.Tab value="samples" leftSection={<IconBrain size={14} />}>
            Approved samples
          </Tabs.Tab>
          <Tabs.Tab value="models" leftSection={<IconDatabase size={14} />}>
            Models
          </Tabs.Tab>
          <Tabs.Tab value="apply" leftSection={<IconPlayerPlay size={14} />}>
            Apply
          </Tabs.Tab>
          <Tabs.Tab
            value="changes"
            leftSection={<IconHistory size={14} />}
            disabled={!data?.has_current}
          >
            Changes{' '}
            {data?.summary.stacks_changed
              ? `(${data.summary.stacks_changed})`
              : ''}
          </Tabs.Tab>
        </Tabs.List>

        <Tabs.Panel value="queue" pt="md">
          <SeedQueue cohortId={cohortId} categories={categories} />
        </Tabs.Panel>

        <Tabs.Panel value="samples" pt="md">
          <SamplesGrid
            cohortId={cohortId}
            categories={categories}
            trainingSummary={trainingSummary}
          />
        </Tabs.Panel>

        <Tabs.Panel value="models" pt="md">
          <ModelsTab
            cohortId={cohortId}
            selectedModelId={data?.selected_model_id ?? null}
            hasTrainingSamples={
              Object.values(trainingSummary).some((v) => v.total > 0)
            }
          />
        </Tabs.Panel>

        <Tabs.Panel value="apply" pt="md">
          <Stack gap="md">
            <Paper withBorder p="md" radius="md">
              <Stack gap="sm">
                <Group justify="space-between" align="center">
                  <Text fw={600}>Apply to cohort</Text>
                  {data?.current_run_at && (
                    <Text size="xs" c="dimmed">
                      last run{' '}
                      {new Date(data.current_run_at).toLocaleString()}
                    </Text>
                  )}
                </Group>

                <Text size="sm" c="dimmed">
                  Embeds and classifies every eligible stack in the
                  cohort using the selected model. The result is{' '}
                  <strong>staged</strong>: nothing is written to the
                  metadata DB until you press <em>Commit</em>. User
                  overrides survive a re-Apply; conflicts with the new
                  model are flagged in the Changes tab.
                </Text>

                {!hasModel && (
                  <Alert color="yellow" icon={<IconInfoCircle size={14} />}>
                    Select a model in the Models tab before applying.
                  </Alert>
                )}

                {applyMutation.isPending && (
                  <Alert
                    color="violet"
                    icon={<Loader size={14} />}
                    title="Applying…"
                  >
                    Streaming central slices through the worker, running
                    inference, and writing per-stack labels. This can
                    take a minute or two for large cohorts.
                  </Alert>
                )}

                {data?.has_current && (
                  <Group gap="lg" wrap="wrap">
                    <Text size="sm">
                      Sessions:{' '}
                      <strong>{data.summary.total_sessions}</strong>
                    </Text>
                    <Text size="sm">
                      Stacks changed:{' '}
                      <strong>{data.summary.stacks_changed}</strong>
                    </Text>
                    <Text size="sm">
                      Sessions changed:{' '}
                      <strong>{data.summary.sessions_changed}</strong>
                    </Text>
                    <Text size="sm">
                      Needs check:{' '}
                      <strong>{data.summary.needs_check}</strong>
                    </Text>
                  </Group>
                )}

                <Group>
                  <Button
                    color="violet"
                    leftSection={<IconPlayerPlay size={14} />}
                    onClick={() => applyMutation.mutate()}
                    loading={applyMutation.isPending}
                    disabled={!hasModel}
                  >
                    {data?.has_current ? 'Re-apply' : 'Apply'}
                  </Button>
                  <Button
                    color="green"
                    leftSection={<IconCloudUpload size={14} />}
                    onClick={() => commitMutation.mutate()}
                    loading={commitMutation.isPending}
                    disabled={
                      !data?.has_current ||
                      stageStatus === 'committed' ||
                      stageStatus === 'none'
                    }
                  >
                    Commit{pendingCount > 0 ? ` (${pendingCount})` : ''}
                  </Button>
                  {data?.has_current && (
                    <StageStatusBadge
                      status={stageStatus}
                      pendingCount={pendingCount}
                      lastCommittedAt={data.last_committed_at}
                    />
                  )}
                </Group>
              </Stack>
            </Paper>

            {/* Run summary (after Apply) */}
            {data?.has_current && (
              <Paper withBorder p="md" radius="md">
                <Stack gap="sm">
                  <Group gap="xs">
                    <IconChartBar size={16} />
                    <Text fw={600}>Run summary</Text>
                  </Group>
                  <ChangeMatrix
                    matrix={data.summary.change_matrix ?? {}}
                    categories={categories}
                  />
                </Stack>
              </Paper>
            )}
          </Stack>
        </Tabs.Panel>

        <Tabs.Panel value="changes" pt="md">
          {data?.has_current ? (
            <ChangesView
              cohortId={cohortId}
              categories={categories}
              picks={data.picks}
            />
          ) : (
            <Text size="sm" c="dimmed">
              Apply first to see relabeled stacks here.
            </Text>
          )}
        </Tabs.Panel>
      </Tabs>

      {/* ── Reset QC confirmation modal ─────────────────────── */}
      <Modal
        opened={resetModalOpen}
        onClose={() => setResetModalOpen(false)}
        title="Reset Body Part QC?"
        centered
      >
        <Stack gap="sm">
          <Text size="sm">
            This will discard <strong>all</strong> Body Part QC state for this
            cohort:
          </Text>
          <ul style={{ margin: 0, paddingLeft: 20 }}>
            <Text component="li" size="sm">Staged and committed predictions</Text>
            <Text component="li" size="sm">Training samples and classifier reference</Text>
            <Text component="li" size="sm">User overrides and undo history</Text>
            <Text component="li" size="sm">Low-confidence review tokens</Text>
          </ul>
          <Text size="sm">
            The keyword-based <code>body_part</code> labels will be restored on
            the <strong>next resort</strong>. Stacks keep their current label
            until then.
          </Text>
          <Text size="sm" c="dimmed">
            Embedding cache and on-disk classifier file are preserved, so
            re-doing the workflow is faster.
          </Text>
          <Text size="sm" fw={700} c="red">
            This cannot be undone.
          </Text>
          <Group justify="flex-end" mt="md">
            <Button variant="default" onClick={() => setResetModalOpen(false)}>
              Cancel
            </Button>
            <Button
              color="red"
              leftSection={<IconTrash size={14} />}
              loading={resetMutation.isPending}
              onClick={() => {
                resetMutation.mutate(undefined, {
                  onSuccess: () => setResetModalOpen(false),
                });
              }}
            >
              Reset
            </Button>
          </Group>
        </Stack>
      </Modal>
    </Stack>
  );
};
