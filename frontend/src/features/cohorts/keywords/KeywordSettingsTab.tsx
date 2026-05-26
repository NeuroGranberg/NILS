/**
 * Cohort detail: Keywords tab.
 *
 * Layout:
 *   - Left rail: axis selector with "edited" badges
 *   - Right pane: bucket cards for the selected axis, plus search
 *
 * See spec section 7. Saving is passive — no sort is triggered.
 */
import {
  ActionIcon,
  Alert,
  Badge,
  Box,
  Button,
  Card,
  Divider,
  Group,
  Loader,
  NavLink,
  Stack,
  Text,
  TextInput,
  Title,
  Tooltip,
} from '@mantine/core';
import { IconAlertCircle, IconRefresh, IconSearch, IconX } from '@tabler/icons-react';
import { useMemo, useState } from 'react';

import { useBodyPartQCStateQuery } from '../body-part-qc/api';

import {
  useCohortKeywordConfigQuery,
  useResetAllKeywordsMutation,
  useResetKeywordBucketMutation,
  useUpdateKeywordBucketMutation,
} from './api';
import { BucketCard } from './BucketCard';
import type { KeywordBucketView } from './types';
import { countEditedBuckets } from './utils';

interface KeywordSettingsTabProps {
  cohortId: number;
}

const normalize = (s: string) => s.toLowerCase();

export const KeywordSettingsTab = ({ cohortId }: KeywordSettingsTabProps) => {
  const { data, isLoading, isError, error, refetch, isFetching } =
    useCohortKeywordConfigQuery(cohortId);
  const updateMutation = useUpdateKeywordBucketMutation(cohortId);
  const resetBucketMutation = useResetKeywordBucketMutation(cohortId);
  const resetAllMutation = useResetAllKeywordsMutation(cohortId);

  // M10: Body Part QC protection banner — show when the cohort has an
  // active QC run and the user is editing the body_part axis. The
  // keyword detector still runs normally, but its body_part output is
  // discarded for QC-decided stacks during step3 upsert.
  const bodyPartQCQuery = useBodyPartQCStateQuery(cohortId);
  const bpProtectedCount = bodyPartQCQuery.data?.summary.total_stacks ?? 0;
  const hasActiveBodyPartQC =
    !!bodyPartQCQuery.data?.has_current && bpProtectedCount > 0;

  const [activeAxis, setActiveAxis] = useState<string | null>(null);
  const [search, setSearch] = useState('');

  // Auto-select first axis once data loads.
  const axes = data?.axes ?? [];
  const effectiveActiveAxis = activeAxis ?? axes[0]?.axis ?? null;
  const activeAxisView = useMemo(
    () => axes.find((a) => a.axis === effectiveActiveAxis) ?? null,
    [axes, effectiveActiveAxis],
  );

  const searchTerm = search.trim();
  const filteredBuckets = useMemo(() => {
    if (!activeAxisView) return [] as KeywordBucketView[];
    if (!searchTerm) return activeAxisView.buckets;
    const needle = normalize(searchTerm);
    return activeAxisView.buckets.filter((b) => {
      if (normalize(b.display_name).includes(needle)) return true;
      if (normalize(b.bucket_path).includes(needle)) return true;
      if (b.description && normalize(b.description).includes(needle)) return true;
      const haystack = [...b.defaults, ...b.added, ...b.removed]
        .map(normalize)
        .join('\n');
      return haystack.includes(needle);
    });
  }, [activeAxisView, searchTerm]);

  const totalEdited = useMemo(
    () =>
      axes.reduce(
        (n, axis) => n + countEditedBuckets(axis.buckets),
        0,
      ),
    [axes],
  );

  if (isLoading) {
    return (
      <Group justify="center" py="xl">
        <Loader />
      </Group>
    );
  }

  if (isError) {
    return (
      <Alert
        color="red"
        icon={<IconAlertCircle size={16} />}
        title="Failed to load keyword configuration"
      >
        {(error as Error)?.message ?? 'Unknown error'}
      </Alert>
    );
  }

  if (!data) return null;

  return (
    <Stack gap="md">
      <Group justify="space-between" align="flex-start" wrap="wrap">
        <Stack gap={2}>
          <Title order={4}>Classification keywords</Title>
          <Text size="sm" c="dimmed" maw={720}>
            Cohort-specific keyword overrides applied on top of the global
            defaults. Trusted DICOM flags remain global; only keyword matching
            is editable here.{' '}
            <Text component="span" fw={500}>
              Saving updates settings only — it does not start a sort.
            </Text>{' '}
            Changes take effect on the next sort run.
          </Text>
        </Stack>
        <Group gap="xs">
          <Tooltip label="Refresh">
            <ActionIcon
              variant="default"
              onClick={() => refetch()}
              loading={isFetching}
              aria-label="Refresh keyword config"
            >
              <IconRefresh size={16} />
            </ActionIcon>
          </Tooltip>
          <Tooltip
            label={
              totalEdited === 0
                ? 'No edits to reset'
                : `Wipe all ${totalEdited} edited bucket(s) and revert to defaults`
            }
          >
            <Button
              variant="subtle"
              color="red"
              size="xs"
              disabled={totalEdited === 0 || resetAllMutation.isPending}
              loading={resetAllMutation.isPending}
              onClick={() => {
                if (
                  window.confirm(
                    `Reset ALL keyword overrides for this cohort? ` +
                      `(${totalEdited} bucket${totalEdited === 1 ? '' : 's'} edited)`,
                  )
                ) {
                  resetAllMutation.mutate();
                }
              }}
            >
              Reset all
            </Button>
          </Tooltip>
        </Group>
      </Group>

      <Group align="flex-start" wrap="nowrap" gap="md" grow={false}>
        {/* Left rail: axes */}
        <Card
          withBorder
          radius="md"
          padding="xs"
          style={{
            flex: '0 0 240px',
            maxWidth: 260,
            backgroundColor: 'var(--nils-bg-secondary)',
            borderColor: 'var(--nils-border-subtle)',
            alignSelf: 'flex-start',
            position: 'sticky',
            top: 12,
          }}
        >
          <Stack gap={2}>
            {axes.map((axis) => {
              const edited = countEditedBuckets(axis.buckets);
              const isActive = axis.axis === effectiveActiveAxis;
              const isQCProtectedAxis =
                axis.axis === 'body_part' && hasActiveBodyPartQC;
              return (
                <NavLink
                  key={axis.axis}
                  label={
                    <Group justify="space-between" wrap="nowrap">
                      <Text size="sm" fw={isActive ? 600 : 500}>
                        {axis.label}
                      </Text>
                      <Group gap={4} wrap="nowrap">
                        {isQCProtectedAxis && (
                          <Tooltip
                            label="Body Part QC is active for this cohort — see the banner."
                            withinPortal
                          >
                            <Badge
                              size="xs"
                              color="violet"
                              variant="light"
                              data-testid="body-part-axis-qc-badge"
                            >
                              QC
                            </Badge>
                          </Tooltip>
                        )}
                        {edited > 0 && (
                          <Badge size="xs" color="green" variant="light">
                            {edited}
                          </Badge>
                        )}
                      </Group>
                    </Group>
                  }
                  active={isActive}
                  onClick={() => setActiveAxis(axis.axis)}
                  variant="light"
                />
              );
            })}
          </Stack>
        </Card>

        {/* Right pane: buckets */}
        <Stack gap="sm" style={{ flex: 1, minWidth: 0 }}>
          {activeAxisView && (
            <Stack gap={4}>
              <Group justify="space-between" align="flex-end">
                <Stack gap={0}>
                  <Title order={5}>{activeAxisView.label}</Title>
                  {activeAxisView.description && (
                    <Text size="xs" c="dimmed">
                      {activeAxisView.description}
                    </Text>
                  )}
                </Stack>
                <Text size="xs" c="dimmed">
                  {filteredBuckets.length} / {activeAxisView.buckets.length} bucket
                  {activeAxisView.buckets.length === 1 ? '' : 's'}
                </Text>
              </Group>
              <TextInput
                size="sm"
                placeholder="Filter buckets by name or keyword…"
                value={search}
                onChange={(e) => setSearch(e.currentTarget.value)}
                leftSection={<IconSearch size={14} />}
                rightSection={
                  search ? (
                    <ActionIcon
                      variant="subtle"
                      size="xs"
                      aria-label="Clear search"
                      onClick={() => setSearch('')}
                    >
                      <IconX size={12} />
                    </ActionIcon>
                  ) : undefined
                }
              />
            </Stack>
          )}
          {effectiveActiveAxis === 'body_part' && hasActiveBodyPartQC && (
            <Alert
              color="violet"
              icon={<IconAlertCircle size={16} />}
              title="Body Part QC is active for this cohort"
              data-testid="body-part-qc-protection-banner"
            >
              <Text size="sm">
                These keyword overrides still take effect, but for the{' '}
                <strong>{bpProtectedCount}</strong> stack
                {bpProtectedCount === 1 ? '' : 's'} already labeled by
                the Body Part QC module the final{' '}
                <code>body_part</code> column is preserved during sort
                step 3 — the keyword detector cannot overwrite a
                QC-applied label. Use the Body Part QC page to relabel
                or restore.
              </Text>
            </Alert>
          )}
          <Divider />
          {filteredBuckets.length === 0 ? (
            <Text size="sm" c="dimmed" ta="center" py="lg">
              No buckets match your search.
            </Text>
          ) : (
            <Stack gap="xs">
              {filteredBuckets.map((bucket) => (
                <BucketCard
                  key={`${bucket.axis}:${bucket.bucket_path}`}
                  bucket={bucket}
                  isSaving={
                    updateMutation.isPending &&
                    updateMutation.variables?.bucket_path === bucket.bucket_path &&
                    updateMutation.variables?.axis === bucket.axis
                  }
                  isResetting={
                    resetBucketMutation.isPending &&
                    resetBucketMutation.variables?.bucket_path === bucket.bucket_path &&
                    resetBucketMutation.variables?.axis === bucket.axis
                  }
                  onSave={(payload) => updateMutation.mutate(payload)}
                  onReset={(payload) => resetBucketMutation.mutate(payload)}
                />
              ))}
            </Stack>
          )}
          <Box h={8} />
        </Stack>
      </Group>
    </Stack>
  );
};
