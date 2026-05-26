/**
 * ChangesView — paginated diff table / compact grid for the most recent Apply.
 *
 * Features:
 * - Filters: from-label, to-label, prior-source, min-confidence.
 * - Row selection with per-row checkboxes + "select all visible".
 * - Action toolbar: Commit selected, Destage selected, Commit all matching.
 * - Two view modes: Table (detailed, 50/page) and Grid (compact thumbnails, 200/page).
 * - Middle-slice preview image.
 * - Clear Was → Now display with colored badges + top-2 probs.
 * - Color-graded confidence bar.
 *
 * Sort order is lowest-confidence-first (server-side) so reviewers
 * tackle the most suspicious changes first.
 */
import {
  ActionIcon,
  Badge,
  Button,
  Checkbox,
  Group,
  Pagination,
  Paper,
  Progress,
  SegmentedControl,
  Select,
  SimpleGrid,
  Slider,
  Stack,
  Table,
  Text,
  Tooltip,
} from '@mantine/core';
import {
  IconAlertTriangle,
  IconArrowRight,
  IconCheck,
  IconCloudUpload,
  IconEdit,
  IconLayoutGrid,
  IconList,
  IconRefresh,
  IconTrash,
} from '@tabler/icons-react';
import { useCallback, useMemo, useState } from 'react';

import {
  useBodyPartChangesQuery,
  useCommitBodyPartMutation,
  useDestageBodyPartMutation,
  useOverrideStackMutation,
} from './api';
import { OverrideModal } from './OverrideModal';
import type {
  BodyPartChangeRow,
  BodyPartSessionPick,
  BodyPartStackPick,
} from './types';

const PRIOR_SOURCES = [
  { value: '', label: 'Any' },
  { value: '(none)', label: '(none)' },
  { value: 'text_keyword', label: 'text_keyword' },
  { value: 'qc_v1', label: 'qc_v1' },
  { value: 'manual', label: 'manual' },
];

const CONFIDENCE_MARKS = [
  { value: 0, label: '0%' },
  { value: 25, label: '25%' },
  { value: 50, label: '50%' },
  { value: 75, label: '75%' },
  { value: 100, label: '100%' },
];

interface ChangesViewProps {
  cohortId: number;
  categories: string[];
  picks: BodyPartSessionPick[];
}

type ViewMode = 'table' | 'grid';
const TABLE_PAGE_SIZE = 50;
const GRID_PAGE_SIZE = 200;

const findStack = (
  picks: BodyPartSessionPick[],
  subjectId: number,
  sessionDate: string | null,
  stackId: number,
): BodyPartStackPick | undefined =>
  picks
    .find((p) => p.subject_id === subjectId && p.session_date === sessionDate)
    ?.stacks.find((s) => s.stack_id === stackId);

/** Color for confidence value. */
const confColor = (c: number): string =>
  c < 0.6 ? 'red' : c < 0.75 ? 'yellow' : 'green';

/** Badge color for the "Now" label. */
const nowBadgeColor = (row: BodyPartChangeRow): string => {
  if (row.is_override) return 'blue';
  if (row.needs_check) return 'yellow';
  return 'green';
};

/** Top-2 probabilities as a readable string. */
const top2Probs = (
  picks: BodyPartSessionPick[],
  row: BodyPartChangeRow,
): string => {
  const stk = findStack(picks, row.subject_id, row.session_date, row.stack_id);
  if (!stk?.probs) return '';
  const sorted = Object.entries(stk.probs)
    .sort(([, a], [, b]) => b - a)
    .slice(0, 2);
  return sorted
    .map(([label, prob]) => `${label} ${(prob * 100).toFixed(0)}%`)
    .join(' · ');
};

export const ChangesView = ({
  cohortId,
  categories,
  picks,
}: ChangesViewProps) => {
  const [viewMode, setViewMode] = useState<ViewMode>('table');
  const [fromLabel, setFromLabel] = useState<string | null>(null);
  const [toLabel, setToLabel] = useState<string | null>(null);
  const [priorSource, setPriorSource] = useState<string | null>(null);
  const [minConfPct, setMinConfPct] = useState(0);
  const [page, setPage] = useState(1);
  const [activeRow, setActiveRow] = useState<BodyPartChangeRow | null>(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [selected, setSelected] = useState<Set<number>>(new Set());

  const pageSize = viewMode === 'grid' ? GRID_PAGE_SIZE : TABLE_PAGE_SIZE;
  const offset = (page - 1) * pageSize;

  const params = useMemo(
    () => ({
      from_label: fromLabel || undefined,
      to_label: toLabel || undefined,
      prior_source: priorSource || undefined,
      min_confidence: minConfPct > 0 ? minConfPct / 100 : undefined,
      offset,
      limit: pageSize,
    }),
    [fromLabel, toLabel, priorSource, minConfPct, offset, pageSize],
  );

  const query = useBodyPartChangesQuery(cohortId, params);
  const data = query.data;
  const overrideMutation = useOverrideStackMutation(cohortId);
  const commitMutation = useCommitBodyPartMutation(cohortId);
  const destageMutation = useDestageBodyPartMutation(cohortId);

  const acceptNewModelPrediction = (row: BodyPartChangeRow) => {
    if (!row.override_conflict) return;
    overrideMutation.mutate({
      subject_id: row.subject_id,
      session_date: row.session_date ?? '',
      stack_id: row.stack_id,
      label: row.override_conflict.label,
      note: 'accepted model prediction over prior override',
    });
  };

  const totalPages = data ? Math.max(1, Math.ceil(data.total / pageSize)) : 1;

  const fromOptions = [
    { value: '', label: 'Any' },
    { value: '(none)', label: '(none)' },
    ...categories.map((c) => ({ value: c, label: c })),
  ];
  const toOptions = [
    { value: '', label: 'Any' },
    ...categories.map((c) => ({ value: c, label: c })),
  ];

  const openOverride = (row: BodyPartChangeRow) => {
    setActiveRow(row);
    setModalOpen(true);
  };

  const reset = () => {
    setFromLabel(null);
    setToLabel(null);
    setPriorSource(null);
    setMinConfPct(0);
    setPage(1);
    setSelected(new Set());
  };

  // Selection helpers
  const visibleIds = useMemo(
    () => new Set(data?.rows.map((r) => r.stack_id) ?? []),
    [data],
  );

  const allVisibleSelected =
    visibleIds.size > 0 &&
    [...visibleIds].every((id) => selected.has(id));

  const toggleAll = useCallback(() => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (allVisibleSelected) {
        for (const id of visibleIds) next.delete(id);
      } else {
        for (const id of visibleIds) next.add(id);
      }
      return next;
    });
  }, [allVisibleSelected, visibleIds]);

  const toggleOne = useCallback((stackId: number) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(stackId)) next.delete(stackId);
      else next.add(stackId);
      return next;
    });
  }, []);

  const handleCommitSelected = () => {
    if (selected.size === 0) return;
    commitMutation.mutate(
      { stack_ids: [...selected] },
      { onSuccess: () => setSelected(new Set()) },
    );
  };

  const handleDestageSelected = () => {
    if (selected.size === 0) return;
    destageMutation.mutate(
      { stack_ids: [...selected] },
      { onSuccess: () => setSelected(new Set()) },
    );
  };

  /** Commit everything matching the current filter (not just the visible page). */
  const hasActiveFilter =
    !!fromLabel || !!toLabel || minConfPct > 0;

  const handleCommitAllFiltered = () => {
    commitMutation.mutate(
      {
        from_label: fromLabel || undefined,
        to_label: toLabel || undefined,
        min_confidence: minConfPct > 0 ? minConfPct / 100 : undefined,
      },
      { onSuccess: () => setSelected(new Set()) },
    );
  };

  return (
    <Stack gap="md" data-testid="body-part-changes-view">
      {/* Filters */}
      <Paper withBorder p="md" radius="md">
        <Stack gap="md">
          <Group gap="md" align="flex-end" wrap="wrap">
            <Select
              label="From"
              data={fromOptions}
              value={fromLabel ?? ''}
              onChange={(v) => {
                setFromLabel(v || null);
                setPage(1);
              }}
              style={{ minWidth: 140 }}
            />
            <Select
              label="To"
              data={toOptions}
              value={toLabel ?? ''}
              onChange={(v) => {
                setToLabel(v || null);
                setPage(1);
              }}
              style={{ minWidth: 140 }}
            />
            <Select
              label="Prior source"
              data={PRIOR_SOURCES}
              value={priorSource ?? ''}
              onChange={(v) => {
                setPriorSource(v || null);
                setPage(1);
              }}
              style={{ minWidth: 160 }}
            />
            <ActionIcon
              variant="subtle"
              onClick={reset}
              aria-label="Reset filters"
              size="lg"
            >
              <IconRefresh size={16} />
            </ActionIcon>
            {data && (
              <Text size="sm" c="dimmed" ml="auto">
                {data.total} change{data.total === 1 ? '' : 's'} match filter
              </Text>
            )}
          </Group>
          <Group gap="md" align="flex-end" justify="space-between">
            <Stack gap={4} style={{ flex: 1, maxWidth: 300 }}>
              <Text size="xs" fw={500}>
                Min confidence: {minConfPct}%
              </Text>
              <Slider
                value={minConfPct}
                onChange={(v) => {
                  setMinConfPct(v);
                  setPage(1);
                }}
                min={0}
                max={100}
                step={5}
                marks={CONFIDENCE_MARKS}
                size="sm"
              />
            </Stack>
            <Group gap="sm">
              {hasActiveFilter && data && data.total > 0 && (
                <Button
                  size="compact-sm"
                  color="green"
                  variant="light"
                  leftSection={<IconCloudUpload size={14} />}
                  onClick={handleCommitAllFiltered}
                  loading={commitMutation.isPending}
                >
                  Commit all {data.total} matching
                </Button>
              )}
              <SegmentedControl
                size="xs"
                value={viewMode}
                onChange={(v) => {
                  setViewMode(v as ViewMode);
                  setPage(1);
                  setSelected(new Set());
                }}
                data={[
                  { value: 'table', label: (<IconList size={14} />) as any },
                  { value: 'grid', label: (<IconLayoutGrid size={14} />) as any },
                ]}
              />
            </Group>
          </Group>
        </Stack>
      </Paper>

      {/* Action toolbar — shown when selection exists */}
      {selected.size > 0 && (
        <Paper withBorder p="xs" radius="md">
          <Group gap="md">
            <Text size="sm" fw={500}>
              {selected.size} selected
            </Text>
            <Button
              size="compact-sm"
              color="green"
              leftSection={<IconCloudUpload size={14} />}
              onClick={handleCommitSelected}
              loading={commitMutation.isPending}
            >
              Commit selected ({selected.size})
            </Button>
            <Button
              size="compact-sm"
              color="red"
              variant="light"
              leftSection={<IconTrash size={14} />}
              onClick={handleDestageSelected}
              loading={destageMutation.isPending}
            >
              Destage selected ({selected.size})
            </Button>
            <Button
              size="compact-xs"
              variant="subtle"
              color="gray"
              onClick={() => setSelected(new Set())}
            >
              Clear
            </Button>
          </Group>
        </Paper>
      )}

      {query.isLoading && (
        <Text size="sm" c="dimmed">
          Loading changes…
        </Text>
      )}

      {data && data.total === 0 && (
        <Paper withBorder p="md" radius="md">
          <Text size="sm" c="dimmed" ta="center">
            No changes match the current filter.
          </Text>
        </Paper>
      )}

      {data && data.total > 0 && viewMode === 'grid' && (
        <>
          <Group gap={4} mb={4}>
            <Checkbox
              label={`Select all ${data.rows.length} visible`}
              checked={allVisibleSelected}
              indeterminate={
                selected.size > 0 && !allVisibleSelected &&
                [...visibleIds].some((id) => selected.has(id))
              }
              onChange={toggleAll}
              size="xs"
            />
          </Group>
          <SimpleGrid cols={{ base: 4, sm: 6, md: 8, lg: 10 }} spacing={6}>
            {data.rows.map((row) => {
              const cc = confColor(row.confidence);
              const isChecked = selected.has(row.stack_id);
              return (
                <Paper
                  key={row.stack_id}
                  data-testid={`body-part-change-row-${row.stack_id}`}
                  withBorder
                  radius="sm"
                  p={4}
                  style={{
                    cursor: 'pointer',
                    outline: isChecked
                      ? '2px solid var(--mantine-color-blue-5)'
                      : 'none',
                    opacity: isChecked ? 1 : 0.85,
                    position: 'relative',
                  }}
                  onClick={() => toggleOne(row.stack_id)}
                >
                  <img
                    src={row.middle_slice_url || row.thumbnail_url}
                    alt=""
                    style={{
                      width: '100%',
                      aspectRatio: '1',
                      objectFit: 'contain',
                      background: '#000',
                      borderRadius: 2,
                      display: 'block',
                    }}
                    loading="lazy"
                  />
                  <Stack gap={2} mt={4}>
                    <Group gap={4} wrap="nowrap">
                      <Badge color="gray" variant="light" size="xs" style={{ flex: '0 0 auto' }}>
                        {row.previous_label ?? '—'}
                      </Badge>
                      <Text size="xs" c="dimmed">→</Text>
                      <Badge
                        color={nowBadgeColor(row)}
                        variant={row.is_override ? 'filled' : 'light'}
                        size="xs"
                        style={{ flex: '0 0 auto' }}
                      >
                        {row.new_label}
                      </Badge>
                    </Group>
                    <Progress
                      value={row.confidence * 100}
                      color={cc}
                      size={4}
                      radius="xl"
                    />
                    <Text size={10} c="dimmed" lineClamp={1}>
                      {(row.confidence * 100).toFixed(0)}% · #{row.stack_id}
                    </Text>
                  </Stack>
                  {row.needs_check && (
                    <Badge
                      size="xs"
                      color="yellow"
                      variant="filled"
                      style={{
                        position: 'absolute',
                        top: 2,
                        right: 2,
                      }}
                    >
                      !
                    </Badge>
                  )}
                  <Checkbox
                    checked={isChecked}
                    onChange={() => toggleOne(row.stack_id)}
                    size="xs"
                    style={{
                      position: 'absolute',
                      top: 4,
                      left: 4,
                    }}
                    onClick={(e) => e.stopPropagation()}
                  />
                </Paper>
              );
            })}
          </SimpleGrid>
        </>
      )}

      {data && data.total > 0 && viewMode === 'table' && (
        <Paper withBorder radius="md" style={{ overflow: 'hidden' }}>
          <Table
            stickyHeader
            highlightOnHover
            verticalSpacing="xs"
            data-testid="body-part-changes-table"
          >
            <Table.Thead>
              <Table.Tr>
                <Table.Th style={{ width: 40 }}>
                  <Checkbox
                    checked={allVisibleSelected}
                    indeterminate={
                      selected.size > 0 && !allVisibleSelected &&
                      [...visibleIds].some((id) => selected.has(id))
                    }
                    onChange={toggleAll}
                    aria-label="Select all visible"
                    size="xs"
                  />
                </Table.Th>
                <Table.Th style={{ width: 96 }}>Preview</Table.Th>
                <Table.Th>Subject</Table.Th>
                <Table.Th>Stack</Table.Th>
                <Table.Th>Was → Now</Table.Th>
                <Table.Th style={{ width: 100 }}>Confidence</Table.Th>
                <Table.Th>Source</Table.Th>
                <Table.Th>Flags</Table.Th>
                <Table.Th />
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {data.rows.map((row) => {
                const probsText = top2Probs(picks, row);
                const cc = confColor(row.confidence);
                return (
                  <Table.Tr
                    key={`${row.study_id}-${row.stack_id}`}
                    data-testid={`body-part-change-row-${row.stack_id}`}
                    bg={selected.has(row.stack_id) ? 'var(--mantine-color-blue-light)' : undefined}
                  >
                    {/* Checkbox */}
                    <Table.Td>
                      <Checkbox
                        checked={selected.has(row.stack_id)}
                        onChange={() => toggleOne(row.stack_id)}
                        size="xs"
                      />
                    </Table.Td>

                    {/* Middle-slice preview */}
                    <Table.Td>
                      <img
                        src={row.middle_slice_url || row.thumbnail_url}
                        alt=""
                        width={96}
                        height={96}
                        style={{
                          objectFit: 'contain',
                          background: '#000',
                          borderRadius: 4,
                        }}
                        loading="lazy"
                      />
                    </Table.Td>

                    {/* Subject */}
                    <Table.Td>
                      <Stack gap={2}>
                        <Text size="sm" fw={500}>
                          {row.subject_code}
                        </Text>
                        {row.session_date && (
                          <Text size="xs" c="dimmed">
                            {row.session_date}
                          </Text>
                        )}
                      </Stack>
                    </Table.Td>

                    {/* Stack */}
                    <Table.Td>
                      <Text size="xs" ff="monospace">
                        #{row.stack_id}
                      </Text>
                      {row.technique && (
                        <Text size="xs" c="dimmed">
                          {row.technique}
                        </Text>
                      )}
                    </Table.Td>

                    {/* Was → Now */}
                    <Table.Td>
                      <Stack gap={4}>
                        <Group gap={6} wrap="nowrap">
                          <Badge color="gray" variant="light" size="sm">
                            {row.previous_label ?? '(none)'}
                          </Badge>
                          <IconArrowRight size={12} color="var(--mantine-color-dimmed)" />
                          <Badge
                            color={nowBadgeColor(row)}
                            variant={row.is_override ? 'filled' : 'light'}
                            size="sm"
                          >
                            {row.new_label}
                          </Badge>
                        </Group>
                        {probsText && (
                          <Text size="xs" c="dimmed">
                            {probsText}
                          </Text>
                        )}
                      </Stack>
                    </Table.Td>

                    {/* Confidence bar */}
                    <Table.Td>
                      <Stack gap={2}>
                        <Text size="xs" fw={500} c={cc}>
                          {(row.confidence * 100).toFixed(0)}%
                        </Text>
                        <Progress
                          value={row.confidence * 100}
                          color={cc}
                          size="xs"
                          radius="xl"
                        />
                      </Stack>
                    </Table.Td>

                    {/* Source */}
                    <Table.Td>
                      <Text size="xs" c="dimmed">
                        {row.prior_source ?? '—'}
                      </Text>
                    </Table.Td>

                    {/* Flags */}
                    <Table.Td>
                      <Group gap={4}>
                        {row.needs_check && (
                          <Badge size="xs" color="yellow" variant="light">
                            low-conf
                          </Badge>
                        )}
                        {row.is_override && (
                          <Badge size="xs" color="blue" variant="light">
                            manual
                          </Badge>
                        )}
                        {row.override_conflict && (
                          <Tooltip
                            label={
                              `Model predicts "${row.override_conflict.label}" ` +
                              `(${(row.override_conflict.prob * 100).toFixed(0)}%) ` +
                              `but override kept "${row.new_label}".`
                            }
                          >
                            <Badge
                              size="xs"
                              color="red"
                              variant="light"
                              leftSection={<IconAlertTriangle size={10} />}
                            >
                              conflict
                            </Badge>
                          </Tooltip>
                        )}
                      </Group>
                    </Table.Td>

                    {/* Actions */}
                    <Table.Td>
                      <Group gap={4} wrap="nowrap">
                        <ActionIcon
                          variant="subtle"
                          onClick={() => openOverride(row)}
                          aria-label="Override this stack"
                        >
                          <IconEdit size={14} />
                        </ActionIcon>
                        {row.override_conflict && (
                          <Button
                            size="compact-xs"
                            color="red"
                            variant="light"
                            leftSection={<IconCheck size={12} />}
                            onClick={() => acceptNewModelPrediction(row)}
                            loading={
                              overrideMutation.isPending &&
                              overrideMutation.variables?.subject_id ===
                                row.subject_id &&
                              overrideMutation.variables?.session_date ===
                                (row.session_date ?? '') &&
                              overrideMutation.variables?.stack_id ===
                                row.stack_id
                            }
                            title={`Accept the new model's prediction (${row.override_conflict.label}).`}
                          >
                            Accept new
                          </Button>
                        )}
                      </Group>
                    </Table.Td>
                  </Table.Tr>
                );
              })}
            </Table.Tbody>
          </Table>
        </Paper>
      )}

      {data && totalPages > 1 && (
        <Group justify="center">
          <Pagination
            value={page}
            onChange={setPage}
            total={totalPages}
            size="sm"
          />
        </Group>
      )}

      <OverrideModal
        cohortId={cohortId}
        categories={categories}
        probs={
          activeRow
            ? findStack(
                picks,
                activeRow.subject_id,
                activeRow.session_date,
                activeRow.stack_id,
              )?.probs
            : undefined
        }
        row={activeRow}
        opened={modalOpen}
        onClose={() => setModalOpen(false)}
      />
    </Stack>
  );
};
