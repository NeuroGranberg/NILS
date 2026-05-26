/**
 * Content legend — top of the page.
 *
 * Two parts:
 *   1. A continuous score gradient bar (color = score). No vague wording.
 *   2. Chips grouped by *content axis* (acquisition type / technique / family /
 *      resolution / status border). Chips are derived dynamically from the
 *      actual picks, so cohorts with no Dixon stacks won't show that chip.
 *
 * Clicking a chip toggles it; cells that don't match the active filter are
 * dimmed in the heatmap. AND across axes, OR within an axis.
 */
import { Badge, Box, Button, Group, Stack, Text } from '@mantine/core';
import { memo, useMemo } from 'react';

import type { ContentFilter, MainQCSessionPick } from './types';
import { EMPTY_FILTER } from './types';
import {
  buildContentChips,
  isFilterActive,
  scoreToColor,
  type ChipOption,
} from './utils';

interface ContentLegendProps {
  picks: MainQCSessionPick[];
  /** Restricts which picks contribute to chip counts. */
  axisMode: 't1w' | 'flair' | 'both';
  filter: ContentFilter;
  onFilterChange: (next: ContentFilter) => void;
}

export const ContentLegend = ({ picks, axisMode, filter, onFilterChange }: ContentLegendProps) => {
  const visiblePicks = useMemo(() => {
    if (axisMode === 'both') return picks;
    return picks.filter((p) => p.axis === axisMode);
  }, [picks, axisMode]);
  const chips = useMemo(() => buildContentChips(visiblePicks), [visiblePicks]);
  const active = isFilterActive(filter);

  const toggle = (axis: keyof ContentFilter, value: string) => {
    const current = filter[axis];
    const next = current.includes(value)
      ? current.filter((v) => v !== value)
      : [...current, value];
    onFilterChange({ ...filter, [axis]: next });
  };

  return (
    <Stack
      gap="xs"
      p="sm"
      style={{
        border: '1px solid var(--nils-border)',
        borderRadius: 6,
        background: 'var(--nils-bg-secondary)',
        color: 'var(--nils-text-primary)',
      }}
      data-testid="content-legend"
    >
      <Group justify="space-between" align="center">
        <Text
          size="xs"
          fw={600}
          tt="uppercase"
          style={{ color: 'var(--nils-text-secondary)', letterSpacing: 0.4 }}
        >
          Filter cells by content — click to dim non-matches
        </Text>
        {active && (
          <Button
            variant="subtle"
            size="compact-xs"
            onClick={() => onFilterChange(EMPTY_FILTER)}
          >
            Clear filter
          </Button>
        )}
      </Group>

      {/* Score gradient bar — color is the only encoding for score. */}
      <Group gap={6} align="center" wrap="nowrap">
        <Text size="xs" w={88} style={{ color: 'var(--nils-text-secondary)' }}>
          Score
        </Text>
        <Text size="xs" style={{ color: 'var(--nils-text-tertiary)' }}>0</Text>
        <Box
          style={{
            flex: '0 0 240px',
            height: 10,
            borderRadius: 4,
            background: `linear-gradient(to right, ${scoreToColor(0)}, ${scoreToColor(0.5)}, ${scoreToColor(1)})`,
            border: '1px solid var(--nils-border)',
          }}
        />
        <Text size="xs" style={{ color: 'var(--nils-text-tertiary)' }}>1.0</Text>
      </Group>

      <ChipGroup
        title="Acquisition"
        options={chips.dim}
        selected={filter.dim}
        onToggle={(v) => toggle('dim', v)}
      />
      <ChipGroup
        title="Technique"
        options={chips.technique}
        selected={filter.technique}
        onToggle={(v) => toggle('technique', v)}
      />
      <ChipGroup
        title="Family"
        options={chips.family}
        selected={filter.family}
        onToggle={(v) => toggle('family', v)}
      />
      <ChipGroup
        title="Resolution"
        options={chips.slice_bucket}
        selected={filter.slice_bucket}
        onToggle={(v) => toggle('slice_bucket', v)}
      />
      <ChipGroup
        title="Status"
        options={chips.border}
        selected={filter.border}
        onToggle={(v) => toggle('border', v)}
      />
    </Stack>
  );
};

interface ChipGroupProps {
  title: string;
  options: ChipOption[];
  selected: string[];
  onToggle: (value: string) => void;
}

const ChipGroup = memo(function ChipGroup({ title, options, selected, onToggle }: ChipGroupProps) {
  if (options.length === 0) return null;
  return (
    <Group gap={6} wrap="wrap" align="center">
      <Text
        size="xs"
        w={88}
        style={{ flexShrink: 0, color: 'var(--nils-text-secondary)' }}
      >
        {title}
      </Text>
      {options.map((opt) => {
        const isOn = selected.includes(opt.value);
        return (
          <Badge
            key={opt.value}
            radius="sm"
            size="sm"
            style={{
              cursor: 'pointer',
              textTransform: 'none',
              background: isOn
                ? 'var(--nils-accent-secondary)'
                : 'var(--nils-bg-tertiary)',
              color: isOn ? '#fff' : 'var(--nils-text-primary)',
              border: `1px solid ${isOn ? 'var(--nils-accent-primary)' : 'var(--nils-border)'}`,
              fontWeight: 500,
            }}
            onClick={() => onToggle(opt.value)}
            data-testid={`chip-${title.toLowerCase()}-${opt.value}`}
          >
            {opt.label} <span style={{ opacity: 0.7 }}>· {opt.count}</span>
          </Badge>
        );
      })}
    </Group>
  );
});
