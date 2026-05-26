/**
 * A bundle card groups stacks that share the "same-acquisition" key
 * (everything except post-contrast status). All tiles scroll in sync.
 *
 * Grid: 2 tiles per row. Extras are hidden behind a "Show N more" button.
 * Pre-selection of MAIN/PRE/POST is driven by the server payload.
 */

import { useMemo, useState } from 'react';
import { Box, Button, Collapse, Group, Paper, SimpleGrid, Stack, Text, Badge } from '@mantine/core';
import { IconChevronDown, IconChevronUp } from '@tabler/icons-react';
import type { Bundle } from '../../api/main-acq';
import { BundleStackTile } from './BundleStackTile';
import type { ViewSize } from '../../pages/MainAcquisitionQCPage';
import { VIEW_SIZE_COLS } from '../../pages/MainAcquisitionQCPage';

interface BundleCardProps {
  bundle: Bundle;
  viewSize: ViewSize;
  includeAcceleration?: boolean;
}

export const BundleCard = ({ bundle, viewSize, includeAcceleration = true }: BundleCardProps) => {
  const [progress, setProgress] = useState(0.5);
  const [expanded, setExpanded] = useState(false);

  const gridCols = VIEW_SIZE_COLS[viewSize];
  // Show more tiles in the first row for compact/medium views
  const firstRowCount = viewSize === 'compact' ? 5 : viewSize === 'medium' ? 3 : 2;

  const [firstRow, rest] = useMemo(() => {
    const stacks = bundle.stacks;
    return [stacks.slice(0, firstRowCount), stacks.slice(firstRowCount)];
  }, [bundle.stacks, firstRowCount]);

  const total = bundle.stacks.length;
  const hasOverflow = rest.length > 0;

  // Strip acceleration tokens from title when toggle is off
  const displayTitle = useMemo(() => {
    if (includeAcceleration || !bundle.acceleration_csv) return bundle.title;
    const accelTokens = new Set<string>();
    // Individual values (e.g. "PartialFourier", "ParallelImaging")
    bundle.acceleration_csv.split(',').map((t) => t.trim()).filter(Boolean)
      .forEach((t) => accelTokens.add(t));
    // Dash-joined combined form as it appears in the title (e.g. "ParallelImaging-PartialFourier")
    accelTokens.add(bundle.acceleration_csv.replace(/,/g, '-'));
    return bundle.title
      .split('_')
      .filter((part) => !accelTokens.has(part))
      .join('_') || bundle.title;
  }, [bundle.title, bundle.acceleration_csv, includeAcceleration]);

  // Provenance styling matching the Viewer
  const PROVENANCE_STYLES: Record<string, { border: string; bg: string; label: string; color: string }> = {
    SyMRI:             { border: 'var(--mantine-color-violet-7)', bg: 'rgba(139, 92, 246, 0.08)', label: 'SyMRI', color: 'violet' },
    SWIRecon:          { border: 'var(--mantine-color-cyan-7)',   bg: 'rgba(34, 211, 238, 0.08)', label: 'SWI', color: 'cyan' },
    EPIMix:            { border: 'var(--mantine-color-pink-6)',   bg: 'rgba(236, 72, 153, 0.08)', label: 'EPIMix', color: 'pink' },
    ProjectionDerived: { border: 'var(--mantine-color-orange-7)', bg: 'rgba(251, 146, 60, 0.08)', label: 'Projections', color: 'orange' },
  };
  const provStyle = bundle.provenance ? PROVENANCE_STYLES[bundle.provenance] : undefined;

  const acqParams = [
    bundle.echo_time != null && `TE:${Math.round(bundle.echo_time * 100) / 100}`,
    bundle.repetition_time != null && `TR:${Math.round(bundle.repetition_time)}`,
    bundle.inversion_time != null && `TI:${Math.round(bundle.inversion_time)}`,
    bundle.flip_angle != null && `FA:${Math.round(bundle.flip_angle)}`,
  ].filter(Boolean).join(' ');

  return (
    <Paper
      withBorder
      p="sm"
      radius="md"
      style={provStyle ? {
        borderColor: provStyle.border,
        borderWidth: 2,
        background: provStyle.bg,
      } : undefined}
    >
      <Group justify="space-between" mb="xs" wrap="nowrap">
        <Group gap="xs" wrap="nowrap">
          <Text fw={600} ff="monospace" style={{ wordBreak: 'break-word' }}>
            {displayTitle}
          </Text>
          <Badge size="xs" color="gray" variant="light">
            {total} stack{total === 1 ? '' : 's'}
          </Badge>
          {bundle.directory_type && (
            <Badge size="xs" color="blue" variant="outline">
              {bundle.directory_type}
            </Badge>
          )}
          {provStyle && (
            <Badge size="xs" color={provStyle.color} variant="light">
              {provStyle.label}
            </Badge>
          )}
        </Group>
        {acqParams && (
          <Text size="xs" c="dimmed" ff="monospace" style={{ whiteSpace: 'nowrap' }}>
            {acqParams}
          </Text>
        )}
      </Group>

      <Stack gap="xs">
        <SimpleGrid cols={gridCols} spacing="xs" verticalSpacing="xs">
          {firstRow.map((s) => (
            <BundleStackTile
              key={s.series_stack_id}
              stack={s}
              progress={progress}
              onProgressChange={setProgress}
            />
          ))}
        </SimpleGrid>

        {hasOverflow && (
          <>
            <Collapse in={expanded}>
              <SimpleGrid cols={gridCols} spacing="xs" verticalSpacing="xs">
                {rest.map((s) => (
                  <BundleStackTile
                    key={s.series_stack_id}
                    stack={s}
                    progress={progress}
                    onProgressChange={setProgress}
                  />
                ))}
              </SimpleGrid>
            </Collapse>
            <Box>
              <Button
                variant="subtle"
                size="xs"
                leftSection={
                  expanded ? <IconChevronUp size={14} /> : <IconChevronDown size={14} />
                }
                onClick={() => setExpanded((v) => !v)}
              >
                {expanded ? 'Hide' : `Show ${rest.length} more`}
              </Button>
            </Box>
          </>
        )}
      </Stack>
    </Paper>
  );
};
