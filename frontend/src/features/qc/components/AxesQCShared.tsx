/**
 * Shared components and utilities for Axes QC functionality.
 * Used by both AxesQCPage and SingleStackQCModal.
 */

import {
  Box,
  Group,
  Stack,
  Text,
  Badge,
  Button,
  Tooltip,
} from '@mantine/core';
import {
  IconQuestionMark,
  IconAlertTriangle,
  IconTrendingDown,
  IconHelp,
  IconEye,
} from '@tabler/icons-react';
import type { AxesQCItem, AxisType, AxisFlagType, AxesQCDraftChanges } from '../types';

// ============================================================================
// Constants & Helpers
// ============================================================================

export function filterNonNull<T>(arr: (T | null | undefined)[]): T[] {
  return arr.filter((x): x is T => x != null);
}

export const AXIS_TO_COLUMN: Record<AxisType, string> = {
  base: 'base',
  technique: 'technique',
  modifier: 'modifier_csv',
  provenance: 'provenance',
  construct: 'construct_csv',
};

export const FLAG_ICONS: Record<AxisFlagType, React.ReactNode> = {
  missing: <IconQuestionMark size={12} />,
  conflict: <IconAlertTriangle size={12} />,
  low_confidence: <IconTrendingDown size={12} />,
  ambiguous: <IconHelp size={12} />,
  review: <IconEye size={12} />,
};

export const getBadgeVisual = (flag: AxisFlagType | null | undefined, hasValue: boolean) => {
  if (flag === 'missing') {
    return { variant: 'filled' as const, styles: { root: { backgroundColor: '#e53935', color: '#fff', border: 'none' } } };
  }
  if (flag === 'conflict') {
    return { variant: 'filled' as const, styles: { root: { backgroundColor: '#ff9800', color: '#1a1a1a', border: 'none' } } };
  }
  if (flag === 'low_confidence') {
    return { variant: 'light' as const, styles: { root: { backgroundColor: '#ffeaa7', color: '#1a1a1a' } } };
  }
  if (flag === 'ambiguous') {
    return { variant: 'light' as const, styles: { root: { backgroundColor: '#c792ea', color: '#1a1a1a' } } };
  }
  if (flag === 'review') {
    return { variant: 'light' as const, styles: { root: { backgroundColor: '#9e9e9e', color: '#1a1a1a' } } };
  }
  if (hasValue) {
    return { variant: 'filled' as const, styles: { root: { backgroundColor: '#2e7d32', color: '#fff' } } };
  }
  return { variant: 'light' as const, styles: { root: { backgroundColor: '#555', color: '#eee' } } };
};

// ============================================================================
// Styles
// ============================================================================

export const hudOverlayStyle: React.CSSProperties = {
  pointerEvents: 'auto',
  fontFamily: '"JetBrains Mono", "Fira Code", "SF Mono", "Consolas", monospace',
  letterSpacing: '0.02em',
  textShadow: '0 1px 3px rgba(0,0,0,0.9), 0 0 8px rgba(0,0,0,0.5)',
};

export const hudContainerStyle: React.CSSProperties = {
  position: 'absolute',
  zIndex: 10,
  display: 'flex',
  flexDirection: 'column',
  gap: 16,
};

export const rowContainerStyle: React.CSSProperties = {
  border: '1px solid rgba(255,255,255,0.12)',
  borderRadius: 8,
  padding: '10px 12px',
  background: 'rgba(0,0,0,0.4)',
};

// ============================================================================
// HUD Overlay Components - Semi-transparent info panels ON the image
// ============================================================================

/** Top-left: Acquisition parameters + image type */
export const AcquisitionHUD = ({ params, tags }: { params: AxesQCItem['params']; tags: AxesQCItem['tags'] }) => {
  const parts: string[] = [];
  if (params.modality) parts.push(params.modality);
  if (params.acq) parts.push(params.acq);

  const timings: string[] = [];
  if (params.te) timings.push(`TE:${params.te}`);
  if (params.tr) timings.push(`TR:${params.tr}`);
  if (params.ti) timings.push(`TI:${params.ti}`);
  if (params.fa) timings.push(`FA:${params.fa}°`);

  const imageTypeParts = tags.image_type?.split(/[\\\/]/).filter(Boolean) ?? [];

  return (
    <Box style={hudOverlayStyle}>
      <Tooltip label="Modality · Acquisition Type" position="right" withArrow>
        <Text size="13px" fw={700} c="#00ff88" style={{ cursor: 'help', textShadow: '0 0 8px rgba(0,255,136,0.3)' }}>
          {parts.join(' · ')}
        </Text>
      </Tooltip>
      {timings.length > 0 && (
        <Tooltip label="TE: Echo Time · TR: Repetition Time · TI: Inversion Time · FA: Flip Angle" position="right" withArrow>
          <Text size="13px" c="#e0e0e0" mt={3} style={{ cursor: 'help' }}>
            {timings.join(' · ')}
          </Text>
        </Tooltip>
      )}
      {imageTypeParts.length > 0 && (
        <Tooltip label="Image Type" position="right" withArrow>
          <Text size="12px" c="#888" mt={3} style={{ cursor: 'help' }}>
            {imageTypeParts.join(' · ')}
          </Text>
        </Tooltip>
      )}
    </Box>
  );
};

/** Bottom-left: Sequence/Protocol info */
export const SequenceHUD = ({ tags }: { tags: AxesQCItem['tags'] }) => {
  if (!tags.protocol && !tags.seq_name && !tags.description && !tags.scanning_seq) {
    return null;
  }

  const parseScanOptions = (opts: string | undefined): string[] => {
    if (!opts) return [];
    if (opts.startsWith('[')) {
      try {
        return JSON.parse(opts.replace(/'/g, '"'));
      } catch {
        // Fall through to normal parsing
      }
    }
    return opts.split(/[\\\/,]/).map(s => s.trim()).filter(Boolean);
  };

  const scanOptsList = parseScanOptions(tags.scan_options);

  return (
    <Box style={{ ...hudOverlayStyle, maxWidth: 300 }}>
      {tags.protocol && (
        <Tooltip label="Protocol Name" position="right" withArrow>
          <Text size="14px" fw={700} c="#ffcc00" truncate style={{ cursor: 'help', textShadow: '0 0 8px rgba(255,204,0,0.2)' }}>
            {tags.protocol}
          </Text>
        </Tooltip>
      )}
      {tags.description && tags.description !== tags.protocol && (
        <Tooltip label="Series Description" position="right" withArrow>
          <Text size="13px" c="#aaa" truncate mt={tags.protocol ? 3 : 0} style={{ cursor: 'help' }}>
            {tags.description}
          </Text>
        </Tooltip>
      )}
      {tags.seq_name && (
        <Tooltip label="Sequence Name" position="right" withArrow>
          <Text size="12px" c="#777" truncate mt={3} style={{ cursor: 'help' }}>
            {tags.seq_name}
          </Text>
        </Tooltip>
      )}
      {(tags.scanning_seq || tags.seq_variant || scanOptsList.length > 0) && (
        <Group gap={4} mt={6} wrap="wrap" style={{ maxWidth: 280 }}>
          {tags.scanning_seq && (
            <Tooltip label="Scanning Sequence" withArrow>
              <Badge size="xs" color="violet" variant="filled" style={{ cursor: 'help' }}>
                {tags.scanning_seq}
              </Badge>
            </Tooltip>
          )}
          {tags.seq_variant && (
            <Tooltip label="Sequence Variant" withArrow>
              <Badge size="xs" color="violet" variant="light" style={{ cursor: 'help' }}>
                {tags.seq_variant}
              </Badge>
            </Tooltip>
          )}
          {scanOptsList.map((opt, i) => (
            <Tooltip key={i} label="Scan Option" withArrow>
              <Badge size="xs" color="gray" variant="outline" style={{ cursor: 'help' }}>
                {opt.replace(/_GEMS|_SIEMENS|_PHILIPS/gi, '')}
              </Badge>
            </Tooltip>
          ))}
        </Group>
      )}
    </Box>
  );
};

/** Top-right: Current classification summary */
export const ClassificationHUD = ({
  current,
  intent,
  draftChanges,
}: {
  current: AxesQCItem['current'];
  intent: AxesQCItem['intent'];
  draftChanges?: AxesQCDraftChanges;
}) => {
  const getVal = (axis: AxisType): { value: string | null; isDraft: boolean } => {
    const col = AXIS_TO_COLUMN[axis];
    if (draftChanges && col in draftChanges) {
      return { value: draftChanges[col] ?? null, isDraft: true };
    }
    return { value: current[axis], isDraft: false };
  };

  const base = getVal('base');
  const tech = getVal('technique');
  const mod = getVal('modifier');
  const construct = getVal('construct');
  const provenance = getVal('provenance');

  return (
    <Box style={{ ...hudOverlayStyle, textAlign: 'right' }}>
      <Group gap={6} justify="flex-end">
        {intent.directory_type && (
          <Badge size="sm" color="blue" variant="filled" styles={{ root: { textTransform: 'uppercase', fontWeight: 700 } }}>
            {intent.directory_type}
          </Badge>
        )}
        {provenance.value && (
          <Badge 
            size="sm" 
            color={provenance.isDraft ? 'green' : 'cyan'} 
            variant="light" 
            styles={{ root: { textTransform: 'uppercase', fontWeight: 600 } }}
          >
            {provenance.value}
          </Badge>
        )}
        {intent.post_contrast === 1 && (
          <Badge size="sm" color="red" variant="light">Gd+</Badge>
        )}
        {intent.spinal_cord === 1 && (
          <Badge size="sm" color="orange" variant="light">Spinal</Badge>
        )}
      </Group>
      <Text 
        size="16px" 
        fw={700} 
        c={base.isDraft ? '#00ff88' : '#fff'} 
        mt={4}
        style={{ textShadow: base.isDraft ? '0 0 12px rgba(0,255,136,0.5)' : '0 0 10px rgba(255,255,255,0.3)' }}
      >
        {base.value || '—'} {tech.value ? `· ${tech.value}` : ''}
      </Text>
      {mod.value && (
        <Text size="14px" c={mod.isDraft ? '#00ff88' : '#ccc'} mt={2} fw={500}>
          {mod.value}
        </Text>
      )}
      {construct.value && (
        <Text size="12px" c={construct.isDraft ? '#00ff88' : '#aaa'} mt={2} fw={400}>
          {construct.value}
        </Text>
      )}
    </Box>
  );
};

/** Bottom-right: FOV display */
export const FovHUD = ({ params }: { params: AxesQCItem['params'] }) => {
  if (!params.fov) return null;
  
  return (
    <Tooltip label="Field of View" withArrow>
      <Box
        style={{
          backgroundColor: 'rgba(0,0,0,0.7)',
          padding: '4px 8px',
          borderRadius: 4,
        }}
      >
        <Text size="xs" c="dimmed" ff="monospace" style={{ cursor: 'help' }}>
          {params.fov} mm
        </Text>
      </Box>
    </Tooltip>
  );
};

// ============================================================================
// Flagged Axis Row - Compact horizontal selection
// ============================================================================

export interface FlaggedAxisRowProps {
  axis: AxisType;
  flag?: AxisFlagType | null;
  currentValue: string | null;
  draftValue: string | null | undefined;
  hasDraft: boolean;
  options: string[];
  metadata?: Record<string, { name: string; family: string }>;
  onSelect: (value: string | null) => void;
  isUpdating: boolean;
}

export const FlaggedAxisRow = ({
  axis,
  flag,
  currentValue,
  draftValue,
  hasDraft,
  options,
  metadata,
  onSelect,
  isUpdating,
}: FlaggedAxisRowProps) => {
  const selectedValue = hasDraft ? draftValue : currentValue;

  const axisLabel = axis.toUpperCase();
  const { variant: badgeVariant, styles: badgeStyles } = getBadgeVisual(flag, !!selectedValue);
  // Both modifier and construct support multi-select (comma-separated values)
  const isMultiSelect = axis === 'modifier' || axis === 'construct';


  const toCsv = (vals: string[]) => vals.filter(Boolean).join(',');
  const currentSet = new Set(
    (selectedValue || '')
      .split(',')
      .map((v) => v.trim())
      .filter(Boolean)
  );
  const toggleMultiSelect = (optId: string) => {
    const next = new Set(currentSet);
    if (next.has(optId)) next.delete(optId);
    else next.add(optId);
    const ordered = options.filter((o) => next.has(o));
    const csv = toCsv(ordered.length > 0 ? ordered : []);
    onSelect(csv || null);
  };


  // Technique: grouped by family using backend metadata (name + family)
  // Always use grouped rendering for technique to ensure name/ID consistency
  if (axis === 'technique') {
    // Build effective metadata: use provided metadata or create fallback from options
    const effectiveMetadata = metadata || options.reduce((acc, opt) => {
      acc[opt] = { name: opt, family: 'OTHER' };
      return acc;
    }, {} as Record<string, { name: string; family: string }>);

    const grouped = options.reduce((acc, opt) => {
      const meta = effectiveMetadata[opt];
      const family = meta?.family || 'OTHER';
      const name = meta?.name || opt;
      if (!acc[family]) acc[family] = [];
      acc[family].push({ id: name, name });
      return acc;
    }, {} as Record<string, { id: string; name: string }[]>);

    const familyOrderBase = ['SE', 'GRE', 'EPI', 'MIXED'];
    const otherFamilies = Object.keys(grouped)
      .filter((f) => !familyOrderBase.includes(f))
      .sort();
    const familyOrder = [...familyOrderBase, ...otherFamilies];

    return (
      <Box style={{ ...rowContainerStyle, width: '100%' }}>
        <Stack gap="xs" style={{ width: '100%' }}>
        <Group gap={4} style={{ flexShrink: 0, minWidth: 130 }}>
          <Badge
            size="sm"
            variant={badgeVariant}
            leftSection={flag ? FLAG_ICONS[flag] : undefined}
            styles={{ root: { textTransform: 'uppercase', fontWeight: 700, ...badgeStyles.root } }}
          >
            {axisLabel}
          </Badge>
          {hasDraft && (
            <Badge size="xs" color="green" variant="filled">
              ✓
            </Badge>
          )}
        </Group>

        <Stack gap={6}>
          {familyOrder.map((family) => {
            const items = grouped[family];
            if (!items || items.length === 0) return null;
            return (
              <Group key={family} gap={6} align="flex-start" wrap="wrap">
                <Text size="xs" c="dimmed" fw={600} style={{ width: 48, flexShrink: 0 }}>
                  {family}
                </Text>
                <Group gap={6} wrap="wrap" style={{ flex: 1 }}>
                  {items.map((opt) => {
                    const isSelected = isMultiSelect ? currentSet.has(opt.id) : selectedValue === opt.id;
                    const isDraftSelected = isMultiSelect
                      ? hasDraft && currentSet.has(opt.id)
                      : hasDraft && draftValue === opt.id;
                    return (
                      <Button
                        key={opt.id}
                        size="compact-xs"
                        variant={isSelected ? 'filled' : 'subtle'}
                        color={isDraftSelected ? 'green' : isSelected ? 'blue' : 'gray'}
                        onClick={() => (isMultiSelect ? toggleMultiSelect(opt.id) : onSelect(opt.id))}
                        disabled={isUpdating}
                        styles={{
                          root: {
                            padding: '3px 8px',
                            height: 'auto',
                            fontWeight: isSelected ? 600 : 400,
                            flexShrink: 0,
                          },
                        }}
                      >
                        {opt.name}
                      </Button>
                    );
                  })}
                </Group>
              </Group>
            );
          })}
        </Stack>
        </Stack>
      </Box>
    );
  }

  return (
    <Box style={{ ...rowContainerStyle, width: '100%' }}>
      <Group gap="sm" wrap="wrap" style={{ minWidth: 0 }}>
        {/* Axis label + flag */}
        <Group gap={4} style={{ flexShrink: 0, minWidth: 130 }}>
          <Badge
            size="sm"
            variant={badgeVariant}
            leftSection={flag ? FLAG_ICONS[flag] : undefined}
            styles={{ root: { textTransform: 'uppercase', fontWeight: 700, ...badgeStyles.root } }}
          >
            {axisLabel}
          </Badge>
          {hasDraft && (
            <Badge size="xs" color="green" variant="filled">
              ✓
            </Badge>
          )}
        </Group>

        {/* Options - wrap, no scroll */}
        <Group gap={6} wrap="wrap" style={{ flex: 1 }}>
          {options.map((opt) => {
            const isSelected = isMultiSelect ? currentSet.has(opt) : selectedValue === opt;
            const isDraftSelected = isMultiSelect 
              ? hasDraft && currentSet.has(opt) 
              : hasDraft && draftValue === opt;

            return (
              <Button
                key={opt}
                size="compact-xs"
                variant={isSelected ? 'filled' : 'subtle'}
                color={isDraftSelected ? 'green' : isSelected ? 'blue' : 'gray'}
                onClick={() => (isMultiSelect ? toggleMultiSelect(opt) : onSelect(opt))}
                disabled={isUpdating}
                styles={{
                  root: {
                    padding: '3px 8px',
                    height: 'auto',
                    fontWeight: isSelected ? 600 : 400,
                    flexShrink: 0,
                  },
                }}
              >
                {opt}
              </Button>
            );
          })}
        </Group>
      </Group>
    </Box>
  );
};