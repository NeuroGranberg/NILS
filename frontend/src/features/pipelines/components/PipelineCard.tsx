/**
 * PipelineCard - displays an analysis-pipeline summary in the inventory grid.
 *
 * Mirrors the Mantine Card + Link structure of cohorts/CohortCard, re-implemented
 * for PipelineDTO. Memoized to avoid re-renders when sibling cards change.
 */
import { memo } from 'react';
import { Badge, Group, Card, Stack, Text, Tooltip } from '@mantine/core';
import { Link } from 'react-router-dom';
import type { PipelineDTO } from '../types';
import { formatDateTime } from '../../../utils/formatters';

interface PipelineCardProps {
  pipeline: PipelineDTO;
}

interface NeedChip {
  key: string;
  label: string;
}

const buildNeedChips = (pipeline: PipelineDTO): NeedChip[] => {
  const needs = pipeline.descriptor['x-nils']?.needs;
  if (!needs) return [];
  const chips: NeedChip[] = [];
  if (needs.gpu) chips.push({ key: 'gpu', label: 'GPU' });
  if (needs.fs_license) chips.push({ key: 'fs_license', label: 'FS license' });
  if (needs.templateflow) chips.push({ key: 'templateflow', label: 'TemplateFlow' });
  if (needs.work_dir) chips.push({ key: 'work_dir', label: 'Work dir' });
  return chips;
};

const shortDigest = (digest?: string | null): string | null => {
  if (!digest) return null;
  // image_digest may be a sha256:... ref; show a short slice for identification.
  const cleaned = digest.includes(':') ? digest.split(':').pop() ?? digest : digest;
  return cleaned.slice(0, 12);
};

const PipelineCardInner = ({ pipeline }: PipelineCardProps) => {
  const needChips = buildNeedChips(pipeline);
  const digest = shortDigest(pipeline.image_digest);

  return (
    <Card
      component={Link}
      to={`/pipelines/${pipeline.id}`}
      padding="lg"
      style={{
        backgroundColor: 'var(--nils-bg-secondary)',
        border: '1px solid var(--nils-border-subtle)',
        borderRadius: 'var(--nils-radius-lg)',
        textDecoration: 'none',
        transition: 'all 150ms ease',
        cursor: 'pointer',
      }}
      styles={{
        root: {
          '&:hover': {
            borderColor: 'var(--nils-border)',
            transform: 'translateY(-2px)',
          },
        },
      }}
    >
      <Stack gap="sm">
        {/* Header */}
        <Stack gap={2} style={{ minWidth: 0 }}>
          <Text fw={600} size="md" c="var(--nils-text-primary)" truncate>
            {pipeline.name}
          </Text>
          <Text size="xs" c="var(--nils-text-tertiary)" lineClamp={2}>
            {pipeline.description || pipeline.slug}
          </Text>
        </Stack>

        {/* Classification badges */}
        <Group gap={6}>
          <Badge
            variant="light"
            size="xs"
            color="blue"
            styles={{ root: { backgroundColor: 'var(--nils-bg-tertiary)' } }}
          >
            {pipeline.work_unit}
          </Badge>
          <Badge
            variant="light"
            size="xs"
            color="grape"
            styles={{ root: { backgroundColor: 'var(--nils-bg-tertiary)' } }}
          >
            {pipeline.analysis_level}
          </Badge>
        </Group>

        {/* Needs chips */}
        {needChips.length > 0 && (
          <Group gap={6}>
            {needChips.map((chip) => (
              <Badge key={chip.key} variant="outline" size="xs" color="gray">
                {chip.label}
              </Badge>
            ))}
          </Group>
        )}

        {/* Image digest */}
        <Tooltip label={pipeline.image_digest || 'No container image'} disabled={!digest}>
          <Text size="xs" c="var(--nils-text-tertiary)" ff="monospace" truncate>
            {digest ? digest : 'no image'}
          </Text>
        </Tooltip>

        {/* Footer */}
        <Group
          justify="space-between"
          pt={4}
          style={{ borderTop: '1px solid var(--nils-border-subtle)' }}
        >
          <Text size="xs" c="var(--nils-text-tertiary)">
            v{pipeline.version}
          </Text>
          <Text size="xs" c="var(--nils-text-tertiary)">
            {formatDateTime(pipeline.updated_at)}
          </Text>
        </Group>
      </Stack>
    </Card>
  );
};

export const PipelineCard = memo(PipelineCardInner);
PipelineCard.displayName = 'PipelineCard';
