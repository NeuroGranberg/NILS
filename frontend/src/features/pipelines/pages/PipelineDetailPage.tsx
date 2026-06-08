/**
 * PipelineDetailPage (Surface 2) - the activated pipeline detail view.
 *
 * Composes, in order:
 *   1. Header (name/description/version/needs badges + input/output contract)
 *   2. Input selection (db_subset via the export resolve flow OR external_path)
 *   3. Schema-driven config form (SchemaForm bound to descriptor.inputs/groups)
 *   4. Run controls (name/output_path/retain_input -> useCreateRun, 202)
 *   5. Runs list (session-scoped) + selected RunDetailPanel
 *
 * Does NOT edit app/routes or app/layout (owned by the Routing phase).
 */
import { useMemo, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import {
  Anchor,
  Badge,
  Box,
  Code,
  Group,
  Loader,
  Stack,
  Text,
} from '@mantine/core';
import { IconArrowLeft } from '@tabler/icons-react';
import { SectionCard } from '../../shared/components/SectionCard';
import { usePipeline } from '../api';
import { SchemaForm } from '../components/SchemaForm';
import { RunInputSelection, type RunInputSelectionValue } from '../components/RunInputSelection';
import { RunControls } from '../components/RunControls';
import { RunsList } from '../components/RunsList';
import { RunDetailPanel } from '../components/RunDetailPanel';
import type { FormValues, PipelineDTO } from '../types';

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
  const cleaned = digest.includes(':') ? digest.split(':').pop() ?? digest : digest;
  return cleaned.slice(0, 12);
};

export const PipelineDetailPage = () => {
  const { id } = useParams<{ id: string }>();
  const pipelineId = Number(id);
  const { data: pipeline, isLoading } = usePipeline(pipelineId);

  const [input, setInput] = useState<RunInputSelectionValue>({ input_source: 'db_subset' });
  const [params, setParams] = useState<FormValues>({});
  const [formValid, setFormValid] = useState(true);
  const [selectedRunId, setSelectedRunId] = useState<number | undefined>(undefined);

  const needChips = useMemo(() => (pipeline ? buildNeedChips(pipeline) : []), [pipeline]);

  if (isLoading) {
    return (
      <Box
        py="xl"
        style={{ display: 'flex', justifyContent: 'center', minHeight: 200 }}
      >
        <Loader size="md" color="var(--nils-accent-primary)" />
      </Box>
    );
  }

  if (!pipeline) {
    return (
      <Stack gap="md" p="md">
        <Anchor component={Link} to="/pipelines" size="sm">
          <Group gap={4}>
            <IconArrowLeft size={14} />
            Back to pipelines
          </Group>
        </Anchor>
        <Text c="dimmed">Pipeline not found.</Text>
      </Stack>
    );
  }

  const descriptor = pipeline.descriptor;
  const commandLine = descriptor['command-line'];
  const xnils = descriptor['x-nils'];
  const digest = shortDigest(pipeline.image_digest);

  const handleLaunched = (runId: number) => {
    // The runs list refreshes via useCreateRun's invalidation; just select it.
    setSelectedRunId(runId);
  };

  return (
    <Stack gap="lg" p="md">
      {/* Back link */}
      <Anchor component={Link} to="/pipelines" size="sm" c="var(--nils-text-secondary)">
        <Group gap={4}>
          <IconArrowLeft size={14} />
          Back to pipelines
        </Group>
      </Anchor>

      {/* 1. Header */}
      <SectionCard title={pipeline.name} description={pipeline.description ?? pipeline.slug}>
        <Stack gap="sm">
          <Group gap={6}>
            <Badge variant="light" color="blue">
              {pipeline.work_unit}
            </Badge>
            <Badge variant="light" color="grape">
              {pipeline.analysis_level}
            </Badge>
            <Badge variant="light" color="gray">
              schema {pipeline.schema_version}
            </Badge>
            <Badge variant="outline" color="gray">
              v{pipeline.version}
            </Badge>
          </Group>

          <Group gap={12}>
            <Text size="xs" c="dimmed" ff="monospace">
              repo {pipeline.descriptor['tool-version'] ?? '—'}
            </Text>
            <Text size="xs" c="dimmed" ff="monospace">
              {digest ? `image ${digest}` : 'no image'}
            </Text>
          </Group>

          {needChips.length > 0 && (
            <Group gap={6}>
              {needChips.map((chip) => (
                <Badge key={chip.key} variant="outline" size="xs" color="gray">
                  {chip.label}
                </Badge>
              ))}
            </Group>
          )}

          {xnils?.input && (
            <Text size="xs" c="dimmed">
              Input contract: {xnils.input.formats.join(', ')} ({xnils.input.layout} layout)
            </Text>
          )}

          {descriptor['output-files'] && descriptor['output-files'].length > 0 && (
            <Text size="xs" c="dimmed">
              Output contract:{' '}
              {descriptor['output-files'].map((o) => o.name).join(', ')}
            </Text>
          )}

          {commandLine && (
            <Code block style={{ fontSize: 'var(--mantine-font-size-xs)' }}>
              {commandLine}
            </Code>
          )}
        </Stack>
      </SectionCard>

      {/* 2. Input selection */}
      <RunInputSelection value={input} onChange={setInput} />

      {/* 3. Config form */}
      <SectionCard
        title="Parameters"
        description="Configure the pipeline's descriptor inputs."
      >
        <SchemaForm
          inputs={descriptor.inputs ?? []}
          groups={descriptor.groups}
          value={params}
          onChange={setParams}
          onValidityChange={setFormValid}
        />
      </SectionCard>

      {/* 4. Run controls */}
      <RunControls
        pipelineId={pipeline.id}
        input={input}
        params={params}
        formValid={formValid}
        onLaunched={handleLaunched}
      />

      {/* 5. Runs list */}
      <SectionCard title="Runs" description="All runs for this pipeline, newest first.">
        <RunsList
          pipelineId={pipeline.id}
          onSelect={setSelectedRunId}
          selectedRunId={selectedRunId}
        />
      </SectionCard>

      {/* 6. Run detail */}
      {selectedRunId != null && (
        <RunDetailPanel key={selectedRunId} runId={selectedRunId} />
      )}
    </Stack>
  );
};

export default PipelineDetailPage;
