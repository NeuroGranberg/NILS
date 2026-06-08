/**
 * RunInputSelection - Surface-2 input picker. Lets the user choose between:
 *   - db_subset: resolve a selection manifest (REUSING the export ManifestInput +
 *     useResolveManifest -> resolved_stack_ids), shown with a small LOCAL summary.
 *     We deliberately do NOT reuse export's ManifestPreview: it renders an
 *     export-specific "Subject Naming" control and fires export-only metadata
 *     requests (useMetadataIdTypes / useSubjectIdentifiers) that are irrelevant to
 *     picking stacks for a pipeline run.
 *   - external_path: a single path TextInput.
 */
import { useState } from 'react';
import { Alert, Group, SegmentedControl, Stack, Text, TextInput } from '@mantine/core';
import { SectionCard } from '../../shared/components/SectionCard';
import { ManifestInput } from '../../export/components/ManifestInput';
import { useResolveManifest } from '../../export/api';
import type { ResolveResponse } from '../../export/types';
import type { InputSource } from '../types';

export interface RunInputSelectionValue {
  input_source: InputSource;
  stack_ids?: number[];
  external_path?: string;
}

interface RunInputSelectionProps {
  value: RunInputSelectionValue;
  onChange: (v: RunInputSelectionValue) => void;
}

const ResolveSummary = ({ result }: { result: ResolveResponse }) => (
  <Stack gap="xs">
    <Group gap="lg">
      <Text size="sm">
        <b>{result.resolved_stack_ids.length}</b> stacks
      </Text>
      <Text size="sm">
        <b>{result.total_subjects}</b> subjects
      </Text>
      <Text size="sm">
        <b>{result.total_sessions}</b> sessions
      </Text>
      {result.detected_cohorts.length > 0 && (
        <Text size="sm" c="dimmed">
          cohorts: {result.detected_cohorts.join(', ')}
        </Text>
      )}
    </Group>
    {result.warnings.length > 0 && (
      <Alert color="yellow" variant="light" title={`${result.warnings.length} warning(s)`}>
        <Stack gap={2}>
          {result.warnings.slice(0, 5).map((w, i) => (
            <Text key={`warn-${i}`} size="xs">
              {w}
            </Text>
          ))}
        </Stack>
      </Alert>
    )}
  </Stack>
);

export const RunInputSelection = ({ value, onChange }: RunInputSelectionProps) => {
  const resolve = useResolveManifest();
  const [resolved, setResolved] = useState<ResolveResponse | null>(null);

  const handleSourceChange = (next: string) => {
    const input_source = next as InputSource;
    if (input_source === 'db_subset') {
      onChange({ input_source, stack_ids: value.stack_ids });
    } else {
      onChange({ input_source, external_path: value.external_path });
    }
  };

  // Matches the export flow's resolve usage: ManifestInput applies any CSV column
  // mapping to the text before calling onResolve, so the resolved content is the
  // single source of truth here too.
  const handleResolve = (content: string) => {
    resolve.mutate(content, {
      onSuccess: (data) => {
        setResolved(data);
        onChange({ input_source: 'db_subset', stack_ids: data.resolved_stack_ids });
      },
    });
  };

  return (
    <SectionCard title="Input selection" description="Choose the data this run operates on.">
      <Stack gap="md">
        <SegmentedControl
          value={value.input_source}
          onChange={handleSourceChange}
          data={[
            { value: 'db_subset', label: 'Database subset' },
            { value: 'external_path', label: 'External path' },
          ]}
        />

        {value.input_source === 'db_subset' && (
          <Stack gap="md">
            <ManifestInput onResolve={handleResolve} isResolving={resolve.isPending} />
            {resolved && <ResolveSummary result={resolved} />}
          </Stack>
        )}

        {value.input_source === 'external_path' && (
          <TextInput
            label="External path"
            description="Absolute path to an existing dataset on the server."
            placeholder="/data/derivatives/my-dataset"
            value={value.external_path ?? ''}
            onChange={(e) =>
              onChange({
                input_source: 'external_path',
                external_path: e.currentTarget.value,
              })
            }
          />
        )}
      </Stack>
    </SectionCard>
  );
};
