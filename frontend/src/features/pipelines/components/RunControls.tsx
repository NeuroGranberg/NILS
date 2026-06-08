/**
 * RunControls - assembles a CreateRunBody from the selected input, the
 * schema-form params, and the run-level controls (name, output_path,
 * retain_input), then launches the run via useCreateRun.
 *
 * The Launch button is disabled unless the schema form is valid AND the input
 * selection is complete (stack_ids for db_subset, a non-empty path for
 * external_path). `config` is left undefined in v1 (materialization config is
 * not user-exposed yet; the backend tolerates null).
 */
import { useState } from 'react';
import { Button, Group, Stack, Switch, TextInput } from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { SectionCard } from '../../shared/components/SectionCard';
import { useCreateRun } from '../api';
import type { CreateRunBody, FormValues, InputSource } from '../types';

interface RunControlsProps {
  pipelineId: number;
  input: { input_source: InputSource; stack_ids?: number[]; external_path?: string };
  params: FormValues;
  formValid: boolean;
  onLaunched: (runId: number) => void;
}

export const RunControls = ({
  pipelineId,
  input,
  params,
  formValid,
  onLaunched,
}: RunControlsProps) => {
  const createRun = useCreateRun(pipelineId);
  const [name, setName] = useState('');
  const [outputPath, setOutputPath] = useState('');
  const [retainInput, setRetainInput] = useState(true);

  const inputReady =
    input.input_source === 'db_subset'
      ? (input.stack_ids?.length ?? 0) > 0
      : (input.external_path?.trim().length ?? 0) > 0;

  const canLaunch = formValid && inputReady && !createRun.isPending;

  const handleLaunch = () => {
    const body: CreateRunBody = {
      input_source: input.input_source,
      params,
      retain_input: retainInput,
    };
    if (input.input_source === 'db_subset') {
      body.stack_ids = input.stack_ids;
    } else {
      body.external_path = input.external_path?.trim();
    }
    if (outputPath.trim()) body.output_path = outputPath.trim();
    if (name.trim()) body.name = name.trim();

    createRun.mutate(body, {
      onSuccess: ({ run }) => {
        notifications.show({ color: 'green', message: `Run #${run.id} launched` });
        onLaunched(run.id);
      },
      onError: (error) => {
        notifications.show({ color: 'red', message: (error as Error).message });
      },
    });
  };

  return (
    <SectionCard title="Launch" description="Name the run and choose where outputs go.">
      <Stack gap="md">
        <TextInput
          label="Run name"
          description="Optional label for this run."
          placeholder="e.g. fmriprep nightly"
          value={name}
          onChange={(e) => setName(e.currentTarget.value)}
        />
        <TextInput
          label="Output path"
          description="Optional absolute path for run outputs. Leave blank to use the default."
          placeholder="/data/derivatives/pipeline-out"
          value={outputPath}
          onChange={(e) => setOutputPath(e.currentTarget.value)}
        />
        <Switch
          label="Retain materialized input"
          description="Keep the materialized input dataset on disk after the run completes."
          checked={retainInput}
          onChange={(e) => setRetainInput(e.currentTarget.checked)}
        />
        <Group justify="flex-end">
          <Button
            onClick={handleLaunch}
            disabled={!canLaunch}
            loading={createRun.isPending}
            style={{
              backgroundColor: 'var(--nils-accent-primary)',
              color: 'var(--nils-bg-primary)',
            }}
          >
            Launch run
          </Button>
        </Group>
      </Stack>
    </SectionCard>
  );
};
