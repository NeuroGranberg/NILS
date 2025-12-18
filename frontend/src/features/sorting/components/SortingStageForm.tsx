/**
 * Sorting stage - shows step overview before running.
 * Clean, dark-theme compatible, no clutter.
 */

import { Box, Button, Group, Stack, Switch, Text } from '@mantine/core';
import { IconPlayerPlay } from '@tabler/icons-react';
import { SORTING_STEPS } from '../types';
import type { SortingConfig } from '../types';

interface SortingStageFormProps {
  config: SortingConfig;
  onChange: (config: SortingConfig) => void;
  onRun: () => void;
  disabled?: boolean;
  isRunning?: boolean;
}

const stepIcons: Record<string, string> = {
  checkup: '🔍',
  stack_fingerprint: '🔬',
  classification: '🏷️',
  unknown_resolution: '❓',
};

export const SortingStageForm = ({
  config,
  onChange,
  onRun,
  disabled,
  isRunning,
}: SortingStageFormProps) => {
  return (
    <Stack gap="lg">
      {/* Steps overview - static before running */}
      <Stack gap="xs">
        {SORTING_STEPS.map((step, index) => (
          <Box
            key={step.id}
            p="sm"
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 'var(--nils-space-sm)',
              backgroundColor: 'var(--nils-bg-tertiary)',
              borderRadius: 'var(--nils-radius-md)',
              border: '1px solid var(--nils-border-subtle)',
            }}
          >
            {/* Step number */}
            <Box
              style={{
                width: 28,
                height: 28,
                borderRadius: '50%',
                backgroundColor: 'var(--nils-bg-elevated)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                flexShrink: 0,
              }}
            >
              <Text size="sm" fw={600} c="var(--nils-text-secondary)">
                {index + 1}
              </Text>
            </Box>

            {/* Step info */}
            <Box style={{ flex: 1, minWidth: 0 }}>
              <Text size="sm" fw={500} c="var(--nils-text-primary)">
                {step.title}
              </Text>
              <Text size="xs" c="var(--nils-text-tertiary)">
                {step.description}
              </Text>
            </Box>

            {/* Icon */}
            <Text size="lg" style={{ flexShrink: 0 }}>
              {stepIcons[step.id] || '📋'}
            </Text>
          </Box>
        ))}
      </Stack>

      {/* Simple config toggle */}
      <Box
        p="md"
        style={{
          backgroundColor: 'var(--nils-bg-tertiary)',
          borderRadius: 'var(--nils-radius-md)',
          border: '1px solid var(--nils-border-subtle)',
        }}
      >
        <Switch
          label="Skip already classified series"
          description="Incremental mode - only process new series"
          checked={config.skipClassified}
          onChange={(e) =>
            onChange({ ...config, skipClassified: e.currentTarget.checked })
          }
          disabled={disabled}
          styles={{
            label: { color: 'var(--nils-text-primary)' },
            description: { color: 'var(--nils-text-tertiary)' },
          }}
        />
      </Box>

      {/* Run button */}
      <Group justify="flex-end">
        <Button
          size="md"
          leftSection={<IconPlayerPlay size={18} />}
          onClick={onRun}
          disabled={disabled || isRunning}
          loading={isRunning}
          style={{
            backgroundColor: disabled || isRunning 
              ? 'var(--nils-bg-elevated)' 
              : 'var(--nils-accent-primary)',
            color: disabled || isRunning 
              ? 'var(--nils-text-tertiary)' 
              : 'var(--nils-bg-primary)',
          }}
        >
          Run Sorting Pipeline
        </Button>
      </Group>
    </Stack>
  );
};

export default SortingStageForm;
