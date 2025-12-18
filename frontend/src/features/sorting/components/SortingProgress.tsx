/**
 * Sorting progress view - shows animated step progress during execution.
 * Dark theme compatible.
 */

import { Box, Group, Stack, Text } from '@mantine/core';
import { IconCheck, IconLoader2, IconAlertTriangle } from '@tabler/icons-react';
import { SORTING_STEPS } from '../types';
import { useSortingStream } from '../hooks/useSortingStream';
import { SortingStepCard } from './SortingStepCard';

interface SortingProgressProps {
  streamUrl: string | null;
  enabled?: boolean;
  onRerunStep?: (stepId: string) => void;
  rerunDisabled?: boolean;
}

export const SortingProgress = ({
  streamUrl,
  enabled = true,
  onRerunStep,
  rerunDisabled,
}: SortingProgressProps) => {
  const {
    steps,
    currentStep,
    isComplete,
    hasError,
    errorMessage,
    summary,
    isConnected,
  } = useSortingStream(streamUrl, enabled);

  return (
    <Stack gap="md">
      {/* Connection status - subtle header */}
      <Group gap="xs" justify="space-between">
        <Group gap="xs">
          {isConnected ? (
            <>
              <IconLoader2 
                size={14} 
                color="var(--nils-accent-primary)" 
                style={{ animation: 'spin 1s linear infinite' }}
              />
              <Text size="xs" c="var(--nils-text-tertiary)">
                Running sorting pipeline...
              </Text>
            </>
          ) : isComplete ? (
            <>
              <IconCheck size={14} color="var(--nils-success)" />
              <Text size="xs" c="var(--nils-success)">
                Pipeline complete
              </Text>
            </>
          ) : hasError ? (
            <>
              <IconAlertTriangle size={14} color="var(--nils-error)" />
              <Text size="xs" c="var(--nils-error)">
                Pipeline error
              </Text>
            </>
          ) : (
            <Text size="xs" c="var(--nils-text-tertiary)">
              Connecting...
            </Text>
          )}
        </Group>

        {/* Summary stats when complete */}
        {isComplete && summary && (
          <Text size="xs" c="var(--nils-text-secondary)">
            {summary.series_to_process.toLocaleString()} series ready • {summary.processing_mode.replace('_', ' ')}
          </Text>
        )}
      </Group>

      {/* Error message */}
      {hasError && errorMessage && (
        <Box
          p="sm"
          style={{
            backgroundColor: 'rgba(248, 81, 73, 0.1)',
            borderRadius: 'var(--nils-radius-md)',
            border: '1px solid rgba(248, 81, 73, 0.3)',
          }}
        >
          <Text size="sm" c="var(--nils-error)">
            {errorMessage}
          </Text>
        </Box>
      )}

      {/* Step cards */}
      <Stack gap="sm">
        {SORTING_STEPS.map((step, index) => (
          <SortingStepCard
            key={step.id}
            step={step}
            state={steps[step.id]}
            isActive={currentStep === step.id}
            index={index}
            onRerun={
              steps[step.id]?.status === 'complete' && onRerunStep
                ? () => onRerunStep(step.id)
                : undefined
            }
            rerunDisabled={rerunDisabled || isConnected}
          />
        ))}
      </Stack>

      {/* Completion summary */}
      {isComplete && summary && (
        <Box
          p="md"
          style={{
            backgroundColor: 'rgba(63, 185, 80, 0.08)',
            borderRadius: 'var(--nils-radius-md)',
            border: '1px solid rgba(63, 185, 80, 0.3)',
          }}
        >
          <Group justify="space-between" align="center">
            <Group gap="sm">
              <IconCheck size={20} color="var(--nils-success)" />
              <Text fw={600} c="var(--nils-success)">
                Step 1 Complete
              </Text>
            </Group>
            <Text size="sm" c="var(--nils-text-secondary)">
              {summary.series_to_process.toLocaleString()} series validated and ready for stack discovery
            </Text>
          </Group>
        </Box>
      )}

      {/* CSS for spinner animation */}
      <style>{`
        @keyframes spin {
          from { transform: rotate(0deg); }
          to { transform: rotate(360deg); }
        }
      `}</style>
    </Stack>
  );
};

export default SortingProgress;
