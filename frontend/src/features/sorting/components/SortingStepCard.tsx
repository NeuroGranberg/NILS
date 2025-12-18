/**
 * Individual step card for sorting pipeline.
 * Dark theme compatible, animated progress.
 */

import { Box, Collapse, Group, Loader, Progress, SimpleGrid, Stack, Text, Tooltip } from '@mantine/core';
import { IconCheck, IconX } from '@tabler/icons-react';
import type { SortingStep, StepState, Step1Metrics, Step3Metrics, Step4Metrics, SortingStepStatus } from '../types';

interface SortingStepCardProps {
  step: SortingStep;
  state: StepState | undefined;
  isActive: boolean;
  index: number;
  onRerun?: () => void;
  rerunDisabled?: boolean;
}

const stepIcons: Record<string, string> = {
  checkup: '🔍',
  stack_fingerprint: '🔬',
  classification: '🏷️',
  completion: '✅',
  unknown_resolution: '❓',
};

const Step1MetricsGrid = ({ metrics }: { metrics: Step1Metrics }) => {
  const entries = [
    { label: 'Subjects', value: metrics.subjects_in_cohort },
    { label: 'Studies', value: metrics.total_studies },
    { label: 'Valid', value: metrics.studies_with_valid_date },
    { label: 'Imputed', value: metrics.studies_date_imputed },
    { label: 'Series', value: metrics.total_series },
    { label: 'To Process', value: metrics.series_to_process_count, highlight: true },
  ];

  return (
    <SimpleGrid cols={{ base: 3, sm: 6 }} spacing="sm">
      {entries.map((entry) => (
        <Box key={entry.label} style={{ textAlign: 'center' }}>
          <Text 
            size="lg" 
            fw={700} 
            c={entry.highlight ? 'var(--nils-accent-primary)' : 'var(--nils-text-primary)'}
          >
            {entry.value?.toLocaleString() ?? '-'}
          </Text>
          <Text size="xs" c="var(--nils-text-tertiary)">
            {entry.label}
          </Text>
        </Box>
      ))}
    </SimpleGrid>
  );
};

const Step3MetricsGrid = ({ metrics }: { metrics: Step3Metrics }) => {
  const entries = [
    { label: 'Classified', value: metrics.total_classified, highlight: true },
    { label: 'Excluded', value: metrics.excluded_count },
    { label: 'Review', value: metrics.review_required_count, warning: metrics.review_required_count > 0 },
    { label: 'Localizers', value: metrics.localizer_count },
    { label: 'Contrast', value: metrics.post_contrast_count },
    { label: 'Spine', value: metrics.spine_detected_count },
  ];

  return (
    <SimpleGrid cols={{ base: 3, sm: 6 }} spacing="sm">
      {entries.map((entry) => (
        <Box key={entry.label} style={{ textAlign: 'center' }}>
          <Text 
            size="lg" 
            fw={700} 
            c={entry.highlight 
              ? 'var(--nils-accent-primary)' 
              : entry.warning 
                ? 'var(--nils-warning)' 
                : 'var(--nils-text-primary)'}
          >
            {entry.value?.toLocaleString() ?? '-'}
          </Text>
          <Text size="xs" c="var(--nils-text-tertiary)">
            {entry.label}
          </Text>
        </Box>
      ))}
    </SimpleGrid>
  );
};

const Step4MetricsGrid = ({ metrics }: { metrics: Step4Metrics }) => {
  const entries = [
    { label: 'Processed', value: metrics.total_processed, highlight: true },
    { label: 'Base Filled', value: metrics.base_filled_count },
    { label: 'Tech Filled', value: metrics.technique_filled_count },
    { label: 'Misc Resolved', value: metrics.misc_resolved_count },
    { label: 'No Match', value: metrics.stacks_with_no_match, warning: metrics.stacks_with_no_match > 0 },
    { label: 'Flagged', value: metrics.stacks_newly_flagged, warning: metrics.stacks_newly_flagged > 0 },
  ];

  return (
    <SimpleGrid cols={{ base: 3, sm: 6 }} spacing="sm">
      {entries.map((entry) => (
        <Box key={entry.label} style={{ textAlign: 'center' }}>
          <Text 
            size="lg" 
            fw={700} 
            c={entry.highlight 
              ? 'var(--nils-accent-primary)' 
              : entry.warning 
                ? 'var(--nils-warning)' 
                : 'var(--nils-text-primary)'}
          >
            {entry.value?.toLocaleString() ?? '-'}
          </Text>
          <Text size="xs" c="var(--nils-text-tertiary)">
            {entry.label}
          </Text>
        </Box>
      ))}
    </SimpleGrid>
  );
};

export const SortingStepCard = ({
  step,
  state,
  isActive,
  index,
  onRerun,
  rerunDisabled,
}: SortingStepCardProps) => {
  const status: SortingStepStatus = state?.status || 'pending';
  const progress = state?.progress ?? 0;
  const message = state?.message;
  const metrics = state?.metrics;

  const isComplete = status === 'complete';
  const isRunning = status === 'running';
  const hasError = status === 'error';
  const isPending = status === 'pending';

  const isStep1Metrics = metrics && 'subjects_in_cohort' in metrics;
  const isStep3Metrics = metrics && 'total_classified' in metrics;
  const isStep4Metrics = metrics && 'base_filled_count' in metrics;

  // Background color based on state
  const getBgColor = () => {
    if (isActive) return 'var(--nils-bg-elevated)';
    if (isComplete) return 'rgba(63, 185, 80, 0.08)';
    if (hasError) return 'rgba(248, 81, 73, 0.08)';
    return 'var(--nils-bg-tertiary)';
  };

  // Border color based on state
  const getBorderColor = () => {
    if (isActive) return 'var(--nils-accent-primary)';
    if (isComplete) return 'var(--nils-success)';
    if (hasError) return 'var(--nils-error)';
    return 'var(--nils-border-subtle)';
  };

  return (
    <Box
      p="md"
      style={{
        backgroundColor: getBgColor(),
        borderRadius: 'var(--nils-radius-md)',
        border: `1px solid ${getBorderColor()}`,
        transition: 'all 0.3s ease',
      }}
    >
      <Group justify="space-between" wrap="nowrap">
        {/* Left: Status icon + Step info */}
        <Group gap="md" wrap="nowrap" style={{ flex: 1, minWidth: 0 }}>
          {/* Status indicator */}
          <Box
            style={{
              width: 36,
              height: 36,
              borderRadius: '50%',
              backgroundColor: isActive 
                ? 'rgba(88, 166, 255, 0.15)' 
                : isComplete 
                  ? 'rgba(63, 185, 80, 0.15)'
                  : hasError
                    ? 'rgba(248, 81, 73, 0.15)'
                    : 'var(--nils-bg-elevated)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              flexShrink: 0,
              cursor: isComplete && onRerun && !rerunDisabled ? 'pointer' : 'default',
            }}
            onClick={isComplete && onRerun && !rerunDisabled ? onRerun : undefined}
          >
            {isRunning ? (
              <Loader size={18} color="var(--nils-accent-primary)" />
            ) : isComplete ? (
              <Tooltip label={onRerun ? "Click to re-run" : "Complete"}>
                <IconCheck size={18} color="var(--nils-success)" />
              </Tooltip>
            ) : hasError ? (
              <IconX size={18} color="var(--nils-error)" />
            ) : (
              <Text size="sm" fw={600} c="var(--nils-text-tertiary)">
                {index + 1}
              </Text>
            )}
          </Box>

          {/* Step title and description */}
          <Box style={{ flex: 1, minWidth: 0 }}>
            <Group gap="xs" wrap="nowrap">
              <Text size="sm" fw={600} c="var(--nils-text-primary)">
                {step.title}
              </Text>
              <Text>{stepIcons[step.id]}</Text>
            </Group>
            <Text size="xs" c="var(--nils-text-tertiary)" lineClamp={1}>
              {isRunning && message ? message : step.description}
            </Text>
          </Box>
        </Group>

        {/* Right: Progress bar or status */}
        <Box style={{ flexShrink: 0, width: 140 }}>
          {isRunning && (
            <Stack gap={4} align="flex-end">
              <Progress 
                value={progress} 
                size="sm" 
                w="100%" 
                radius="md" 
                animated
                color="var(--nils-accent-primary)"
                style={{ backgroundColor: 'var(--nils-bg-tertiary)' }}
              />
              <Text size="xs" c="var(--nils-text-tertiary)">
                {progress}%
              </Text>
            </Stack>
          )}
          {isComplete && !isActive && (
            <Text size="xs" fw={500} c="var(--nils-success)" ta="right">
              ✓ Complete
            </Text>
          )}
          {hasError && (
            <Text size="xs" fw={500} c="var(--nils-error)" ta="right">
              ✗ Failed
            </Text>
          )}
          {isPending && !isActive && (
            <Text size="xs" c="var(--nils-text-tertiary)" ta="right">
              Waiting...
            </Text>
          )}
        </Box>
      </Group>

      {/* Expanded metrics when active or complete - Step 1 */}
      <Collapse in={isActive && !!isStep1Metrics}>
        <Box 
          mt="md" 
          pt="md" 
          style={{ borderTop: '1px solid var(--nils-border-subtle)' }}
        >
          <Step1MetricsGrid metrics={metrics as Step1Metrics} />
        </Box>
      </Collapse>

      {/* Expanded metrics when active or complete - Step 3 */}
      <Collapse in={isActive && !!isStep3Metrics}>
        <Box 
          mt="md" 
          pt="md" 
          style={{ borderTop: '1px solid var(--nils-border-subtle)' }}
        >
          <Step3MetricsGrid metrics={metrics as Step3Metrics} />
        </Box>
      </Collapse>

      {/* Expanded metrics when active or complete - Step 4 */}
      <Collapse in={isActive && !!isStep4Metrics}>
        <Box 
          mt="md" 
          pt="md" 
          style={{ borderTop: '1px solid var(--nils-border-subtle)' }}
        >
          <Step4MetricsGrid metrics={metrics as Step4Metrics} />
        </Box>
      </Collapse>

      {/* Show warnings/errors for Step 1 */}
      {isStep1Metrics && (metrics as Step1Metrics).warnings?.length ? (
        <Box 
          mt="sm" 
          p="xs" 
          style={{ 
            backgroundColor: 'rgba(210, 153, 34, 0.1)', 
            borderRadius: 'var(--nils-radius-sm)',
            border: '1px solid rgba(210, 153, 34, 0.3)',
          }}
        >
          {(metrics as Step1Metrics).warnings!.map((w, i) => (
            <Text key={i} size="xs" c="var(--nils-warning)">
              ⚠ {w}
            </Text>
          ))}
        </Box>
      ) : null}

      {/* Show warnings/errors for Step 3 */}
      {isStep3Metrics && (metrics as Step3Metrics).warnings?.length ? (
        <Box 
          mt="sm" 
          p="xs" 
          style={{ 
            backgroundColor: 'rgba(210, 153, 34, 0.1)', 
            borderRadius: 'var(--nils-radius-sm)',
            border: '1px solid rgba(210, 153, 34, 0.3)',
          }}
        >
          {(metrics as Step3Metrics).warnings!.map((w, i) => (
            <Text key={i} size="xs" c="var(--nils-warning)">
              ⚠ {w}
            </Text>
          ))}
        </Box>
      ) : null}

      {/* Show warnings/errors for Step 4 */}
      {isStep4Metrics && (metrics as Step4Metrics).warnings?.length ? (
        <Box 
          mt="sm" 
          p="xs" 
          style={{ 
            backgroundColor: 'rgba(210, 153, 34, 0.1)', 
            borderRadius: 'var(--nils-radius-sm)',
            border: '1px solid rgba(210, 153, 34, 0.3)',
          }}
        >
          {(metrics as Step4Metrics).warnings!.map((w, i) => (
            <Text key={i} size="xs" c="var(--nils-warning)">
              ⚠ {w}
            </Text>
          ))}
        </Box>
      ) : null}
    </Box>
  );
};

export default SortingStepCard;
