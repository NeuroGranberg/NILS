/**
 * Sorting Pipeline Component with Persistence and SSE Streaming
 * 
 * Features:
 * - Real-time SSE streaming with micro-step progress display
 * - Smooth transitions between states
 * - Persistent state across navigation
 * - Step-wise execution with visual feedback
 */

import { Box, Button, Collapse, Group, Progress, Stack, Text, UnstyledButton } from '@mantine/core';
import { IconPlayerPlay, IconRefresh, IconCheck, IconLoader2, IconAlertTriangle, IconChevronDown, IconChevronRight } from '@tabler/icons-react';
import { useCallback, useEffect, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { notifications } from '@mantine/notifications';
import { sortingKeys, useRecoverDates, useSortingStatus } from '../api';
import { SORTING_STEPS, type SortingConfig, type Step1Metrics, type Step2Metrics } from '../types';
import { DateRecoveryCard } from './DateRecoveryCard';

// ============================================================================
// SSE Event Types
// ============================================================================

interface SSEStepProgress {
  step_id: string;
  progress: number;
  message: string;
  current_action?: string;
  metrics?: Record<string, unknown>;
  logs?: string[];  // Log lines streamed from backend
}

interface SSEState {
  currentStepId: string | null;
  progress: number;
  message: string;
  currentAction: string | null;
  isComplete: boolean;
  hasError: boolean;
  errorMessage: string | null;
  logs: string[];  // Log buffer for display
}

// ============================================================================
// Animated Counter Component
// ============================================================================

interface AnimatedCounterProps {
  value: number;
  duration?: number;
  label: string;
  highlight?: boolean;
  color?: string;
  borderColor?: string;
}

const AnimatedCounter = ({ value, duration = 1200, label, highlight, color, borderColor }: AnimatedCounterProps) => {
  const [displayValue, setDisplayValue] = useState(0);
  const animationRef = useRef<number | undefined>(undefined);
  const hasAnimatedRef = useRef(false);
  const prevValueRef = useRef(0);

  useEffect(() => {
    // Skip animation if value hasn't changed or is 0
    if (value === prevValueRef.current || value === 0) {
      setDisplayValue(value);
      prevValueRef.current = value;
      return;
    }

    // Only animate if we're going from 0 to something (first time)
    if (prevValueRef.current === 0 && !hasAnimatedRef.current) {
      hasAnimatedRef.current = true;

      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current);
      }

      const startValue = 0;
      const endValue = value;
      const adjustedDuration = endValue > 1000 ? duration * 1.5 : duration;
      const startTime = performance.now();

      const animate = (currentTime: number) => {
        const elapsed = currentTime - startTime;
        const progress = Math.min(elapsed / adjustedDuration, 1);

        // Cubic ease-out for smooth deceleration
        const easeOut = 1 - Math.pow(1 - progress, 3);
        const current = Math.floor(startValue + (endValue - startValue) * easeOut);

        setDisplayValue(current);

        if (progress < 1) {
          animationRef.current = requestAnimationFrame(animate);
        } else {
          setDisplayValue(endValue);
        }
      };

      animationRef.current = requestAnimationFrame(animate);
    } else {
      // Just update immediately for subsequent changes
      setDisplayValue(value);
    }

    prevValueRef.current = value;

    return () => {
      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current);
      }
    };
  }, [value, duration]);

  const textColor = color || (highlight ? 'var(--nils-accent-primary)' : 'var(--nils-text-primary)');

  return (
    <Box
      style={{
        textAlign: 'left',
        padding: '16px 20px',
        backgroundColor: 'var(--nils-bg-elevated)',
        borderRadius: '8px',
        minWidth: 120,
        flex: 1,
        border: borderColor ? `2px solid ${borderColor}` : 'none',
      }}
    >
      {/* Large bold number */}
      <Text
        style={{
          fontSize: '28px',
          fontWeight: 700,
          fontFamily: 'monospace',
          color: textColor,
          transition: 'color 0.5s ease',
          lineHeight: 1.2,
        }}
      >
        {displayValue.toLocaleString()}
      </Text>
      {/* Smaller label below */}
      <Text
        size="xs"
        c="var(--nils-text-quaternary, rgba(255,255,255,0.4))"
        mt={8}
        style={{ lineHeight: 1.3 }}
      >
        {label}
      </Text>
    </Box>
  );
};

// ============================================================================
// Inline Metrics Summary (for collapsed completed steps)
// ============================================================================

const formatNumber = (n: number): string => {
  if (n >= 1000000) return `${(n / 1000000).toFixed(1)}M`;
  if (n >= 1000) return `${(n / 1000).toFixed(1)}k`;
  return n.toLocaleString();
};

interface InlineMetricsProps {
  metrics: Step1Metrics | Step2Metrics;
}

const InlineMetrics = ({ metrics }: InlineMetricsProps) => {
  // Step 1 metrics
  if ('subjects_in_cohort' in metrics) {
    return (
      <Text size="sm" c="var(--nils-text-secondary)">
        {formatNumber(metrics.subjects_in_cohort || 0)} subjects · {formatNumber(metrics.studies_with_valid_date || metrics.total_studies || 0)} studies · {formatNumber(metrics.total_series || 0)} series
      </Text>
    );
  }

  // Step 2 metrics
  if ('total_fingerprints_created' in metrics) {
    return (
      <Text size="sm" c="var(--nils-text-secondary)">
        {formatNumber(metrics.total_fingerprints_created || 0)} fingerprints
      </Text>
    );
  }

  return null;
};

// ============================================================================
// Initial SSE State
// ============================================================================

const initialSSEState: SSEState = {
  currentStepId: null,
  progress: 0,
  message: '',
  currentAction: null,
  isComplete: false,
  hasError: false,
  errorMessage: null,
  logs: [],
};

// ============================================================================
// Main Component
// ============================================================================

const STEP_ID_TO_INDEX: Record<string, number> = {
  'checkup': 0,
  'stack_fingerprint': 1,
  'classification': 2,
  'deduplication': 3,
  'verification': 4,
};

interface SortingPipelineSimpleProps {
  cohortId: number;
  config: SortingConfig;
  onConfigChange: (config: SortingConfig) => void;
  onRunStep: (stepId: string) => void;
  isLoading?: boolean;
  disabled?: boolean;
  jobId: number | null;
  streamUrl?: string | null;
}

export const SortingPipelineSimple = ({
  cohortId,
  config,
  onConfigChange,
  onRunStep,
  isLoading,
  disabled,
  jobId,
  streamUrl,
}: SortingPipelineSimpleProps) => {
  const queryClient = useQueryClient();

  // Load persisted sorting status
  const { data: sortingStatus, refetch: refetchStatus } = useSortingStatus(cohortId);

  // Date recovery mutation
  const recoverDatesMutation = useRecoverDates();

  // Track expanded step (null = auto-expand next step)
  const [expandedStepId, setExpandedStepId] = useState<string | null>(null);

  // Track if date recovery is in progress (to hide the form)
  const [isRecovering, setIsRecovering] = useState(false);

  // SSE streaming state - tracks real-time progress
  const [sseState, setSSEState] = useState<SSEState>(initialSSEState);
  const eventSourceRef = useRef<EventSource | null>(null);

  // Reset SSE state when no job
  const resetSSEState = useCallback(() => {
    setSSEState(initialSSEState);
  }, []);

  // Connect to SSE stream when streamUrl changes
  useEffect(() => {
    // Clean up previous connection
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }

    if (!streamUrl || !jobId) {
      resetSSEState();
      return;
    }

    console.log('[SSE] Connecting:', streamUrl);
    const eventSource = new EventSource(streamUrl);
    eventSourceRef.current = eventSource;

    // Step started
    eventSource.addEventListener('step_start', (event) => {
      try {
        const data = JSON.parse(event.data);
        console.log('[SSE] step_start:', data);
        setSSEState(prev => ({
          ...prev,
          currentStepId: data.step_id,
          progress: 0,
          message: `Starting ${data.step_title || data.step_id}...`,
          currentAction: null,
          isComplete: false,
          hasError: false,
          logs: [],  // Clear logs on new step
        }));
        // Auto-expand the running step
        setExpandedStepId(data.step_id);
      } catch (e) {
        console.error('[SSE] Failed to parse step_start:', e);
      }
    });

    // Step progress
    eventSource.addEventListener('step_progress', (event) => {
      try {
        const data: SSEStepProgress = JSON.parse(event.data);
        console.log('[SSE] step_progress:', data);
        setSSEState(prev => ({
          ...prev,
          currentStepId: data.step_id,
          progress: data.progress || 0,
          message: data.message || '',
          currentAction: data.current_action || null,
          logs: data.logs || prev.logs,  // Update logs if provided
        }));
      } catch (e) {
        console.error('[SSE] Failed to parse step_progress:', e);
      }
    });

    // Step complete
    eventSource.addEventListener('step_complete', (event) => {
      try {
        const data = JSON.parse(event.data);
        console.log('[SSE] step_complete:', data);
        // Clear currentStepId so step no longer shows as "running"
        setSSEState(prev => ({
          ...prev,
          currentStepId: null,  // Clear so step shows as complete, not running
          progress: 100,
          message: 'Complete',
          currentAction: null,
          isComplete: true,  // Mark SSE as complete for this step
        }));
        // Refresh status to get final metrics from backend
        queryClient.invalidateQueries({ queryKey: sortingKeys.status(cohortId) });
        refetchStatus();
      } catch (e) {
        console.error('[SSE] Failed to parse step_complete:', e);
      }
    });

    // Step error
    eventSource.addEventListener('step_error', (event) => {
      try {
        const data = JSON.parse(event.data);
        console.log('[SSE] step_error:', data);
        setSSEState(prev => ({
          ...prev,
          currentStepId: data.step_id,  // Keep for error display on this step
          hasError: true,
          errorMessage: data.error || 'An error occurred',
          isComplete: true,  // Mark as complete (with error) to prevent running state
        }));
      } catch (e) {
        console.error('[SSE] Failed to parse step_error:', e);
      }
    });

    // Pipeline complete
    eventSource.addEventListener('pipeline_complete', (event) => {
      console.log('[SSE] pipeline_complete');
      try {
        JSON.parse(event.data);
      } catch {
        // Ignore parse errors for completion event
      }
      setSSEState(prev => ({
        ...prev,
        isComplete: true,
        progress: 100,
        message: 'Pipeline complete',
      }));
      eventSource.close();
      eventSourceRef.current = null;
      // Final refresh
      refetchStatus();
      queryClient.invalidateQueries({ queryKey: sortingKeys.status(cohortId) });
    });

    // Pipeline error
    eventSource.addEventListener('pipeline_error', (event) => {
      console.log('[SSE] pipeline_error');
      try {
        const data = JSON.parse(event.data);
        setSSEState(prev => ({
          ...prev,
          hasError: true,
          errorMessage: data.error || 'Pipeline failed',
        }));
      } catch {
        // Ignore parse errors
      }
      eventSource.close();
      eventSourceRef.current = null;
    });

    // Connection error
    eventSource.onerror = () => {
      console.log('[SSE] Connection error/closed');
      // Don't set error state on normal close
      eventSource.close();
      eventSourceRef.current = null;
    };

    return () => {
      eventSource.close();
      eventSourceRef.current = null;
    };
  }, [streamUrl, jobId, refetchStatus, queryClient, cohortId, resetSSEState]);

  // Clear SSE state after completion with delay for smooth transition
  useEffect(() => {
    if (sseState.isComplete) {
      const timer = setTimeout(() => {
        resetSSEState();
      }, 1500);  // Delay to allow animation to finish
      return () => clearTimeout(timer);
    }
  }, [sseState.isComplete, resetSSEState]);

  // Determine completed steps from persisted status
  const completedSteps = sortingStatus?.steps
    ? Object.entries(sortingStatus.steps).filter(([, status]) => status === 'completed').map(([id]) => id)
    : [];

  // Get metrics for a step from persisted status
  const getStepMetrics = (stepId: string): Step1Metrics | Step2Metrics | null => {
    if (sortingStatus?.metrics?.[stepId]) {
      return sortingStatus.metrics[stepId];
    }
    return null;
  };

  // Check if a step is currently running via SSE
  const isStepRunningViaSSE = (stepId: string): boolean => {
    return sseState.currentStepId === stepId && !sseState.isComplete && !sseState.hasError;
  };

  // Determine step status
  const getStepStatus = (stepId: string, stepIndex: number): 'pending' | 'active' | 'running' | 'complete' | 'warning' | 'error' => {
    // Check SSE state first - this gives real-time updates
    if (isStepRunningViaSSE(stepId)) {
      return 'running';
    }

    // Check for SSE error on this step
    if (sseState.hasError && sseState.currentStepId === stepId) {
      return 'error';
    }

    // Check if step is complete from persisted status
    if (completedSteps.includes(stepId)) {
      // Check if step has data quality issues (warning status)
      const metrics = getStepMetrics(stepId);
      if (stepId === 'checkup' && metrics && 'studies_excluded_no_date' in metrics && metrics.studies_excluded_no_date && metrics.studies_excluded_no_date > 0) {
        return 'warning';  // Orange status for excluded studies
      }
      return 'complete';
    }

    // Next step to run is active
    const nextStepIndex = completedSteps.length > 0
      ? Math.max(...completedSteps.map(s => STEP_ID_TO_INDEX[s] ?? -1)) + 1
      : 0;
    if (stepIndex === nextStepIndex) {
      return 'active';
    }

    return 'pending';
  };

  // Determine which step should be expanded
  const getIsExpanded = (stepId: string, status: string): boolean => {
    // If user explicitly expanded/collapsed a step, respect that
    if (expandedStepId !== null) {
      return expandedStepId === stepId;
    }
    // Auto-expand running step
    if (status === 'running') return true;
    // Auto-expand active step (next to run)
    if (status === 'active') return true;
    // Auto-expand warning step (has data quality issues that need attention)
    if (status === 'warning') return true;
    return false;
  };

  const handleStepClick = (stepId: string) => {
    // Toggle expansion
    setExpandedStepId(prev => prev === stepId ? null : stepId);
  };

  return (
    <Stack gap="sm">
      {SORTING_STEPS.map((step, index) => {
        const status = getStepStatus(step.id, index);
        const isStepRunning = status === 'running';
        const isStepComplete = status === 'complete';
        const isStepWarning = status === 'warning';
        const isStepActive = status === 'active';
        const isStepError = status === 'error';
        const isStepPending = status === 'pending';

        const isExpanded = getIsExpanded(step.id, status);
        const stepMetrics = getStepMetrics(step.id);

        const getBorderColor = () => {
          if (isStepRunning) return 'var(--nils-accent-primary)';
          if (isStepComplete) return 'var(--nils-success)';
          if (isStepWarning) return 'var(--nils-warning)';
          if (isStepError) return 'var(--nils-error)';
          if (isStepActive) return 'var(--nils-accent-primary)';
          return 'var(--nils-border-subtle)';
        };

        return (
          <Box
            key={step.id}
            style={{
              backgroundColor: isStepRunning ? 'rgba(88, 166, 255, 0.05)' :
                isStepError ? 'rgba(248, 81, 73, 0.05)' :
                  'var(--nils-bg-tertiary)',
              borderRadius: 'var(--nils-radius-md)',
              border: `2px solid ${getBorderColor()}`,
              transition: 'all 0.3s ease',
              overflow: 'hidden',
            }}
          >
            {/* Header - clickable to expand/collapse */}
            <UnstyledButton
              onClick={() => handleStepClick(step.id)}
              style={{ width: '100%' }}
            >
              <Box p="md">
                <Group justify="space-between" wrap="nowrap">
                  <Group gap="md" wrap="nowrap" style={{ flex: 1 }}>
                    {/* Step indicator */}
                    <Box
                      style={{
                        width: 36,
                        height: 36,
                        borderRadius: '50%',
                        backgroundColor: isStepComplete ? 'rgba(63, 185, 80, 0.2)' :
                          isStepWarning ? 'rgba(255, 193, 7, 0.2)' :
                            isStepRunning ? 'rgba(88, 166, 255, 0.2)' :
                              isStepError ? 'rgba(248, 81, 73, 0.2)' :
                                'var(--nils-bg-elevated)',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        flexShrink: 0,
                      }}
                    >
                      {isStepComplete && <IconCheck size={18} color="var(--nils-success)" />}
                      {isStepWarning && <IconAlertTriangle size={18} color="var(--nils-warning)" />}
                      {isStepRunning && <IconLoader2 size={18} color="var(--nils-accent-primary)" className="spinning" />}
                      {isStepError && <IconAlertTriangle size={18} color="var(--nils-error)" />}
                      {!isStepComplete && !isStepWarning && !isStepRunning && !isStepError && (
                        <Text size="sm" fw={600} c="var(--nils-text-secondary)">{index + 1}</Text>
                      )}
                    </Box>

                    {/* Step info */}
                    <Box style={{ flex: 1 }}>
                      <Group gap="xs" wrap="nowrap">
                        <Text fw={600} c="var(--nils-text-primary)">{step.title}</Text>
                      </Group>
                      {/* Show real-time progress when running */}
                      {isStepRunning && sseState.currentStepId === step.id ? (
                        <Stack gap={4}>
                          <Group gap="xs" wrap="nowrap">
                            <Progress
                              value={sseState.progress}
                              size="xs"
                              style={{ flex: 1, maxWidth: 200 }}
                              color="blue"
                              animated
                            />
                            <Text size="xs" c="var(--nils-accent-primary)" fw={500}>
                              {sseState.progress}%
                            </Text>
                          </Group>
                          {/* Terminal-style action log */}
                          <Box
                            style={{
                              display: 'flex',
                              alignItems: 'center',
                              gap: 6,
                              padding: '3px 8px',
                              backgroundColor: 'var(--nils-bg-primary)',
                              borderRadius: 'var(--nils-radius-xs)',
                              maxWidth: 300,
                              overflow: 'hidden',
                            }}
                          >
                            <Text
                              size="xs"
                              c="var(--nils-accent-primary)"
                              style={{
                                flexShrink: 0,
                                fontFamily: 'ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace',
                              }}
                            >
                              ›
                            </Text>
                            <Text
                              size="xs"
                              c="var(--nils-text-tertiary)"
                              style={{
                                whiteSpace: 'nowrap',
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                fontFamily: 'ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace',
                              }}
                            >
                              {sseState.currentAction || sseState.message || 'Processing...'}
                            </Text>
                          </Box>
                        </Stack>
                      ) : (isStepComplete || isStepWarning) && !isExpanded && stepMetrics ? (
                        <InlineMetrics metrics={stepMetrics} />
                      ) : (
                        <Text size="xs" c="var(--nils-text-tertiary)">{step.description}</Text>
                      )}
                    </Box>
                  </Group>

                  {/* Expand/collapse indicator */}
                  <Box c="var(--nils-text-tertiary)">
                    {isExpanded ? <IconChevronDown size={18} /> : <IconChevronRight size={18} />}
                  </Box>
                </Group>
              </Box>
            </UnstyledButton>

            {/* Expanded content */}
            <Collapse in={isExpanded}>
              <Box p="md" pt={0} style={{ borderTop: '1px solid var(--nils-border-subtle)' }}>
                {/* Config options (step-specific) */}
                {step.id === 'checkup' && (isStepActive || isStepComplete || isStepWarning) && (
                  <Box mb="md">
                    <Group gap="sm" align="center">
                      {/* Incremental mode pill */}
                      <Box
                        onClick={() => !isStepRunning && onConfigChange({ ...config, skipClassified: !config.skipClassified })}
                        style={{
                          display: 'inline-flex',
                          alignItems: 'center',
                          gap: 6,
                          padding: '6px 12px',
                          borderRadius: 'var(--nils-radius-sm)',
                          backgroundColor: config.skipClassified ? 'rgba(88, 166, 255, 0.15)' : 'var(--nils-bg-elevated)',
                          border: config.skipClassified ? '1px solid var(--nils-accent-primary)' : '1px solid var(--nils-border-subtle)',
                          cursor: isStepRunning ? 'not-allowed' : 'pointer',
                          opacity: isStepRunning ? 0.6 : 1,
                          transition: 'all 0.2s ease',
                        }}
                      >
                        <Box
                          style={{
                            width: 6,
                            height: 6,
                            borderRadius: '50%',
                            backgroundColor: config.skipClassified ? 'var(--nils-accent-primary)' : 'var(--nils-text-tertiary)',
                          }}
                        />
                        <Text size="xs" fw={config.skipClassified ? 500 : 400} c={config.skipClassified ? 'var(--nils-accent-primary)' : 'var(--nils-text-secondary)'}>
                          Skip classified
                        </Text>
                      </Box>

                      <Text size="xs" c="var(--nils-text-tertiary)">·</Text>

                      {/* Modality pills */}
                      {[
                        { value: 'MR', label: 'MR' },
                        { value: 'CT', label: 'CT' },
                        { value: 'PT', label: 'PET' },
                      ].map((modality) => {
                        const isSelected = config.selectedModalities?.includes(modality.value) ?? true;
                        return (
                          <Box
                            key={modality.value}
                            onClick={() => {
                              if (isStepRunning) return;
                              const current = config.selectedModalities || ['MR', 'CT', 'PT'];
                              const updated = isSelected
                                ? current.filter(m => m !== modality.value)
                                : [...current, modality.value];
                              onConfigChange({ ...config, selectedModalities: updated });
                            }}
                            style={{
                              display: 'inline-flex',
                              alignItems: 'center',
                              gap: 6,
                              padding: '6px 12px',
                              borderRadius: 'var(--nils-radius-sm)',
                              backgroundColor: isSelected ? 'rgba(88, 166, 255, 0.15)' : 'var(--nils-bg-elevated)',
                              border: isSelected ? '1px solid var(--nils-accent-primary)' : '1px solid var(--nils-border-subtle)',
                              cursor: isStepRunning ? 'not-allowed' : 'pointer',
                              opacity: isStepRunning ? 0.6 : 1,
                              transition: 'all 0.2s ease',
                            }}
                          >
                            <Box
                              style={{
                                width: 6,
                                height: 6,
                                borderRadius: '50%',
                                backgroundColor: isSelected ? 'var(--nils-accent-primary)' : 'var(--nils-text-tertiary)',
                              }}
                            />
                            <Text size="xs" fw={isSelected ? 500 : 400} c={isSelected ? 'var(--nils-accent-primary)' : 'var(--nils-text-secondary)'}>
                              {modality.label}
                            </Text>
                          </Box>
                        );
                      })}
                    </Group>
                  </Box>
                )}

                {/* Metrics display (for running, complete, or warning) */}
                {(isStepRunning || isStepComplete || isStepWarning) && stepMetrics && step.id === 'checkup' && 'subjects_in_cohort' in stepMetrics && (() => {
                  const step1Metrics = stepMetrics as Step1Metrics;
                  return (
                    <Stack gap="md" mb="md">
                      {/* Progress bar */}
                      <Box>
                        <Progress
                          value={100}
                          size="sm"
                          radius="sm"
                          color={isStepComplete ? 'green' : isStepWarning ? 'orange' : 'blue'}
                          striped={isStepRunning}
                          animated={isStepRunning}
                          styles={{
                            root: { backgroundColor: 'var(--nils-bg-elevated)' },
                            section: { transition: 'background-color 0.5s ease' },
                          }}
                        />
                      </Box>

                      {/* Metric Cards */}
                      <Group justify="center" gap="md" py="sm">
                        <AnimatedCounter
                          value={step1Metrics.subjects_in_cohort || 0}
                          label="Subjects under cohort"
                          color={isStepComplete ? 'var(--nils-success)' : undefined}
                        />
                        <AnimatedCounter
                          value={step1Metrics.studies_with_valid_date || step1Metrics.total_studies || 0}
                          label="Valid studies"
                          color={isStepComplete ? 'var(--nils-success)' : undefined}
                        />
                        <AnimatedCounter
                          value={step1Metrics.total_series || 0}
                          label="Unique series"
                          color={isStepComplete ? 'var(--nils-success)' : undefined}
                        />
                        {'series_to_process_count' in stepMetrics && (
                          <AnimatedCounter
                            value={step1Metrics.series_to_process_count || 0}
                            label="Series to classify"
                            color={isStepComplete ? 'var(--nils-success)' : undefined}
                          />
                        )}
                        {/* Individual Modality Cards - show for all selected modalities */}
                        {'series_by_modality' in stepMetrics && step1Metrics.series_by_modality && (
                          <>
                            {config.selectedModalities?.includes('MR') && (
                              <AnimatedCounter
                                value={step1Metrics.series_by_modality.MR ?? 0}
                                label="MR series"
                                color={isStepComplete ? 'var(--nils-success)' : undefined}
                              />
                            )}
                            {config.selectedModalities?.includes('CT') && (
                              <AnimatedCounter
                                value={step1Metrics.series_by_modality.CT ?? 0}
                                label="CT series"
                                color={isStepComplete ? 'var(--nils-success)' : undefined}
                              />
                            )}
                            {config.selectedModalities?.includes('PT') && (
                              <AnimatedCounter
                                value={step1Metrics.series_by_modality.PT ?? 0}
                                label="PET series"
                                color={isStepComplete ? 'var(--nils-success)' : undefined}
                              />
                            )}
                          </>
                        )}
                        {/* Excluded Studies Card - only shown when there are excluded studies */}
                        {'studies_excluded_no_date' in stepMetrics && step1Metrics.studies_excluded_no_date > 0 && (
                          <AnimatedCounter
                            value={step1Metrics.studies_excluded_no_date}
                            label="Studies excluded (no date)"
                            color="var(--nils-error)"
                            borderColor="var(--nils-error)"
                          />
                        )}
                      </Group>

                      {/* Date Recovery Card (hide when recovering, running, or no excluded studies) */}
                      {'studies_excluded_no_date' in stepMetrics && step1Metrics.studies_excluded_no_date > 0 && !isStepRunning && !isRecovering && (
                        <Box mt="md">
                          <DateRecoveryCard
                            cohortId={cohortId}
                            excludedCount={step1Metrics.studies_excluded_no_date}
                            onRecover={(minYear, maxYear) => {
                              setIsRecovering(true);  // Hide the form immediately

                              recoverDatesMutation.mutate(
                                { cohortId, config: { minYear, maxYear } },
                                {
                                  onSuccess: (result) => {
                                    const message = `Recovered ${result.recovered_count} dates. ${result.failed_count > 0 ? `${result.failed_count} still missing.` : ''
                                      }`;

                                    // After date recovery, backend deletes Step 1 handover to force re-run
                                    if (result.updated_metrics && result.recovered_count > 0) {
                                      // Invalidate status to get fresh data
                                      queryClient.invalidateQueries({
                                        queryKey: sortingKeys.status(cohortId)
                                      });

                                      notifications.show({
                                        title: 'Date Recovery Complete',
                                        message: `Recovered ${result.recovered_count} study dates. Re-running Step 1 to include new series...`,
                                        color: 'green',
                                      });

                                      // Automatically trigger Step 1 re-run
                                      // Wait a moment for invalidation to complete, then run
                                      setTimeout(() => {
                                        setIsRecovering(false);
                                        onRunStep('checkup');  // Trigger Step 1 re-run
                                      }, 500);
                                    } else {
                                      notifications.show({
                                        title: 'Date Recovery Complete',
                                        message: message || 'No dates could be recovered.',
                                        color: 'yellow',
                                      });
                                      setIsRecovering(false);
                                    }
                                  },
                                  onError: () => {
                                    notifications.show({
                                      title: 'Date Recovery Failed',
                                      message: 'Failed to recover dates. Please try again.',
                                      color: 'red',
                                    });
                                    setIsRecovering(false);
                                  },
                                }
                              );
                            }}
                            isLoading={recoverDatesMutation.isPending}
                          />
                        </Box>
                      )}
                    </Stack>
                  );
                })()}

                {/* Step 2: Stack Fingerprint Metrics */}
                {stepMetrics && step.id === 'stack_fingerprint' && 'total_fingerprints_created' in stepMetrics && (() => {
                  const step2Metrics = stepMetrics as unknown as Step2Metrics;
                  return (
                    <Stack gap="md" mb="md">
                      {/* Progress bar */}
                      <Box>
                        <Progress
                          value={100}
                          size="sm"
                          radius="sm"
                          color={isStepComplete ? 'green' : isStepWarning ? 'orange' : 'blue'}
                          striped={isStepRunning}
                          animated={isStepRunning}
                          styles={{
                            root: { backgroundColor: 'var(--nils-bg-elevated)' },
                            section: { transition: 'background-color 0.5s ease' },
                          }}
                        />
                      </Box>

                      {/* Metric Cards */}
                      <Group justify="center" gap="md" py="sm">
                        <AnimatedCounter
                          value={step2Metrics.total_fingerprints_created || 0}
                          label="Fingerprints created"
                          color={isStepComplete ? 'var(--nils-success)' : undefined}
                        />
                      </Group>
                    </Stack>
                  );
                })()}

                {/* Log stream display when running */}
                {isStepRunning && sseState.logs.length > 0 && (
                  <Box
                    mb="md"
                    style={{
                      backgroundColor: 'var(--nils-bg-primary)',
                      borderRadius: 'var(--nils-radius-sm)',
                      border: '1px solid var(--nils-border-subtle)',
                      maxHeight: 200,
                      overflow: 'auto',
                      fontFamily: 'ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace',
                    }}
                  >
                    <Box p="xs" style={{ borderBottom: '1px solid var(--nils-border-subtle)' }}>
                      <Text size="xs" fw={500} c="var(--nils-text-secondary)">
                        Processing Log
                      </Text>
                    </Box>
                    <Box p="xs">
                      {sseState.logs.slice(-20).map((line, i) => (
                        <Text
                          key={i}
                          size="xs"
                          c="var(--nils-text-tertiary)"
                          style={{
                            whiteSpace: 'pre-wrap',
                            wordBreak: 'break-word',
                            lineHeight: 1.4,
                          }}
                        >
                          {line}
                        </Text>
                      ))}
                    </Box>
                  </Box>
                )}

                {/* Error message */}
                {isStepError && (
                  <Box mb="md" p="sm" style={{ backgroundColor: 'rgba(248, 81, 73, 0.1)', borderRadius: 'var(--nils-radius-sm)' }}>
                    <Text size="sm" c="var(--nils-error)">
                      {sseState.errorMessage || 'An error occurred'}
                    </Text>
                  </Box>
                )}

                {/* Action buttons */}
                <Group justify="flex-end">
                  {(isStepActive || isStepError) && (
                    <Button
                      size="sm"
                      leftSection={<IconPlayerPlay size={16} />}
                      onClick={() => onRunStep(step.id)}
                      disabled={disabled || isStepPending || isStepRunning}
                      loading={isLoading}
                      style={{
                        backgroundColor: 'var(--nils-accent-primary)',
                        color: 'var(--nils-bg-primary)',
                      }}
                    >
                      {isStepError ? 'Retry' : 'Run'}
                    </Button>
                  )}
                  {(isStepComplete || isStepWarning) && !isStepRunning && (
                    <Button
                      size="sm"
                      variant="subtle"
                      leftSection={<IconRefresh size={16} />}
                      onClick={() => onRunStep(step.id)}
                      disabled={disabled || !!sseState.currentStepId}
                      c="var(--nils-text-secondary)"
                    >
                      Redo
                    </Button>
                  )}
                </Group>
              </Box>
            </Collapse>
          </Box>
        );
      })}

      {/* Spinning animation */}
      <style>{`
        @keyframes spin {
          from { transform: rotate(0deg); }
          to { transform: rotate(360deg); }
        }
        .spinning {
          animation: spin 1s linear infinite;
        }
      `}</style>
    </Stack>
  );
};

export default SortingPipelineSimple;
