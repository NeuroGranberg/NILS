/**
 * Unified Sorting Pipeline Component
 * 
 * Each step contains its own:
 * - Configuration options
 * - Run/Redo button
 * - Animated progress with jackpot-style counters
 * 
 * Only the active step is expanded.
 * Completed steps show summary + Redo button.
 * Future steps are collapsed/disabled.
 */

import { Box, Button, Collapse, Group, Stack, Switch, Text } from '@mantine/core';
import { IconPlayerPlay, IconRefresh, IconCheck, IconLoader2 } from '@tabler/icons-react';
import { useEffect, useRef, useState } from 'react';
import { SORTING_STEPS, type SortingConfig, type Step1Metrics, type StepState } from '../types';
import { useSortingStream } from '../hooks/useSortingStream';

// ============================================================================
// Animated Counter Component (Jackpot style)
// ============================================================================

interface AnimatedCounterProps {
  value: number;
  duration?: number;
  label: string;
  highlight?: boolean;
}

const AnimatedCounter = ({ value, duration = 2000, label, highlight }: AnimatedCounterProps) => {
  const [displayValue, setDisplayValue] = useState(0);
  const prevValue = useRef(0);
  const animationRef = useRef<number | undefined>(undefined);

  useEffect(() => {
    // Cancel any existing animation
    if (animationRef.current) {
      cancelAnimationFrame(animationRef.current);
    }

    const startValue = prevValue.current;
    const endValue = value;
    
    // If jumping from 0 to a large number, use longer duration
    const adjustedDuration = startValue === 0 && endValue > 1000 ? duration * 1.5 : duration;
    const startTime = performance.now();

    const animate = (currentTime: number) => {
      const elapsed = currentTime - startTime;
      const progress = Math.min(elapsed / adjustedDuration, 1);
      
      // Easing function for smooth deceleration (cubic ease-out)
      const easeOut = 1 - Math.pow(1 - progress, 3);
      const current = Math.floor(startValue + (endValue - startValue) * easeOut);
      
      setDisplayValue(current);

      if (progress < 1) {
        animationRef.current = requestAnimationFrame(animate);
      } else {
        prevValue.current = endValue;
        setDisplayValue(endValue); // Ensure final value is exact
      }
    };

    animationRef.current = requestAnimationFrame(animate);

    return () => {
      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current);
      }
    };
  }, [value, duration]);

  return (
    <Box style={{ textAlign: 'center' }}>
      <Text 
        size="xl" 
        fw={700} 
        ff="monospace"
        c={highlight ? 'var(--nils-accent-primary)' : 'var(--nils-text-primary)'}
        style={{ 
          transition: 'color 0.3s ease, text-shadow 0.3s ease',
          textShadow: highlight ? '0 0 10px rgba(88, 166, 255, 0.5)' : 'none',
        }}
      >
        {displayValue.toLocaleString()}
      </Text>
      <Text size="xs" c="var(--nils-text-tertiary)">
        {label}
      </Text>
    </Box>
  );
};

// ============================================================================
// Scrolling Status Text (Jackpot window effect)
// ============================================================================

interface ScrollingStatusProps {
  currentMessage: string | null;
}

const ScrollingStatus = ({ currentMessage }: ScrollingStatusProps) => {
  return (
    <Box
      style={{
        height: 60,
        overflow: 'hidden',
        position: 'relative',
        backgroundColor: 'var(--nils-bg-primary)',
        borderRadius: 'var(--nils-radius-sm)',
        border: '1px solid var(--nils-border-subtle)',
      }}
    >
      {/* Gradient masks for window effect */}
      <Box
        style={{
          position: 'absolute',
          top: 0,
          left: 0,
          right: 0,
          height: 20,
          background: 'linear-gradient(to bottom, var(--nils-bg-primary), transparent)',
          zIndex: 1,
          pointerEvents: 'none',
        }}
      />
      <Box
        style={{
          position: 'absolute',
          bottom: 0,
          left: 0,
          right: 0,
          height: 20,
          background: 'linear-gradient(to top, var(--nils-bg-primary), transparent)',
          zIndex: 1,
          pointerEvents: 'none',
        }}
      />
      
      {/* Scrolling content */}
      <Box
        style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          height: '100%',
          padding: 'var(--nils-space-sm)',
        }}
      >
        {currentMessage && (
          <Text 
            size="sm" 
            c="var(--nils-accent-primary)" 
            ta="center"
            style={{
              animation: 'slideUp 0.3s ease-out',
            }}
            key={currentMessage}
          >
            {currentMessage}
          </Text>
        )}
      </Box>
    </Box>
  );
};

// ============================================================================
// Step Card Component
// ============================================================================

interface StepCardProps {
  step: typeof SORTING_STEPS[0];
  index: number;
  status: 'pending' | 'active' | 'running' | 'complete' | 'error';
  state: StepState | undefined;
  config: SortingConfig;
  onConfigChange: (config: SortingConfig) => void;
  onRun: () => void;
  onRedo: () => void;
  disabled?: boolean;
  isLoading?: boolean;
}

const StepCard = ({
  step,
  index,
  status,
  state,
  config,
  onConfigChange,
  onRun,
  onRedo,
  disabled,
  isLoading,
}: StepCardProps) => {
  const isActive = status === 'active';
  const isRunning = status === 'running';
  const isComplete = status === 'complete';
  const isPending = status === 'pending';
  const hasError = status === 'error';

  const metrics = state?.metrics as Step1Metrics | undefined;
  const progress = state?.progress ?? 0;
  const message = state?.message;

  // Get border/background colors
  const getBorderColor = () => {
    if (isRunning) return 'var(--nils-accent-primary)';
    if (isComplete) return 'var(--nils-success)';
    if (hasError) return 'var(--nils-error)';
    if (isActive) return 'var(--nils-accent-primary)';
    return 'var(--nils-border-subtle)';
  };

  const getBgColor = () => {
    if (isRunning) return 'rgba(88, 166, 255, 0.05)';
    if (isComplete) return 'rgba(63, 185, 80, 0.05)';
    if (hasError) return 'rgba(248, 81, 73, 0.05)';
    return 'var(--nils-bg-tertiary)';
  };

  return (
    <Box
      style={{
        backgroundColor: getBgColor(),
        borderRadius: 'var(--nils-radius-md)',
        border: `2px solid ${getBorderColor()}`,
        transition: 'all 0.3s ease',
        overflow: 'hidden',
      }}
    >
      {/* Header - Always visible */}
      <Box p="md">
        <Group justify="space-between" wrap="nowrap">
          <Group gap="md" wrap="nowrap">
            {/* Step number/status indicator */}
            <Box
              style={{
                width: 40,
                height: 40,
                borderRadius: '50%',
                backgroundColor: isComplete 
                  ? 'rgba(63, 185, 80, 0.2)'
                  : isRunning
                    ? 'rgba(88, 166, 255, 0.2)'
                    : 'var(--nils-bg-elevated)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                flexShrink: 0,
              }}
            >
              {isRunning ? (
                <IconLoader2 
                  size={20} 
                  color="var(--nils-accent-primary)"
                  style={{ animation: 'spin 1s linear infinite' }}
                />
              ) : isComplete ? (
                <IconCheck size={20} color="var(--nils-success)" />
              ) : (
                <Text size="md" fw={700} c="var(--nils-text-secondary)">
                  {index + 1}
                </Text>
              )}
            </Box>

            {/* Title and description */}
            <Box>
              <Text size="md" fw={600} c="var(--nils-text-primary)">
                {step.title}
              </Text>
              <Text size="xs" c="var(--nils-text-tertiary)">
                {isRunning && message ? message : step.description}
              </Text>
            </Box>
          </Group>

          {/* Right side: Status or action button */}
          <Box>
            {isComplete && (
              <Button
                size="xs"
                variant="subtle"
                leftSection={<IconRefresh size={14} />}
                onClick={onRedo}
                disabled={disabled}
                style={{ color: 'var(--nils-text-secondary)' }}
              >
                Redo
              </Button>
            )}
            {isPending && (
              <Text size="xs" c="var(--nils-text-tertiary)">
                Waiting...
              </Text>
            )}
          </Box>
        </Group>
      </Box>

      {/* Expanded content for active/running step */}
      <Collapse in={isActive || isRunning}>
        <Box 
          p="md" 
          pt={0}
          style={{ borderTop: '1px solid var(--nils-border-subtle)' }}
        >
          {/* Configuration options (only when active, not running) */}
          {isActive && !isRunning && step.id === 'checkup' && (
            <Stack gap="md" mb="md">
              <Box
                p="sm"
                style={{
                  backgroundColor: 'var(--nils-bg-elevated)',
                  borderRadius: 'var(--nils-radius-sm)',
                }}
              >
                <Switch
                  label="Skip already classified series"
                  description="Incremental mode - only process new series"
                  checked={config.skipClassified}
                  onChange={(e) => onConfigChange({ ...config, skipClassified: e.currentTarget.checked })}
                  styles={{
                    label: { color: 'var(--nils-text-primary)', fontSize: '14px' },
                    description: { color: 'var(--nils-text-tertiary)' },
                  }}
                />
              </Box>
            </Stack>
          )}

          {/* Running state: Animated progress */}
          {isRunning && (
            <Stack gap="md">
              {/* Status window */}
              <ScrollingStatus 
                currentMessage={message || 'Processing...'} 
              />

              {/* Progress bar */}
              <Box>
                <Box
                  style={{
                    height: 8,
                    backgroundColor: 'var(--nils-bg-elevated)',
                    borderRadius: 4,
                    overflow: 'hidden',
                  }}
                >
                  <Box
                    style={{
                      height: '100%',
                      width: `${progress}%`,
                      backgroundColor: 'var(--nils-accent-primary)',
                      borderRadius: 4,
                      transition: 'width 0.3s ease',
                      boxShadow: '0 0 10px rgba(88, 166, 255, 0.5)',
                    }}
                  />
                </Box>
                <Text size="xs" c="var(--nils-text-tertiary)" ta="right" mt={4}>
                  {progress}%
                </Text>
              </Box>

              {/* Animated counters */}
              {metrics && Object.keys(metrics).length > 0 && (
                <Group justify="space-around" mt="sm">
                  <AnimatedCounter value={metrics.subjects_in_cohort || 0} label="Subjects" />
                  <AnimatedCounter value={metrics.total_studies || 0} label="Studies" />
                  <AnimatedCounter value={metrics.total_series || 0} label="Series" />
                  <AnimatedCounter 
                    value={metrics.series_to_process_count || 0} 
                    label="To Process" 
                    highlight 
                  />
                </Group>
              )}
            </Stack>
          )}

          {/* Run button (only when active, not running) */}
          {isActive && !isRunning && (
            <Button
              fullWidth
              size="md"
              leftSection={<IconPlayerPlay size={18} />}
              onClick={onRun}
              disabled={disabled}
              loading={isLoading}
              style={{
                backgroundColor: 'var(--nils-accent-primary)',
                color: 'var(--nils-bg-primary)',
              }}
            >
              Run {step.title}
            </Button>
          )}
        </Box>
      </Collapse>

      {/* Completed summary */}
      <Collapse in={isComplete}>
        <Box 
          p="md" 
          pt={0}
        >
          {metrics && (
            <Group justify="space-around">
              <Box style={{ textAlign: 'center' }}>
                <Text size="lg" fw={700} c="var(--nils-text-primary)">
                  {metrics.subjects_in_cohort?.toLocaleString()}
                </Text>
                <Text size="xs" c="var(--nils-text-tertiary)">Subjects</Text>
              </Box>
              <Box style={{ textAlign: 'center' }}>
                <Text size="lg" fw={700} c="var(--nils-text-primary)">
                  {metrics.total_studies?.toLocaleString()}
                </Text>
                <Text size="xs" c="var(--nils-text-tertiary)">Studies</Text>
              </Box>
              <Box style={{ textAlign: 'center' }}>
                <Text size="lg" fw={700} c="var(--nils-accent-primary)">
                  {metrics.series_to_process_count?.toLocaleString()}
                </Text>
                <Text size="xs" c="var(--nils-text-tertiary)">Ready</Text>
              </Box>
            </Group>
          )}
        </Box>
      </Collapse>

      {/* Animation styles */}
      <style>{`
        @keyframes spin {
          from { transform: rotate(0deg); }
          to { transform: rotate(360deg); }
        }
        @keyframes slideUp {
          from { 
            opacity: 0;
            transform: translateY(10px);
          }
          to { 
            opacity: 1;
            transform: translateY(0);
          }
        }
      `}</style>
    </Box>
  );
};

// ============================================================================
// Main Pipeline Component
// ============================================================================

interface SortingPipelineProps {
  streamUrl: string | null;
  config: SortingConfig;
  onConfigChange: (config: SortingConfig) => void;
  onRunStep: (stepId: string) => void;
  isLoading?: boolean;
  disabled?: boolean;
}

export const SortingPipeline = ({
  streamUrl,
  config,
  onConfigChange,
  onRunStep,
  isLoading,
  disabled,
}: SortingPipelineProps) => {
  const {
    steps: stepStates,
    currentStep,
    isConnected,
  } = useSortingStream(streamUrl, !!streamUrl);

  // Determine the status of each step
  const getStepStatus = (stepId: string, index: number): 'pending' | 'active' | 'running' | 'complete' | 'error' => {
    const state = stepStates[stepId];
    
    if (state?.status === 'complete') return 'complete';
    if (state?.status === 'error') return 'error';
    if (currentStep === stepId) return 'running';
    
    // If no step is running and this is the first incomplete step, it's active
    if (!currentStep && !isConnected) {
      const firstIncomplete = SORTING_STEPS.findIndex(s => stepStates[s.id]?.status !== 'complete');
      if (firstIncomplete === -1 || firstIncomplete === index) return 'active';
    }
    
    return 'pending';
  };

  return (
    <Stack gap="sm">
      {SORTING_STEPS.map((step, index) => (
        <StepCard
          key={step.id}
          step={step}
          index={index}
          status={getStepStatus(step.id, index)}
          state={stepStates[step.id]}
          config={config}
          onConfigChange={onConfigChange}
          onRun={() => onRunStep(step.id)}
          onRedo={() => onRunStep(step.id)}
          disabled={disabled}
          isLoading={isLoading}
        />
      ))}
    </Stack>
  );
};

export default SortingPipeline;
