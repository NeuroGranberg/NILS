import { Box, Stepper, Text } from '@mantine/core';
import { useMemo } from 'react';
import type { StageSummary } from '../../../types';
import { findSuggestedActiveIndex } from './pipelineStepperUtils';

const statusConfig: Record<StageSummary['status'], { color: string; mantineColor: string }> = {
  idle: { color: 'var(--nils-stage-idle)', mantineColor: 'gray' },
  pending: { color: 'var(--nils-stage-pending)', mantineColor: 'violet' },
  running: { color: 'var(--nils-stage-running)', mantineColor: 'blue' },
  completed: { color: 'var(--nils-stage-completed)', mantineColor: 'teal' },
  failed: { color: 'var(--nils-stage-failed)', mantineColor: 'red' },
  paused: { color: 'var(--nils-stage-paused)', mantineColor: 'yellow' },
  blocked: { color: 'var(--nils-stage-blocked)', mantineColor: 'gray' },
};

interface PipelineStepperProps {
  stages: StageSummary[];
  activeStageIndex?: number;
  onStageClick?: (stageIndex: number) => void;
}

export const PipelineStepper = ({ stages, activeStageIndex, onStageClick }: PipelineStepperProps) => {
  const computedActiveIndex = useMemo(() => findSuggestedActiveIndex(stages), [stages]);
  const activeIndex = stages.length === 0 ? 0 : activeStageIndex ?? computedActiveIndex;

  return (
    <Box
      p="md"
      style={{
        backgroundColor: 'var(--nils-bg-secondary)',
        borderRadius: 'var(--nils-radius-lg)',
        border: '1px solid var(--nils-border-subtle)',
      }}
    >
      <Stepper
        active={activeIndex}
        color="blue"
        size="sm"
        allowNextStepsSelect={Boolean(onStageClick)}
        onStepClick={onStageClick}
        styles={{
          stepLabel: {
            fontWeight: 500,
            fontSize: '13px',
          },
          stepDescription: {
            fontSize: '11px',
          },
        }}
      >
        {stages.map((stage) => {
          const config = statusConfig[stage.status];
          return (
            <Stepper.Step
              key={stage.id}
              label={stage.title}
              description={
                <Box style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                  <Box
                    style={{
                      width: 6,
                      height: 6,
                      borderRadius: '50%',
                      backgroundColor: config.color,
                      animation: stage.status === 'running' ? 'pulse 2s infinite' : 'none',
                    }}
                  />
                  <Text size="xs" c="var(--nils-text-tertiary)" tt="capitalize">
                    {stage.status}
                  </Text>
                </Box>
              }
              color={config.mantineColor}
            />
          );
        })}
        <Stepper.Completed>
          <Text size="sm" fw={500} c="var(--nils-success)">
            Pipeline complete
          </Text>
        </Stepper.Completed>
      </Stepper>
    </Box>
  );
};
