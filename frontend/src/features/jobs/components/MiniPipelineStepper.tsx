/**
 * Compact horizontal pipeline progress indicator.
 * Shows stages as connected dots with status colors.
 */

import { Box, Group, Text, Tooltip } from '@mantine/core';
import { IconCheck, IconPlayerPlay, IconClock, IconX, IconLock } from '@tabler/icons-react';
import { Fragment } from 'react';
import type { StageSummary, StageStatus } from '../../../types';

const statusConfig: Record<StageStatus, { color: string; icon: React.ReactNode; label: string }> = {
  idle: { 
    color: 'var(--nils-text-tertiary)', 
    icon: <IconClock size={10} />,
    label: 'Idle'
  },
  pending: { 
    color: 'var(--nils-stage-pending)', 
    icon: <IconClock size={10} />,
    label: 'Pending'
  },
  running: { 
    color: 'var(--nils-stage-running)', 
    icon: <IconPlayerPlay size={10} />,
    label: 'Running'
  },
  completed: { 
    color: 'var(--nils-stage-completed)', 
    icon: <IconCheck size={10} />,
    label: 'Completed'
  },
  failed: { 
    color: 'var(--nils-stage-failed)', 
    icon: <IconX size={10} />,
    label: 'Failed'
  },
  paused: { 
    color: 'var(--nils-stage-paused)', 
    icon: <IconClock size={10} />,
    label: 'Paused'
  },
  blocked: { 
    color: 'var(--nils-text-tertiary)', 
    icon: <IconLock size={10} />,
    label: 'Blocked'
  },
};

interface StatusDotProps {
  status: StageStatus;
  animate?: boolean;
}

const StatusDot = ({ status, animate }: StatusDotProps) => {
  const config = statusConfig[status];
  return (
    <Box
      style={{
        width: 8,
        height: 8,
        borderRadius: '50%',
        backgroundColor: config.color,
        animation: animate && status === 'running' ? 'pulse 2s infinite' : 'none',
        flexShrink: 0,
      }}
    />
  );
};

interface MiniPipelineStepperProps {
  stages: StageSummary[];
}

export const MiniPipelineStepper = ({ stages }: MiniPipelineStepperProps) => {
  if (!stages || stages.length === 0) {
    return (
      <Text size="xs" c="var(--nils-text-tertiary)">
        No pipeline stages configured
      </Text>
    );
  }

  return (
    <Group gap={0} wrap="nowrap" style={{ overflow: 'auto' }}>
      <Text size="xs" c="var(--nils-text-tertiary)" fw={500} mr="xs" style={{ flexShrink: 0 }}>
        Pipeline:
      </Text>
      {stages.map((stage, idx) => {
        const config = statusConfig[stage.status];
        const isLast = idx === stages.length - 1;
        
        return (
          <Fragment key={stage.id}>
            <Tooltip 
              label={
                <Box>
                  <Text size="xs" fw={600}>{stage.title}</Text>
                  <Text size="xs">{config.label} ({stage.progress}%)</Text>
                </Box>
              }
              withArrow
            >
              <Group 
                gap={4} 
                wrap="nowrap" 
                style={{ 
                  cursor: 'default',
                  padding: '2px 6px',
                  borderRadius: 'var(--nils-radius-sm)',
                  backgroundColor: stage.status === 'running' ? 'rgba(88, 166, 255, 0.1)' : 'transparent',
                }}
              >
                <StatusDot status={stage.status} animate={stage.status === 'running'} />
                <Text 
                  size="xs" 
                  c={stage.status === 'blocked' ? 'var(--nils-text-tertiary)' : config.color}
                  fw={stage.status === 'running' ? 600 : 400}
                  style={{ whiteSpace: 'nowrap' }}
                >
                  {stage.title}
                </Text>
              </Group>
            </Tooltip>
            
            {/* Connector line */}
            {!isLast && (
              <Box
                style={{
                  width: 20,
                  height: 2,
                  backgroundColor: stage.status === 'completed' 
                    ? 'var(--nils-stage-completed)' 
                    : 'var(--nils-border-subtle)',
                  flexShrink: 0,
                }}
              />
            )}
          </Fragment>
        );
      })}
    </Group>
  );
};
