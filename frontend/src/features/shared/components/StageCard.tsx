import { Box, Button, Card, Group, Stack, Text } from '@mantine/core';
import { IconPlayerPlay, IconPlayerPause, IconRefresh } from '@tabler/icons-react';
import type { ReactNode } from 'react';
import type { StageSummary } from '../../../types';
import { formatDateTime } from '../../../utils/formatters';

const statusConfig: Record<StageSummary['status'], { color: string; bgColor: string; label: string }> = {
  idle: { color: 'var(--nils-stage-idle)', bgColor: 'rgba(110, 118, 129, 0.15)', label: 'Idle' },
  pending: { color: 'var(--nils-stage-pending)', bgColor: 'rgba(163, 113, 247, 0.15)', label: 'Pending' },
  running: { color: 'var(--nils-stage-running)', bgColor: 'rgba(88, 166, 255, 0.15)', label: 'Running' },
  completed: { color: 'var(--nils-stage-completed)', bgColor: 'rgba(63, 185, 80, 0.15)', label: 'Completed' },
  failed: { color: 'var(--nils-stage-failed)', bgColor: 'rgba(248, 81, 73, 0.15)', label: 'Failed' },
  paused: { color: 'var(--nils-stage-paused)', bgColor: 'rgba(210, 153, 34, 0.15)', label: 'Paused' },
  blocked: { color: 'var(--nils-stage-blocked)', bgColor: 'rgba(72, 79, 88, 0.15)', label: 'Blocked' },
};

interface StageCardProps {
  stage: StageSummary;
  disabled?: boolean;
  onRun?: () => void;
  onRetry?: () => void;
  onPause?: () => void;
  actionLabel?: string;
  children?: ReactNode;
  blockedReason?: string;
}

export const StageCard = ({
  stage,
  disabled,
  onRun,
  onRetry,
  onPause,
  actionLabel = 'Run stage',
  children,
  blockedReason,
}: StageCardProps) => {
  const status = statusConfig[stage.status];
  const isBlocked = stage.status === 'blocked';
  const reason = isBlocked ? blockedReason ?? 'Complete the previous stage to unlock this step.' : blockedReason;

  return (
    <Card
      padding="lg"
      style={{
        backgroundColor: 'var(--nils-bg-secondary)',
        border: '1px solid var(--nils-border-subtle)',
        borderRadius: 'var(--nils-radius-lg)',
      }}
    >
      <Stack gap="md">
        {/* Header */}
        <Group justify="space-between" align="flex-start">
          <Stack gap={4}>
            <Text fw={600} size="md" c="var(--nils-text-primary)">
              {stage.title}
            </Text>
            <Text size="sm" c="var(--nils-text-secondary)">
              {stage.description}
            </Text>
          </Stack>
          <Box
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              padding: '4px 10px',
              borderRadius: 'var(--nils-radius-sm)',
              backgroundColor: status.bgColor,
            }}
          >
            <Box
              style={{
                width: 6,
                height: 6,
                borderRadius: '50%',
                backgroundColor: status.color,
                animation: stage.status === 'running' ? 'pulse 2s infinite' : 'none',
              }}
            />
            <Text size="xs" fw={500} c={status.color}>
              {status.label}
            </Text>
          </Box>
        </Group>

        {/* Content */}
        {children && (
          <Stack gap="sm">
            {reason && isBlocked && (
              <Box
                p="sm"
                style={{
                  backgroundColor: 'var(--nils-bg-tertiary)',
                  borderRadius: 'var(--nils-radius-md)',
                  borderLeft: '3px solid var(--nils-stage-blocked)',
                }}
              >
                <Text size="xs" c="var(--nils-text-secondary)">
                  {reason}
                </Text>
              </Box>
            )}
            <fieldset disabled={isBlocked} style={{ border: 'none', padding: 0, margin: 0, minInlineSize: 'auto' }}>
              {children}
            </fieldset>
          </Stack>
        )}

        {/* Footer */}
        <Group justify="space-between" align="center" pt="sm" style={{ borderTop: '1px solid var(--nils-border-subtle)' }}>
          <Stack gap={0}>
            <Text size="xs" c="var(--nils-text-tertiary)">
              Last run
            </Text>
            <Text size="sm" c="var(--nils-text-primary)">
              {formatDateTime(stage.lastRunAt)}
            </Text>
          </Stack>

          <Group gap="xs">
            {onPause && stage.status === 'running' && (
              <Button
                size="xs"
                variant="default"
                leftSection={<IconPlayerPause size={14} />}
                onClick={onPause}
                styles={{
                  root: {
                    backgroundColor: 'var(--nils-bg-tertiary)',
                    borderColor: 'var(--nils-border)',
                  },
                }}
              >
                Pause
              </Button>
            )}
            {onRetry && stage.status === 'failed' && (
              <Button
                size="xs"
                variant="light"
                leftSection={<IconRefresh size={14} />}
                onClick={onRetry}
              >
                Retry
              </Button>
            )}
            {onRun && (
              <Button
                size="xs"
                leftSection={<IconPlayerPlay size={14} />}
                onClick={onRun}
                disabled={disabled || stage.status === 'running' || isBlocked}
                style={{
                  backgroundColor: disabled || stage.status === 'running' || isBlocked
                    ? 'var(--nils-bg-tertiary)'
                    : 'var(--nils-accent-primary)',
                  color: disabled || stage.status === 'running' || isBlocked
                    ? 'var(--nils-text-tertiary)'
                    : 'var(--nils-bg-primary)',
                }}
              >
                {actionLabel}
              </Button>
            )}
          </Group>
        </Group>
      </Stack>
    </Card>
  );
};
