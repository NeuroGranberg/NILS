import { Box, Text } from '@mantine/core';
import type { StageStatus } from '../../../types/stage';
import type { JobStatus } from '../../../types/job';
import {
  STAGE_STATUS_CONFIG,
  JOB_STATUS_CONFIG,
  type StatusConfig,
} from '../../../constants/status';

type StatusType = StageStatus | JobStatus;

interface StatusBadgeProps {
  /** The status to display */
  status: StatusType;
  /** Size of the badge */
  size?: 'xs' | 'sm' | 'md';
  /** Show the animated dot indicator */
  showDot?: boolean;
  /** Show the text label */
  showLabel?: boolean;
  /** Visual variant */
  variant?: 'badge' | 'pill' | 'minimal';
  /** Override the label text */
  label?: string;
}

const sizeConfig = {
  xs: { dotSize: 5, fontSize: '10px', padding: '2px 6px', gap: 4 },
  sm: { dotSize: 6, fontSize: '11px', padding: '4px 8px', gap: 5 },
  md: { dotSize: 8, fontSize: '12px', padding: '4px 10px', gap: 6 },
};

function getStatusConfig(status: StatusType): StatusConfig {
  if (status in STAGE_STATUS_CONFIG) {
    return STAGE_STATUS_CONFIG[status as StageStatus];
  }
  return JOB_STATUS_CONFIG[status as JobStatus];
}

/**
 * StatusBadge - Unified status indicator component.
 *
 * Displays a status with optional dot indicator and label.
 * Supports both StageStatus and JobStatus types.
 */
export const StatusBadge = ({
  status,
  size = 'sm',
  showDot = true,
  showLabel = true,
  variant = 'badge',
  label,
}: StatusBadgeProps) => {
  const config = getStatusConfig(status);
  const sizeStyles = sizeConfig[size];
  const isRunning = status === 'running';
  const displayLabel = label ?? config.label;

  // Minimal variant - just the dot
  if (variant === 'minimal') {
    return (
      <Box
        style={{
          width: sizeStyles.dotSize,
          height: sizeStyles.dotSize,
          borderRadius: '50%',
          backgroundColor: config.color,
          animation: isRunning ? 'pulse 2s infinite' : 'none',
          flexShrink: 0,
        }}
      />
    );
  }

  // Pill variant - no background, just inline display
  if (variant === 'pill') {
    return (
      <Box
        style={{
          display: 'inline-flex',
          alignItems: 'center',
          gap: sizeStyles.gap,
        }}
      >
        {showDot && (
          <Box
            style={{
              width: sizeStyles.dotSize,
              height: sizeStyles.dotSize,
              borderRadius: '50%',
              backgroundColor: config.color,
              animation: isRunning ? 'pulse 2s infinite' : 'none',
              flexShrink: 0,
            }}
          />
        )}
        {showLabel && (
          <Text
            size={size}
            c="var(--nils-text-tertiary)"
            tt="capitalize"
            style={{ fontSize: sizeStyles.fontSize }}
          >
            {displayLabel}
          </Text>
        )}
      </Box>
    );
  }

  // Badge variant - with background
  return (
    <Box
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: sizeStyles.gap,
        padding: sizeStyles.padding,
        borderRadius: 'var(--nils-radius-sm)',
        backgroundColor: config.bgColor,
      }}
    >
      {showDot && (
        <Box
          style={{
            width: sizeStyles.dotSize,
            height: sizeStyles.dotSize,
            borderRadius: '50%',
            backgroundColor: config.color,
            animation: isRunning ? 'pulse 2s infinite' : 'none',
            flexShrink: 0,
          }}
        />
      )}
      {showLabel && (
        <Text
          size={size}
          fw={500}
          c={config.color}
          style={{ fontSize: sizeStyles.fontSize }}
        >
          {displayLabel}
        </Text>
      )}
    </Box>
  );
};
