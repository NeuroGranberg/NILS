/**
 * RunStatusBadge - maps a RunStatus (or unknown status string) to a colored
 * Mantine Badge. Used in RunsList rows and the RunDetailPanel header.
 */
import { Badge } from '@mantine/core';
import type { RunStatus } from '../types';

interface RunStatusBadgeProps {
  status: RunStatus | string;
}

const STATUS_COLOR: Record<string, string> = {
  completed: 'green',
  completed_with_warnings: 'yellow',
  failed: 'red',
  canceled: 'gray',
  running: 'blue',
  materializing: 'blue',
  launching: 'blue',
  pending: 'violet',
};

export const RunStatusBadge = ({ status }: RunStatusBadgeProps) => {
  const color = STATUS_COLOR[status] ?? 'gray';
  return (
    <Badge color={color} variant="light">
      {status.toUpperCase()}
    </Badge>
  );
};
