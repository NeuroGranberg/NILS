import { SegmentedControl } from '@mantine/core';

import type { AxisDisplayMode } from './types';

interface AxisModePickerProps {
  value: AxisDisplayMode;
  onChange: (next: AxisDisplayMode) => void;
}

export const AxisModePicker = ({ value, onChange }: AxisModePickerProps) => (
  <SegmentedControl
    size="xs"
    value={value}
    onChange={(v) => onChange(v as AxisDisplayMode)}
    data={[
      { label: 'T1w', value: 't1w' },
      { label: 'T2w-FLAIR', value: 'flair' },
      { label: 'Both', value: 'both' },
    ]}
    aria-label="Axis display mode"
    data-testid="axis-mode-picker"
  />
);
