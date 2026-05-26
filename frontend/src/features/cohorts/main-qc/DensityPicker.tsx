import { SegmentedControl } from '@mantine/core';

import type { Density } from './types';

interface DensityPickerProps {
  value: Density;
  onChange: (next: Density) => void;
}

export const DensityPicker = ({ value, onChange }: DensityPickerProps) => (
  <SegmentedControl
    size="xs"
    value={value}
    onChange={(v) => onChange(v as Density)}
    data={[
      { label: 'Compact', value: 'compact' },
      { label: 'Medium', value: 'medium' },
      { label: 'Comfortable', value: 'comfortable' },
    ]}
    aria-label="Cell density"
    data-testid="density-picker"
  />
);
