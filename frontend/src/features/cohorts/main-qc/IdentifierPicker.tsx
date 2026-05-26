import { Select } from '@mantine/core';

interface IdentifierPickerProps {
  value: string;
  options: string[];
  onChange: (next: string) => void;
}

const PRETTY: Record<string, string> = {
  code: 'Subject code',
};

export const IdentifierPicker = ({ value, options, onChange }: IdentifierPickerProps) => {
  const data = options.map((v) => ({ value: v, label: PRETTY[v] ?? v }));
  return (
    <Select
      size="xs"
      label={null}
      value={value}
      onChange={(v) => v && onChange(v)}
      data={data}
      checkIconPosition="right"
      allowDeselect={false}
      style={{ minWidth: 160 }}
      aria-label="Display identifier"
      data-testid="identifier-picker"
    />
  );
};
