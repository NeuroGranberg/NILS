import {
  Checkbox,
  NumberInput,
  Radio,
  Select,
  Stack,
  TagsInput,
  TextInput,
} from '@mantine/core';
import { memo, useMemo } from 'react';
import type { DescriptorInput, FormValue } from '../types';

interface SchemaFieldProps {
  input: DescriptorInput;
  value: FormValue;
  onChange: (v: FormValue) => void;
}

const isRequired = (input: DescriptorInput): boolean => input.optional === false;

const choicesToData = (choices: unknown[]): string[] => choices.map((c) => String(c));

const toStringArray = (value: FormValue): string[] => {
  if (Array.isArray(value)) return value.map((v) => String(v));
  if (value == null || value === '') return [];
  return [String(value)];
};

/**
 * SchemaField - renders a single descriptor input as the appropriate Mantine
 * control per the §15.2 mapping table. It is a pure function of `input`; unknown
 * `type` values degrade to a TextInput and the value is preserved verbatim.
 */
const SchemaFieldImpl = ({ input, value, onChange }: SchemaFieldProps) => {
  const required = isRequired(input);
  const description = input.description ?? undefined;
  const choices = useMemo(
    () => (Array.isArray(input['value-choices']) ? input['value-choices'] : null),
    [input],
  );

  // list:true (any base type except Flag) -> repeatable multi-value control.
  if (input.list && input.type !== 'Flag') {
    const isNumberList = input.type === 'Number';
    const arrayValue = toStringArray(value);
    return (
      <TagsInput
        label={input.name}
        description={description}
        required={required}
        withAsterisk={required}
        value={arrayValue}
        placeholder="Type a value and press Enter"
        onChange={(next) => {
          if (isNumberList) {
            const nums = next
              .map((s) => Number(s))
              .filter((n) => !Number.isNaN(n));
            onChange(nums as number[]);
          } else {
            onChange(next);
          }
        }}
      />
    );
  }

  switch (input.type) {
    case 'Flag':
      return (
        <Checkbox
          label={input.name}
          description={description}
          checked={Boolean(value)}
          onChange={(e) => onChange(e.currentTarget.checked)}
        />
      );

    case 'Number': {
      const numValue =
        value === null || value === undefined || value === ''
          ? ''
          : (value as number | string);
      return (
        <NumberInput
          label={input.name}
          description={description}
          required={required}
          withAsterisk={required}
          value={numValue as number | string}
          min={input.minimum ?? undefined}
          max={input.maximum ?? undefined}
          allowDecimal={input.integer !== true}
          step={input.integer ? 1 : undefined}
          onChange={(v) => {
            if (v === '' || v === null || v === undefined) {
              onChange(null);
            } else {
              onChange(typeof v === 'number' ? v : Number(v));
            }
          }}
        />
      );
    }

    case 'String': {
      if (choices && choices.length > 0) {
        const data = choicesToData(choices);
        // few choices -> Radio.Group; many -> Select.
        if (data.length <= 4) {
          return (
            <Radio.Group
              label={input.name}
              description={description}
              required={required}
              withAsterisk={required}
              value={value == null ? '' : String(value)}
              onChange={(v) => onChange(v)}
            >
              <Stack gap="xs" mt="xs">
                {data.map((c) => (
                  <Radio key={c} value={c} label={c} />
                ))}
              </Stack>
            </Radio.Group>
          );
        }
        return (
          <Select
            label={input.name}
            description={description}
            required={required}
            withAsterisk={required}
            data={data}
            value={value == null ? null : String(value)}
            onChange={(v) => onChange(v)}
            clearable={!required}
            searchable
          />
        );
      }
      return (
        <TextInput
          label={input.name}
          description={description}
          required={required}
          withAsterisk={required}
          value={value == null ? '' : String(value)}
          onChange={(e) => onChange(e.currentTarget.value)}
        />
      );
    }

    case 'File':
      // v1: path string input; native file picker deferred.
      return (
        <TextInput
          label={input.name}
          description={description ?? 'Path'}
          required={required}
          withAsterisk={required}
          placeholder="/path/to/file"
          value={value == null ? '' : String(value)}
          onChange={(e) => onChange(e.currentTarget.value)}
        />
      );

    default:
      // Graceful degradation: unknown type -> base TextInput, value carried verbatim.
      return (
        <TextInput
          label={input.name}
          description={description}
          required={required}
          withAsterisk={required}
          value={value == null ? '' : String(value)}
          onChange={(e) => onChange(e.currentTarget.value)}
        />
      );
  }
};

export const SchemaField = memo(SchemaFieldImpl);
