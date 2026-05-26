/**
 * Inline "+ Add keyword" input. Enter or comma commits the value.
 */
import { ActionIcon, Group, TextInput, Tooltip } from '@mantine/core';
import { IconPlus } from '@tabler/icons-react';
import { useState, type KeyboardEvent } from 'react';

interface AddKeywordInputProps {
  onAdd: (keyword: string) => void;
  disabled?: boolean;
}

export const AddKeywordInput = ({ onAdd, disabled }: AddKeywordInputProps) => {
  const [value, setValue] = useState('');

  const commit = () => {
    const trimmed = value.trim();
    if (!trimmed) return;
    onAdd(value); // preserve original whitespace
    setValue('');
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' || e.key === ',') {
      e.preventDefault();
      commit();
    }
  };

  return (
    <Group gap="xs" wrap="nowrap">
      <TextInput
        size="sm"
        placeholder="Add keyword…"
        value={value}
        onChange={(e) => setValue(e.currentTarget.value)}
        onKeyDown={handleKeyDown}
        disabled={disabled}
        style={{ flex: 1 }}
        aria-label="Add new keyword"
      />
      <Tooltip label="Add">
        <ActionIcon
          variant="light"
          onClick={commit}
          disabled={disabled || !value.trim()}
          aria-label="Add keyword"
        >
          <IconPlus size={16} />
        </ActionIcon>
      </Tooltip>
    </Group>
  );
};
