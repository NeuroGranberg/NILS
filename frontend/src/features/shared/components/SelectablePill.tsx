import { Box, Text, UnstyledButton } from '@mantine/core';
import type { ReactNode } from 'react';

interface SelectablePillProps {
  /** Whether the pill is currently selected */
  selected: boolean;
  /** Click handler to toggle selection */
  onClick: () => void;
  /** Whether the pill is disabled */
  disabled?: boolean;
  /** Content to display in the pill */
  children: ReactNode;
  /** Optional icon or indicator to show on the left */
  leftSection?: ReactNode;
  /** Optional icon or indicator to show on the right */
  rightSection?: ReactNode;
  /** Size variant */
  size?: 'sm' | 'md' | 'lg';
  /** Show a colored dot indicator based on selection state */
  showDot?: boolean;
  /** Custom color for the selected state (CSS variable or color) */
  selectedColor?: string;
}

const sizeConfig = {
  sm: {
    padding: '4px 10px',
    fontSize: '12px',
    height: '28px',
    dotSize: 6,
    gap: 6,
  },
  md: {
    padding: '6px 12px',
    fontSize: '13px',
    height: '32px',
    dotSize: 8,
    gap: 8,
  },
  lg: {
    padding: '8px 16px',
    fontSize: '14px',
    height: '36px',
    dotSize: 8,
    gap: 8,
  },
};

/**
 * SelectablePill - A toggleable pill/chip component.
 *
 * Used for multi-select options like modalities, tags, or filters.
 * Provides consistent styling for selected/unselected states.
 */
export const SelectablePill = ({
  selected,
  onClick,
  disabled = false,
  children,
  leftSection,
  rightSection,
  size = 'md',
  showDot = false,
  selectedColor = 'var(--nils-accent-primary)',
}: SelectablePillProps) => {
  const sizeStyles = sizeConfig[size];

  return (
    <UnstyledButton
      onClick={onClick}
      disabled={disabled}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        gap: sizeStyles.gap,
        height: sizeStyles.height,
        padding: sizeStyles.padding,
        borderRadius: 'var(--nils-radius-md)',
        fontSize: sizeStyles.fontSize,
        fontWeight: 500,
        cursor: disabled ? 'not-allowed' : 'pointer',
        transition: 'all var(--nils-transition-fast)',
        border: selected
          ? `1px solid ${selectedColor}`
          : '1px solid var(--nils-border)',
        backgroundColor: selected
          ? `color-mix(in srgb, ${selectedColor} 15%, transparent)`
          : 'var(--nils-bg-tertiary)',
        color: selected ? selectedColor : 'var(--nils-text-secondary)',
        opacity: disabled ? 0.5 : 1,
      }}
    >
      {showDot && (
        <Box
          style={{
            width: sizeStyles.dotSize,
            height: sizeStyles.dotSize,
            borderRadius: '50%',
            backgroundColor: selected
              ? selectedColor
              : 'var(--nils-text-tertiary)',
            flexShrink: 0,
            transition: 'background-color var(--nils-transition-fast)',
          }}
        />
      )}
      {leftSection}
      <Text
        component="span"
        style={{
          fontSize: sizeStyles.fontSize,
          fontWeight: 500,
          lineHeight: 1,
        }}
      >
        {children}
      </Text>
      {rightSection}
    </UnstyledButton>
  );
};

interface SelectablePillGroupProps {
  /** Available options */
  options: Array<{ value: string; label: string; disabled?: boolean }>;
  /** Currently selected values */
  selected: string[];
  /** Handler when selection changes */
  onChange: (selected: string[]) => void;
  /** Size variant for all pills */
  size?: 'sm' | 'md' | 'lg';
  /** Show dot indicators */
  showDots?: boolean;
  /** Allow multiple selections */
  multiple?: boolean;
}

/**
 * SelectablePillGroup - A group of selectable pills.
 *
 * Manages selection state for a set of options.
 */
export const SelectablePillGroup = ({
  options,
  selected,
  onChange,
  size = 'md',
  showDots = false,
  multiple = true,
}: SelectablePillGroupProps) => {
  const handleClick = (value: string) => {
    if (multiple) {
      if (selected.includes(value)) {
        onChange(selected.filter((v) => v !== value));
      } else {
        onChange([...selected, value]);
      }
    } else {
      onChange([value]);
    }
  };

  return (
    <Box
      style={{
        display: 'flex',
        flexWrap: 'wrap',
        gap: 'var(--nils-space-sm)',
      }}
    >
      {options.map((option) => (
        <SelectablePill
          key={option.value}
          selected={selected.includes(option.value)}
          onClick={() => handleClick(option.value)}
          disabled={option.disabled}
          size={size}
          showDot={showDots}
        >
          {option.label}
        </SelectablePill>
      ))}
    </Box>
  );
};
