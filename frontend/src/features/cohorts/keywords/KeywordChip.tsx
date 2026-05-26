/**
 * One keyword rendered as a dismissible chip.
 *
 * Visual states (spec section 3 UI visual states):
 *   default  - neutral, × removes (default -> removed)
 *   added    - accent green, × removes it completely (added -> gone)
 *   removed  - strikethrough grey, ↻ restores
 */
import { ActionIcon, Badge, Group, Tooltip } from '@mantine/core';
import { IconRotate2, IconX } from '@tabler/icons-react';
import type { ChipState } from './types';

interface KeywordChipProps {
  keyword: string;
  state: ChipState;
  onRemove: () => void;
  onRestore: () => void;
}

const STATE_STYLES: Record<ChipState, { bg: string; color: string; border: string }> = {
  default: {
    bg: 'var(--nils-bg-tertiary)',
    color: 'var(--nils-text-secondary)',
    border: 'var(--nils-border-subtle)',
  },
  added: {
    bg: 'rgba(63, 185, 80, 0.18)',
    color: 'var(--nils-success)',
    border: 'rgba(63, 185, 80, 0.45)',
  },
  removed: {
    bg: 'transparent',
    color: 'var(--nils-text-tertiary)',
    border: 'var(--nils-border-subtle)',
  },
};

export const KeywordChip = ({
  keyword,
  state,
  onRemove,
  onRestore,
}: KeywordChipProps) => {
  const style = STATE_STYLES[state];
  const display = keyword.length === 0 ? '\u00a0' : keyword;

  return (
    <Badge
      variant="outline"
      radius="sm"
      size="lg"
      styles={{
        root: {
          backgroundColor: style.bg,
          color: style.color,
          borderColor: style.border,
          paddingLeft: 10,
          paddingRight: 4,
          textTransform: 'none',
          fontWeight: 500,
          fontFamily: 'var(--nils-font-mono, monospace)',
          letterSpacing: 0,
        },
      }}
      rightSection={
        state === 'removed' ? (
          <Tooltip label="Restore default" withArrow>
            <ActionIcon
              variant="subtle"
              size="xs"
              color="gray"
              onClick={onRestore}
              aria-label={`Restore ${keyword}`}
            >
              <IconRotate2 size={12} />
            </ActionIcon>
          </Tooltip>
        ) : (
          <Tooltip
            label={state === 'added' ? 'Remove' : 'Remove (mark for deletion)'}
            withArrow
          >
            <ActionIcon
              variant="subtle"
              size="xs"
              color="gray"
              onClick={onRemove}
              aria-label={`Remove ${keyword}`}
            >
              <IconX size={12} />
            </ActionIcon>
          </Tooltip>
        )
      }
    >
      <Group gap={6} wrap="nowrap">
        <span
          style={{
            textDecoration: state === 'removed' ? 'line-through' : 'none',
          }}
        >
          {display}
        </span>
      </Group>
    </Badge>
  );
};
