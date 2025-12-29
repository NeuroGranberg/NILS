import { Box, Collapse, Group, Paper, Stack, Text, UnstyledButton } from '@mantine/core';
import { IconChevronDown, IconChevronRight } from '@tabler/icons-react';
import { useState, type ReactNode } from 'react';

interface SectionCardProps {
  /** Section title */
  title: string;
  /** Optional description text */
  description?: string;
  /** Content to render inside the section */
  children: ReactNode;
  /** Whether the section can be collapsed */
  collapsible?: boolean;
  /** Initial expanded state (only used if collapsible) */
  defaultExpanded?: boolean;
  /** Content to render in the header right side (e.g., actions) */
  headerRight?: ReactNode;
  /** Padding inside the section */
  padding?: 'none' | 'sm' | 'md' | 'lg';
  /** Whether to show a border */
  withBorder?: boolean;
  /** Optional icon or badge to show next to title */
  titleBadge?: ReactNode;
}

const paddingMap = {
  none: 0,
  sm: 'var(--nils-space-sm)',
  md: 'var(--nils-space-md)',
  lg: 'var(--nils-space-lg)',
};

/**
 * SectionCard - A consistent card component for form sections.
 *
 * Provides standard styling for section headers, optional collapsing,
 * and consistent spacing throughout the application.
 */
export const SectionCard = ({
  title,
  description,
  children,
  collapsible = false,
  defaultExpanded = true,
  headerRight,
  padding = 'md',
  withBorder = true,
  titleBadge,
}: SectionCardProps) => {
  const [expanded, setExpanded] = useState(defaultExpanded);

  const headerContent = (
    <Group justify="space-between" align="flex-start" wrap="nowrap">
      <Stack gap={2}>
        <Group gap="sm" align="center">
          <Text
            fw={600}
            size="sm"
            c="var(--nils-text-primary)"
            style={{ lineHeight: 1.3 }}
          >
            {title}
          </Text>
          {titleBadge}
        </Group>
        {description && (
          <Text size="xs" c="var(--nils-text-tertiary)">
            {description}
          </Text>
        )}
      </Stack>
      {(headerRight || collapsible) && (
        <Group gap="xs" wrap="nowrap">
          {headerRight}
          {collapsible && (
            <Box
              style={{
                color: 'var(--nils-text-tertiary)',
                transition: 'transform var(--nils-transition-fast)',
                transform: expanded ? 'rotate(0deg)' : 'rotate(-90deg)',
              }}
            >
              <IconChevronDown size={16} />
            </Box>
          )}
        </Group>
      )}
    </Group>
  );

  return (
    <Paper
      p={paddingMap[padding]}
      style={{
        backgroundColor: 'var(--nils-bg-secondary)',
        border: withBorder ? '1px solid var(--nils-border-subtle)' : 'none',
        borderRadius: 'var(--nils-radius-md)',
      }}
    >
      <Stack gap="md">
        {collapsible ? (
          <UnstyledButton
            onClick={() => setExpanded(!expanded)}
            style={{
              width: '100%',
              padding: padding === 'none' ? 'var(--nils-space-md)' : 0,
            }}
          >
            {headerContent}
          </UnstyledButton>
        ) : (
          headerContent
        )}

        {collapsible ? (
          <Collapse in={expanded}>
            <Box pt={padding === 'none' ? 'md' : 0}>{children}</Box>
          </Collapse>
        ) : (
          children
        )}
      </Stack>
    </Paper>
  );
};

interface FormFieldGroupProps {
  /** Group label */
  label: string;
  /** Optional description */
  description?: string;
  /** Form fields */
  children: ReactNode;
  /** Whether to show indented border on left */
  indent?: boolean;
  /** Gap between child elements */
  gap?: 'xs' | 'sm' | 'md';
}

/**
 * FormFieldGroup - A wrapper for related form fields.
 *
 * Used within SectionCard to group related inputs with
 * consistent spacing and optional indentation.
 */
export const FormFieldGroup = ({
  label,
  description,
  children,
  indent = false,
  gap = 'sm',
}: FormFieldGroupProps) => {
  const content = (
    <Stack gap={gap}>
      <Stack gap={2}>
        <Text size="sm" fw={500} c="var(--nils-text-secondary)">
          {label}
        </Text>
        {description && (
          <Text size="xs" c="var(--nils-text-tertiary)">
            {description}
          </Text>
        )}
      </Stack>
      {children}
    </Stack>
  );

  if (indent) {
    return (
      <Box
        ml="lg"
        pl="lg"
        style={{
          borderLeft: '1px solid var(--nils-border-subtle)',
        }}
      >
        {content}
      </Box>
    );
  }

  return content;
};

interface EmptyStateProps {
  /** Icon to display */
  icon?: ReactNode;
  /** Main message */
  title: string;
  /** Optional description */
  description?: string;
  /** Optional action button or link */
  action?: ReactNode;
}

/**
 * EmptyState - A consistent empty state display.
 *
 * Used when there's no data to show in a section or list.
 */
export const EmptyState = ({
  icon,
  title,
  description,
  action,
}: EmptyStateProps) => {
  return (
    <Stack
      align="center"
      justify="center"
      gap="md"
      py="xl"
      style={{ textAlign: 'center' }}
    >
      {icon && (
        <Box style={{ color: 'var(--nils-text-tertiary)' }}>{icon}</Box>
      )}
      <Stack gap="xs" align="center">
        <Text fw={600} size="md" c="var(--nils-text-primary)">
          {title}
        </Text>
        {description && (
          <Text size="sm" c="var(--nils-text-secondary)" maw={320}>
            {description}
          </Text>
        )}
      </Stack>
      {action}
    </Stack>
  );
};
