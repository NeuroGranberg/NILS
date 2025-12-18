import { Badge, Card, Group, Stack, Text, Tooltip, ThemeIcon, Button, Divider } from '@mantine/core';
import {
  IconUsers,
  IconCalendarStats,
  IconStack2,
  IconClock,
  IconShieldCheck,
  IconChecklist,
  IconDatabase,
} from '@tabler/icons-react';
import type { Cohort } from '../../../types';
import { formatDateTime } from '../../../utils/formatters';

interface QCCohortCardProps {
  cohort: Cohort;
  onQCClick: () => void;
  onViewerClick: () => void;
}

export const QCCohortCard = ({ cohort, onQCClick, onViewerClick }: QCCohortCardProps) => {
  return (
    <Card
      padding="sm"
      radius="md"
      withBorder
      style={{
        backgroundColor: 'var(--nils-bg-secondary)',
        transition: 'all 0.2s ease',
        display: 'flex',
        flexDirection: 'column',
      }}
      styles={{
        root: {
          '&:hover': {
            borderColor: 'var(--nils-accent-primary)',
            transform: 'translateY(-2px)',
            boxShadow: '0 4px 12px rgba(0, 0, 0, 0.1)',
          },
        },
      }}
    >
      <Stack gap="xs" style={{ flex: 1 }}>
        {/* Header */}
        <Group justify="space-between" align="flex-start" wrap="nowrap">
          <Stack gap={2} style={{ flex: 1, minWidth: 0 }}>
            <Text fw={700} size="md" c="var(--nils-text-primary)" truncate>
              {cohort.name}
            </Text>
            {cohort.description && (
              <Text size="xs" c="var(--nils-text-tertiary)" lineClamp={1}>
                {cohort.description}
              </Text>
            )}
          </Stack>
          {cohort.anonymization_enabled && (
            <Tooltip label="PHI Protected (Anonymized)">
              <ThemeIcon variant="light" color="green" size="sm" radius="sm">
                <IconShieldCheck size={14} />
              </ThemeIcon>
            </Tooltip>
          )}
        </Group>

        {/* Stats Row */}
        <Card.Section
          inheritPadding
          py="xs"
          bg="var(--nils-bg-tertiary)"
          style={{ marginTop: 4, marginBottom: 4 }}
        >
          <Group gap="md" wrap="nowrap">
            <Group gap={6}>
              <ThemeIcon variant="light" color="blue" size="md" radius="md">
                <IconUsers size={16} />
              </ThemeIcon>
              <Stack gap={0}>
                <Text size="sm" fw={700} c="var(--nils-text-primary)" lh={1.2}>
                  {cohort.total_subjects ?? 0}
                </Text>
                <Text size="xs" c="var(--nils-text-tertiary)" lh={1}>
                  Subjects
                </Text>
              </Stack>
            </Group>

            <Divider orientation="vertical" />

            <Group gap={6}>
              <ThemeIcon variant="light" color="violet" size="md" radius="md">
                <IconCalendarStats size={16} />
              </ThemeIcon>
              <Stack gap={0}>
                <Text size="sm" fw={700} c="var(--nils-text-primary)" lh={1.2}>
                  {cohort.total_sessions ?? 0}
                </Text>
                <Text size="xs" c="var(--nils-text-tertiary)" lh={1}>
                  Sessions
                </Text>
              </Stack>
            </Group>

            <Divider orientation="vertical" />

            <Group gap={6}>
              <ThemeIcon variant="light" color="orange" size="md" radius="md">
                <IconStack2 size={16} />
              </ThemeIcon>
              <Stack gap={0}>
                <Text size="sm" fw={700} c="var(--nils-text-primary)" lh={1.2}>
                  {cohort.total_series ?? 0}
                </Text>
                <Text size="xs" c="var(--nils-text-tertiary)" lh={1}>
                  Stacks
                </Text>
              </Stack>
            </Group>
          </Group>
        </Card.Section>

        {/* Tags & Time */}
        <Group justify="space-between" align="center">
          <Group gap={4}>
            {cohort.tags?.slice(0, 2).map((tag) => (
              <Badge key={tag} size="xs" variant="outline" color="gray">
                {tag}
              </Badge>
            ))}
          </Group>
          <Group gap={4}>
            <IconClock size={12} color="var(--nils-text-tertiary)" />
            <Text size="xs" c="var(--nils-text-tertiary)" style={{ fontSize: '10px' }}>
              {formatDateTime(cohort.updated_at)}
            </Text>
          </Group>
        </Group>
      </Stack>

      <Divider my="sm" color="var(--nils-border-subtle)" />

      {/* Actions - Only QC and Viewer */}
      <Group grow gap="xs">
        <Button
          variant="light"
          color="blue"
          size="xs"
          leftSection={<IconChecklist size={14} />}
          onClick={(e) => {
            e.stopPropagation();
            onQCClick();
          }}
        >
          QC
        </Button>
        <Button
          variant="light"
          color="violet"
          size="xs"
          leftSection={<IconDatabase size={14} />}
          onClick={(e) => {
            e.stopPropagation();
            onViewerClick();
          }}
        >
          Viewer
        </Button>
      </Group>
    </Card>
  );
};
