/**
 * Session navigation + totals header for the Main Acquisition QC page.
 *
 * Includes a "Go to subject" text input that accepts either the canonical
 * ``subject_code`` or any alternative identifier. On Enter or click, we ask
 * the backend for the canonical code, then jump to the first session in the
 * current filter set that belongs to that subject.
 */

import { useState } from 'react';
import {
  ActionIcon,
  Group,
  Text,
  Stack,
  Badge,
  TextInput,
  Button,
  Tooltip,
  SegmentedControl,
  Select,
} from '@mantine/core';
import { IconChevronLeft, IconChevronRight, IconSearch } from '@tabler/icons-react';
import type { SessionSummary } from '../../api/main-acq';
import { resolveSubjectIdentifier } from '../../api/main-acq';
import type { ViewSize } from '../../pages/MainAcquisitionQCPage';

interface SessionHeaderProps {
  totalSubjects: number;
  totalSessions: number;
  totalStacks: number;
  session: SessionSummary | null;
  sessionIndex: number;
  sessions: SessionSummary[];
  onPrev: () => void;
  onNext: () => void;
  onJumpToSessionIndex: (index: number) => void;
  viewSize: ViewSize;
  onViewSizeChange: (size: ViewSize) => void;
  displayIdType: string;
  onDisplayIdTypeChange: (type: string) => void;
  availableIdTypes: string[];
  seenCount?: number;
}

export const SessionHeader = ({
  totalSubjects,
  totalSessions,
  totalStacks,
  session,
  sessionIndex,
  sessions,
  onPrev,
  onNext,
  onJumpToSessionIndex,
  viewSize,
  onViewSizeChange,
  displayIdType,
  onDisplayIdTypeChange,
  availableIdTypes,
  seenCount,
}: SessionHeaderProps) => {
  const canPrev = sessionIndex > 0;
  const canNext = sessionIndex < totalSessions - 1;

  const [jumpValue, setJumpValue] = useState('');
  const [jumpError, setJumpError] = useState<string | null>(null);
  const [resolving, setResolving] = useState(false);

  const handleJump = async () => {
    const q = jumpValue.trim();
    if (!q) return;
    setJumpError(null);
    setResolving(true);
    try {
      // First try a direct match against the session list (subject_code).
      let targetCode: string | null = null;
      const direct = sessions.find((s) => s.subject_code.toLowerCase() === q.toLowerCase());
      if (direct) {
        targetCode = direct.subject_code;
      } else {
        const resp = await resolveSubjectIdentifier(q);
        targetCode = resp.subject_code;
      }
      const idx = sessions.findIndex((s) => s.subject_code === targetCode);
      if (idx < 0) {
        setJumpError(`Subject "${targetCode}" has no sessions in the current filter`);
        return;
      }
      onJumpToSessionIndex(idx);
      setJumpValue('');
    } catch (e) {
      setJumpError(
        e instanceof Error && e.message ? e.message : `Subject "${q}" not found`
      );
    } finally {
      setResolving(false);
    }
  };

  return (
    <Stack gap={4}>
      <Group gap="xs">
        <Badge size="sm" variant="light" color="blue">
          {totalSubjects} subjects
        </Badge>
        <Badge size="sm" variant="light" color="teal">
          {totalSessions} sessions
        </Badge>
        <Badge size="sm" variant="light" color="grape">
          {totalStacks} stacks
        </Badge>
        {seenCount !== undefined && (
          <Badge size="sm" variant="light" color="green">
            {seenCount} seen
          </Badge>
        )}
      </Group>
      <Group gap="xs" align="center" wrap="nowrap">
        <ActionIcon
          variant="subtle"
          disabled={!canPrev}
          onClick={onPrev}
          size="md"
          aria-label="Previous session"
        >
          <IconChevronLeft size={18} />
        </ActionIcon>
        <Text fw={600} ff="monospace">
          {session
            ? `${session.display_id || session.subject_code} / ${session.study_date}`
            : totalSessions === 0
              ? 'No sessions match the current filter'
              : 'Loading…'}
        </Text>
        {totalSessions > 0 && (
          <Text size="sm" c="dimmed" ff="monospace">
            ({sessionIndex + 1} / {totalSessions})
          </Text>
        )}
        <ActionIcon
          variant="subtle"
          disabled={!canNext}
          onClick={onNext}
          size="md"
          aria-label="Next session"
        >
          <IconChevronRight size={18} />
        </ActionIcon>
        <Group gap={4} ml="lg" wrap="nowrap">
          <Tooltip label="Accepts subject_code or any alternative identifier" withinPortal>
            <TextInput
              size="xs"
              placeholder="Go to subject…"
              value={jumpValue}
              onChange={(e) => setJumpValue(e.currentTarget.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                  void handleJump();
                }
              }}
              leftSection={<IconSearch size={12} />}
              w={220}
              error={jumpError ?? undefined}
              disabled={resolving || totalSessions === 0}
            />
          </Tooltip>
          <Button
            size="compact-xs"
            variant="light"
            onClick={handleJump}
            disabled={!jumpValue.trim() || resolving || totalSessions === 0}
            loading={resolving}
          >
            Go
          </Button>
        </Group>
        {availableIdTypes.length > 1 && (
          <Group gap={4} wrap="nowrap">
            <Text size="xs" c="dimmed">ID:</Text>
            <Select
              size="xs"
              w={140}
              data={availableIdTypes.map((t) => ({
                value: t,
                label: t === 'code' ? 'Subject Code' : t,
              }))}
              value={displayIdType}
              onChange={(v) => onDisplayIdTypeChange(v || 'code')}
              allowDeselect={false}
            />
          </Group>
        )}
        <SegmentedControl
          size="xs"
          value={viewSize}
          onChange={(v) => onViewSizeChange(v as ViewSize)}
          data={[
            { label: 'Compact', value: 'compact' },
            { label: 'Medium', value: 'medium' },
            { label: 'Full', value: 'full' },
          ]}
        />
      </Group>
    </Stack>
  );
};
