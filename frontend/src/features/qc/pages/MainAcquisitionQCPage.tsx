/**
 * Main Acquisition Selection QC (MASQC).
 *
 * Lets reviewers walk through a cohort one session at a time, filter to a
 * class of acquisitions, see matching stacks bundled by same-acquisition
 * identity, and tag each stack as MAIN / PRE / POST. Writes are auto-saved
 * server-side.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import {
  ActionIcon,
  Badge,
  Button,
  Center,
  Group,
  Loader,
  Menu,
  Paper,
  Stack,
  Text,
  TextInput,
  Title,
  Tooltip,
} from '@mantine/core';
import { useLocalStorage } from '@mantine/hooks';
import { IconArrowLeft, IconBookmark, IconCheck, IconPlus, IconTrash } from '@tabler/icons-react';
import type { Cohort } from '../../../types';
import { FilterBar } from '../components/main-acq/FilterBar';
import { SessionHeader } from '../components/main-acq/SessionHeader';
import { BundleCard } from '../components/main-acq/BundleCard';
import type { FilterState, SavedMASQCSession } from '../api/main-acq';
import {
  FACETS,
  fetchMainAcqBundles,
  mainAcqKeys,
  useCreateSavedSession,
  useDeleteSavedSession,
  useMainAcqBundles,
  useMainAcqSessions,
  useSavedMASQCSessions,
  useUpdateSavedSession,
} from '../api/main-acq';

interface Props {
  cohort: Cohort;
  onBack: () => void;
}

const emptyFilters = (): FilterState => {
  const out: FilterState = {};
  FACETS.forEach((f) => {
    out[f] = null;
  });
  return out;
};

export type ViewSize = 'compact' | 'medium' | 'full';

export const VIEW_SIZE_COLS: Record<ViewSize, Record<string, number>> = {
  compact: { base: 2, sm: 3, md: 4, lg: 5 },
  medium: { base: 1, sm: 2, md: 3 },
  full: { base: 1, sm: 2 },
};

export const MainAcquisitionQCPage = ({ cohort, onBack }: Props) => {
  // Filters committed via the Apply button. Anything below only reacts to THIS.
  const [filters, setFilters] = useState<FilterState>(emptyFilters);
  const [sessionIndex, setSessionIndex] = useState(0);
  const [viewSize, setViewSize] = useLocalStorage<ViewSize>({
    key: 'masqc-view-size',
    defaultValue: 'medium',
  });
  const [displayIdType, setDisplayIdType] = useLocalStorage<string>({
    key: 'qc-viewer-display-id-type',
    defaultValue: 'code',
  });
  const [includeAccel, setIncludeAccel] = useLocalStorage<boolean>({
    key: 'qc-include-accel-in-name',
    defaultValue: true,
  });
  const [onlyMultiStack, setOnlyMultiStack] = useLocalStorage<boolean>({
    key: 'masqc-only-multi-stack',
    defaultValue: false,
  });


  // --- Saved sessions ---
  const [activeSavedSession, setActiveSavedSession] = useState<SavedMASQCSession | null>(null);
  const [newSessionName, setNewSessionName] = useState('');
  const { data: savedSessions } = useSavedMASQCSessions(cohort.id);
  const createSession = useCreateSavedSession();
  const updateSession = useUpdateSavedSession();
  const deleteSession = useDeleteSavedSession();

  // Track seen indices for the active session (state, not ref, so the
  // "X seen" badge re-renders when we restore a saved session).
  const [seen, setSeen] = useState<Set<number>>(new Set());

  // Suppresses the implicit "reset to session 0" that would otherwise fire
  // after a saved-session load updates filters / display settings.
  const justRestoredRef = useRef(false);

  // Auto-save position + flags when active saved session is dirty (debounced).
  const saveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => {
    if (!activeSavedSession) return;
    setSeen((prev) => {
      if (prev.has(sessionIndex)) return prev;
      const next = new Set(prev);
      next.add(sessionIndex);
      return next;
    });
    if (saveTimerRef.current) clearTimeout(saveTimerRef.current);
    saveTimerRef.current = setTimeout(() => {
      // Read latest seen via functional setter to avoid stale closure.
      setSeen((current) => {
        updateSession.mutate({
          sessionId: activeSavedSession.id,
          cohortId: cohort.id,
          session_index: sessionIndex,
          seen_indices: Array.from(current).sort((a, b) => a - b),
          display_id_type: displayIdType,
          only_multi_stack: onlyMultiStack,
        });
        return current;
      });
    }, 1000);
    return () => { if (saveTimerRef.current) clearTimeout(saveTimerRef.current); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionIndex, activeSavedSession?.id, displayIdType, onlyMultiStack]);

  const handleLoadSavedSession = useCallback((s: SavedMASQCSession) => {
    // Block the "user changed a filter" reset path during this restore.
    justRestoredRef.current = true;
    setActiveSavedSession(s);
    setFilters(s.filters || emptyFilters());
    if (s.display_id_type) setDisplayIdType(s.display_id_type);
    setOnlyMultiStack(!!s.only_multi_stack);
    setSessionIndex(s.session_index || 0);
    setSeen(new Set(s.seen_indices || []));
    // Release the guard after the React batch + downstream effects settle.
    queueMicrotask(() => { justRestoredRef.current = false; });
  }, [setDisplayIdType, setOnlyMultiStack]);

  const handleCreateSession = useCallback(() => {
    const name = newSessionName.trim();
    if (!name) return;
    createSession.mutate(
      {
        cohortId: cohort.id,
        name,
        filters,
        session_index: sessionIndex,
        display_id_type: displayIdType,
        only_multi_stack: onlyMultiStack,
      },
      {
        onSuccess: (data) => {
          setActiveSavedSession(data);
          setSeen(new Set());
          setNewSessionName('');
        },
      },
    );
  }, [cohort.id, createSession, filters, newSessionName, sessionIndex, displayIdType, onlyMultiStack]);

  const handleApplyFilters = useCallback((next: FilterState) => {
    setFilters(next);
    setSessionIndex(0);
  }, []);

  const handleOnlyMultiStackChange = useCallback((v: boolean) => {
    setOnlyMultiStack(v);
    if (!justRestoredRef.current) setSessionIndex(0);
  }, [setOnlyMultiStack]);

  const handleDisplayIdTypeChange = useCallback((v: string) => {
    setDisplayIdType(v);
    if (!justRestoredRef.current) setSessionIndex(0);
  }, [setDisplayIdType]);

  const { data: sessionsData, isLoading: sessionsLoading } = useMainAcqSessions(
    cohort.id,
    filters,
    displayIdType,
    onlyMultiStack,
  );
  const {
    data: bundlesData,
    isLoading: bundlesLoading,
    isFetching: bundlesFetching,
  } = useMainAcqBundles(cohort.id, filters, sessionIndex, displayIdType, onlyMultiStack);

  const totalSessions = sessionsData?.total_sessions ?? 0;

  // Prefetch the neighbour sessions (N-1, N+1) so Prev/Next lands instantly.
  // Runs only after the current bundles query has settled, gated by index
  // bounds. Uses the same queryKey shape as `useMainAcqBundles` so a click
  // hits the React-Query cache instead of refetching.
  const queryClient = useQueryClient();
  useEffect(() => {
    if (bundlesLoading || totalSessions === 0) return;
    const targets: number[] = [];
    if (sessionIndex - 1 >= 0) targets.push(sessionIndex - 1);
    if (sessionIndex + 1 < totalSessions) targets.push(sessionIndex + 1);
    targets.forEach((idx) => {
      queryClient.prefetchQuery({
        queryKey: mainAcqKeys.bundles(cohort.id, filters, idx, displayIdType, onlyMultiStack),
        queryFn: () =>
          fetchMainAcqBundles(cohort.id, filters, idx, displayIdType, onlyMultiStack),
        staleTime: 60_000,
      });
    });
  }, [
    queryClient,
    cohort.id,
    filters,
    sessionIndex,
    displayIdType,
    onlyMultiStack,
    totalSessions,
    bundlesLoading,
  ]);

  const currentSession = useMemo(() => {
    if (!sessionsData) return null;
    return sessionsData.sessions[sessionIndex] ?? null;
  }, [sessionsData, sessionIndex]);

  const onPrev = useCallback(() => {
    setSessionIndex((i) => Math.max(0, i - 1));
  }, []);

  const onNext = useCallback(() => {
    setSessionIndex((i) => Math.min(totalSessions - 1, i + 1));
  }, [totalSessions]);

  // Global arrow-key navigation (skipped when the focus is inside an input).
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement | null;
      const tag = target?.tagName?.toLowerCase();
      if (tag === 'input' || tag === 'textarea' || tag === 'select' || target?.isContentEditable) {
        return;
      }
      if (e.key === 'ArrowLeft') {
        e.preventDefault();
        onPrev();
      } else if (e.key === 'ArrowRight') {
        e.preventDefault();
        onNext();
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onPrev, onNext]);

  return (
    <Stack gap="md">
      <Group gap="md" align="center">
        <ActionIcon variant="subtle" size="lg" onClick={onBack} aria-label="Back">
          <IconArrowLeft size={22} />
        </ActionIcon>
        <Title order={2} c="var(--nils-text-primary)">
          Main Acquisition QC
        </Title>
        <Text c="dimmed" size="sm">
          Cohort: {cohort.name}
        </Text>
      </Group>

      <Paper withBorder p="sm" radius="md">
        <FilterBar
          cohortId={cohort.id}
          appliedFilters={filters}
          onApply={handleApplyFilters}
          includeAcceleration={includeAccel}
          onIncludeAccelerationChange={setIncludeAccel}
          onlyMultiStack={onlyMultiStack}
          onOnlyMultiStackChange={handleOnlyMultiStackChange}
        />
      </Paper>

      <Paper withBorder p="xs" radius="md">
        <Group gap="xs" align="center">
          <IconBookmark size={16} color="var(--mantine-color-dimmed)" />
          {savedSessions && savedSessions.length > 0 ? (
            <Menu shadow="md" width={240}>
              <Menu.Target>
                <Button size="compact-xs" variant={activeSavedSession ? 'filled' : 'light'}>
                  {activeSavedSession ? activeSavedSession.name : 'Load Session'}
                </Button>
              </Menu.Target>
              <Menu.Dropdown>
                {savedSessions.map((s) => (
                  <Menu.Item
                    key={s.id}
                    onClick={() => handleLoadSavedSession(s)}
                    rightSection={
                      <Group gap={4}>
                        {s.seen_indices.length > 0 && (
                          <Badge size="xs" variant="light" color="green">
                            {s.seen_indices.length} seen
                          </Badge>
                        )}
                        <ActionIcon
                          size="xs"
                          variant="subtle"
                          color="red"
                          onClick={(e) => {
                            e.stopPropagation();
                            deleteSession.mutate({ sessionId: s.id, cohortId: cohort.id });
                            if (activeSavedSession?.id === s.id) setActiveSavedSession(null);
                          }}
                        >
                          <IconTrash size={12} />
                        </ActionIcon>
                      </Group>
                    }
                  >
                    <Group gap={4}>
                      {activeSavedSession?.id === s.id && <IconCheck size={14} />}
                      <Text size="sm" truncate>{s.name}</Text>
                    </Group>
                  </Menu.Item>
                ))}
              </Menu.Dropdown>
            </Menu>
          ) : (
            <Text size="xs" c="dimmed">No saved sessions</Text>
          )}
          <Group gap={4} ml="auto">
            <TextInput
              size="xs"
              placeholder="Session nameu2026"
              value={newSessionName}
              onChange={(e) => setNewSessionName(e.currentTarget.value)}
              onKeyDown={(e) => { if (e.key === 'Enter') handleCreateSession(); }}
              w={160}
            />
            <Tooltip label="Save current filters & position as a named session">
              <Button
                size="compact-xs"
                variant="light"
                leftSection={<IconPlus size={14} />}
                onClick={handleCreateSession}
                disabled={!newSessionName.trim()}
                loading={createSession.isPending}
              >
                Save
              </Button>
            </Tooltip>
          </Group>
          {activeSavedSession && (
            <Button
              size="compact-xs"
              variant="subtle"
              color="gray"
              onClick={() => setActiveSavedSession(null)}
            >
              Detach
            </Button>
          )}
        </Group>
      </Paper>

      <Paper withBorder p="sm" radius="md">
        <SessionHeader
          totalSubjects={sessionsData?.total_subjects ?? 0}
          totalSessions={totalSessions}
          totalStacks={sessionsData?.total_stacks ?? 0}
          session={currentSession}
          sessionIndex={sessionIndex}
          sessions={sessionsData?.sessions ?? []}
          onPrev={onPrev}
          onNext={onNext}
          onJumpToSessionIndex={setSessionIndex}
          viewSize={viewSize}
          onViewSizeChange={setViewSize}
          displayIdType={displayIdType}
          onDisplayIdTypeChange={handleDisplayIdTypeChange}
          availableIdTypes={sessionsData?.available_id_types ?? ['code']}
          seenCount={activeSavedSession ? seen.size : undefined}
        />
      </Paper>

      {sessionsLoading && (
        <Center py="lg">
          <Loader />
        </Center>
      )}

      {!sessionsLoading && totalSessions === 0 && (
        <Paper withBorder p="xl" radius="md">
          <Center>
            <Text c="dimmed">No sessions match the current filter.</Text>
          </Center>
        </Paper>
      )}

      {totalSessions > 0 && (
        <Stack gap="md" pos="relative">
          {bundlesLoading && (
            <Center py="lg">
              <Loader />
            </Center>
          )}
          {!bundlesLoading && bundlesData && bundlesData.bundles.length === 0 && (
            <Paper withBorder p="lg" radius="md">
              <Text c="dimmed">No matching stacks in this session.</Text>
            </Paper>
          )}
          {bundlesData?.bundles.map((b) => (
            <BundleCard key={b.bundle_id} bundle={b} viewSize={viewSize} includeAcceleration={includeAccel} />
          ))}
          {bundlesFetching && !bundlesLoading && (
            <Text size="xs" c="dimmed">
              Refreshing…
            </Text>
          )}
        </Stack>
      )}
    </Stack>
  );
};
