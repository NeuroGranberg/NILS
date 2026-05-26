/**
 * Per-session pick modal — image-tile edition.
 *
 * Replaces the previous table view with a grouped grid of stack tiles like
 * the per-session MASQC viewer. Three sections:
 *
 *   1. Current pick      — stacks with `is_main`.
 *   2. Family siblings   — `in_winning_family && !is_main` (Dixon / MP2RAGE).
 *   3. Other candidates  — the rest, sorted by score desc.
 *
 * Manual override semantics: the per-tile checkbox drives a local `selected`
 * Set<stack_id>. Save POSTs that set to .../main-qc/pick. The tiles also expose
 * the per-stack MAIN/PRE/POST role buttons (same as the per-session MASQC
 * viewer) for fine edits without leaving the modal.
 */
import {
  Badge,
  Button,
  Group,
  Modal,
  ScrollArea,
  SimpleGrid,
  Stack,
  Text,
  Title,
} from '@mantine/core';
import { IconAlertTriangle, IconInfoCircle, IconRefresh, IconShieldCheck } from '@tabler/icons-react';
import { useEffect, useMemo, useState } from 'react';

import {
  useAcknowledgeSessionPickMutation,
  useResetSessionPickMutation,
  useUpdateSessionPickMutation,
} from './api';
import { CohortMainQCCandidateTile } from './CohortMainQCCandidateTile';
import type { MainQCCandidate, MainQCSessionPick } from './types';
import {
  classifyReasons,
  formatScore,
  reasonsLabel,
  subjectDisplayLabel,
} from './utils';

interface SessionPickModalProps {
  cohortId: number;
  pick: MainQCSessionPick | null;
  opened: boolean;
  onClose: () => void;
  /** Currently selected display id type (e.g. "code", "BROMS", "MRN"). */
  displayIdType?: string;
}

export const SessionPickModal = ({
  cohortId,
  pick,
  opened,
  onClose,
  displayIdType = 'code',
}: SessionPickModalProps) => {
  const updateMutation = useUpdateSessionPickMutation(cohortId);
  const resetMutation = useResetSessionPickMutation(cohortId);
  const ackMutation = useAcknowledgeSessionPickMutation(cohortId);

  const [selected, setSelected] = useState<Set<number>>(new Set());

  // Sync `selected` when the modal opens / the underlying pick changes.
  useEffect(() => {
    if (pick) setSelected(new Set(pick.winning_stack_ids));
  }, [pick]);

  const groups = useMemo(() => {
    if (!pick) return { current: [], family: [], others: [] };
    const current: MainQCCandidate[] = [];
    const family: MainQCCandidate[] = [];
    const others: MainQCCandidate[] = [];
    for (const c of pick.candidate_summary) {
      if (selected.has(c.stack_id)) {
        current.push(c);
      } else if (c.in_winning_family) {
        family.push(c);
      } else {
        others.push(c);
      }
    }
    others.sort((a, b) => b.score - a.score);
    family.sort((a, b) => b.score - a.score);
    return { current, family, others };
  }, [pick, selected]);

  if (!pick) return null;

  const toggle = (id: number) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const dirty = (() => {
    const a = [...selected].sort();
    const b = [...pick.winning_stack_ids].sort();
    if (a.length !== b.length) return true;
    for (let i = 0; i < a.length; i += 1) if (a[i] !== b[i]) return true;
    return false;
  })();

  const handleSave = async () => {
    await updateMutation.mutateAsync({
      subject_id: pick.subject_id,
      session_date: pick.session_date ?? '',
      axis: pick.axis,
      stack_ids: [...selected],
    });
    onClose();
  };

  const handleResetToAuto = async () => {
    await resetMutation.mutateAsync({
      subject_id: pick.subject_id,
      session_date: pick.session_date ?? '',
      axis: pick.axis,
    });
    onClose();
  };

  const handleAcknowledge = async () => {
    await ackMutation.mutateAsync({
      subject_id: pick.subject_id,
      session_date: pick.session_date ?? '',
      axis: pick.axis,
    });
    onClose();
  };

  return (
    <Modal
      opened={opened}
      onClose={onClose}
      size="lg"
      title={
        <Stack gap={2}>
          <Group gap="xs">
            <Title order={4}>{subjectDisplayLabel(pick, displayIdType)}</Title>
            {pick.acknowledged && (
              <Badge
                color="green"
                variant="light"
                leftSection={<IconShieldCheck size={12} />}
                data-testid="main-qc-reviewed-badge"
              >
                Reviewed
              </Badge>
            )}
          </Group>
          <Text size="sm" c="dimmed">
            {pick.session_date ?? '(no date)'} · axis: <strong>{pick.axis.toUpperCase()}</strong> · score: {formatScore(pick.score)}
            {displayIdType !== 'code' && pick.subject_code
              ? <> · code: <span style={{ fontFamily: 'monospace' }}>{pick.subject_code}</span></>
              : null}
          </Text>
        </Stack>
      }
      data-testid="main-qc-session-modal"
    >
      <Stack gap="md">
        <ReasonBanners reasons={pick.needs_check_reasons} />

        {pick.candidate_summary.length === 0 ? (
          <Text size="sm" c="dimmed">No candidate stacks for this session.</Text>
        ) : (
          <ScrollArea h={520} type="auto">
            <Stack gap="md">
              <CandidateGroup
                title="Current pick"
                candidates={groups.current}
                selected={selected}
                onToggle={toggle}
              />
              <CandidateGroup
                title="Family siblings (Dixon / MP2RAGE)"
                candidates={groups.family}
                selected={selected}
                onToggle={toggle}
              />
              <CandidateGroup
                title="Other candidates"
                candidates={groups.others}
                selected={selected}
                onToggle={toggle}
              />
            </Stack>
          </ScrollArea>
        )}

        <Group justify="space-between">
          <Button
            variant="subtle"
            leftSection={<IconRefresh size={14} />}
            onClick={handleResetToAuto}
            loading={resetMutation.isPending}
          >
            Reset to auto
          </Button>
          <Group gap="xs">
            <Button variant="subtle" onClick={onClose}>Cancel</Button>
            <Button
              color="green"
              variant="filled"
              leftSection={<IconShieldCheck size={14} />}
              onClick={handleAcknowledge}
              disabled={dirty || pick.acknowledged === true}
              loading={ackMutation.isPending}
              data-testid="main-qc-acknowledge-button"
              title={
                dirty
                  ? 'Save your changes first'
                  : pick.acknowledged
                    ? 'Already acknowledged'
                    : 'Mark this pick as reviewed and confirmed'
              }
            >
              In NILS we trust
            </Button>
            <Button
              onClick={handleSave}
              disabled={!dirty}
              loading={updateMutation.isPending}
            >
              Save & Close
            </Button>
          </Group>
        </Group>
      </Stack>
    </Modal>
  );
};

interface CandidateGroupProps {
  title: string;
  candidates: MainQCCandidate[];
  selected: Set<number>;
  onToggle: (id: number) => void;
}

const CandidateGroup = ({ title, candidates, selected, onToggle }: CandidateGroupProps) => {
  if (candidates.length === 0) return null;
  return (
    <Stack gap={6}>
      <Text size="xs" fw={600} tt="uppercase" c="dimmed">
        {title} <span style={{ opacity: 0.6 }}>· {candidates.length}</span>
      </Text>
      <SimpleGrid cols={{ base: 1, sm: 2 }} spacing="sm" verticalSpacing="sm">
        {candidates.map((c) => (
          <CohortMainQCCandidateTile
            key={c.stack_id}
            candidate={c}
            selected={selected.has(c.stack_id)}
            onSelectedChange={() => onToggle(c.stack_id)}
          />
        ))}
      </SimpleGrid>
    </Stack>
  );
};

interface ReasonBannersProps {
  reasons: string[];
}

/** Render needs_check reasons grouped into three semantic banners:
 *   - Review (red): multi-candidate ambiguity, user must adjudicate.
 *   - Alert (amber): single-stack alerts — picked stack is unusual.
 *   - Info (subtle): algorithm-transparency tokens, no action needed.
 */
const ReasonBanners = ({ reasons }: ReasonBannersProps) => {
  const { review, alert, info } = classifyReasons(reasons);
  if (review.length === 0 && alert.length === 0 && info.length === 0) {
    return null;
  }
  return (
    <Stack gap="xs">
      {review.length > 0 && (
        <Stack gap={4} bg="red.0" p="sm" style={{ borderRadius: 4 }}>
          <Group gap={4}>
            <IconAlertTriangle size={16} color="var(--mantine-color-red-7)" />
            <Text size="sm" fw={600} c="red.8">Needs review · pick the right stack</Text>
          </Group>
          {review.map((r) => (
            <Text key={r} size="xs" c="red.8">• {reasonsLabel(r)}</Text>
          ))}
        </Stack>
      )}
      {alert.length > 0 && (
        <Stack gap={4} bg="orange.0" p="sm" style={{ borderRadius: 4 }}>
          <Group gap={4}>
            <IconAlertTriangle size={16} color="var(--mantine-color-orange-7)" />
            <Text size="sm" fw={600} c="orange.8">Single-stack alert</Text>
          </Group>
          {alert.map((r) => (
            <Text key={r} size="xs" c="orange.8">• {reasonsLabel(r)}</Text>
          ))}
        </Stack>
      )}
      {info.length > 0 && (
        <Stack gap={4} bg="gray.0" p="sm" style={{ borderRadius: 4 }}>
          <Group gap={4}>
            <IconInfoCircle size={16} color="var(--mantine-color-gray-7)" />
            <Text size="sm" fw={600} c="gray.8">Algorithm note</Text>
          </Group>
          {info.map((r) => (
            <Text key={r} size="xs" c="gray.8">• {reasonsLabel(r)}</Text>
          ))}
        </Stack>
      )}
    </Stack>
  );
};
