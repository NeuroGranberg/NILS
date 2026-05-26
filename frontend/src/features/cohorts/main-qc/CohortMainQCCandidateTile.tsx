/**
 * One candidate stack tile inside the cohort-level Session Pick modal.
 *
 * - Shows the same wheel-scrollable server PNG as the per-session MASQC view.
 * - The MAIN button is the single source of truth for the cohort-level
 *   "main pick" selection: clicking it stages the selection (toggles the
 *   `selected` Set in the parent modal). The actual write happens on
 *   Save & Close via POST /main-qc/pick — that endpoint also rewrites the
 *   metadata-DB main_acquisition token, so we don't need a second
 *   fast-path mutation here. This eliminates the previous footgun where
 *   MAIN wrote the metadata DB but left the cohort snapshot stale.
 * - PRE / POST still drive `post_contrast` directly (orthogonal to the
 *   cohort pick).
 * - The tile border is green when this candidate is part of the staged
 *   main pick, mirroring the MAIN button.
 */
import { Badge, Box, Button, Group, Stack, Text } from '@mantine/core';
import { useEffect, useState } from 'react';

import { StackImagePane } from '../../qc/components/main-acq/StackImagePane';
import { useSetStackRole } from '../../qc/api/main-acq';
import { formatScore } from './utils';
import type { MainQCCandidate } from './types';

interface CohortMainQCCandidateTileProps {
  candidate: MainQCCandidate;
  /** Cohort-level "main pick" selection state (driven by the modal). */
  selected: boolean;
  onSelectedChange: (next: boolean) => void;
}

const ORIENT_ABBREV: Record<string, string> = {
  Axial: 'Ax',
  Coronal: 'Cor',
  Sagittal: 'Sag',
};

/** Build a concise human label for a candidate, formatted as
 * `orientation_base_acqtype_technique_modifier` (null parts skipped).
 * Example: "Ax_T1w_3D_VIBE_Dixon". Falls back to "#<stack_id>" if every
 * component is missing. */
const candidateLabel = (c: MainQCCandidate): string => {
  const parts: string[] = [];
  if (c.orientation) parts.push(ORIENT_ABBREV[c.orientation] ?? c.orientation);
  if (c.base) parts.push(c.base);
  if (c.dim) parts.push(c.dim);
  if (c.technique) parts.push(c.technique);
  if (c.modifier_csv) parts.push(c.modifier_csv.replace(/,/g, '-'));
  return parts.join('_') || `#${c.stack_id}`;
};

/** Format a number to 1 decimal, trimming a trailing ".0" so "1.0" → "1". */
const fmt1 = (n: number): string => {
  const s = n.toFixed(1);
  return s.endsWith('.0') ? s.slice(0, -2) : s;
};

/** Format the resolution × FOV summary line, e.g.
 *   "1×1×1 mm · 240×240 mm"
 * or partial fragments when some components are missing. */
const resolutionAndFov = (c: MainQCCandidate): string | null => {
  const fragments: string[] = [];
  const r = c.pixsp_row_mm;
  const co = c.pixsp_col_mm;
  const sl = c.slice_thickness_mm;
  if (r != null && co != null && sl != null) {
    fragments.push(`${fmt1(r)}×${fmt1(co)}×${fmt1(sl)} mm`);
  } else if (r != null && co != null) {
    fragments.push(`${fmt1(r)}×${fmt1(co)} mm`);
  }
  const fx = c.fov_x_mm;
  const fy = c.fov_y_mm;
  if (fx != null && fy != null) {
    fragments.push(`${Math.round(fx)}×${Math.round(fy)} mm FOV`);
  } else if (fx != null) {
    fragments.push(`${Math.round(fx)} mm FOV`);
  }
  return fragments.length > 0 ? fragments.join(' · ') : null;
};

export const CohortMainQCCandidateTile = ({
  candidate,
  selected,
  onSelectedChange,
}: CohortMainQCCandidateTileProps) => {
  const setRole = useSetStackRole();

  // Independent slice progress per tile (no synchronised scrolling here).
  const [progress, setProgress] = useState(0.5);

  // Local optimistic state for PRE / POST.
  const [postContrast, setPostContrast] = useState<number | null>(candidate.post_contrast);

  useEffect(() => setPostContrast(candidate.post_contrast), [candidate.post_contrast]);

  const togglePre = () => {
    const turningOn = postContrast !== 0;
    setPostContrast(turningOn ? 0 : null);
    setRole.mutate(
      { seriesStackId: candidate.stack_id, role: 'pre', value: turningOn },
      { onError: () => setPostContrast(candidate.post_contrast) },
    );
  };

  const togglePost = () => {
    const turningOn = postContrast !== 1;
    setPostContrast(turningOn ? 1 : null);
    setRole.mutate(
      { seriesStackId: candidate.stack_id, role: 'post', value: turningOn },
      { onError: () => setPostContrast(candidate.post_contrast) },
    );
  };

  const toggleMain = () => onSelectedChange(!selected);

  const label = candidateLabel(candidate);

  // Border highlights the cohort-level pick selection.
  const borderColor = selected
    ? 'var(--mantine-color-green-6)'
    : 'var(--nils-border)';
  const borderWidth = selected ? 2 : 1;

  return (
    <Stack
      gap={4}
      data-testid="main-qc-candidate-tile"
      data-stack-id={candidate.stack_id}
      style={{
        border: `${borderWidth}px solid ${borderColor}`,
        borderRadius: 6,
        padding: 6,
        background: 'var(--nils-bg-secondary)',
      }}
    >
      {candidate.series_instance_uid ? (
        <StackImagePane
          seriesInstanceUid={candidate.series_instance_uid}
          stackIndex={candidate.stack_index ?? 0}
          progress={progress}
          onProgressChange={setProgress}
          altLabel={`stack ${candidate.stack_id}`}
        />
      ) : (
        <Box
          style={{
            backgroundColor: '#000',
            color: 'rgba(255,255,255,0.5)',
            borderRadius: 4,
            aspectRatio: '1 / 1',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: 12,
          }}
        >
          no preview
        </Box>
      )}

      <Group gap={4} wrap="nowrap" justify="space-between">
        <Text size="xs" ff="monospace" style={{ wordBreak: 'break-word' }}>
          {label}
        </Text>
        <Text size="xs" c="dimmed" ff="monospace" style={{ whiteSpace: 'nowrap' }}>
          {formatScore(candidate.score)}
        </Text>
      </Group>
      <Group gap={4} wrap="wrap">
        <Text size="xs" c="dimmed" ff="monospace">
          #{candidate.stack_id}
          {resolutionAndFov(candidate) ? ` · ${resolutionAndFov(candidate)}` : ''}
        </Text>
        {candidate.post_contrast === 1 && (
          <Badge size="xs" color="blue" variant="light">post</Badge>
        )}
        {candidate.post_contrast === 0 && (
          <Badge size="xs" color="gray" variant="light">pre</Badge>
        )}
        {candidate.provenance && candidate.provenance !== 'RawRecon' && (
          <Badge size="xs" color="violet" variant="light">{candidate.provenance}</Badge>
        )}
      </Group>

      <Group gap="xs" justify="flex-end" wrap="nowrap" align="center">
        <Button
          size="compact-xs"
          variant={selected ? 'filled' : 'outline'}
          color="green"
          onClick={toggleMain}
        >
          MAIN
        </Button>
        <Button.Group>
          <Button
            size="compact-xs"
            variant={postContrast === 0 ? 'filled' : 'outline'}
            color="blue"
            onClick={togglePre}
          >
            PRE
          </Button>
          <Button
            size="compact-xs"
            variant={postContrast === 1 ? 'filled' : 'outline'}
            color="blue"
            onClick={togglePost}
          >
            POST
          </Button>
        </Button.Group>
      </Group>
    </Stack>
  );
};
