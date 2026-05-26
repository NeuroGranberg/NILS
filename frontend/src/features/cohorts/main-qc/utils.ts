/**
 * Pure helpers for the cohort Main QC heatmap UI (rev 4).
 *
 * - groupPicksBySubject: rows = subjects, cols = visit ordinal.
 * - scoreToColor: continuous red→amber→green gradient.
 * - buildContentChips: derive legend chip groups from the actual picks.
 * - pickMatchesFilter: AND-across-axes / OR-within-axis filter evaluation.
 * - reasonsLabel: humanise needs_check_reason tokens.
 */
import type {
  ContentFilter,
  Density,
  MainQCSessionPick,
} from './types';

export interface SubjectRow {
  subject_id: number;
  subject_code: string;
  /** Picks ordered chronologically (no sparseness — only real visits). */
  visits: MainQCSessionPick[];
}

/** Build subject rows for one axis, sorted by display id (or fallback to code). */
export function groupPicksBySubject(
  picks: MainQCSessionPick[],
  axis: string,
  displayIdType: string = 'code',
): { rows: SubjectRow[]; maxVisits: number } {
  const filtered = picks.filter((p) => p.axis === axis);
  const bySubject = new Map<number, MainQCSessionPick[]>();
  for (const p of filtered) {
    const key = p.subject_id;
    if (!bySubject.has(key)) bySubject.set(key, []);
    bySubject.get(key)!.push(p);
  }
  let maxVisits = 0;
  const rows: SubjectRow[] = [];
  for (const [subject_id, list] of bySubject.entries()) {
    list.sort((a, b) => {
      const da = a.session_date ?? '';
      const db = b.session_date ?? '';
      if (da !== db) return da < db ? -1 : 1;
      // After backend collapse, same-date duplicates per axis should be
      // impossible; keep a deterministic tie-breaker on primary_study_id.
      return a.primary_study_id - b.primary_study_id;
    });
    if (list.length > maxVisits) maxVisits = list.length;
    rows.push({
      subject_id,
      subject_code: list[0]?.subject_code ?? String(subject_id),
      visits: list,
    });
  }
  rows.sort((a, b) => {
    const aLabel = subjectDisplayLabel(a.visits[0], displayIdType);
    const bLabel = subjectDisplayLabel(b.visits[0], displayIdType);
    if (!aLabel && !bLabel) return a.subject_id - b.subject_id;
    if (!aLabel) return 1;
    if (!bLabel) return -1;
    return aLabel.localeCompare(bLabel, undefined, { numeric: true });
  });
  return { rows, maxVisits };
}

/** Resolve the display label for a subject given the chosen identifier type. */
export function subjectDisplayLabel(
  pick: MainQCSessionPick | undefined,
  displayIdType: string,
): string {
  if (!pick) return '';
  if (displayIdType === 'code') return pick.subject_code;
  return pick.subject_other_ids?.[displayIdType] ?? pick.subject_code;
}

// ---------------------------------------------------------------------------
// Score → color (continuous red→amber→green gradient, HSL)
// ---------------------------------------------------------------------------

/**
 * Map a 0..1 score to a CSS color along a red→amber→green gradient.
 * Lightness is held mid so chips/cells render well on a white background.
 */
export function scoreToColor(score: number | null | undefined): string {
  if (score === null || score === undefined) return '#dee2e6';
  const s = Math.max(0, Math.min(1, score));
  // Hue 0 (red) → 50 (amber) → 130 (green)
  const hue = s < 0.5 ? 0 + s * 2 * 50 : 50 + (s - 0.5) * 2 * 80;
  return `hsl(${hue.toFixed(0)}, 75%, 45%)`;
}

// ---------------------------------------------------------------------------
// Density presets — drive cell size + how many cells per inner row in a strip.
// ---------------------------------------------------------------------------

export interface DensityPreset {
  cellSize: number;
  innerCols: number;
  gap: number;
  labelFontSize: number;
}

export const DENSITY_PRESETS: Record<Density, DensityPreset> = {
  compact:     { cellSize: 10, innerCols: 8, gap: 1, labelFontSize: 10 },
  medium:      { cellSize: 14, innerCols: 6, gap: 2, labelFontSize: 11 },
  comfortable: { cellSize: 20, innerCols: 5, gap: 3, labelFontSize: 12 },
};

// ---------------------------------------------------------------------------
// Content chips & filter evaluation
// ---------------------------------------------------------------------------

export interface ChipOption {
  value: string;
  label: string;
  count: number;
}

export interface ChipGroups {
  dim: ChipOption[];
  technique: ChipOption[];
  family: ChipOption[];
  slice_bucket: ChipOption[];
  border: ChipOption[];
}

const SLICE_BUCKET_LABEL: Record<string, string> = {
  hi: '≥176 slices',
  std: '100–175 slices',
  lo: '<100 slices',
};

const FAMILY_LABEL: Record<string, string> = {
  dixon: 'Dixon family',
  waterexc: 'WaterExc',
  plain: 'Plain (no fat-sup)',
};

const DIM_LABEL: Record<string, string> = {
  '3D': '3D',
  '2D': '2D',
  unknown: 'Unknown',
};

const BORDER_LABEL: Record<string, string> = {
  needs_check: 'Needs review',
  manual_pick: 'Manual override',
};

/** Build chip groups from actually-present picks. Empty groups are omitted by callers. */
export function buildContentChips(picks: MainQCSessionPick[]): ChipGroups {
  const dim: Record<string, number> = {};
  const technique: Record<string, number> = {};
  const family: Record<string, number> = {};
  const slice_bucket: Record<string, number> = {};
  const border: Record<string, number> = {};

  for (const p of picks) {
    if (p.content) {
      const d = p.content.dim ?? 'unknown';
      dim[d] = (dim[d] ?? 0) + 1;
      const t = p.content.technique;
      if (t) technique[t] = (technique[t] ?? 0) + 1;
      const f = p.content.family;
      if (f) family[f] = (family[f] ?? 0) + 1;
      const sb = p.content.slice_bucket;
      if (sb) slice_bucket[sb] = (slice_bucket[sb] ?? 0) + 1;
    }
    if (p.needs_check) border.needs_check = (border.needs_check ?? 0) + 1;
    if (p.needs_check_reasons?.includes('manual_override')) {
      border.manual_pick = (border.manual_pick ?? 0) + 1;
    }
  }

  const toOpts = (
    counts: Record<string, number>,
    labels: Record<string, string> = {},
    order?: string[],
  ): ChipOption[] => {
    const entries = Object.entries(counts);
    if (order) {
      entries.sort(
        (a, b) =>
          (order.indexOf(a[0]) === -1 ? 99 : order.indexOf(a[0])) -
          (order.indexOf(b[0]) === -1 ? 99 : order.indexOf(b[0])),
      );
    } else {
      entries.sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
    }
    return entries.map(([value, count]) => ({
      value,
      label: labels[value] ?? value,
      count,
    }));
  };

  return {
    dim: toOpts(dim, DIM_LABEL, ['3D', '2D', 'unknown']),
    technique: toOpts(technique),
    family: toOpts(family, FAMILY_LABEL, ['dixon', 'waterexc', 'plain']),
    slice_bucket: toOpts(slice_bucket, SLICE_BUCKET_LABEL, ['hi', 'std', 'lo']),
    border: toOpts(border, BORDER_LABEL, ['needs_check', 'manual_pick']),
  };
}

/**
 * Return true if a pick matches the active filter.
 * AND across non-empty axes, OR within an axis. An empty axis is unrestricted.
 */
export function pickMatchesFilter(
  pick: MainQCSessionPick | undefined,
  filter: ContentFilter,
): boolean {
  if (!pick) return true;

  const dim = pick.content?.dim ?? 'unknown';
  if (filter.dim.length > 0 && !filter.dim.includes(dim)) return false;

  const tech = pick.content?.technique;
  if (filter.technique.length > 0 && (!tech || !filter.technique.includes(tech))) {
    return false;
  }

  const family = pick.content?.family;
  if (filter.family.length > 0 && (!family || !filter.family.includes(family))) {
    return false;
  }

  const sb = pick.content?.slice_bucket;
  if (filter.slice_bucket.length > 0 && (!sb || !filter.slice_bucket.includes(sb))) {
    return false;
  }

  if (filter.border.length > 0) {
    const has: string[] = [];
    if (pick.needs_check) has.push('needs_check');
    if (pick.needs_check_reasons?.includes('manual_override')) has.push('manual_pick');
    if (!filter.border.some((b) => has.includes(b))) return false;
  }

  return true;
}

/** True iff the filter has at least one chip selected. */
export function isFilterActive(filter: ContentFilter): boolean {
  return (
    filter.dim.length > 0 ||
    filter.technique.length > 0 ||
    filter.family.length > 0 ||
    filter.slice_bucket.length > 0 ||
    filter.border.length > 0
  );
}

// ---------------------------------------------------------------------------
// Misc
// ---------------------------------------------------------------------------

/** Humanise a needs_check_reason token. */
export function reasonsLabel(token: string): string {
  switch (token) {
    case 'no_eligible_stacks': return 'No eligible stacks';
    case 'no_canonical_construct': return 'Family has no canonical output';
    case 'retake': return 'Retake (≥2 stacks in winning bundle)';
    case 'retake_dixon_canonical': return 'Repeat Dixon canonical';
    case 'retake_mp2rage': return 'Repeat MP2RAGE';
    case 'close_runner_up': return 'Top 2 bundles within 5%';
    case 'unknown_dim': return 'Acquisition dimension unknown';
    case 'slice_count_outlier': return 'Slice count outside cohort 5–95th pctile';
    case 'pre_post_twin': return 'Pre+post twin (both tagged)';
    case 'epimix_fallback': return 'EPIMix fallback (no RawRecon)';
    case 'rare_technique': return 'Rare technique (<10% of cohort)';
    case 'dixon_vs_plain': return 'Dixon family wins, plain alternative is close';
    case 'manual_override': return 'Manually overridden';
    case 'dropped_short_partial_volume':
      return 'Short partial-volume stacks auto-demoted from main';
    case 'acknowledged': return 'Reviewed — In NILS we trust 🫡';
    default: return token;
  }
}

/** Reason tokens that are informational only (algorithm transparency): they
 * describe what the picker did rather than something the user must adjudicate.
 * Mirrors `INFO_ONLY_REASONS` in the backend service. */
export const INFO_ONLY_REASONS: ReadonlySet<string> = new Set([
  'dropped_short_partial_volume',
  'acknowledged',
]);

/** Reason tokens that flag a single-stack alert (no candidate ambiguity, but
 * the chosen stack is unusual in some way). Rendered in an amber banner. */
export const SINGLE_STACK_ALERT_REASONS: ReadonlySet<string> = new Set([
  'rare_technique',
  'unknown_dim',
  'slice_count_outlier',
  'epimix_fallback',
  'no_canonical_construct',
  'no_eligible_stacks',
]);

export interface ClassifiedReasons {
  /** Multi-candidate ambiguity — user must pick. Rendered in a red banner. */
  review: string[];
  /** Single-stack alerts — chosen stack is unusual. Rendered in amber. */
  alert: string[];
  /** Informational — transparency only, no action needed. Rendered in subtle. */
  info: string[];
}

/** Split needs_check_reasons into the three banner buckets. */
export function classifyReasons(reasons: readonly string[]): ClassifiedReasons {
  const review: string[] = [];
  const alert: string[] = [];
  const info: string[] = [];
  for (const r of reasons) {
    if (INFO_ONLY_REASONS.has(r)) info.push(r);
    else if (SINGLE_STACK_ALERT_REASONS.has(r)) alert.push(r);
    else review.push(r);
  }
  return { review, alert, info };
}

/** Format a small score (0..1) as a percentage with one decimal. */
export function formatScore(score: number | null | undefined): string {
  if (score === null || score === undefined) return '—';
  return `${(score * 100).toFixed(0)}%`;
}
