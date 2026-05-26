/**
 * Pure helper tests for rev 4: continuous color, content chips, filter eval,
 * subject grouping/sort by display id, density presets.
 */
import { describe, expect, it } from 'vitest';

import type { ContentFilter, MainQCSessionPick } from '../types';
import { EMPTY_FILTER } from '../types';
import {
  buildContentChips,
  DENSITY_PRESETS,
  formatScore,
  groupPicksBySubject,
  isFilterActive,
  pickMatchesFilter,
  reasonsLabel,
  scoreToColor,
  subjectDisplayLabel,
} from '../utils';

const pick = (
  overrides: Partial<MainQCSessionPick> & { study_id?: number } = {},
): MainQCSessionPick => {
  const { study_id, ...rest } = overrides as Partial<MainQCSessionPick> & { study_id?: number };
  const primary = rest.primary_study_id ?? study_id ?? 1;
  return {
    subject_id: 1,
    session_date: '2024-01-01',
    subject_code: 'S001',
    axis: 't1w',
    study_ids: rest.study_ids ?? [primary],
    primary_study_id: primary,
    winning_stack_ids: [10],
    score: 0.8,
    needs_check: false,
    needs_check_reasons: [],
    candidate_summary: [],
    content: {
      technique: 'MPRAGE',
      dim: '3D',
      family: 'plain',
      slice_bucket: 'hi',
      slices: 192,
    },
    subject_other_ids: {},
    ...rest,
  };
};

describe('scoreToColor', () => {
  it('returns gray for null/undefined', () => {
    expect(scoreToColor(null)).toBe('#dee2e6');
    expect(scoreToColor(undefined)).toBe('#dee2e6');
  });

  it('returns hsl with hue increasing with score', () => {
    const low = scoreToColor(0);
    const mid = scoreToColor(0.5);
    const high = scoreToColor(1);
    expect(low).toMatch(/^hsl\(0,/);     // red
    expect(mid).toMatch(/^hsl\(50,/);    // amber
    expect(high).toMatch(/^hsl\(130,/);  // green
  });

  it('clamps out-of-range scores', () => {
    expect(scoreToColor(-1)).toMatch(/^hsl\(0,/);
    expect(scoreToColor(2)).toMatch(/^hsl\(130,/);
  });
});

describe('groupPicksBySubject', () => {
  it('groups by subject and sorts visits chronologically', () => {
    const picks = [
      pick({ study_id: 1, subject_id: 1, session_date: '2024-03-01' }),
      pick({ study_id: 2, subject_id: 1, session_date: '2024-01-15' }),
      pick({ study_id: 3, subject_id: 2, subject_code: 'S002', session_date: '2024-02-10' }),
    ];
    const { rows, maxVisits } = groupPicksBySubject(picks, 't1w');
    expect(rows).toHaveLength(2);
    expect(maxVisits).toBe(2);
    const s001 = rows.find((r) => r.subject_code === 'S001')!;
    expect(s001.visits[0].primary_study_id).toBe(2);  // earlier date first
    expect(s001.visits[1].primary_study_id).toBe(1);
  });

  it('filters by axis', () => {
    const picks = [
      pick({ subject_id: 1, axis: 't1w' }),
      pick({ subject_id: 2, axis: 'flair' }),
    ];
    const { rows } = groupPicksBySubject(picks, 't1w');
    expect(rows).toHaveLength(1);
    expect(rows[0].subject_id).toBe(1);
  });

  it('sorts subjects by chosen identifier when provided', () => {
    const picks = [
      pick({ subject_id: 2, subject_code: 'B', subject_other_ids: { PID: 'A1' } }),
      pick({ subject_id: 1, subject_code: 'A', subject_other_ids: { PID: 'Z9' } }),
    ];
    const { rows: byCode } = groupPicksBySubject(picks, 't1w', 'code');
    expect(byCode.map((r) => r.subject_code)).toEqual(['A', 'B']);
    const { rows: byPid } = groupPicksBySubject(picks, 't1w', 'PID');
    // PID A1 first, Z9 second → subject 2 first.
    expect(byPid.map((r) => r.subject_id)).toEqual([2, 1]);
  });
});

describe('subjectDisplayLabel', () => {
  it('returns subject_code for "code"', () => {
    expect(subjectDisplayLabel(pick(), 'code')).toBe('S001');
  });
  it('returns the other id when available', () => {
    expect(
      subjectDisplayLabel(pick({ subject_other_ids: { MRN: '123' } }), 'MRN'),
    ).toBe('123');
  });
  it('falls back to code when the other id is missing', () => {
    expect(subjectDisplayLabel(pick(), 'MRN')).toBe('S001');
  });
});

describe('buildContentChips', () => {
  it('only includes chip groups present in the picks', () => {
    const picks = [
      pick({
        content: {
          technique: 'MPRAGE',
          dim: '3D',
          family: 'plain',
          slice_bucket: 'hi',
          slices: 192,
        },
      }),
      pick({
        study_id: 2,
        content: {
          technique: 'TSE',
          dim: '2D',
          family: 'plain',
          slice_bucket: 'std',
          slices: 120,
        },
      }),
    ];
    const chips = buildContentChips(picks);
    expect(chips.dim.map((c) => c.value).sort()).toEqual(['2D', '3D']);
    expect(chips.technique.map((c) => c.value).sort()).toEqual(['MPRAGE', 'TSE']);
    expect(chips.family.map((c) => c.value)).toEqual(['plain']);
    expect(chips.slice_bucket.map((c) => c.value).sort()).toEqual(['hi', 'std']);
    expect(chips.border).toEqual([]);  // no needs_check / manual_override
  });

  it('exposes border chips when picks carry flags', () => {
    const picks = [
      pick({ needs_check: true }),
      pick({ study_id: 2, needs_check_reasons: ['manual_override'] }),
    ];
    const chips = buildContentChips(picks);
    expect(chips.border.map((c) => c.value).sort()).toEqual(['manual_pick', 'needs_check']);
  });

  it('counts occurrences', () => {
    const picks = [pick({}), pick({ study_id: 2 }), pick({ study_id: 3, content: { technique: 'TSE', dim: '2D', family: 'plain', slice_bucket: 'lo', slices: 30 } })];
    const chips = buildContentChips(picks);
    expect(chips.technique.find((c) => c.value === 'MPRAGE')?.count).toBe(2);
    expect(chips.technique.find((c) => c.value === 'TSE')?.count).toBe(1);
  });
});

describe('pickMatchesFilter', () => {
  const p = pick({
    content: { technique: 'MPRAGE', dim: '3D', family: 'dixon', slice_bucket: 'hi', slices: 200 },
    needs_check: true,
  });

  it('returns true for empty filter', () => {
    expect(pickMatchesFilter(p, EMPTY_FILTER)).toBe(true);
  });

  it('matches AND across axes', () => {
    const f: ContentFilter = { ...EMPTY_FILTER, dim: ['3D'], family: ['dixon'] };
    expect(pickMatchesFilter(p, f)).toBe(true);
    const fail: ContentFilter = { ...EMPTY_FILTER, dim: ['3D'], family: ['plain'] };
    expect(pickMatchesFilter(p, fail)).toBe(false);
  });

  it('matches OR within an axis', () => {
    const f: ContentFilter = { ...EMPTY_FILTER, family: ['plain', 'dixon'] };
    expect(pickMatchesFilter(p, f)).toBe(true);
  });

  it('respects status border filter', () => {
    expect(pickMatchesFilter(p, { ...EMPTY_FILTER, border: ['needs_check'] })).toBe(true);
    expect(pickMatchesFilter(p, { ...EMPTY_FILTER, border: ['manual_pick'] })).toBe(false);
  });

  it('rejects when content is null and a content axis is filtered', () => {
    const noContent = pick({ content: null });
    expect(pickMatchesFilter(noContent, { ...EMPTY_FILTER, technique: ['MPRAGE'] })).toBe(false);
  });
});

describe('isFilterActive', () => {
  it('false for empty filter', () => {
    expect(isFilterActive(EMPTY_FILTER)).toBe(false);
  });
  it('true when any axis has a selection', () => {
    expect(isFilterActive({ ...EMPTY_FILTER, dim: ['3D'] })).toBe(true);
  });
});

describe('DENSITY_PRESETS', () => {
  it('orders compact < medium < comfortable for cellSize', () => {
    expect(DENSITY_PRESETS.compact.cellSize).toBeLessThan(DENSITY_PRESETS.medium.cellSize);
    expect(DENSITY_PRESETS.medium.cellSize).toBeLessThan(DENSITY_PRESETS.comfortable.cellSize);
  });
});

describe('reasonsLabel', () => {
  it('humanises known tokens', () => {
    expect(reasonsLabel('close_runner_up')).toMatch(/within 5%/i);
    expect(reasonsLabel('dixon_vs_plain')).toMatch(/Dixon/i);
    expect(reasonsLabel('manual_override')).toMatch(/manual/i);
  });
  it('passes unknown tokens through', () => {
    expect(reasonsLabel('something_new')).toBe('something_new');
  });
});

describe('formatScore', () => {
  it('formats normal scores as percent', () => {
    expect(formatScore(0.847)).toBe('85%');
    expect(formatScore(0)).toBe('0%');
  });
  it('handles null', () => {
    expect(formatScore(null)).toBe('—');
    expect(formatScore(undefined)).toBe('—');
  });
});
