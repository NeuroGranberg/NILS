/**
 * Tests for the pure chip/delta helpers.
 */
import { describe, expect, it } from 'vitest';

import {
  buildChips,
  chipsContain,
  chipsToDelta,
  countEditedBuckets,
  deltasEqual,
} from '../utils';
import type { KeywordBucketView } from '../types';

const bucket = (
  overrides: Partial<KeywordBucketView> = {},
): KeywordBucketView => ({
  axis: 'contrast',
  bucket_path: 'negative_keywords',
  display_name: 'Negative (no contrast)',
  group_label: null,
  description: null,
  defaults: ['utan gd', 'ohne km', 'no contrast'],
  added: [],
  removed: [],
  effective: ['utan gd', 'ohne km', 'no contrast'],
  ...overrides,
});

describe('buildChips', () => {
  it('returns all defaults as "default" when no deltas', () => {
    const chips = buildChips(bucket());
    expect(chips.map((c) => c.state)).toEqual(['default', 'default', 'default']);
    expect(chips.map((c) => c.keyword)).toEqual([
      'utan gd',
      'ohne km',
      'no contrast',
    ]);
  });

  it('marks removed defaults as "removed" in place', () => {
    const chips = buildChips(bucket({ removed: ['ohne km'] }));
    expect(chips[1].state).toBe('removed');
    expect(chips[0].state).toBe('default');
    expect(chips[2].state).toBe('default');
  });

  it('appends user-added as "added" after defaults', () => {
    const chips = buildChips(bucket({ added: ['sem contraste'] }));
    expect(chips).toHaveLength(4);
    expect(chips[3].state).toBe('added');
    expect(chips[3].keyword).toBe('sem contraste');
  });

  it('is case-insensitive when checking removed vs defaults', () => {
    const chips = buildChips(bucket({ removed: ['OHNE KM'] }));
    expect(chips[1].state).toBe('removed');
  });
});

describe('chipsToDelta', () => {
  it('is empty for untouched chips', () => {
    const chips = buildChips(bucket());
    const delta = chipsToDelta(chips, bucket().defaults);
    expect(delta).toEqual({ added: [], removed: [] });
  });

  it('recovers added/removed from chip states', () => {
    const chips = buildChips(
      bucket({ added: ['custom'], removed: ['no contrast'] }),
    );
    const delta = chipsToDelta(chips, bucket().defaults);
    expect(delta.added).toEqual(['custom']);
    expect(delta.removed).toEqual(['no contrast']);
  });

  it('ignores case-only dup chips in added', () => {
    const chips = [
      { keyword: 'UTAN GD', state: 'added' as const }, // dup of default
      { keyword: 'foo', state: 'added' as const },
    ];
    const delta = chipsToDelta(chips, bucket().defaults);
    expect(delta.added).toEqual(['foo']);
  });
});

describe('deltasEqual', () => {
  it('equal for empty deltas', () => {
    expect(deltasEqual({ added: [], removed: [] }, { added: [], removed: [] })).toBe(true);
  });

  it('order-insensitive equality', () => {
    expect(
      deltasEqual(
        { added: ['a', 'b'], removed: ['x'] },
        { added: ['B', 'A'], removed: ['X'] },
      ),
    ).toBe(true);
  });

  it('detects inequality', () => {
    expect(
      deltasEqual(
        { added: ['a'], removed: [] },
        { added: [], removed: ['a'] },
      ),
    ).toBe(false);
  });
});

describe('chipsContain', () => {
  it('case-insensitive', () => {
    const chips = [{ keyword: 'Foo', state: 'default' as const }];
    expect(chipsContain(chips, 'foo')).toBe(true);
    expect(chipsContain(chips, 'bar')).toBe(false);
  });
});

describe('countEditedBuckets', () => {
  it('counts only buckets with non-empty deltas', () => {
    expect(
      countEditedBuckets([
        { added: [], removed: [] },
        { added: ['a'], removed: [] },
        { added: [], removed: ['b'] },
        { added: [], removed: [] },
      ]),
    ).toBe(2);
  });
});
