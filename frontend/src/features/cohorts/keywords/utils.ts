/**
 * Pure helpers for the keyword editor.
 *
 * The UI models each bucket as a list of chips in one of three states:
 *   - 'default'  : present in global defaults, user hasn't touched it
 *   - 'added'    : user added on top of defaults
 *   - 'removed'  : in defaults, user marked for removal (still rendered,
 *                  struck-through, with a "restore" affordance)
 *
 * These helpers translate between that UI model and the backend's
 * { added, removed } delta representation.
 */
import type { ChipState, KeywordBucketView } from './types';

const normalize = (kw: string) => kw.trim().toLowerCase();

export interface Chip {
  keyword: string;   // original form (preserves whitespace/case)
  state: ChipState;
}

/**
 * Build the ordered chip list for a bucket.
 *
 * Order: defaults first (in their original order, with removed ones
 * still shown in-place with 'removed' state), then user-added chips.
 */
export function buildChips(bucket: KeywordBucketView): Chip[] {
  const removedSet = new Set(bucket.removed.map(normalize));
  const addedSet = new Set(bucket.added.map(normalize));

  const chips: Chip[] = [];
  const seen = new Set<string>();

  for (const kw of bucket.defaults) {
    const key = normalize(kw);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    chips.push({
      keyword: kw,
      state: removedSet.has(key) ? 'removed' : 'default',
    });
  }

  for (const kw of bucket.added) {
    const key = normalize(kw);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    chips.push({ keyword: kw, state: 'added' });
  }

  // A word-level remove that doesn't match any default is effectively dead.
  // Surface it once at the end so the user can clean it up.
  for (const kw of bucket.removed) {
    const key = normalize(kw);
    if (!seen.has(key)) {
      seen.add(key);
      chips.push({ keyword: kw, state: 'removed' });
    }
  }

  // Filter out no-op added chips that are a pure case-only dup of defaults.
  // (apply-level dedup is case-insensitive; showing both is noise.)
  void addedSet;

  return chips;
}

/**
 * Convert a chip list back to the backend delta.
 *
 * Relative to ``bucketDefaults``:
 *   - ``added``   = chips whose state is 'added' (keyword not in defaults by normalized compare)
 *   - ``removed`` = chips whose state is 'removed' (keyword in defaults by normalized compare)
 */
export function chipsToDelta(
  chips: Chip[],
  bucketDefaults: string[],
): { added: string[]; removed: string[] } {
  const defaultsSet = new Set(bucketDefaults.map(normalize));
  const added: string[] = [];
  const removed: string[] = [];
  for (const chip of chips) {
    const key = normalize(chip.keyword);
    if (chip.state === 'added') {
      // Don't push a default keyword as "added" (case-insensitive de-dup).
      if (!defaultsSet.has(key)) added.push(chip.keyword);
    } else if (chip.state === 'removed') {
      removed.push(chip.keyword);
    }
  }
  return { added, removed };
}

/**
 * Case-insensitive containment check.
 */
export function chipsContain(chips: Chip[], keyword: string): boolean {
  const target = normalize(keyword);
  return chips.some((c) => normalize(c.keyword) === target);
}

/**
 * Compare two delta payloads (for save-button disable state).
 */
export function deltasEqual(
  a: { added: string[]; removed: string[] },
  b: { added: string[]; removed: string[] },
): boolean {
  const norm = (xs: string[]) =>
    xs.map(normalize).filter((x) => x.length > 0).sort().join('|');
  return norm(a.added) === norm(b.added) && norm(a.removed) === norm(b.removed);
}

/**
 * Count the number of edited buckets under an axis (for the left-rail badge).
 */
export function countEditedBuckets(
  buckets: Array<Pick<KeywordBucketView, 'added' | 'removed'>>,
): number {
  return buckets.filter((b) => b.added.length + b.removed.length > 0).length;
}
