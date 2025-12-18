import { DateTime } from 'luxon';

export const formatDateTime = (value?: string, fallback = '—'): string => {
  if (!value) return fallback;
  const dt = DateTime.fromISO(value);
  if (!dt.isValid) return fallback;
  return dt.toFormat('yyyy-LL-dd HH:mm');
};

export const formatRelativeTime = (value?: string, fallback = '—'): string => {
  if (!value) return fallback;
  const dt = DateTime.fromISO(value);
  if (!dt.isValid) return fallback;
  return dt.toRelative({ base: DateTime.now() }) ?? fallback;
};

export const formatPercent = (value: number, digits = 0): string =>
  `${value.toFixed(digits)}%`;
