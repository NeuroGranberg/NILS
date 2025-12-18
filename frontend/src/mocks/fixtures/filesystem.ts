import type { DirectoryEntry } from '../../types';

const DEFAULT_ROOT = '/data';

const normalizeRoot = (value?: string) => {
  const candidate = (value ?? '').replace(/\\/g, '/').replace(/\/+/g, '/').trim();
  if (!candidate) return DEFAULT_ROOT;
  if (candidate === '/') return '/';
  const trimmed = candidate.endsWith('/') ? candidate.slice(0, -1) : candidate;
  return trimmed.startsWith('/') ? trimmed : `/${trimmed}`;
};

const DATA_ROOT = normalizeRoot(import.meta.env.VITE_DATA_ROOT);

const normalizePath = (value: string | undefined) => {
  const fallback = DATA_ROOT || DEFAULT_ROOT;
  if (!value) return fallback;
  const replaced = value.replace(/\\/g, '/').replace(/\/+/g, '/').trim();
  if (!replaced) return fallback;
  if (replaced === '/') return '/';
  const trimmed = replaced.endsWith('/') ? replaced.slice(0, -1) : replaced;
  if (trimmed.startsWith('/')) return trimmed;
  return `${fallback === '/' ? '' : fallback}/${trimmed}`.replace(/\/+/g, '/');
};

const applyRoot = (value: string) => {
  if (!value.startsWith(DEFAULT_ROOT)) {
    return normalizePath(value);
  }
  const suffix = value.slice(DEFAULT_ROOT.length);
  if (!suffix) {
    return DATA_ROOT;
  }
  if (DATA_ROOT === '/') {
    return normalizePath(`/${suffix.replace(/^\/+/, '')}`);
  }
  return normalizePath(`${DATA_ROOT}${suffix}`);
};

const baseDirectoryMap: Record<string, DirectoryEntry[]> = {
  '/data': [
    { name: 'incoming', path: '/data/incoming', type: 'directory' },
    { name: 'research', path: '/data/research', type: 'directory' },
    { name: 'archive', path: '/data/archive', type: 'directory' },
  ],
  '/data/incoming': [
    { name: 'stopms', path: '/data/incoming/stopms', type: 'directory' },
    { name: 'ki-prospective', path: '/data/incoming/ki-prospective', type: 'directory' },
    { name: 'ctrials', path: '/data/incoming/ctrials', type: 'directory' },
  ],
  '/data/incoming/stopms': [
    { name: '2023-11-visit', path: '/data/incoming/stopms/2023-11-visit', type: 'directory' },
    { name: '2024-05-visit', path: '/data/incoming/stopms/2024-05-visit', type: 'directory' },
  ],
  '/data/incoming/ki-prospective': [
    { name: 'baseline', path: '/data/incoming/ki-prospective/baseline', type: 'directory' },
    { name: 'followup', path: '/data/incoming/ki-prospective/followup', type: 'directory' },
  ],
  '/data/incoming/ctrials': [],
  '/data/research': [
    { name: 'broms', path: '/data/research/broms', type: 'directory' },
    { name: 'controls', path: '/data/research/controls', type: 'directory' },
  ],
  '/data/research/broms': [
    { name: 'export-2022', path: '/data/research/broms/export-2022', type: 'directory' },
    { name: 'export-2023', path: '/data/research/broms/export-2023', type: 'directory' },
  ],
  '/data/research/controls': [
    { name: 'batch-a', path: '/data/research/controls/batch-a', type: 'directory' },
    { name: 'batch-b', path: '/data/research/controls/batch-b', type: 'directory' },
  ],
  '/data/archive': [
    { name: 'legacy', path: '/data/archive/legacy', type: 'directory' },
  ],
  '/data/archive/legacy': [],
};

const directoryMap: Record<string, DirectoryEntry[]> = Object.fromEntries(
  Object.entries(baseDirectoryMap).map(([key, entries]) => [
    applyRoot(key),
    entries.map((entry) => ({
      ...entry,
      path: applyRoot(entry.path),
    })),
  ]),
);

export const listDirectories = (path: string) => directoryMap[normalizePath(path)] ?? [];
