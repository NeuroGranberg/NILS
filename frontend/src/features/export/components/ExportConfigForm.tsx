import {
  Button,
  Group,
  Loader,
  NumberInput,
  Paper,
  SegmentedControl,
  Select,
  SimpleGrid,
  Stack,
  Switch,
  Text,
  TextInput,
} from '@mantine/core';
import { useMemo, useState } from 'react';
import { SectionCard } from '../../shared/components/SectionCard';
import { useDirectoryQuery, useDataRootsQuery } from '../../files/api';
import type { ExportConfig } from '../types';

const resolveDataRoot = () => {
  const envValue = (import.meta.env['VITE_DATA_ROOT'] ?? import.meta.env['VITE_DATA_PATH']) as string | undefined;
  if (!envValue) return '/data';
  const normalized = envValue.trim().replace(/\\/g, '/').replace(/\/+/g, '/');
  if (!normalized) return '/data';
  const trimmed = normalized.endsWith('/') && normalized !== '/' ? normalized.slice(0, -1) : normalized;
  if (trimmed.startsWith('/')) return trimmed || '/data';
  return `/${trimmed}`;
};

const normalizePath = (value: string) => {
  if (!value) return '';
  const replaced = value.replace(/\\/g, '/').replace(/\/+/g, '/').trim();
  if (!replaced) return '';
  if (replaced === '/') return '/';
  return replaced.endsWith('/') ? replaced.slice(0, -1) : replaced;
};

const clampToRoot = (value: string, root: string) => {
  const rootPath = root || '/data';
  const normalizedRoot = normalizePath(rootPath) || '/data';
  if (!value) return normalizedRoot;
  let candidate = normalizePath(value);
  if (!candidate.startsWith('/')) {
    candidate = normalizePath(`${normalizedRoot}/${candidate}`) || normalizedRoot;
  }
  if (normalizedRoot === '/') return candidate || '/';
  return candidate.startsWith(normalizedRoot) ? candidate : normalizedRoot;
};

const buildBreadcrumbs = (path: string, root: string) => {
  const normalizedRoot = root === '' ? '/' : root;
  const breadcrumbs: Array<{ label: string; path: string }> = [];
  const rootLabel = normalizedRoot === '/' ? '/' : normalizedRoot.split('/').filter(Boolean).pop() ?? normalizedRoot;
  breadcrumbs.push({ label: rootLabel, path: normalizedRoot });

  if (path === normalizedRoot) return breadcrumbs;

  const relative = normalizedRoot === '/'
    ? path.replace(/^\/+/, '')
    : path.startsWith(`${normalizedRoot}/`)
      ? path.slice(normalizedRoot.length + 1)
      : '';

  if (!relative) return breadcrumbs;

  const parts = relative.split('/').filter(Boolean);
  let current = normalizedRoot;
  for (const part of parts) {
    current = current === '/' ? `/${part}` : `${current}/${part}`;
    breadcrumbs.push({ label: part, path: current });
  }
  return breadcrumbs;
};

interface ExportConfigFormProps {
  config: ExportConfig;
  onChange: <K extends keyof ExportConfig>(key: K, value: ExportConfig[K]) => void;
  outputPath: string;
  onOutputPathChange: (path: string) => void;
}


export const ExportConfigForm = ({
  config,
  onChange,
  outputPath,
  onOutputPathChange,
}: ExportConfigFormProps) => {
  const layoutVal = config.layout;
  const layoutLabel = layoutVal === 'flat' ? 'Flat' : 'BIDS';

  const exportDicom = config.outputModes.includes('dcm');
  const niftiModeVal = config.outputModes.find((m) => m === 'nii' || m === 'nii.gz') as
    | 'nii'
    | 'nii.gz'
    | undefined;

  const applyOutputModes = (nextDicom: boolean, nextNifti: 'nii' | 'nii.gz' | null) => {
    const next: string[] = [];
    if (nextDicom) next.push('dcm');
    if (nextNifti) next.push(nextNifti);
    onChange('outputModes', next.length ? next : ['dcm']);
  };

  // Directory browser state
  const fallbackRoot = useMemo(() => resolveDataRoot(), []);
  const { data: dataRoots } = useDataRootsQuery();
  const availableRoots = useMemo(() => {
    const roots = dataRoots && dataRoots.length ? dataRoots : [fallbackRoot];
    const normalized = roots.map((root) => normalizePath(root) || '/');
    const deduped = Array.from(new Set(normalized));
    return deduped.length ? deduped : [fallbackRoot];
  }, [dataRoots, fallbackRoot]);

  const [currentRoot, setCurrentRoot] = useState(() => availableRoots[0] ?? fallbackRoot);
  const browsePath = useMemo(() => clampToRoot(outputPath || currentRoot, currentRoot), [outputPath, currentRoot]);
  const { data: directoryEntries, isFetching: isLoadingDirectories } = useDirectoryQuery(browsePath);
  const subdirectories = useMemo(
    () => (directoryEntries ?? []).filter((entry) => entry.type === 'directory'),
    [directoryEntries],
  );

  const handlePathChange = (next: string) => {
    onOutputPathChange(clampToRoot(next, currentRoot));
  };

  const handleRootChange = (newRoot: string | null) => {
    if (newRoot) {
      setCurrentRoot(newRoot);
      onOutputPathChange(newRoot);
    }
  };

  return (
    <Stack gap="md">
      {/* Output directory with browser */}
      <SectionCard title="Output Directory" description="Browse and select a destination for the exported data">
        <Stack gap="sm">
          {availableRoots.length > 1 && (
            <Select
              label="Data root"
              description="Select which data directory to browse"
              value={currentRoot}
              onChange={handleRootChange}
              data={availableRoots.map((root) => {
                const parts = root.split('/').filter(Boolean);
                const label = parts.length > 2 ? `.../${parts.slice(-2).join('/')}` : root;
                return { value: root, label };
              })}
            />
          )}
          <TextInput
            label="Destination path"
            placeholder={`${currentRoot}/exports/my-subset`}
            value={outputPath}
            onChange={(e) => handlePathChange(e.currentTarget.value)}
          />
          <Paper withBorder radius="md" p="sm">
            <Stack gap="xs">
              <Group gap={6} wrap="wrap">
                {buildBreadcrumbs(browsePath, currentRoot).map((crumb, index, list) => (
                  <Button
                    key={crumb.path}
                    size="xs"
                    variant={index === list.length - 1 ? 'light' : 'subtle'}
                    onClick={() => handlePathChange(crumb.path)}
                  >
                    {crumb.label}
                  </Button>
                ))}
              </Group>
              {isLoadingDirectories ? (
                <Group justify="center" py="sm">
                  <Loader size="sm" />
                </Group>
              ) : subdirectories.length > 0 ? (
                <SimpleGrid cols={{ base: 2, sm: 3 }} spacing="xs">
                  {subdirectories.map((entry) => (
                    <Button
                      key={entry.path}
                      variant="outline"
                      onClick={() => handlePathChange(entry.path)}
                    >
                      {entry.name}
                    </Button>
                  ))}
                </SimpleGrid>
              ) : (
                <Text size="sm" c="dimmed">
                  No subfolders detected in this location.
                </Text>
              )}
            </Stack>
          </Paper>
        </Stack>
      </SectionCard>

      {/* Layout & Mode */}
      <SectionCard title="Layout & Output" description="Choose export layout and file formats">
        <Stack gap="sm">
          <SegmentedControl
            data={[
              { label: 'BIDS layout', value: 'bids' },
              { label: 'Flat layout', value: 'flat' },
            ]}
            value={layoutVal}
            onChange={(value) => onChange('layout', (value ?? 'bids') as 'bids' | 'flat')}
          />
          <SegmentedControl
            value={config.overwriteMode}
            onChange={(value) =>
              onChange('overwriteMode', (value ?? 'skip') as 'skip' | 'clean' | 'overwrite')
            }
            data={[
              { label: 'Skip existing', value: 'skip' },
              { label: 'Clean', value: 'clean' },
              { label: 'Overwrite', value: 'overwrite' },
            ]}
          />
        </Stack>
      </SectionCard>

      {/* Export Formats */}
      <SectionCard title="Export Formats" description="Select output formats and directories">
        <Stack gap="sm">
          <Group gap="md" align="center">
            <Switch
              label="Export DICOM"
              checked={exportDicom}
              onChange={(e) => applyOutputModes(e.currentTarget.checked, niftiModeVal ?? null)}
            />
            {exportDicom && (
              <TextInput
                label={`${layoutLabel} DICOM root`}
                value={layoutVal === 'flat' ? config.flatDcmRootName : config.bidsDcmRootName}
                style={{ flex: 1 }}
                onChange={(e) =>
                  onChange(
                    layoutVal === 'flat' ? 'flatDcmRootName' : 'bidsDcmRootName',
                    e.currentTarget.value,
                  )
                }
              />
            )}
          </Group>
          <Group gap="md" align="center">
            <Switch
              label="Export NIfTI"
              checked={Boolean(niftiModeVal)}
              onChange={(e) =>
                applyOutputModes(exportDicom, e.currentTarget.checked ? (niftiModeVal ?? 'nii.gz') : null)
              }
            />
            {Boolean(niftiModeVal) && (
              <TextInput
                label={`${layoutLabel} NIfTI root`}
                value={layoutVal === 'flat' ? config.flatNiftiRootName : config.bidsNiftiRootName}
                style={{ flex: 1 }}
                onChange={(e) =>
                  onChange(
                    layoutVal === 'flat' ? 'flatNiftiRootName' : 'bidsNiftiRootName',
                    e.currentTarget.value,
                  )
                }
              />
            )}
          </Group>
          {Boolean(niftiModeVal) && (
            <SegmentedControl
              data={[
                { label: 'NIfTI (.nii)', value: 'nii' },
                { label: 'Compressed (.nii.gz)', value: 'nii.gz' },
              ]}
              value={niftiModeVal}
              onChange={(value) => applyOutputModes(exportDicom, value as 'nii' | 'nii.gz')}
            />
          )}
        </Stack>
      </SectionCard>

      {/* Performance */}
      <SectionCard title="Performance" description="Configure worker counts for parallel processing">
        <Group grow>
          <NumberInput
            label="Copy workers (DICOM)"
            min={1}
            max={64}
            value={config.copyWorkers}
            onChange={(value) => onChange('copyWorkers', Number(value ?? 8))}
          />
          <NumberInput
            label="Convert workers (NIfTI)"
            min={1}
            max={64}
            value={config.convertWorkers}
            onChange={(value) => onChange('convertWorkers', Number(value ?? 8))}
          />
        </Group>
      </SectionCard>
    </Stack>
  );
};
