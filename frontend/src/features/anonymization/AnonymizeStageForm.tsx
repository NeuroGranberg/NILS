import {
  Accordion,
  Box,
  Button,
  Checkbox,
  Divider,
  FileButton,
  Group,
  NumberInput,
  PasswordInput,
  Radio,
  SegmentedControl,
  Select,
  Stack,
  Switch,
  Tabs,
  Text,
  TextInput,
} from '@mantine/core';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import type {
  AnonymizeCategory,
  AnonymizeStageConfig,
  CompressionOptions,
  PatientIdStrategy,
  SystemResources,
} from '../../types';
import { apiClient } from '../../utils/api-client';
import { useFolderSamples, useCsvColumns } from './queries';
import { getDefaultFilenameForFormat } from './defaults';
import { ANONYMIZE_CATEGORY_OPTIONS, CATEGORY_TAGS } from './constants';

interface AnonymizeStageFormProps {
  cohortName: string;
  cohortId?: number;
  config: AnonymizeStageConfig;
  onChange: (config: AnonymizeStageConfig) => void;
  onRecommendResources?: () => void;
  recommendLoading?: boolean;
  recommendation?: SystemResources;
}

const strategyOptions: Array<{ label: string; value: PatientIdStrategy }> = [
  { label: 'Sequential map', value: 'sequential' },
  { label: 'Derived from folder name', value: 'folder' },
  { label: 'Lookup from CSV mapping', value: 'csv' },
  { label: 'Deterministic hash', value: 'deterministic' },
];

const LITERAL_FOLDER_REGEX = '(.+)';
const DEFAULT_FOLDER_REGEX = String.raw`\b(\d+)[-_](?:[Mm]\d+|\d+)`;

interface CsvUploadResponse {
  token: string;
  filename: string;
  columns: string[];
}

export const AnonymizeStageForm = ({
  cohortName,
  cohortId,
  config,
  onChange,
  onRecommendResources,
  recommendLoading,
  recommendation,
}: AnonymizeStageFormProps) => {
  const queryClient = useQueryClient();
  const [samplePath, setSamplePath] = useState('');
  const [activeCategory, setActiveCategory] = useState<AnonymizeCategory>('Patient_Information');
  const [selectedSamplePath, setSelectedSamplePath] = useState('');
  const [customFolderRegex, setCustomFolderRegex] = useState(
    config.folderRegex === LITERAL_FOLDER_REGEX ? DEFAULT_FOLDER_REGEX : config.folderRegex,
  );
  const [csvUploadError, setCsvUploadError] = useState<string | null>(null);
  const [csvUploading, setCsvUploading] = useState(false);

  const regexError = useMemo(() => {
    try {
      RegExp(config.folderRegex);
      return undefined;
    } catch (error) {
      return (error as Error).message;
    }
  }, [config.folderRegex]);

  const autoFetchFolderSamples =
    cohortId != null && config.updatePatientIds && config.patientIdStrategy === 'folder';

  const {
    data: folderSamples,
    isFetching: folderSamplesLoading,
    refetch: refetchFolderSamples,
    error: folderSamplesError,
  } = useFolderSamples(cohortId ?? null, { enabled: autoFetchFolderSamples });

  const updateConfig = useCallback(
    (patch: Partial<AnonymizeStageConfig>) => {
      onChange({
        ...config,
        ...patch,
      });
    },
    [config, onChange],
  );

  const handleFieldChange = useCallback(
    <K extends keyof AnonymizeStageConfig>(key: K, value: AnonymizeStageConfig[K]) => {
      updateConfig({ [key]: value } as Partial<AnonymizeStageConfig>);
    },
    [updateConfig],
  );


  const samplePaths = (folderSamples?.paths ?? []).filter((path) => Boolean(path));

  useEffect(() => {
    if (samplePaths.length) {
      setSelectedSamplePath(samplePaths[0]);
    } else {
      setSelectedSamplePath('');
    }
  }, [samplePaths]);

  useEffect(() => {
    if (config.updatePatientIds && config.patientIdStrategy === 'none') {
      handleFieldChange('patientIdStrategy', 'folder');
    }
  }, [config.updatePatientIds, config.patientIdStrategy, handleFieldChange]);

  useEffect(() => {
    if (config.patientIdStrategy !== 'folder') {
      return;
    }
    if (selectedSamplePath) {
      setSamplePath(selectedSamplePath);
    }
  }, [selectedSamplePath, config.patientIdStrategy]);

  const handleOutputFormatChange = (nextFormat: AnonymizeStageConfig['outputFormat']) => {
    const expectedFilename = getDefaultFilenameForFormat(nextFormat);
    const currentName = config.metadataFilename;
    let nextFilename = currentName;
    const expectedExt = expectedFilename.slice(expectedFilename.lastIndexOf('.'));

    if (!currentName) {
      nextFilename = expectedFilename;
    } else if (!currentName.toLowerCase().endsWith(expectedExt)) {
      const sanitized = currentName.replace(/\.[^/.]+$/u, '');
      nextFilename = `${sanitized}${expectedExt}`;
    }

    updateConfig({
      outputFormat: nextFormat,
      metadataFilename: nextFilename,
      excelPassword: nextFormat === 'encrypted_excel' ? config.excelPassword ?? '' : undefined,
    });
  };

  const handleTagToggle = (code: string, checked: boolean) => {
    const next = new Set(config.scrubbedTagCodes ?? []);
    if (checked) {
      next.add(code);
    } else {
      next.delete(code);
    }
    handleFieldChange('scrubbedTagCodes', Array.from(next));
  };

  const handleCategorySelectAll = (category: AnonymizeCategory) => {
    const categoryCodes = CATEGORY_TAGS[category]?.map((tag) => tag.code) ?? [];
    if (categoryCodes.length === 0) return;
    const next = new Set(config.scrubbedTagCodes ?? []);
    categoryCodes.forEach((code) => next.add(code));
    handleFieldChange('scrubbedTagCodes', Array.from(next));
  };

  const handleCategoryClear = (category: AnonymizeCategory) => {
    const categoryCodes = CATEGORY_TAGS[category]?.map((tag) => tag.code) ?? [];
    if (categoryCodes.length === 0) return;
    const next = new Set(config.scrubbedTagCodes ?? []);
    categoryCodes.forEach((code) => next.delete(code));
    handleFieldChange('scrubbedTagCodes', Array.from(next));
  };

  const csvConfig = useMemo(
    () => ({
      filePath: config.csvMapping?.filePath ?? '',
      fileToken: config.csvMapping?.fileToken ?? undefined,
      fileName: config.csvMapping?.fileName ?? undefined,
      sourceColumn: config.csvMapping?.sourceColumn ?? '',
      targetColumn: config.csvMapping?.targetColumn ?? '',
      missingMode: config.csvMapping?.missingMode ?? 'hash',
      missingPattern: config.csvMapping?.missingPattern ?? 'MISSEDXXXXX',
      missingSalt: config.csvMapping?.missingSalt ?? 'csv-missed',
      preserveTopFolderOrder: config.csvMapping?.preserveTopFolderOrder ?? true,
    }),
    [config.csvMapping],
  );

  const compressionConfig = useMemo<CompressionOptions>(
    () => ({
      enabled: config.compression?.enabled ?? false,
      chunk: config.compression?.chunk ?? '100GB',
      strategy: config.compression?.strategy ?? 'ordered',
      compression: config.compression?.compression ?? 3,
      workers: config.compression?.workers ?? 2,
      password: config.compression?.password ?? '',
      verify: config.compression?.verify ?? true,
      par2: config.compression?.par2 ?? 0,
    }),
    [config.compression],
  );

  const updateCsv = (patch: Partial<typeof csvConfig>) =>
    handleFieldChange('csvMapping', { ...csvConfig, ...patch });

  const updateCompression = (patch: Partial<CompressionOptions>) =>
    handleFieldChange('compression', { ...compressionConfig, ...patch });

  const handleCsvUpload = async (file: File | null) => {
    if (!file) return;
    setCsvUploadError(null);
    setCsvUploading(true);
    const form = new FormData();
    form.append('file', file);

    try {
      const response = await apiClient.postForm<CsvUploadResponse>('/uploads/csv', form);
      queryClient.setQueryData(['csv-columns', response.token], { columns: response.columns });
      updateCsv({
        fileToken: response.token,
        fileName: response.filename,
        filePath: '',
        sourceColumn: '',
        targetColumn: '',
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to upload CSV mapping file.';
      setCsvUploadError(message);
    } finally {
      setCsvUploading(false);
    }
  };

  const handleCsvClear = () => {
    if (csvToken) {
      queryClient.removeQueries({ queryKey: ['csv-columns', csvToken] });
    }
    setCsvUploadError(null);
    updateCsv({
      fileToken: undefined,
      fileName: undefined,
      filePath: '',
      sourceColumn: '',
      targetColumn: '',
    });
  };

  const csvToken = csvConfig.fileToken;
  const {
    data: csvColumnsData,
    isLoading: csvColumnsLoading,
    error: csvColumnsError,
  } = useCsvColumns(csvToken);
  const csvColumns = csvColumnsData?.columns ?? [];

  const regexPreview = useMemo(() => {
    if (!samplePath.trim() || regexError) return null;
    try {
      const regex = new RegExp(config.folderRegex);
      const match = regex.exec(samplePath);
      if (!match) {
        return { before: samplePath, match: null as string | null, after: '' };
      }
      const start = match.index ?? samplePath.indexOf(match[0]);
      const end = start + match[0].length;
      return {
        before: samplePath.slice(0, start),
        match: match[0],
        after: samplePath.slice(end),
      };
    } catch {
      return null;
    }
  }, [config.folderRegex, samplePath, regexError]);

  const folderSegments = useMemo(() => {
    if (!selectedSamplePath) return [] as string[];
    return selectedSamplePath.split(/[/\\]+/u).filter(Boolean);
  }, [selectedSamplePath]);

  const depthSegments = useMemo(() => {
    if (!folderSegments.length) return [] as string[];
    const last = folderSegments[folderSegments.length - 1];
    if (last && last.includes('.')) {
      return folderSegments.slice(0, -1);
    }
    return folderSegments;
  }, [folderSegments]);

  const usingLiteralSegment = config.folderRegex === LITERAL_FOLDER_REGEX;

  const cohortTemplateExample = useMemo(() => {
    const fallback = 'COHORT';
    if (!cohortName) return `${fallback}XXXX`;
    const normalized = cohortName
      .normalize('NFKD')
      .replace(/[^\p{ASCII}]+/gu, '')
      .replace(/[^A-Za-z0-9]+/g, '')
      .toUpperCase();
    const base = normalized || fallback;
    return base.includes('X') ? base : `${base}XXXX`;
  }, [cohortName]);

  const cohortSaltExample = useMemo(() => {
    const fallback = 'cohort-salt';
    if (!cohortName) return fallback;
    const slug = cohortName
      .normalize('NFKD')
      .replace(/[^\p{ASCII}]+/gu, '')
      .replace(/[^A-Za-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '');
    const base = slug || 'cohort';
    return `${base.toLowerCase()}-salt`;
  }, [cohortName]);

  useEffect(() => {
    if (!usingLiteralSegment) {
      setCustomFolderRegex(config.folderRegex);
    }
  }, [usingLiteralSegment, config.folderRegex, config.folderFallbackTemplate]);

  const formatBytes = (value: number) => {
    if (value <= 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let size = value;
    let index = 0;
    while (size >= 1024 && index < units.length - 1) {
      size /= 1024;
      index += 1;
    }
    return `${size.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
  };

  const formatBytesPerSecond = (value: number) => `${formatBytes(value)}/s`;

  return (
    <Stack gap="lg">
      <Stack gap="xs">
        <Text size="sm" fw={500}>
          Categories to scrub
        </Text>
        <Tabs
          value={activeCategory}
          onChange={(value) => value && setActiveCategory(value as AnonymizeCategory)}
          keepMounted={false}
        >
          <Tabs.List>
            {ANONYMIZE_CATEGORY_OPTIONS.map((option) => (
              <Tabs.Tab key={option.value} value={option.value}>
                {option.label}
              </Tabs.Tab>
            ))}
          </Tabs.List>
          {ANONYMIZE_CATEGORY_OPTIONS.map((option) => {
            const tags = CATEGORY_TAGS[option.value];
            const selectedSet = new Set(config.scrubbedTagCodes ?? []);
            const allSelected = tags.every((tag) => selectedSet.has(tag.code));
            const someSelected = tags.some((tag) => selectedSet.has(tag.code));

            return (
              <Tabs.Panel key={option.value} value={option.value} pt="md">
                <Stack gap="xs">
                  <Group justify="space-between" align="flex-start">
                    <Stack gap={4}>
                      <Text size="sm" fw={500}>
                        {option.label}
                      </Text>
                      {option.description && (
                        <Text size="xs" c="dimmed">
                          {option.description}
                        </Text>
                      )}
                    </Stack>
                    <Group gap="xs">
                      <Button
                        size="xs"
                        variant="subtle"
                        onClick={() => handleCategorySelectAll(option.value)}
                      >
                        Select all
                      </Button>
                      <Button
                        size="xs"
                        variant="subtle"
                        color="gray"
                        onClick={() => handleCategoryClear(option.value)}
                        disabled={!someSelected}
                      >
                        Clear
                      </Button>
                    </Group>
                  </Group>
                  <Box
                    h={220}
                    p="xs"
                    style={{
                      overflowY: 'auto',
                      border: '1px solid var(--mantine-color-default-border)',
                      borderRadius: 'var(--mantine-radius-md)',
                      backgroundColor: 'var(--mantine-color-body)',
                    }}
                  >
                    <Stack gap="xs">
                      {tags.map((tag) => (
                        <Checkbox
                          key={tag.code}
                          label={`${tag.name} (${tag.code})`}
                          checked={selectedSet.has(tag.code)}
                          onChange={(event) => handleTagToggle(tag.code, event.currentTarget.checked)}
                        />
                      ))}
                      {!tags.length && (
                        <Text size="xs" c="dimmed">
                          No tags defined for this category.
                        </Text>
                      )}
                    </Stack>
                  </Box>
                  {!allSelected && (
                    <Text size="xs" c="dimmed">
                      Partial selection detected. Ensure compliance before leaving date fields in place.
                    </Text>
                  )}
                </Stack>
              </Tabs.Panel>
            );
          })}
        </Tabs>
      </Stack>

      <Accordion multiple chevronPosition="left" defaultValue={['identifiers']}>
        <Accordion.Item value="identifiers">
          <Accordion.Control>Update Patient's ID</Accordion.Control>
          <Accordion.Panel>
            <Stack gap="md">
              <Switch
                label="Update Patient IDs"
                checked={config.updatePatientIds}
                onChange={(event) => handleFieldChange('updatePatientIds', event.currentTarget.checked)}
              />

              <Stack gap="sm" ml="lg" pl="lg" style={{ borderLeft: '1px solid var(--mantine-color-default-border)' }}>
                <SegmentedControl
                  value={config.patientIdStrategy}
                  onChange={(value) => handleFieldChange('patientIdStrategy', value as PatientIdStrategy)}
                  data={strategyOptions}
                  disabled={!config.updatePatientIds}
                />

                {config.patientIdStrategy === 'folder' && config.updatePatientIds && (
                  <Stack gap="sm">
                    <Radio.Group
                      value={config.folderStrategy}
                      onChange={(value) => handleFieldChange('folderStrategy', (value ?? 'depth') as 'depth' | 'regex')}
                      label="Patient ID extraction mode"
                      description="Choose how to derive the subject identifier from the folder path"
                    >
                      <Stack gap="xs">
                        <Radio value="depth" label="Use segment at depth" />
                        <Radio value="regex" label="Extract using regex" />
                      </Stack>
                    </Radio.Group>

                    <Switch
                      label="Use entire folder segment"
                      checked={usingLiteralSegment}
                      onChange={(event) => {
                        const checked = event.currentTarget.checked;
                        if (checked) {
                          if (!usingLiteralSegment) {
                            setCustomFolderRegex(config.folderRegex || DEFAULT_FOLDER_REGEX);
                          }
                          handleFieldChange('folderRegex', LITERAL_FOLDER_REGEX);
                        } else {
                          const fallbackRegex =
                            customFolderRegex && customFolderRegex !== LITERAL_FOLDER_REGEX
                              ? customFolderRegex
                              : DEFAULT_FOLDER_REGEX;
                          handleFieldChange('folderRegex', fallbackRegex);
                        }
                      }}
                      description="Shortcut: sets the regex to match the whole segment. Editing the regex below will turn this off automatically."
                    />

                    {config.folderStrategy === 'depth' && (
                      <Stack gap="xs">
                        <Group justify="space-between" align="center">
                          <Text size="sm" fw={500}>
                            Choose a folder segment to use as the Patient ID
                          </Text>
                          <Button
                            size="xs"
                            variant="subtle"
                            onClick={() => {
                              void refetchFolderSamples();
                            }}
                            loading={folderSamplesLoading}
                          >
                            Refresh
                          </Button>
                        </Group>
                        {selectedSamplePath ? (
                          <Group gap="xs" wrap="wrap">
                            {depthSegments.map((segment, index) => (
                              <Button
                                key={`${segment}-${index}`}
                                size="xs"
                                variant={config.folderDepthAfterRoot === index + 1 ? 'filled' : 'light'}
                                onClick={() => handleFieldChange('folderDepthAfterRoot', index + 1)}
                              >
                                {segment}
                              </Button>
                            ))}
                          </Group>
                        ) : (
                          <Text size="xs" c="dimmed">
                            No folder example available. Refresh to load a sample path.
                          </Text>
                        )}
                        {folderSamplesError && (
                          <Text size="xs" c="red">
                            Unable to load sample paths from the backend. You can still provide a custom path.
                          </Text>
                        )}
                      </Stack>
                    )}

                    {config.folderStrategy === 'regex' && (
                      <Stack gap="sm">
                        <Group justify="space-between" align="center">
                          <Text size="sm" fw={500}>
                            Extract Patient ID using regex
                          </Text>
                          <Button
                            size="xs"
                            variant="subtle"
                            onClick={() => {
                              void refetchFolderSamples();
                            }}
                            loading={folderSamplesLoading}
                          >
                            Refresh
                          </Button>
                        </Group>
                        {selectedSamplePath && (
                          <Text size="xs" c="dimmed" style={{ wordBreak: 'break-all' }}>
                            Sample: {selectedSamplePath}
                          </Text>
                        )}
                        <TextInput
                          label="Folder regex"
                          value={config.folderRegex}
                          onChange={(event) => {
                            const value = event.currentTarget.value;
                            if (usingLiteralSegment && value !== LITERAL_FOLDER_REGEX) {
                              setCustomFolderRegex(value || DEFAULT_FOLDER_REGEX);
                              handleFieldChange('folderRegex', value || DEFAULT_FOLDER_REGEX);
                              return;
                            }
                            if (!usingLiteralSegment) {
                              handleFieldChange('folderRegex', value || DEFAULT_FOLDER_REGEX);
                            }
                          }}
                          error={regexError}
                        />
                        <TextInput
                          label="Sample folder path"
                          placeholder="/data/incoming/stopms/2024-05-visit"
                          value={samplePath}
                          onChange={(event) => setSamplePath(event.currentTarget.value)}
                          disabled={!selectedSamplePath && samplePaths.length > 0}
                        />
                        {regexPreview && (
                          <Text size="xs" c="dimmed">
                            {regexPreview.match ? (
                              <>
                                <Text span component="span">{regexPreview.before}</Text>
                                <Text span component="span" fw={700}>
                                  {regexPreview.match}
                                </Text>
                                <Text span component="span">{regexPreview.after}</Text>
                              </>
                            ) : (
                              'No match found in sample path. Folder name will be used as-is.'
                            )}
                          </Text>
                        )}
                      </Stack>
                    )}

                    {!usingLiteralSegment && (
                      <TextInput
                        label="Prefix"
                        placeholder="COHORT"
                        value={config.folderFallbackTemplate ?? ''}
                        onChange={(event) => handleFieldChange('folderFallbackTemplate', event.currentTarget.value)}
                        description="Optional text prepended to the extracted value. Include X placeholders (e.g., COHORTXXXX) if you want zero-padded numbers; leave blank to keep folder names as-is."
                      />
                    )}
                  </Stack>
                )}

                {config.patientIdStrategy === 'csv' && config.updatePatientIds && (
                  <Stack gap="sm">
                    <Text size="sm" c="dimmed">
                      Upload a CSV with two columns: one for the current Patient ID values and one for the desired
                      replacement IDs.
                    </Text>
                    <Group gap="sm" align="center">
                      <FileButton accept=".csv" onChange={handleCsvUpload} disabled={csvUploading}>
                        {(props) => (
                          <Button {...props} loading={csvUploading}>
                            {csvConfig.fileToken ? 'Replace CSV' : 'Upload CSV'}
                          </Button>
                        )}
                      </FileButton>
                      {csvConfig.fileName && (
                        <Button variant="subtle" color="red" onClick={handleCsvClear} size="xs" disabled={csvUploading}>
                          Remove
                        </Button>
                      )}
                    </Group>
                    {csvConfig.fileName && (
                      <Text size="sm" c="dimmed">
                        Using file: <Text span fw={600}>{csvConfig.fileName}</Text>
                      </Text>
                    )}
                    {csvUploadError && (
                      <Text size="xs" c="red">
                        {csvUploadError}
                      </Text>
                    )}
                    {csvColumnsError && (
                      <Text size="xs" c="red">
                        Failed to read CSV columns. Upload again or try a different file.
                      </Text>
                    )}

                    <Group grow>
                      {csvConfig.fileToken && csvColumns.length > 0 ? (
                        <>
                          <Select
                            label="Source column (current IDs)"
                            data={csvColumns
                              .filter((column) => column !== csvConfig.targetColumn)
                              .map((column) => ({ value: column, label: column }))}
                            value={csvConfig.sourceColumn || null}
                            onChange={(value) => updateCsv({ sourceColumn: value ?? '', filePath: '' })}
                            placeholder={csvColumnsLoading ? 'Loading columns…' : 'Select column'}
                            disabled={csvColumnsLoading}
                          />
                          <Select
                            label="Target column (new IDs)"
                            data={csvColumns
                              .filter((column) => column !== csvConfig.sourceColumn)
                              .map((column) => ({ value: column, label: column }))}
                            value={csvConfig.targetColumn || null}
                            onChange={(value) => updateCsv({ targetColumn: value ?? '', filePath: '' })}
                            placeholder={csvColumnsLoading ? 'Loading columns…' : 'Select column'}
                            disabled={csvColumnsLoading}
                          />
                        </>
                      ) : (
                        <>
                          <TextInput
                            label="Source column (current IDs)"
                            value={csvConfig.sourceColumn}
                            onChange={(event) => updateCsv({ sourceColumn: event.currentTarget.value })}
                            placeholder={csvColumnsLoading ? 'Loading columns…' : 'Enter column name'}
                          />
                          <TextInput
                            label="Target column (new IDs)"
                            value={csvConfig.targetColumn}
                            onChange={(event) => updateCsv({ targetColumn: event.currentTarget.value })}
                            placeholder={csvColumnsLoading ? 'Loading columns…' : 'Enter column name'}
                          />
                        </>
                      )}
                    </Group>

                    {!csvConfig.fileToken && (
                      <Text size="xs" c="dimmed">
                        Upload a CSV to enable column pickers and reuse the mapping during anonymization runs.
                      </Text>
                    )}

                    <Select
                      label="Missing ID strategy"
                      description="How to assign IDs when the CSV lacks a mapping"
                      data={[
                        { value: 'hash', label: 'Deterministic hash (stable)' },
                        { value: 'per_top_folder_seq', label: 'Sequential by top folder order' },
                      ]}
                      value={csvConfig.missingMode}
                      onChange={(value) => updateCsv({ missingMode: (value ?? 'hash') as typeof csvConfig.missingMode })}
                    />
                    <Group grow>
                      <TextInput
                        label="Fallback pattern"
                        value={csvConfig.missingPattern}
                        onChange={(event) => updateCsv({ missingPattern: event.currentTarget.value })}
                      />
                      <TextInput
                        label="Fallback salt"
                        value={csvConfig.missingSalt}
                        onChange={(event) => updateCsv({ missingSalt: event.currentTarget.value })}
                      />
                    </Group>
                    <Switch
                      label="Preserve top folder order when assigning sequential IDs"
                      checked={csvConfig.preserveTopFolderOrder}
                      onChange={(event) => updateCsv({ preserveTopFolderOrder: event.currentTarget.checked })}
                    />
                  </Stack>
                )}

                {config.patientIdStrategy === 'deterministic' && config.updatePatientIds && (
                  <Stack gap="sm">
                    <TextInput
                      label="ID pattern"
                      placeholder={cohortTemplateExample}
                      description={`Template containing X placeholders (e.g., ${cohortTemplateExample})`}
                      value={config.deterministicPattern ?? ''}
                      onChange={(event) => handleFieldChange('deterministicPattern', event.currentTarget.value)}
                    />
                    <TextInput
                      label="Hash salt"
                      placeholder={cohortSaltExample}
                      description={`Value mixed into deterministic hashing (e.g., ${cohortSaltExample}).`}
                      value={config.deterministicSalt ?? ''}
                      onChange={(event) => handleFieldChange('deterministicSalt', event.currentTarget.value)}
                    />
                  </Stack>
                )}

                {config.patientIdStrategy === 'sequential' && config.updatePatientIds && (
                  <Stack gap="sm">
                    <TextInput
                      label="ID pattern"
                      placeholder={cohortTemplateExample}
                      value={config.sequentialPattern ?? ''}
                      onChange={(event) => handleFieldChange('sequentialPattern', event.currentTarget.value)}
                    />
                    <Group grow>
                      <NumberInput
                        label="Starting number"
                        min={0}
                        value={config.sequentialStartingNumber ?? 1}
                        onChange={(value) => handleFieldChange('sequentialStartingNumber', Number(value ?? 1))}
                      />
                      <Select
                        label="Participant discovery"
                        data={[
                          { value: 'per_top_folder', label: 'One per top-level folder' },
                          { value: 'one_per_study', label: 'One per study UID' },
                          { value: 'all', label: 'Scan all instances' },
                        ]}
                        value={config.sequentialDiscovery ?? 'per_top_folder'}
                        onChange={(value) =>
                          handleFieldChange('sequentialDiscovery', (value ?? 'per_top_folder') as typeof config.sequentialDiscovery)
                        }
                      />
                    </Group>
                  </Stack>
                )}
              </Stack>
            </Stack>
          </Accordion.Panel>
        </Accordion.Item>

        <Accordion.Item value="dates">
          <Accordion.Control>Study date handling</Accordion.Control>
          <Accordion.Panel>
            <Stack gap="md">
              <Switch
                label="Update Study Dates"
                checked={config.updateStudyDates}
                onChange={(event) => handleFieldChange('updateStudyDates', event.currentTarget.checked)}
              />
              <Stack gap="sm" ml="lg" pl="lg" style={{ borderLeft: '1px solid var(--mantine-color-default-border)' }}>
                <Text size="sm" c="dimmed">
                  Dates are converted to MXX offsets relative to the first session per participant.
                </Text>
                <Switch
                  label="Snap to multiples of six months"
                  checked={config.snapToSixMonths}
                  onChange={(event) => handleFieldChange('snapToSixMonths', event.currentTarget.checked)}
                  disabled={!config.updateStudyDates}
                />
                <NumberInput
                  label="Minimum offset (months)"
                  min={0}
                  value={config.minimumOffsetMonths}
                  onChange={(value) => handleFieldChange('minimumOffsetMonths', Number(value ?? 0))}
                  disabled={!config.updateStudyDates}
                />
              </Stack>
            </Stack>
          </Accordion.Panel>
        </Accordion.Item>

        <Accordion.Item value="audit">
          <Accordion.Control>Metadata export</Accordion.Control>
          <Accordion.Panel>
            <Stack gap="sm">
              <Select
                label="Metadata output format"
                value={config.outputFormat}
                data={[
                  { label: 'Encrypted Excel (.xlsx)', value: 'encrypted_excel' },
                  { label: 'CSV (.csv)', value: 'csv' },
                ]}
                onChange={(value) => handleOutputFormatChange((value ?? 'encrypted_excel') as AnonymizeStageConfig['outputFormat'])}
              />
              <Stack gap="xs" ml="lg" pl="lg" style={{ borderLeft: '1px solid var(--mantine-color-default-border)' }}>
                <TextInput
                  label="Metadata file name"
                  value={config.metadataFilename}
                  onChange={(event) => handleFieldChange('metadataFilename', event.currentTarget.value)}
                  description="Stored in the source root. Use .csv or .xlsx extension."
                />
                {config.outputFormat === 'encrypted_excel' && (
                  <PasswordInput
                    label="Excel password"
                    value={config.excelPassword ?? ''}
                    onChange={(event) => handleFieldChange('excelPassword', event.currentTarget.value)}
                    description="Password required to open the encrypted Excel audit file."
                  />
                )}
              </Stack>
            </Stack>
          </Accordion.Panel>
        </Accordion.Item>

        <Accordion.Item value="compression">
          <Accordion.Control>Compression (optional)</Accordion.Control>
          <Accordion.Panel>
            <Stack gap="md">
              <Switch
                label="Enable compression after anonymization"
                checked={compressionConfig.enabled}
                onChange={(event) => updateCompression({ enabled: event.currentTarget.checked })}
              />

              {compressionConfig.enabled && (
                <Stack gap="sm" ml="lg" pl="lg" style={{ borderLeft: '1px solid var(--mantine-color-default-border)' }}>
                  <Text size="sm" c="dimmed">
                    Archives will pack the original DICOMs in <code>derivatives/dcm-original</code> into encrypted
                    7-Zip bundles written to <code>derivatives/archives</code>.
                  </Text>

                  <Group gap="md" grow>
                    <TextInput
                      label="Max chunk size"
                      placeholder="e.g., 100GB, 500GiB, 102400MiB"
                      value={compressionConfig.chunk}
                      onChange={(event) => updateCompression({ chunk: event.currentTarget.value })}
                    />
                    <Select
                      label="Packing strategy"
                      data={[
                        { value: 'ordered', label: 'Keep folder order' },
                        { value: 'ffd', label: 'First-fit decreasing' },
                      ]}
                      value={compressionConfig.strategy}
                      onChange={(value) =>
                        updateCompression({ strategy: (value as 'ordered' | 'ffd') ?? 'ordered' })
                      }
                    />
                  </Group>

                  <Group gap="md" grow>
                    <NumberInput
                      label="7z compression level"
                      min={0}
                      max={9}
                      value={compressionConfig.compression}
                      onChange={(value) => updateCompression({ compression: Number(value ?? 0) })}
                    />
                    <NumberInput
                      label="Parallel workers"
                      min={1}
                      max={16}
                      value={compressionConfig.workers}
                      onChange={(value) => updateCompression({ workers: Number(value ?? 1) })}
                    />
                  </Group>

                  <PasswordInput
                    label="Archive password (AES-256)"
                    placeholder="Required"
                    value={compressionConfig.password}
                    onChange={(event) => updateCompression({ password: event.currentTarget.value })}
                    required
                  />

                  <Group gap="md" grow>
                    <Switch
                      label="Verify archives (7z t)"
                      checked={compressionConfig.verify}
                      onChange={(event) => updateCompression({ verify: event.currentTarget.checked })}
                    />
                    <NumberInput
                      label="PAR2 redundancy %"
                      min={0}
                      max={50}
                      value={compressionConfig.par2}
                      onChange={(value) => updateCompression({ par2: Number(value ?? 0) })}
                    />
                  </Group>
                </Stack>
              )}
            </Stack>
          </Accordion.Panel>
        </Accordion.Item>

        <Accordion.Item value="folder-rename">
          <Accordion.Control>Folder name check</Accordion.Control>
          <Accordion.Panel>
            <Stack gap="sm">
              <Text size="sm" c="dimmed">
                After anonymization completes, update any folder names that still contain the original Patient ID to
                use the anonymized ID. This inspects directories under <code>derivatives/dcm-raw</code>.
              </Text>
              <Switch
                label="Rename folders containing old Patient IDs"
                checked={config.renamePatientFolders ?? false}
                onChange={(event) => handleFieldChange('renamePatientFolders', event.currentTarget.checked)}
              />
            </Stack>
          </Accordion.Panel>
        </Accordion.Item>

        <Accordion.Item value="execution">
          <Accordion.Control>Execution options</Accordion.Control>
          <Accordion.Panel>
            <Stack gap="sm">
              <Group gap="md" align="flex-end">
                <NumberInput
                  label="Process count"
                  min={1}
                  value={config.processCount}
                  onChange={(value) => handleFieldChange('processCount', Number(value ?? 1))}
                />
                <NumberInput
                  label="Worker threads"
                  min={1}
                  value={config.workerCount}
                  onChange={(value) => handleFieldChange('workerCount', Number(value ?? 1))}
                />
                {onRecommendResources && (
                  <Button
                    size="sm"
                    variant="light"
                    onClick={onRecommendResources}
                    loading={recommendLoading}
                  >
                    Recommend
                  </Button>
                )}
              </Group>
              {recommendation && (
                <Stack gap={2} p="sm" style={{ border: '1px solid var(--mantine-color-default-border)', borderRadius: 'var(--mantine-radius-md)' }}>
                  <Text size="xs" c="dimmed">
                    CPU cores: {recommendation.cpu_count}
                  </Text>
                  <Text size="xs" c="dimmed">
                    RAM available: {formatBytes(recommendation.memory_available)} / total {formatBytes(recommendation.memory_total)}
                  </Text>
                  <Text size="xs" c="dimmed">
                    Disk throughput: {formatBytesPerSecond(recommendation.disk_read_bytes_per_sec)} read · {formatBytesPerSecond(recommendation.disk_write_bytes_per_sec)} write
                  </Text>
                  <Text size="xs" c="dimmed">
                    Recommended allocation: {recommendation.recommended_workers} workers / {recommendation.recommended_processes} processes
                  </Text>
                </Stack>
              )}
              <Switch
                label="Skip missing files and continue"
                checked={config.skipMissingFiles}
                onChange={(event) => handleFieldChange('skipMissingFiles', event.currentTarget.checked)}
              />
              <Stack gap={2}>
                <Switch
                  label="Reuse audited studies when resuming"
                  checked={config.auditResumePerLeaf ?? true}
                  onChange={(event) => handleFieldChange('auditResumePerLeaf', event.currentTarget.checked)}
                />
                <Text size="xs" c="dimmed">
                  Skips heavy DICOM re-reads for leaves already marked complete.
                </Text>
              </Stack>
              <Switch
                label="Preserve UID encoding when writing"
                checked={config.preserveUids ?? true}
                onChange={(event) => handleFieldChange('preserveUids', event.currentTarget.checked)}
              />
            </Stack>
          </Accordion.Panel>
        </Accordion.Item>
      </Accordion>

      <Divider label={`Previewing configuration for ${cohortName}`} />
    </Stack>
  );
};
