import type { AnonymizeStageConfig, CompressionOptions } from '../../types';
import { ALL_CATEGORY_TAG_CODES, TIME_AND_DATE_CODES } from './constants';

const DEFAULT_CSV_FILENAME = 'metadata.csv';
const DEFAULT_EXCEL_FILENAME = 'metadata_audit.xlsx';

const sanitizeCohortNameForPrefix = (name?: string) => {
  const fallback = 'COHORT';
  if (!name) return `${fallback}XXXX`;
  const upper = name
    .normalize('NFKD')
    .replace(/[^\p{Letter}\p{Number}]+/gu, '')
    .toUpperCase();
  const base = upper || fallback;
  return base.includes('X') ? base : `${base}XXXX`;
};

const sanitizeCohortNameForSalt = (name?: string) => {
  const fallback = 'cohort';
  if (!name) return `${fallback}-salt`;
  const slug = name
    .normalize('NFKD')
    .replace(/[^\p{ASCII}]+/gu, '')
    .replace(/[^A-Za-z0-9]+/g, '-');
  const trimmed = slug.replace(/^-+|-+$/g, '') || fallback;
  return `${trimmed.toLowerCase()}-salt`;
};

export interface BuildAnonymizeConfigOptions {
  cohortName?: string;
  sourcePath?: string;
}

export const buildDefaultAnonymizeConfig = (
  options: BuildAnonymizeConfigOptions,
  system?: { recommendedProcesses?: number; recommendedWorkers?: number },
): AnonymizeStageConfig => {
  const sourceRoot = options.sourcePath ?? '/data';
  const fallbackTemplate = sanitizeCohortNameForPrefix(options.cohortName);
  const folderPrefix = fallbackTemplate.replace(/X+$/u, '');
  const deterministicSalt = sanitizeCohortNameForSalt(options.cohortName);
  const recommended = system?.recommendedProcesses ?? system?.recommendedWorkers ?? 1;
  const boundedRecommended = Math.min(Math.max(recommended, 1), 128);

  return {
    sourceRoot,
    metadataFilename: DEFAULT_EXCEL_FILENAME,
    scrubbedTagCodes: ALL_CATEGORY_TAG_CODES.filter((code) => !TIME_AND_DATE_CODES.includes(code)),
    updatePatientIds: true,
    patientIdStrategy: 'sequential',
    patientIdPrefixTemplate: fallbackTemplate,
    patientIdStartingNumber: 1,
    folderStrategy: 'depth',
    folderDepthAfterRoot: 1,
    folderRegex: '(.+)',
    numberRanges: [],
    folderFallbackTemplate: folderPrefix,
    csvMapping: {
      filePath: '',
      fileToken: undefined,
      fileName: undefined,
      sourceColumn: '',
      targetColumn: '',
      missingMode: 'hash',
      missingPattern: 'MISSEDXXXXX',
      missingSalt: 'csv-missed',
      preserveTopFolderOrder: true,
    },
    deterministicPattern: fallbackTemplate,
    deterministicSalt,
    sequentialPattern: fallbackTemplate,
    sequentialStartingNumber: 1,
    sequentialDiscovery: 'per_top_folder',
    updateStudyDates: false,
    snapToSixMonths: true,
    minimumOffsetMonths: 0,
    outputFormat: 'encrypted_excel',
    excelPassword: '',
    processCount: boundedRecommended,
    workerCount: boundedRecommended,
    skipMissingFiles: false,
    preserveUids: true,
    derivativesRetryMode: 'prompt',
    renamePatientFolders: false,
    resume: false,
    auditResumePerLeaf: true,
    compression: {
      enabled: false,
      chunk: '100GB',
      strategy: 'ordered',
      compression: 3,
      workers: 2,
      verify: true,
      par2: 0,
      password: '',
    },
  };
};

export const buildAnonymizeConfigFromExisting = (
  config: Partial<AnonymizeStageConfig> | undefined,
  options: BuildAnonymizeConfigOptions,
  system?: { recommendedProcesses?: number; recommendedWorkers?: number },
) => {
  const base = buildDefaultAnonymizeConfig(options, system);
  const merged: AnonymizeStageConfig = {
    ...base,
    ...(config ?? {}),
  };

  const baseCompression = base.compression ?? {
    enabled: false,
    chunk: '100GB',
    strategy: 'ordered' as const,
    compression: 3,
    workers: 2,
    password: '',
    verify: true,
    par2: 0,
  };
  const partialCompression: Partial<CompressionOptions> = merged.compression ?? {};
  merged.compression = {
    enabled: partialCompression.enabled ?? baseCompression.enabled,
    chunk: partialCompression.chunk ?? baseCompression.chunk,
    strategy: partialCompression.strategy ?? baseCompression.strategy,
    compression: partialCompression.compression ?? baseCompression.compression,
    workers: partialCompression.workers ?? baseCompression.workers,
    password: partialCompression.password ?? baseCompression.password,
    verify: partialCompression.verify ?? baseCompression.verify,
    par2: partialCompression.par2 ?? baseCompression.par2,
  };

  const baseCsv: NonNullable<AnonymizeStageConfig['csvMapping']> = base.csvMapping ?? {
    filePath: '',
    fileToken: undefined,
    fileName: undefined,
    sourceColumn: '',
    targetColumn: '',
    missingMode: 'hash' as const,
    missingPattern: 'MISSEDXXXXX',
    missingSalt: 'csv-missed',
    preserveTopFolderOrder: true,
  };
  const partialCsv: Partial<NonNullable<AnonymizeStageConfig['csvMapping']>> = merged.csvMapping ?? {};
  merged.csvMapping = {
    filePath: partialCsv.filePath ?? baseCsv.filePath,
    fileToken: partialCsv.fileToken ?? baseCsv.fileToken,
    fileName: partialCsv.fileName ?? baseCsv.fileName,
    sourceColumn: partialCsv.sourceColumn ?? baseCsv.sourceColumn,
    targetColumn: partialCsv.targetColumn ?? baseCsv.targetColumn,
    missingMode: partialCsv.missingMode ?? baseCsv.missingMode,
    missingPattern: partialCsv.missingPattern ?? baseCsv.missingPattern,
    missingSalt: partialCsv.missingSalt ?? baseCsv.missingSalt,
    preserveTopFolderOrder: partialCsv.preserveTopFolderOrder ?? baseCsv.preserveTopFolderOrder,
  };

  if (merged.csvMapping?.fileToken === '') {
    merged.csvMapping.fileToken = undefined;
  }

  if (merged.csvMapping?.fileName === '') {
    merged.csvMapping.fileName = undefined;
  }

  merged.deterministicPattern = merged.deterministicPattern ?? base.deterministicPattern;
  merged.deterministicSalt = merged.deterministicSalt ?? base.deterministicSalt;
  merged.sequentialPattern = merged.sequentialPattern ?? base.sequentialPattern;
  merged.sequentialStartingNumber = merged.sequentialStartingNumber ?? base.sequentialStartingNumber;
  merged.sequentialDiscovery = merged.sequentialDiscovery ?? base.sequentialDiscovery;
  merged.preserveUids = merged.preserveUids ?? base.preserveUids;
  merged.derivativesRetryMode = merged.derivativesRetryMode ?? base.derivativesRetryMode;
  merged.renamePatientFolders = merged.renamePatientFolders ?? base.renamePatientFolders;
  merged.resume = merged.resume ?? base.resume;
  merged.auditResumePerLeaf = merged.auditResumePerLeaf ?? base.auditResumePerLeaf;

  const expectedFilename = getDefaultFilenameForFormat(merged.outputFormat);
  if (!merged.metadataFilename) {
    merged.metadataFilename = expectedFilename;
  } else {
    const lower = merged.metadataFilename.toLowerCase();
    const expectedExt = expectedFilename.slice(expectedFilename.lastIndexOf('.'));
    if (!lower.endsWith(expectedExt)) {
      const sanitized = merged.metadataFilename.replace(/\.[^/.]+$/u, '');
      merged.metadataFilename = `${sanitized}${expectedExt}`;
    }
  }

  const uniqueCodes = Array.from(new Set(merged.scrubbedTagCodes ?? []));
  merged.scrubbedTagCodes = uniqueCodes.length > 0 ? uniqueCodes : [...base.scrubbedTagCodes];

  merged.excelPassword = merged.outputFormat === 'encrypted_excel' ? merged.excelPassword ?? '' : undefined;

  return merged;
};

export const getDefaultFilenameForFormat = (format: AnonymizeStageConfig['outputFormat']) =>
  format === 'encrypted_excel' ? DEFAULT_EXCEL_FILENAME : DEFAULT_CSV_FILENAME;
