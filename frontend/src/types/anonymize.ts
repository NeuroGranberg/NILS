export type PatientIdStrategy =
  | 'none'
  | 'folder'
  | 'csv'
  | 'deterministic'
  | 'sequential';

export type CsvMissingMode = 'hash' | 'per_top_folder_seq';

export type SequentialDiscovery = 'per_top_folder' | 'one_per_study' | 'all';

export interface NumberRange {
  start: number;
  end: number;
}

export interface CompressionOptions {
  enabled: boolean;
  chunk: string;
  strategy: 'ordered' | 'ffd';
  compression: number;
  workers: number;
  password: string;
  verify: boolean;
  par2?: number;
}

export type AnonymizeCategory =
  | 'Patient_Information'
  | 'Clinical_Trial_Information'
  | 'Healthcare_Provider_Information'
  | 'Institution_Information'
  | 'Time_And_Date_Information';

export interface AnonymizeStageConfig {
  sourceRoot: string;
  metadataFilename: string;
  scrubbedTagCodes: string[];

  updatePatientIds: boolean;
  patientIdStrategy: PatientIdStrategy;
  patientIdPrefixTemplate: string;
  patientIdStartingNumber: number;
  folderStrategy: 'depth' | 'regex';
  folderDepthAfterRoot: number;
  folderRegex: string;
  numberRanges: NumberRange[];
  folderFallbackTemplate?: string;
  csvMapping?: {
    filePath: string;
    fileToken?: string;
    fileName?: string;
    sourceColumn: string;
    targetColumn: string;
    missingMode: CsvMissingMode;
    missingPattern: string;
    missingSalt: string;
    preserveTopFolderOrder: boolean;
  };
  deterministicPattern?: string;
  deterministicSalt?: string;
  sequentialPattern?: string;
  sequentialStartingNumber?: number;
  sequentialDiscovery?: SequentialDiscovery;

  updateStudyDates: boolean;
  snapToSixMonths: boolean;
  minimumOffsetMonths: number;

  outputFormat: 'csv' | 'encrypted_excel';
  excelPassword?: string;

  processCount: number;
  workerCount: number;
  skipMissingFiles: boolean;
  preserveUids: boolean;
  derivativesRetryMode?: 'prompt' | 'clean' | 'overwrite';
  renamePatientFolders?: boolean;
  resume?: boolean;
  auditResumePerLeaf?: boolean;

  compression?: CompressionOptions;
}
