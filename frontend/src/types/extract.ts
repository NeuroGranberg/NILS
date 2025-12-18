export interface ExtractSubjectCodeCsvConfig {
  fileToken?: string;
  filePath?: string;
  fileName?: string;
  patientColumn?: string;
  subjectCodeColumn?: string;
}

export interface ExtractStageConfig {
  extensionMode: 'all' | 'dcm' | 'DCM' | 'all_dcm' | 'no_ext';
  maxWorkers: number;
  batchSize: number;
  queueSize: number;
  seriesWorkersPerSubject: number;
  duplicatePolicy: 'skip' | 'overwrite' | 'append_series';
  resume: boolean;
  resumeByPath: boolean;
  subjectIdTypeId?: number | null;
  subjectCodeCsv?: ExtractSubjectCodeCsvConfig | null;
  subjectCodeSeed?: string;
  adaptiveBatchingEnabled: boolean;
  adaptiveTargetTxMs: number;
  adaptiveMinBatchSize: number;
  adaptiveMaxBatchSize: number;
  useProcessPool?: boolean;
  processPoolWorkers?: number | null;
  dbWriterPoolSize?: number;
}

export type ExtractPerformanceConfigPatch = Partial<
  Pick<
    ExtractStageConfig,
    | 'maxWorkers'
    | 'batchSize'
    | 'queueSize'
    | 'seriesWorkersPerSubject'
    | 'adaptiveBatchingEnabled'
    | 'adaptiveTargetTxMs'
    | 'adaptiveMinBatchSize'
    | 'adaptiveMaxBatchSize'
    | 'useProcessPool'
    | 'processPoolWorkers'
    | 'dbWriterPoolSize'
  >
>;
