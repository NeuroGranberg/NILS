import type { ExtractStageConfig } from '../../types';

export const buildDefaultExtractConfig = (): ExtractStageConfig => ({
  extensionMode: 'all',
  maxWorkers: 4,
  batchSize: 100,
  queueSize: 10,
  seriesWorkersPerSubject: 1,
  duplicatePolicy: 'skip',
  resume: true,
  resumeByPath: true,
  subjectIdTypeId: null,
  subjectCodeCsv: null,
  subjectCodeSeed: '',
  adaptiveBatchingEnabled: false,
  adaptiveTargetTxMs: 200,
  adaptiveMinBatchSize: 50,
  adaptiveMaxBatchSize: 1000,
  useProcessPool: true,
  processPoolWorkers: null,
  dbWriterPoolSize: 3,
});
