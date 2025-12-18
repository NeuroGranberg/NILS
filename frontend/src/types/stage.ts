import type { BidsStageConfig } from './bids';

export type StageId = 'anonymize' | 'extract' | 'sort' | 'bids';

export type StageStatus =
  | 'idle'
  | 'pending'
  | 'running'
  | 'completed'
  | 'failed'
  | 'paused'
  | 'blocked';

export interface StageRun {
  id: string;
  stageId: StageId;
  startedAt: string;
  finishedAt?: string;
  status: StageStatus;
  progress: number;
  configSnapshot: Record<string, unknown>;
  notes?: string;
  metrics?: import('./job').JobMetrics | null;
}

import type { AnonymizeStageConfig } from './anonymize';
import type { ExtractStageConfig } from './extract';

export interface StageConfigById {
  anonymize: AnonymizeStageConfig;
  extract: ExtractStageConfig;
  sort: Record<string, unknown>;
  bids: BidsStageConfig;
}

export interface StageSummary<Id extends StageId = StageId> {
  id: Id;
  title: string;
  description: string;
  status: StageStatus;
  progress: number;
  lastRunAt?: string;
  nextActionLabel?: string;
  jobId?: string;
  runs: StageRun[];
  artifacts?: Array<{
    id: string;
    name: string;
    type: 'table' | 'file' | 'log';
    previewPath?: string;
  }>;
  config?: StageConfigById[Id];
}

export const STAGE_LABELS: Record<StageId, string> = {
  anonymize: 'Anonymization',
  extract: 'Metadata Extraction',
  sort: 'Sorting',
  bids: 'BIDS Export',
};

export const STAGE_ORDER: StageId[] = [
  'anonymize',
  'extract',
  'sort',
  'bids',
];
