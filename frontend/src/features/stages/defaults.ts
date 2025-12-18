import type { StageConfigById } from '../../types';
import { buildDefaultExtractConfig } from '../extraction/defaults';

export type NonAnonymizeStageConfigDefaults = Pick<
  StageConfigById,
  'extract' | 'sort' | 'bids'
>;

export const buildNonAnonymizeStageDefaults = (): NonAnonymizeStageConfigDefaults => ({
  extract: buildDefaultExtractConfig(),
  sort: {
    profile: 'standard',
    applyLLMAssist: true,
    allowManualOverrides: true,
  },
  bids: {
    outputModes: ['dcm'],
    outputMode: 'dcm',
    layout: 'bids',
    overwriteMode: 'skip',
    includeIntents: ['anat', 'dwi', 'func', 'fmap', 'perf'],
    includeProvenance: ['SyMRI', 'SWIRecon', 'EPIMix'],
    excludeProvenance: [],
    groupSyMRI: true,
    copyWorkers: 8,
    convertWorkers: 8,
    bidsDcmRootName: 'bids-dcm',
    bidsNiftiRootName: 'bids-nifti',
    flatDcmRootName: 'flat-dcm',
    flatNiftiRootName: 'flat-nifti',
    subjectIdentifierSource: 'subject_code',
  },
});
