export type BidsOutputMode = 'dcm' | 'nii' | 'nii.gz';
export type BidsLayout = 'bids' | 'flat';
export type BidsOverwriteMode = 'clean' | 'overwrite' | 'skip';

export interface BidsStageConfig {
  outputModes: BidsOutputMode[];
  // Legacy single selection for backwards compatibility
  outputMode?: BidsOutputMode;
  layout: BidsLayout;
  overwriteMode: BidsOverwriteMode;
  includeIntents: string[];
  includeProvenance: string[];
  excludeProvenance: string[];
  groupSyMRI: boolean;
  copyWorkers: number;
  convertWorkers: number;
  bidsDcmRootName: string;
  bidsNiftiRootName: string;
  flatDcmRootName: string;
  flatNiftiRootName: string;
  dcm2niixPath?: string;
  // Subject identifier source: "subject_code" (default) or id_type_id number
  subjectIdentifierSource: string | number;
  // Field strength filter: empty = all; [3.0, 7.0] = only 3T and 7T
  includeFieldStrengths: number[];
}

