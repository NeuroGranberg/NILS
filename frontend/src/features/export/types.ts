export interface ManifestEntry {
  subject_code: string;
  study_date?: string | null;
  series_stack_id?: number | null;
  series_stack_ids?: number[] | null;
}

export interface ColumnMapping {
  subjectCode: string | null;
  studyDate: string | null;
  stackId: string | null;
}

export interface ResolvedEntry {
  series_stack_id: number;
  subject_code: string;
  study_date: string;
  directory_type: string | null;
  base: string | null;
  modifier: string | null;
  technique: string | null;
  dicom_origin_cohort: string | null;
}

export interface ResolveResponse {
  total_subjects: number;
  total_sessions: number;
  total_stacks: number;
  resolved_stack_ids: number[];
  resolved_entries: ResolvedEntry[];
  warnings: string[];
  detected_cohorts: string[];
}

export interface ExportConfig {
  layout: 'bids' | 'flat';
  outputModes: string[];
  overwriteMode: 'skip' | 'clean' | 'overwrite';
  bidsDcmRootName: string;
  bidsNiftiRootName: string;
  flatDcmRootName: string;
  flatNiftiRootName: string;
  includeIntents: string[];
  includeProvenance: string[];
  excludeProvenance: string[];
  includeFieldStrengths: number[];
  copyWorkers: number;
  convertWorkers: number;
  subjectIdentifierSource: string | number;
}

export interface RunExportPayload {
  stack_ids: number[];
  detected_cohorts: string[];
  output_path: string;
  export_name: string;
  export_description: string;
  manifest_text: string;
  resolve_summary: {
    total_subjects: number;
    total_sessions: number;
  };
  config: ExportConfig;
}

export interface RunExportResponse {
  job: Record<string, unknown>;
}

export const DEFAULT_EXPORT_CONFIG: ExportConfig = {
  layout: 'bids',
  outputModes: ['dcm'],
  overwriteMode: 'skip',
  bidsDcmRootName: 'bids-dcm',
  bidsNiftiRootName: 'bids-nifti',
  flatDcmRootName: 'flat-dcm',
  flatNiftiRootName: 'flat-nifti',
  // No intent/provenance/field-strength filters for manifest-based exports:
  // the user explicitly selected stacks, so export all of them.
  includeIntents: ['anat', 'dwi', 'func', 'fmap', 'perf'],
  includeProvenance: [],
  excludeProvenance: [],
  includeFieldStrengths: [],
  copyWorkers: 8,
  convertWorkers: 8,
  subjectIdentifierSource: 'subject_code',
};
