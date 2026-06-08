// Types for the analysis-pipelines feature.
// DTO fields mirror the backend (snake_case); descriptor sub-keys are HYPHENATED
// wire keys (Boutiques + x-nils). `optional` defaults to `true` on the backend,
// so "required" means `optional === false`. The list flag wire key is literally `list`.

// ---- Enums / unions ----
export type InputSource = 'db_subset' | 'external_path';

export type RunStatus =
  | 'pending'
  | 'materializing'
  | 'launching'
  | 'running'
  | 'completed'
  | 'completed_with_warnings'
  | 'failed'
  | 'canceled';

export const TERMINAL_RUN_STATUSES: readonly RunStatus[] = [
  'completed',
  'completed_with_warnings',
  'failed',
  'canceled',
] as const;

export const isTerminalRunStatus = (s: string | undefined): boolean =>
  s != null && (TERMINAL_RUN_STATUSES as readonly string[]).includes(s);

// ---- Descriptor (Boutiques subset + x-nils, HYPHENATED wire keys) ----
export interface ContainerImage {
  type: string; // "docker" | "singularity"
  image: string;
  'container-hash'?: string | null;
}

export interface DescriptorInput {
  id: string;
  name: string;
  type: 'String' | 'File' | 'Flag' | 'Number' | string; // tolerate unknown future types
  description?: string | null;
  'command-line-flag'?: string | null;
  'value-key'?: string | null;
  optional?: boolean; // backend DEFAULTS true -> required = optional === false
  'default-value'?: unknown;
  'value-choices'?: unknown[] | null;
  list?: boolean; // Boutiques list flag (wire key is literally "list")
  minimum?: number | null;
  maximum?: number | null;
  integer?: boolean;
}

export interface DescriptorGroup {
  id: string;
  name?: string | null;
  members: string[];
  'mutually-exclusive'?: boolean;
  'one-is-required'?: boolean;
  'all-or-none'?: boolean;
}

export interface DescriptorOutputFile {
  id: string;
  name: string;
  'path-template': string;
}

export interface XNilsInput {
  formats: string[]; // ["nifti"] | ["dicom"] | ["bids"]
  layout: string; // "bids" | "flat" | "dicom"
}

export interface XNilsNeeds {
  fs_license?: boolean;
  templateflow?: boolean;
  gpu?: boolean;
  work_dir?: boolean;
  [k: string]: unknown; // unknown needs carried
}

export interface XNils {
  'analysis-level': string;
  input?: XNilsInput;
  needs?: XNilsNeeds;
  steps?: { name: string; command: string }[];
  ingest?: { enabled: boolean; entrypoint?: string | null };
  [k: string]: unknown;
}

export interface Descriptor {
  name: string;
  'schema-version': string;
  'tool-version'?: string | null;
  description?: string | null;
  'command-line'?: string | null;
  'container-image'?: ContainerImage | null;
  inputs?: DescriptorInput[];
  groups?: DescriptorGroup[];
  'output-files'?: DescriptorOutputFile[];
  'suggested-resources'?: {
    'cpu-cores'?: number;
    ram?: number;
    'walltime-estimate'?: number;
  } | null;
  'x-nils'?: XNils;
  [k: string]: unknown; // forward-compat: carry unknown top-level keys
}

// ---- DTOs (mirror backend AnalysisPipeline*DTO) ----
export interface RepoDTO {
  id: number;
  url: string;
  sha: string;
  default_branch: string | null;
  version: number;
  created_at: string;
  updated_at: string;
}

export interface PipelineDTO {
  id: number;
  repo_id: number;
  slug: string;
  name: string;
  description?: string | null;
  work_unit: string;
  analysis_level: string;
  image_digest?: string | null;
  schema_version: string;
  descriptor: Descriptor;
  version: number;
  created_at: string;
  updated_at: string;
}

export interface RunDTO {
  id: number;
  pipeline_id: number;
  input_source: string; // InputSource at the type boundary; backend stores str
  stack_ids?: number[] | null;
  external_path?: string | null;
  params: Record<string, unknown>;
  repo_sha: string;
  image_digest?: string | null;
  input_path?: string | null;
  output_path?: string | null;
  work_dir?: string | null;
  retain_input: boolean;
  prefect_run_id?: string | null;
  provenance: Record<string, unknown>;
  status: RunStatus | string;
  last_error?: string | null;
  job_id?: number | null;
  created_at: string;
  updated_at: string;
}

// Bridge state (GET /runs/{id}/state -> { state: RunState }); extra:"allow" on backend
export interface RunState {
  prefect_run_id: string;
  state: string; // pending|running|completed|failed|crashed|canceled
  units_total: number;
  units_done: number;
  units_failed: number;
  [k: string]: unknown;
}

// ---- Request bodies ----
export interface RegisterRepoBody {
  url: string;
  ref?: string;
}

export interface RefreshRepoBody {
  ref?: string;
}

export interface CreateRunBody {
  input_source: InputSource;
  stack_ids?: number[];
  external_path?: string;
  params?: Record<string, unknown>;
  output_path?: string;
  retain_input: boolean;
  config?: Record<string, unknown>;
  name?: string;
}

// ---- Schema-form value type (the params object the renderer yields) ----
export type FormValue = string | number | boolean | string[] | number[] | null;
export type FormValues = Record<string, FormValue>;

// ---- Response envelope types ----
export interface ReposResponse {
  repos: RepoDTO[];
}
export interface RegisterRepoResponse {
  repo: RepoDTO;
  pipelines: PipelineDTO[];
}
export interface PipelinesResponse {
  pipelines: PipelineDTO[];
}
export interface PipelineResponse {
  pipeline: PipelineDTO;
}
export interface RunResponse {
  run: RunDTO;
  job: JobDTO | null;
}
export interface CreateRunResponse {
  run: RunDTO;
  job: JobDTO | null;
}
export interface RunStateResponse {
  state: RunState;
}
export interface RunLogsResponse {
  since: number;
  next_cursor: number;
  lines: string[];
}
export interface CancelRunResponse {
  run: RunDTO | null;
}
export interface IngestResponse {
  job?: JobDTO;
  plan?: Record<string, unknown>;
}

// ---- Per-unit results (results.json) + list-runs ----
export interface UnitResult {
  unit_id: string;
  work_unit: string;
  status: string; // 'succeeded' | 'failed' | 'skipped'
  derivatives: string[];
  metrics: Record<string, unknown>;
  error?: string | null;
}
export interface RunResults {
  schema_version: string;
  units: UnitResult[];
}
export interface RunsResponse {
  runs: RunDTO[];
}
export interface RunResultsResponse {
  results: RunResults;
}

// JobDTO: backend returns serialize_job(...) -> shape is the existing Job serializer.
// Reuse the app-wide Job type rather than redefining.
export type JobDTO = import('../../types').Job;
