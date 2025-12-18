/**
 * Data import types for subjects, cohorts, and identifiers.
 */

// Common field mapping types
export interface SubjectImportFieldMapping {
  column?: string;
  default?: string;
  parser?: string;
}

export interface SubjectImportFieldDefinition {
  name: string;
  label: string;
  required?: boolean;
  parsers: string[];
  defaultParser: string;
}

export interface SubjectImportIdTypeSummary {
  id: number;
  name: string;
  description?: string | null;
}

// Subject Import types
export interface SubjectImportFieldsResponse {
  subjectFields: SubjectImportFieldDefinition[];
  cohortFields: SubjectImportFieldDefinition[];
  identifierFields: SubjectImportFieldDefinition[];
  idTypes: SubjectImportIdTypeSummary[];
}

export interface SubjectImportCohortConfig {
  enabled: boolean;
  assignSubjects?: boolean;
  membershipMode?: 'append' | 'replace';
  name?: SubjectImportFieldMapping;
  owner?: SubjectImportFieldMapping;
  path?: SubjectImportFieldMapping;
  description?: SubjectImportFieldMapping;
  isActive?: SubjectImportFieldMapping;
}

export interface SubjectImportIdentifierConfig {
  idTypeId?: number;
  idTypeName?: string;
  value: SubjectImportFieldMapping;
}

export interface SubjectImportPayload {
  fileToken?: string;
  filePath?: string;
  subjectFields: Record<string, SubjectImportFieldMapping>;
  cohort?: SubjectImportCohortConfig;
  identifiers?: SubjectImportIdentifierConfig[];
  options?: {
    skipBlankUpdates?: boolean;
  };
  dryRun?: boolean;
}

export interface SubjectImportPreviewRow {
  subject: Record<string, unknown>;
  cohort?: Record<string, unknown> | null;
  identifiers: Record<string, unknown>[];
  existing?: boolean;
  existingSubject?: Record<string, unknown> | null;
}

export interface SubjectImportPreview {
  totalRows: number;
  processedRows: number;
  skippedRows: number;
  warnings: string[];
  rows: SubjectImportPreviewRow[];
}

export interface SubjectImportResult {
  subjectsInserted: number;
  subjectsUpdated: number;
  cohortsInserted?: number;
  cohortsUpdated?: number;
  identifiersInserted: number;
  identifiersSkipped: number;
}

// Cohort Import types
export interface CohortImportFieldDefinition {
  name: string;
  label: string;
  required?: boolean;
  parsers: string[];
  defaultParser: string;
}

export interface CohortImportFieldsResponse {
  cohortFields: CohortImportFieldDefinition[];
}

export interface CohortImportPayload {
  fileToken?: string;
  filePath?: string;
  cohortFields: Record<string, SubjectImportFieldMapping>;
  options?: {
    skipBlankUpdates?: boolean;
  };
  dryRun?: boolean;
}

export interface CohortImportPreviewRow {
  cohort: Record<string, unknown>;
  existing?: boolean;
  existingCohort?: Record<string, unknown> | null;
}

export interface CohortImportPreview {
  totalRows: number;
  processedRows: number;
  skippedRows: number;
  warnings: string[];
  rows: CohortImportPreviewRow[];
}

export interface CohortImportResult {
  cohortsInserted: number;
  cohortsUpdated: number;
}

// Subject-Cohort Import types
export interface SubjectCohortImportFieldDefinition {
  name: string;
  label: string;
  required?: boolean;
  parsers: string[];
  defaultParser: string;
}

export interface SubjectCohortImportFieldsResponse {
  subjectField: SubjectCohortImportFieldDefinition;
}

export interface SubjectCohortImportPayload {
  fileToken?: string;
  filePath?: string;
  subjectField: SubjectImportFieldMapping;
  staticCohortName: string;
  options?: {
    membershipMode?: 'append' | 'replace';
  };
  dryRun?: boolean;
}

export interface SubjectCohortImportPreviewRow {
  subjectCode: string;
  cohortName: string;
  subjectExists: boolean;
  cohortExists: boolean;
  alreadyMember: boolean;
}

export interface SubjectCohortImportPreview {
  totalRows: number;
  processedRows: number;
  skippedRows: number;
  warnings: string[];
  rows: SubjectCohortImportPreviewRow[];
}

export interface SubjectCohortImportResult {
  membershipsInserted: number;
  membershipsExisting: number;
  subjectsMissing: number;
  cohortsMissing: number;
  rowsSkipped: number;
  warnings: string[];
}

// Subject Identifier Import types
export interface SubjectIdentifierImportFieldsResponse {
  subjectField: {
    name: string;
    label: string;
    required?: boolean;
  };
  identifierField: {
    name: string;
    label: string;
    required?: boolean;
  };
  idTypes: SubjectImportIdTypeSummary[];
}

export interface SubjectIdentifierImportPayload {
  fileToken?: string;
  filePath?: string;
  subjectField: SubjectImportFieldMapping;
  identifierField: SubjectImportFieldMapping;
  staticIdTypeId: number;
  options?: {
    mode?: 'append' | 'replace';
  };
  dryRun?: boolean;
}

export interface SubjectIdentifierImportPreviewRow {
  subjectCode: string;
  idTypeId?: number | null;
  idTypeName?: string | null;
  identifierValue?: string | null;
  subjectExists: boolean;
  idTypeExists: boolean;
  existingValue: boolean;
}

export interface SubjectIdentifierImportPreview {
  totalRows: number;
  processedRows: number;
  skippedRows: number;
  identifiersInserted: number;
  identifiersSkipped: number;
  warnings: string[];
  rows: SubjectIdentifierImportPreviewRow[];
}

export interface SubjectIdentifierImportResult {
  identifiersInserted: number;
  identifiersUpdated: number;
  identifiersSkipped: number;
  subjectsMissing: number;
  idTypesMissing: number;
  rowsSkipped: number;
  warnings: string[];
}
