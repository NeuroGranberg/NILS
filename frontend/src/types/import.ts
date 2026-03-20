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

// Observation Type Import types
export interface ObservationTypeDetail {
  observationTypeId: number;
  category: string;
  name: string;
  description?: string | null;
  unit?: string | null;
  valueType?: string | null;
  minValue?: number | null;
  maxValue?: number | null;
  isActive: boolean;
  isPrimary: boolean;
}

export interface ObservationTypeImportFieldDefinition {
  name: string;
  label: string;
  required?: boolean;
  parsers: string[];
  defaultParser: string;
}

export interface ObservationTypeFieldsResponse {
  observationTypeFields: ObservationTypeImportFieldDefinition[];
}

export interface ObservationTypeImportPayload {
  fileToken?: string;
  filePath?: string;
  fields: Record<string, SubjectImportFieldMapping>;
  options?: {
    skipBlankUpdates?: boolean;
  };
  dryRun?: boolean;
}

export interface ObservationTypeImportPreviewRow {
  observationType: Record<string, unknown>;
  existing?: boolean;
  existingObservationType?: Record<string, unknown> | null;
}

export interface ObservationTypeImportPreview {
  totalRows: number;
  processedRows: number;
  skippedRows: number;
  warnings: string[];
  rows: ObservationTypeImportPreviewRow[];
}

export interface ObservationTypeImportResult {
  typesInserted: number;
  typesUpdated: number;
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

// ---------------------------------------------------------------------------
// Event import
// ---------------------------------------------------------------------------

export interface EventImportObservationTypeSummary {
  id: number;
  category: string;
  name: string;
  valueType: string | null;
  unit: string | null;
  minValue: number | null;
  maxValue: number | null;
}

export interface EventImportIdTypeSummary {
  id: number;
  name: string;
}

export interface EventImportFieldDefinition {
  name: string;
  label: string;
  required: boolean;
  parsers: string[];
  defaultParser: string;
}

export interface EventImportFieldsResponse {
  eventFields: EventImportFieldDefinition[];
  measureFields: EventImportFieldDefinition[];
  observationTypes: EventImportObservationTypeSummary[];
  idTypes: EventImportIdTypeSummary[];
}

export interface EventImportPayload {
  fileToken?: string;
  filePath?: string;
  observationTypeId: number;
  subjectIdentifierType: string;
  fields: Record<string, SubjectImportFieldMapping>;
  options?: { skipBlankUpdates?: boolean };
  dryRun?: boolean;
}

export interface EventImportPreviewRow {
  subjectIdentifier: string;
  resolvedSubjectCode: string | null;
  subjectFound: boolean;
  eventDate: string;
  eventTime: string | null;
  notes: string | null;
  value: string | number | boolean | null;
  existing: boolean;
  existingEvent: Record<string, unknown> | null;
}

export interface EventImportPreview {
  totalRows: number;
  processedRows: number;
  skippedRows: number;
  warnings: string[];
  rows: EventImportPreviewRow[];
}

export interface EventImportResult {
  eventsInserted: number;
  eventsUpdated: number;
  measuresInserted: number;
  measuresUpdated: number;
  subjectsMissing: number;
}

// ---------------------------------------------------------------------------
// Disease import
// ---------------------------------------------------------------------------

export interface DiseaseImportFieldDefinition {
  name: string;
  label: string;
  required: boolean;
  parsers: string[];
  defaultParser: string;
}

export interface DiseaseFieldsResponse {
  diseaseFields: DiseaseImportFieldDefinition[];
}

export interface DiseaseImportPayload {
  fileToken?: string;
  filePath?: string;
  fields: Record<string, SubjectImportFieldMapping>;
  options?: { skipBlankUpdates?: boolean };
  dryRun?: boolean;
}

export interface DiseaseDetail {
  diseaseId: number;
  diseaseName: string;
  diseaseCode: string | null;
  description: string | null;
}

export interface DiseaseImportPreviewRow {
  disease: Record<string, unknown>;
  existing: boolean;
  existingDisease: Record<string, unknown> | null;
}

export interface DiseaseImportPreview {
  totalRows: number;
  processedRows: number;
  skippedRows: number;
  warnings: string[];
  rows: DiseaseImportPreviewRow[];
}

export interface DiseaseImportResult {
  diseasesInserted: number;
  diseasesUpdated: number;
}

// ---------------------------------------------------------------------------
// Disease type import
// ---------------------------------------------------------------------------

export interface DiseaseTypeDiseaseSummary {
  id: number;
  name: string;
  code: string | null;
}

export interface DiseaseTypeFieldsResponse {
  diseases: DiseaseTypeDiseaseSummary[];
}

export interface DiseaseTypeDetail {
  diseaseTypeId: number;
  diseaseId: number;
  typeName: string;
  description: string | null;
  sortOrder: number | null;
}

export interface DiseaseTypeUpsertPayload {
  diseaseTypeId?: number | null;
  diseaseId: number;
  typeName: string;
  description?: string | null;
  sortOrder?: number | null;
  dryRun?: boolean;
}

export interface DiseaseTypeUpsertResult {
  diseaseTypeId: number;
  inserted: boolean;
  updated: boolean;
}

// ---------------------------------------------------------------------------
// Subject disease import
// ---------------------------------------------------------------------------

export interface SubjectDiseaseDiseaseSummary {
  id: number;
  name: string;
  code: string | null;
}

export interface SubjectDiseaseCohortSummary {
  id: number;
  name: string;
  subjectCount: number;
}

export interface SubjectDiseaseFieldDefinition {
  name: string;
  label: string;
  required: boolean;
  parsers: string[];
  defaultParser: string;
}

export interface SubjectDiseaseFieldsResponse {
  fields: SubjectDiseaseFieldDefinition[];
  diseases: SubjectDiseaseDiseaseSummary[];
  cohorts: SubjectDiseaseCohortSummary[];
  idTypes: { id: number; name: string }[];
}

export interface SubjectDiseaseImportPayload {
  fileToken?: string;
  filePath?: string;
  diseaseId: number;
  subjectIdentifierType: string;
  fields: Record<string, SubjectImportFieldMapping>;
  options?: { skipBlankUpdates?: boolean };
  dryRun?: boolean;
}

export interface SubjectDiseasePreviewRow {
  subjectIdentifier: string;
  resolvedSubjectCode: string | null;
  subjectFound: boolean;
  diagnosisNotes: string | null;
  familyHistory: string | null;
  hasDiagnosisEvent: boolean;
  hasOnsetEvent: boolean;
  existing: boolean;
}

export interface SubjectDiseaseImportPreview {
  totalRows: number;
  processedRows: number;
  skippedRows: number;
  warnings: string[];
  rows: SubjectDiseasePreviewRow[];
}

export interface SubjectDiseaseImportResult {
  inserted: number;
  updated: number;
  subjectsMissing: number;
  diagnosisEventsLinked: number;
  onsetEventsLinked: number;
}

export interface CohortAssignPayload {
  diseaseId: number;
  cohortId: number;
  dryRun?: boolean;
}

export interface CohortAssignResult {
  inserted: number;
  skipped: number;
  diagnosisEventsLinked: number;
  onsetEventsLinked: number;
  totalSubjects: number;
}

// ---------------------------------------------------------------------------
// Subject disease type import
// ---------------------------------------------------------------------------

export interface SDTDiseaseTypeSummary {
  id: number;
  diseaseId: number;
  name: string;
  description: string | null;
  aliases: string[];
}

export interface SDTFieldDefinition {
  name: string;
  label: string;
  required: boolean;
  parsers: string[];
  defaultParser: string;
}

export interface SDTFieldsResponse {
  fields: SDTFieldDefinition[];
  diseases: SubjectDiseaseDiseaseSummary[];
  diseaseTypes: SDTDiseaseTypeSummary[];
  idTypes: { id: number; name: string }[];
}

export interface SDTImportPayload {
  fileToken?: string;
  filePath?: string;
  diseaseId: number;
  subjectIdentifierType: string;
  fields: Record<string, SubjectImportFieldMapping>;
  dryRun?: boolean;
}

export interface SDTPreviewRow {
  subjectIdentifier: string;
  resolvedSubjectCode: string | null;
  subjectFound: boolean;
  diseaseTypeInput: string;
  resolvedDiseaseType: string | null;
  diseaseTypeFound: boolean;
  assignmentDate: string | null;
  hasTransitionEvent: boolean;
  notes: string | null;
  existing: boolean;
}

export interface SDTImportPreview {
  totalRows: number;
  processedRows: number;
  skippedRows: number;
  warnings: string[];
  rows: SDTPreviewRow[];
}

export interface SDTImportResult {
  inserted: number;
  updated: number;
  subjectsMissing: number;
  typesUnresolved: number;
  transitionEventsLinked: number;
}

export interface SDTManualPayload {
  subjectDiseaseTypeId?: number;
  subjectIdentifier: string;
  subjectIdentifierType: string;
  diseaseId: number;
  diseaseTypeId: number;
  assignmentDate?: string;
  notes?: string;
  dryRun?: boolean;
}

export interface SDTManualResult {
  subjectDiseaseTypeId: number;
  inserted: boolean;
  updated: boolean;
}

export interface SDTDetailResponse {
  subjectDiseaseTypeId: number;
  subjectDiseaseId: number;
  diseaseTypeId: number;
  diseaseTypeName: string;
  assignmentDate: string;
  transitionEventId: number | null;
  notes: string | null;
  isActive: number;
}
