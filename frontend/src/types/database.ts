/**
 * Database backup and management types.
 */

import type { JobSummary } from './job';

export type DatabaseKey = 'metadata' | 'application';

export interface DatabaseBackup {
  database: DatabaseKey;
  database_label: string;
  filename: string;
  path: string;
  size_bytes: number;
  created_at: string;
  note?: string;
}

export interface DatabaseSummary {
  database: DatabaseKey;
  database_label: string;
  tables: Record<string, number>;
}

export interface MetadataTableColumn {
  name: string;
  label: string;
  type: string;
  searchable: boolean;
  orderable: boolean;
}

export interface MetadataTableInfo {
  name: string;
  label: string;
  row_count: number;
  columns: MetadataTableColumn[];
}

// Application table types (same structure as metadata tables)
export type ApplicationTableColumn = MetadataTableColumn;

export interface ApplicationTableInfo {
  name: string;
  label: string;
  row_count: number;
  columns: ApplicationTableColumn[];
}

export interface CreateDatabaseBackupPayload {
  database: DatabaseKey;
  directory?: string;
  note?: string;
}

export interface RestoreDatabaseBackupPayload {
  database: DatabaseKey;
  path?: string;
}

export interface RestoreDatabaseBackupResponse {
  job: JobSummary;
  backup: DatabaseBackup;
}

export interface DeleteDatabaseBackupPayload {
  database: DatabaseKey;
  path: string;
}

export interface IdTypeInfo {
  id: number;
  name: string;
  description?: string | null;
  identifiersCount: number;
}

export interface IdTypeListResponse {
  items: IdTypeInfo[];
}

export interface CreateIdTypePayload {
  name: string;
  description?: string | null;
}

export interface UpdateIdTypePayload {
  id: number;
  name: string;
  description?: string | null;
}

export interface DeleteIdTypePayload {
  id: number;
}

export interface DeleteIdTypeResponse {
  id: number;
  name: string;
  identifiersDeleted: number;
}

export interface SubjectDetail {
  subjectCode: string;
  patientName: string | null;
  patientBirthDate: string | null;
  patientSex: string | null;
  ethnicGroup: string | null;
  occupation: string | null;
  additionalPatientHistory: string | null;
  isActive: boolean | null;
}

export interface CohortDetail {
  cohortId: number;
  name: string;
  owner: string | null;
  path: string | null;
  description: string | null;
  isActive: boolean | null;
}

export interface SubjectCohortMetadataCohort {
  cohortId: number;
  name: string;
  owner?: string | null;
  path?: string | null;
  description?: string | null;
  isActive?: boolean | null;
  createdAt?: string | null;
  updatedAt?: string | null;
}

export interface UpsertMetadataCohortPayload {
  name: string;
  owner: string;
  path: string;
  description?: string | null;
  isActive?: boolean | null;
}

export interface UpdateMetadataCohortPayload {
  owner: string;
  path: string;
  description?: string | null;
  isActive?: boolean | null;
}

export interface SubjectCohortMembership {
  subjectCode: string;
  cohortId: number;
  cohortName: string;
  owner?: string | null;
  path?: string | null;
  description?: string | null;
  notes?: string | null;
  createdAt?: string | null;
  updatedAt?: string | null;
}

export interface SubjectCohortMembershipsResponse {
  subjectCode: string;
  memberships: SubjectCohortMembership[];
}

export interface DeleteSubjectCohortMembershipPayload {
  subjectCode: string;
  cohortId?: number;
  cohortName?: string;
}

export interface SubjectIdentifierDetail {
  idTypeId: number;
  idTypeName: string;
  description?: string | null;
  identifierValue?: string | null;
  updatedAt?: string | null;
}

export interface SubjectIdentifierDetailResponse {
  subjectCode: string;
  subjectExists: boolean;
  identifiers: SubjectIdentifierDetail[];
}

export interface UpsertSubjectIdentifierPayload {
  subjectCode: string;
  idTypeId: number;
  identifierValue: string;
}

export interface DeleteSubjectIdentifierPayload {
  subjectCode: string;
  idTypeId: number;
}
