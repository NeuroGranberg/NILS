/**
 * Database management API hooks.
 */

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { notifications } from '@mantine/notifications';
import { apiClient } from '../../utils/api-client';
import { QUERY_KEYS, REFETCH_INTERVALS, STALE_TIMES } from '../../constants/api';
import type {
  // Database types
  DatabaseBackup,
  DatabaseSummary,
  MetadataTableInfo,
  CreateDatabaseBackupPayload,
  RestoreDatabaseBackupPayload,
  RestoreDatabaseBackupResponse,
  DeleteDatabaseBackupPayload,
  IdTypeInfo,
  IdTypeListResponse,
  CreateIdTypePayload,
  UpdateIdTypePayload,
  DeleteIdTypePayload,
  DeleteIdTypeResponse,
  SubjectDetail,
  CohortDetail,
  SubjectCohortMetadataCohort,
  UpsertMetadataCohortPayload,
  UpdateMetadataCohortPayload,
  SubjectCohortMembershipsResponse,
  DeleteSubjectCohortMembershipPayload,
  SubjectIdentifierDetail,
  SubjectIdentifierDetailResponse,
  UpsertSubjectIdentifierPayload,
  DeleteSubjectIdentifierPayload,
  // Import types
  SubjectImportFieldMapping,
  SubjectImportFieldDefinition,
  SubjectImportIdTypeSummary,
  SubjectImportFieldsResponse,
  SubjectImportCohortConfig,
  SubjectImportIdentifierConfig,
  SubjectImportPayload,
  SubjectImportPreviewRow,
  SubjectImportPreview,
  SubjectImportResult,
  CohortImportFieldDefinition,
  CohortImportFieldsResponse,
  CohortImportPayload,
  CohortImportPreviewRow,
  CohortImportPreview,
  CohortImportResult,
  SubjectCohortImportFieldDefinition,
  SubjectCohortImportFieldsResponse,
  SubjectCohortImportPayload,
  SubjectCohortImportPreviewRow,
  SubjectCohortImportPreview,
  SubjectCohortImportResult,
  SubjectIdentifierImportFieldsResponse,
  SubjectIdentifierImportPayload,
  SubjectIdentifierImportPreviewRow,
  SubjectIdentifierImportPreview,
  SubjectIdentifierImportResult,
  ObservationTypeDetail,
  ObservationTypeImportFieldDefinition,
  ObservationTypeFieldsResponse,
  ObservationTypeImportPayload,
  ObservationTypeImportPreviewRow,
  ObservationTypeImportPreview,
  ObservationTypeImportResult,
  EventImportFieldsResponse,
  EventImportPayload,
  EventImportPreview,
  EventImportResult,
  DiseaseDetail,
  DiseaseImportFieldDefinition,
  DiseaseFieldsResponse,
  DiseaseImportPayload,
  DiseaseImportPreview,
  DiseaseImportResult,
  DiseaseTypeFieldsResponse,
  DiseaseTypeDetail,
  DiseaseTypeUpsertPayload,
  DiseaseTypeUpsertResult,
  SubjectDiseaseFieldsResponse,
  SubjectDiseaseImportPayload,
  SubjectDiseaseImportPreview,
  SubjectDiseaseImportResult,
  CohortAssignPayload,
  CohortAssignResult,
  SDTFieldsResponse,
  SDTImportPayload,
  SDTImportPreview,
  SDTImportResult,
  SDTManualPayload,
  SDTManualResult,
} from '../../types';

// Re-export types for backwards compatibility
export type {
  // Database types
  DatabaseBackup,
  DatabaseSummary,
  MetadataTableInfo,
  CreateDatabaseBackupPayload,
  RestoreDatabaseBackupPayload,
  RestoreDatabaseBackupResponse,
  DeleteDatabaseBackupPayload,
  IdTypeInfo,
  IdTypeListResponse,
  CreateIdTypePayload,
  UpdateIdTypePayload,
  DeleteIdTypePayload,
  DeleteIdTypeResponse,
  SubjectDetail,
  CohortDetail,
  SubjectCohortMetadataCohort,
  UpsertMetadataCohortPayload,
  UpdateMetadataCohortPayload,
  SubjectCohortMembershipsResponse,
  DeleteSubjectCohortMembershipPayload,
  SubjectIdentifierDetail,
  SubjectIdentifierDetailResponse,
  UpsertSubjectIdentifierPayload,
  DeleteSubjectIdentifierPayload,
  // Import types
  SubjectImportFieldMapping,
  SubjectImportFieldDefinition,
  SubjectImportIdTypeSummary,
  SubjectImportFieldsResponse,
  SubjectImportCohortConfig,
  SubjectImportIdentifierConfig,
  SubjectImportPayload,
  SubjectImportPreviewRow,
  SubjectImportPreview,
  SubjectImportResult,
  CohortImportFieldDefinition,
  CohortImportFieldsResponse,
  CohortImportPayload,
  CohortImportPreviewRow,
  CohortImportPreview,
  CohortImportResult,
  SubjectCohortImportFieldDefinition,
  SubjectCohortImportFieldsResponse,
  SubjectCohortImportPayload,
  SubjectCohortImportPreviewRow,
  SubjectCohortImportPreview,
  SubjectCohortImportResult,
  SubjectIdentifierImportFieldsResponse,
  SubjectIdentifierImportPayload,
  SubjectIdentifierImportPreviewRow,
  SubjectIdentifierImportPreview,
  SubjectIdentifierImportResult,
  ObservationTypeDetail,
  ObservationTypeImportFieldDefinition,
  ObservationTypeFieldsResponse,
  ObservationTypeImportPayload,
  ObservationTypeImportPreviewRow,
  ObservationTypeImportPreview,
  ObservationTypeImportResult,
};

// Also re-export DatabaseKey for backwards compatibility
export type { DatabaseKey } from '../../types';

// Helper for showing error notifications
const showErrorNotification = (title: string, message: string) => {
  notifications.show({
    title,
    message,
    color: 'red',
    autoClose: 5000,
  });
};

// Helper for showing success notifications
const showSuccessNotification = (title: string, message: string) => {
  notifications.show({
    title,
    message,
    color: 'green',
    autoClose: 3000,
  });
};

// =============================================================================
// Database Backup Hooks
// =============================================================================

export const useDatabaseBackupsQuery = () =>
  useQuery<DatabaseBackup[]>({
    queryKey: QUERY_KEYS.databaseBackups,
    queryFn: () => apiClient.get<DatabaseBackup[]>('/database/backups'),
    // No refetchInterval - administrative queries don't need real-time updates
  });

export const useDatabaseSummaryQuery = () =>
  useQuery<DatabaseSummary[]>({
    queryKey: QUERY_KEYS.databaseSummary,
    queryFn: () => apiClient.get<DatabaseSummary[]>('/database/summary'),
    // No refetchInterval - administrative queries don't need real-time updates
  });

export const useCreateDatabaseBackup = () => {
  const queryClient = useQueryClient();
  return useMutation<DatabaseBackup, Error, CreateDatabaseBackupPayload>({
    mutationFn: (payload) => apiClient.post<DatabaseBackup>('/database/backups', payload),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.databaseBackups });
      showSuccessNotification('Backup Created', `Created backup: ${data.filename}`);
    },
    onError: (error) => {
      showErrorNotification('Backup Failed', error.message);
    },
  });
};

export const useRestoreDatabaseBackup = () => {
  const queryClient = useQueryClient();
  return useMutation<RestoreDatabaseBackupResponse, Error, RestoreDatabaseBackupPayload>({
    mutationFn: (payload) => apiClient.post<RestoreDatabaseBackupResponse>('/database/restore', payload),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.databaseBackups });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.cohorts });
      showSuccessNotification('Restore Started', 'Database restore job has been queued');
    },
    onError: (error) => {
      showErrorNotification('Restore Failed', error.message);
    },
  });
};

export const useDeleteDatabaseBackup = () => {
  const queryClient = useQueryClient();
  return useMutation<void, Error, DeleteDatabaseBackupPayload>({
    mutationFn: (payload) => apiClient.delete<void>('/database/backups', payload),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.databaseBackups });
      showSuccessNotification('Backup Deleted', 'Backup has been deleted');
    },
    onError: (error) => {
      showErrorNotification('Delete Failed', error.message);
    },
  });
};

// =============================================================================
// Metadata Table Hooks
// =============================================================================

export const useMetadataTables = () =>
  useQuery<MetadataTableInfo[]>({
    queryKey: QUERY_KEYS.metadataTables,
    queryFn: () => apiClient.get<MetadataTableInfo[]>('/metadata/tables'),
    staleTime: STALE_TIMES.medium,
    // No refetchInterval - table schema rarely changes
  });

// =============================================================================
// Application Table Hooks
// =============================================================================

export const useApplicationTables = () =>
  useQuery<MetadataTableInfo[]>({
    queryKey: QUERY_KEYS.applicationTables,
    queryFn: () => apiClient.get<MetadataTableInfo[]>('/application/tables'),
    staleTime: STALE_TIMES.medium,
    // No refetchInterval - table schema rarely changes
  });

// =============================================================================
// Subject Import Hooks
// =============================================================================

export const useSubjectImportFields = () =>
  useQuery<SubjectImportFieldsResponse>({
    queryKey: QUERY_KEYS.subjectImportFields,
    queryFn: () => apiClient.get<SubjectImportFieldsResponse>('/metadata/imports/subjects/fields'),
    staleTime: STALE_TIMES.long,
  });

export const useSubjectDetail = (subjectCode?: string | null) =>
  useQuery<SubjectDetail>({
    queryKey: QUERY_KEYS.subjectDetail(subjectCode ?? ''),
    enabled: Boolean(subjectCode),
    queryFn: () => apiClient.get<SubjectDetail>(`/metadata/subjects/${encodeURIComponent(subjectCode ?? '')}`),
    staleTime: STALE_TIMES.short,
  });

export const useSubjectImportPreview = () =>
  useMutation<SubjectImportPreview, Error, SubjectImportPayload>({
    mutationFn: (payload) => apiClient.post<SubjectImportPreview>('/metadata/imports/subjects/preview', payload),
    onError: (error) => {
      showErrorNotification('Preview Failed', error.message);
    },
  });

export const useSubjectImportApply = () => {
  const queryClient = useQueryClient();
  return useMutation<SubjectImportResult, Error, SubjectImportPayload>({
    mutationFn: (payload) => apiClient.post<SubjectImportResult>('/metadata/imports/subjects/apply', payload),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.metadataTables });
      const inserted = data.subjectsInserted;
      const updated = data.subjectsUpdated;
      showSuccessNotification('Import Complete', `Inserted: ${inserted}, Updated: ${updated}`);
    },
    onError: (error) => {
      showErrorNotification('Import Failed', error.message);
    },
  });
};

// =============================================================================
// Cohort Import Hooks
// =============================================================================

export const useCohortImportFields = () =>
  useQuery<CohortImportFieldsResponse>({
    queryKey: QUERY_KEYS.cohortImportFields,
    queryFn: () => apiClient.get<CohortImportFieldsResponse>('/metadata/imports/cohorts/fields'),
    staleTime: STALE_TIMES.long,
  });

export const useCohortDetailByName = (cohortName?: string | null) =>
  useQuery<CohortDetail>({
    queryKey: QUERY_KEYS.cohortDetail(cohortName ?? ''),
    enabled: Boolean(cohortName && cohortName.trim().length > 0),
    queryFn: () =>
      apiClient.get<CohortDetail>(
        `/metadata/cohorts/by-name/${encodeURIComponent((cohortName ?? '').trim())}`,
      ),
    staleTime: STALE_TIMES.short,
  });

export const useCohortImportPreview = () =>
  useMutation<CohortImportPreview, Error, CohortImportPayload>({
    mutationFn: (payload) => apiClient.post<CohortImportPreview>('/metadata/imports/cohorts/preview', payload),
    onError: (error) => {
      showErrorNotification('Preview Failed', error.message);
    },
  });

export const useCohortImportApply = () =>
  useMutation<CohortImportResult, Error, CohortImportPayload>({
    mutationFn: (payload) => apiClient.post<CohortImportResult>('/metadata/imports/cohorts/apply', payload),
    onSuccess: (data) => {
      showSuccessNotification('Import Complete', `Inserted: ${data.cohortsInserted}, Updated: ${data.cohortsUpdated}`);
    },
    onError: (error) => {
      showErrorNotification('Import Failed', error.message);
    },
  });

// =============================================================================
// Subject-Cohort Import Hooks
// =============================================================================

export const useSubjectCohortImportFields = () =>
  useQuery<SubjectCohortImportFieldsResponse>({
    queryKey: QUERY_KEYS.subjectCohortImportFields,
    queryFn: () => apiClient.get<SubjectCohortImportFieldsResponse>('/metadata/imports/subject-cohorts/fields'),
    staleTime: STALE_TIMES.long,
  });

export const useSubjectCohortImportPreview = () =>
  useMutation<SubjectCohortImportPreview, Error, SubjectCohortImportPayload>({
    mutationFn: (payload) =>
      apiClient.post<SubjectCohortImportPreview>('/metadata/imports/subject-cohorts/preview', payload),
    onError: (error) => {
      showErrorNotification('Preview Failed', error.message);
    },
  });

export const useSubjectCohortImportApply = () => {
  const queryClient = useQueryClient();
  return useMutation<SubjectCohortImportResult, Error, SubjectCohortImportPayload>({
    mutationFn: (payload) =>
      apiClient.post<SubjectCohortImportResult>('/metadata/imports/subject-cohorts/apply', payload),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.metadataTables });
      showSuccessNotification('Import Complete', `Memberships inserted: ${data.membershipsInserted}`);
    },
    onError: (error) => {
      showErrorNotification('Import Failed', error.message);
    },
  });
};

export const useSubjectCohortMemberships = (subjectCode?: string | null) =>
  useQuery<SubjectCohortMembershipsResponse>({
    queryKey: QUERY_KEYS.subjectCohortMemberships(subjectCode ?? ''),
    enabled: Boolean(subjectCode && subjectCode.trim().length > 0),
    queryFn: () => {
      const normalized = (subjectCode ?? '').trim();
      return apiClient.get<SubjectCohortMembershipsResponse>(
        `/metadata/subject-cohorts/${encodeURIComponent(normalized)}`,
      );
    },
    staleTime: STALE_TIMES.short,
  });

export const useDeleteSubjectCohortMembership = () => {
  const queryClient = useQueryClient();
  return useMutation<void, Error, DeleteSubjectCohortMembershipPayload>({
    mutationFn: (payload) => apiClient.delete<void>('/metadata/subject-cohorts', payload),
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.subjectCohortMemberships(variables.subjectCode) });
      showSuccessNotification('Membership Deleted', 'Subject cohort membership has been removed');
    },
    onError: (error) => {
      showErrorNotification('Delete Failed', error.message);
    },
  });
};

// =============================================================================
// Metadata Cohort Hooks
// =============================================================================

export const useMetadataCohorts = () =>
  useQuery<SubjectCohortMetadataCohort[]>({
    queryKey: QUERY_KEYS.metadataCohorts,
    queryFn: () => apiClient.get<SubjectCohortMetadataCohort[]>('/metadata/cohorts'),
    staleTime: STALE_TIMES.short,
  });

export const useUpsertMetadataCohort = () => {
  const queryClient = useQueryClient();
  return useMutation<SubjectCohortMetadataCohort, Error, UpsertMetadataCohortPayload>({
    mutationFn: (payload) => {
      const normalizedName = payload.name.trim().toLowerCase();
      const body: UpdateMetadataCohortPayload = {
        owner: payload.owner,
        path: payload.path,
        description: payload.description ?? null,
        isActive: payload.isActive,
      };
      return apiClient.put<SubjectCohortMetadataCohort>(
        `/metadata/cohorts/${encodeURIComponent(normalizedName)}`,
        body,
      );
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.metadataCohorts });
      showSuccessNotification('Cohort Saved', 'Metadata cohort has been saved');
    },
    onError: (error) => {
      showErrorNotification('Save Failed', error.message);
    },
  });
};

// =============================================================================
// ID Type Hooks
// =============================================================================

export const useIdTypes = () =>
  useQuery<IdTypeListResponse>({
    queryKey: QUERY_KEYS.idTypes,
    queryFn: () => apiClient.get<IdTypeListResponse>('/metadata/id-types'),
    staleTime: STALE_TIMES.medium,
  });

export const useCreateIdType = () => {
  const queryClient = useQueryClient();
  return useMutation<IdTypeInfo, Error, CreateIdTypePayload>({
    mutationFn: (payload) => apiClient.post<IdTypeInfo>('/metadata/id-types', payload),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.idTypes });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.subjectImportFields });
      showSuccessNotification('ID Type Created', `Created: ${data.name}`);
    },
    onError: (error) => {
      showErrorNotification('Create Failed', error.message);
    },
  });
};

export const useUpdateIdType = () => {
  const queryClient = useQueryClient();
  return useMutation<IdTypeInfo, Error, UpdateIdTypePayload>({
    mutationFn: ({ id, ...payload }) => apiClient.put<IdTypeInfo>(`/metadata/id-types/${id}`, payload),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.idTypes });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.subjectImportFields });
      showSuccessNotification('ID Type Updated', `Updated: ${data.name}`);
    },
    onError: (error) => {
      showErrorNotification('Update Failed', error.message);
    },
  });
};

export const useDeleteIdType = () => {
  const queryClient = useQueryClient();
  return useMutation<DeleteIdTypeResponse, Error, DeleteIdTypePayload>({
    mutationFn: ({ id }) => apiClient.delete<DeleteIdTypeResponse>(`/metadata/id-types/${id}`),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.idTypes });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.subjectImportFields });
      showSuccessNotification('ID Type Deleted', `Deleted: ${data.name} (${data.identifiersDeleted} identifiers removed)`);
    },
    onError: (error) => {
      showErrorNotification('Delete Failed', error.message);
    },
  });
};

// =============================================================================
// Subject Identifier Import Hooks
// =============================================================================

export const useSubjectIdentifierImportFields = () =>
  useQuery<SubjectIdentifierImportFieldsResponse>({
    queryKey: QUERY_KEYS.subjectIdentifierFields,
    queryFn: () => apiClient.get<SubjectIdentifierImportFieldsResponse>('/metadata/imports/subject-other-identifiers/fields'),
    staleTime: STALE_TIMES.long,
  });

export const useSubjectIdentifierImportPreview = () =>
  useMutation<SubjectIdentifierImportPreview, Error, SubjectIdentifierImportPayload>({
    mutationFn: (payload) =>
      apiClient.post<SubjectIdentifierImportPreview>('/metadata/imports/subject-other-identifiers/preview', payload),
    onError: (error) => {
      showErrorNotification('Preview Failed', error.message);
    },
  });

export const useSubjectIdentifierImportApply = () => {
  const queryClient = useQueryClient();
  return useMutation<SubjectIdentifierImportResult, Error, SubjectIdentifierImportPayload>({
    mutationFn: (payload) =>
      apiClient.post<SubjectIdentifierImportResult>('/metadata/imports/subject-other-identifiers/apply', payload),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.subjectIdentifierFields });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.subjectImportFields });
      showSuccessNotification('Import Complete', `Inserted: ${data.identifiersInserted}, Updated: ${data.identifiersUpdated}`);
    },
    onError: (error) => {
      showErrorNotification('Import Failed', error.message);
    },
  });
};

export const useSubjectIdentifierDetail = (subjectCode?: string | null) =>
  useQuery<SubjectIdentifierDetailResponse>({
    queryKey: QUERY_KEYS.subjectIdentifiers(subjectCode ?? ''),
    enabled: Boolean(subjectCode && subjectCode.trim().length > 0),
    queryFn: () =>
      apiClient.get<SubjectIdentifierDetailResponse>(
        `/metadata/subject-other-identifiers/${encodeURIComponent((subjectCode ?? '').trim())}`,
      ),
    staleTime: STALE_TIMES.short,
  });

export const useUpsertSubjectIdentifier = () => {
  const queryClient = useQueryClient();
  return useMutation<SubjectIdentifierDetail, Error, UpsertSubjectIdentifierPayload>({
    mutationFn: (payload) => apiClient.post<SubjectIdentifierDetail>('/metadata/subject-other-identifiers', payload),
    onSuccess: (_data, variables) => {
      const key = (variables.subjectCode ?? '').trim();
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.subjectIdentifiers(key) });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.subjectIdentifierFields });
      showSuccessNotification('Identifier Saved', 'Subject identifier has been saved');
    },
    onError: (error) => {
      showErrorNotification('Save Failed', error.message);
    },
  });
};

export const useDeleteSubjectIdentifier = () => {
  const queryClient = useQueryClient();
  return useMutation<void, Error, DeleteSubjectIdentifierPayload>({
    mutationFn: (payload) => apiClient.delete<void>('/metadata/subject-other-identifiers', payload),
    onSuccess: (_data, variables) => {
      const key = (variables.subjectCode ?? '').trim();
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.subjectIdentifiers(key) });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.subjectIdentifierFields });
      showSuccessNotification('Identifier Deleted', 'Subject identifier has been removed');
    },
    onError: (error) => {
      showErrorNotification('Delete Failed', error.message);
    },
  });
};

// =============================================================================
// Observation Type Import Hooks
// =============================================================================

export const useObservationTypeImportFields = () =>
  useQuery<ObservationTypeFieldsResponse>({
    queryKey: QUERY_KEYS.observationTypeImportFields,
    queryFn: () => apiClient.get<ObservationTypeFieldsResponse>('/metadata/imports/observation-types/fields'),
    staleTime: STALE_TIMES.long,
  });

export const useObservationTypeImportPreview = () =>
  useMutation<ObservationTypeImportPreview, Error, ObservationTypeImportPayload>({
    mutationFn: (payload) =>
      apiClient.post<ObservationTypeImportPreview>('/metadata/imports/observation-types/preview', payload),
    onError: (error) => {
      showErrorNotification('Preview Failed', error.message);
    },
  });

export const useObservationTypeImportApply = () => {
  const queryClient = useQueryClient();
  return useMutation<ObservationTypeImportResult, Error, ObservationTypeImportPayload>({
    mutationFn: (payload) =>
      apiClient.post<ObservationTypeImportResult>('/metadata/imports/observation-types/apply', payload),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.metadataTables });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.observationTypeImportFields });
      showSuccessNotification('Import Complete', `Inserted: ${data.typesInserted}, Updated: ${data.typesUpdated}`);
    },
    onError: (error) => {
      showErrorNotification('Import Failed', error.message);
    },
  });
};

// =============================================================================
// Event Import Hooks
// =============================================================================

export const useEventImportFields = () =>
  useQuery<EventImportFieldsResponse>({
    queryKey: QUERY_KEYS.eventImportFields,
    queryFn: () => apiClient.get<EventImportFieldsResponse>('/metadata/imports/events/fields'),
    staleTime: STALE_TIMES.long,
  });

export const useEventImportPreview = () =>
  useMutation<EventImportPreview, Error, EventImportPayload>({
    mutationFn: (payload) =>
      apiClient.post<EventImportPreview>('/metadata/imports/events/preview', payload),
    onError: (error) => {
      showErrorNotification('Preview Failed', error.message);
    },
  });

export const useEventImportApply = () => {
  const queryClient = useQueryClient();
  return useMutation<EventImportResult, Error, EventImportPayload>({
    mutationFn: (payload) =>
      apiClient.post<EventImportResult>('/metadata/imports/events/apply', payload),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.metadataTables });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.eventImportFields });
      showSuccessNotification(
        'Event Import Complete',
        `Events: ${data.eventsInserted} inserted, ${data.eventsUpdated} updated. Measures: ${data.measuresInserted} inserted, ${data.measuresUpdated} updated.${data.subjectsMissing > 0 ? ` ${data.subjectsMissing} subjects not found.` : ''}`,
      );
    },
    onError: (error) => {
      showErrorNotification('Event Import Failed', error.message);
    },
  });
};

// =============================================================================
// Disease Import Hooks
// =============================================================================

export const useDiseaseImportFields = () =>
  useQuery<DiseaseFieldsResponse>({
    queryKey: QUERY_KEYS.diseaseImportFields,
    queryFn: () => apiClient.get<DiseaseFieldsResponse>('/metadata/imports/diseases/fields'),
    staleTime: STALE_TIMES.long,
  });

export const useDiseaseImportPreview = () =>
  useMutation<DiseaseImportPreview, Error, DiseaseImportPayload>({
    mutationFn: (payload) =>
      apiClient.post<DiseaseImportPreview>('/metadata/imports/diseases/preview', payload),
    onError: (error) => {
      showErrorNotification('Preview Failed', error.message);
    },
  });

export const useDiseaseImportApply = () => {
  const queryClient = useQueryClient();
  return useMutation<DiseaseImportResult, Error, DiseaseImportPayload>({
    mutationFn: (payload) =>
      apiClient.post<DiseaseImportResult>('/metadata/imports/diseases/apply', payload),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.metadataTables });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.diseaseImportFields });
      showSuccessNotification('Disease Import Complete', `Inserted: ${data.diseasesInserted}, Updated: ${data.diseasesUpdated}`);
    },
    onError: (error) => {
      showErrorNotification('Disease Import Failed', error.message);
    },
  });
};

// =============================================================================
// Disease Type Hooks
// =============================================================================

export const useDiseaseTypeFields = () =>
  useQuery<DiseaseTypeFieldsResponse>({
    queryKey: QUERY_KEYS.diseaseTypeFields,
    queryFn: () => apiClient.get<DiseaseTypeFieldsResponse>('/metadata/imports/disease-types/fields'),
    staleTime: STALE_TIMES.long,
  });

export const useDiseaseTypeUpsert = () => {
  const queryClient = useQueryClient();
  return useMutation<DiseaseTypeUpsertResult, Error, DiseaseTypeUpsertPayload>({
    mutationFn: (payload) =>
      apiClient.post<DiseaseTypeUpsertResult>('/metadata/imports/disease-types/upsert', payload),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.metadataTables });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.diseaseTypeFields });
      const action = data.inserted ? 'Created' : data.updated ? 'Updated' : 'No changes';
      showSuccessNotification('Disease Type', `${action} disease type (ID: ${data.diseaseTypeId})`);
    },
    onError: (error) => {
      showErrorNotification('Disease Type Failed', error.message);
    },
  });
};

// =============================================================================
// Subject Disease Import Hooks
// =============================================================================

export const useSubjectDiseaseFields = () =>
  useQuery<SubjectDiseaseFieldsResponse>({
    queryKey: QUERY_KEYS.subjectDiseaseFields,
    queryFn: () => apiClient.get<SubjectDiseaseFieldsResponse>('/metadata/imports/subject-diseases/fields'),
    staleTime: STALE_TIMES.long,
  });

export const useSubjectDiseaseImportPreview = () =>
  useMutation<SubjectDiseaseImportPreview, Error, SubjectDiseaseImportPayload>({
    mutationFn: (payload) =>
      apiClient.post<SubjectDiseaseImportPreview>('/metadata/imports/subject-diseases/preview', payload),
    onError: (error) => {
      showErrorNotification('Preview Failed', error.message);
    },
  });

export const useSubjectDiseaseImportApply = () => {
  const queryClient = useQueryClient();
  return useMutation<SubjectDiseaseImportResult, Error, SubjectDiseaseImportPayload>({
    mutationFn: (payload) =>
      apiClient.post<SubjectDiseaseImportResult>('/metadata/imports/subject-diseases/apply', payload),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.metadataTables });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.subjectDiseaseFields });
      const parts = [`Inserted: ${data.inserted}`, `Updated: ${data.updated}`];
      if (data.diagnosisEventsLinked) parts.push(`Diagnosis linked: ${data.diagnosisEventsLinked}`);
      if (data.onsetEventsLinked) parts.push(`Onset linked: ${data.onsetEventsLinked}`);
      if (data.subjectsMissing) parts.push(`Missing subjects: ${data.subjectsMissing}`);
      showSuccessNotification('Subject Disease Import', parts.join(', '));
    },
    onError: (error) => {
      showErrorNotification('Subject Disease Import Failed', error.message);
    },
  });
};

export const useCohortDiseaseAssign = () => {
  const queryClient = useQueryClient();
  return useMutation<CohortAssignResult, Error, CohortAssignPayload>({
    mutationFn: (payload) =>
      apiClient.post<CohortAssignResult>('/metadata/imports/subject-diseases/cohort-assign', payload),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.metadataTables });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.subjectDiseaseFields });
      const parts = [
        `${data.inserted} assigned out of ${data.totalSubjects}`,
        `${data.skipped} already existed`,
      ];
      if (data.diagnosisEventsLinked) parts.push(`${data.diagnosisEventsLinked} diagnosis events linked`);
      if (data.onsetEventsLinked) parts.push(`${data.onsetEventsLinked} onset events linked`);
      showSuccessNotification('Cohort Disease Assign', parts.join(', '));
    },
    onError: (error) => {
      showErrorNotification('Cohort Assign Failed', error.message);
    },
  });
};

// =============================================================================
// Subject Disease Type Import Hooks
// =============================================================================

export const useSDTFields = () =>
  useQuery<SDTFieldsResponse>({
    queryKey: QUERY_KEYS.sdtFields,
    queryFn: () => apiClient.get<SDTFieldsResponse>('/metadata/imports/subject-disease-types/fields'),
    staleTime: STALE_TIMES.long,
  });

export const useSDTImportPreview = () =>
  useMutation<SDTImportPreview, Error, SDTImportPayload>({
    mutationFn: (payload) =>
      apiClient.post<SDTImportPreview>('/metadata/imports/subject-disease-types/preview', payload),
    onError: (error) => {
      showErrorNotification('Preview Failed', error.message);
    },
  });

export const useSDTImportApply = () => {
  const queryClient = useQueryClient();
  return useMutation<SDTImportResult, Error, SDTImportPayload>({
    mutationFn: (payload) =>
      apiClient.post<SDTImportResult>('/metadata/imports/subject-disease-types/apply', payload),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.metadataTables });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.sdtFields });
      const parts = [`Inserted: ${data.inserted}`, `Updated: ${data.updated}`];
      if (data.transitionEventsLinked) parts.push(`Transitions linked: ${data.transitionEventsLinked}`);
      if (data.subjectsMissing) parts.push(`Missing subjects: ${data.subjectsMissing}`);
      if (data.typesUnresolved) parts.push(`Unresolved types: ${data.typesUnresolved}`);
      showSuccessNotification('Subject Disease Type Import', parts.join(', '));
    },
    onError: (error) => {
      showErrorNotification('SDT Import Failed', error.message);
    },
  });
};

export const useSDTManualUpsert = () => {
  const queryClient = useQueryClient();
  return useMutation<SDTManualResult, Error, SDTManualPayload>({
    mutationFn: (payload) =>
      apiClient.post<SDTManualResult>('/metadata/imports/subject-disease-types/upsert', payload),
    onSuccess: (data) => {
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.metadataTables });
      void queryClient.invalidateQueries({ queryKey: QUERY_KEYS.sdtFields });
      const action = data.inserted ? 'Created' : data.updated ? 'Updated' : 'No changes';
      showSuccessNotification('Subject Disease Type', `${action} (ID: ${data.subjectDiseaseTypeId})`);
    },
    onError: (error) => {
      showErrorNotification('SDT Upsert Failed', error.message);
    },
  });
};
