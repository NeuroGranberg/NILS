/**
 * Job Center page - Cohort-centric view of pipeline jobs.
 * Shows each cohort with its pipeline progress and job history.
 * Also shows system jobs (backup/restore) in a separate section.
 */

import { Box, Loader, Stack, Text, Title } from '@mantine/core';
import { IconFolderOff } from '@tabler/icons-react';
import { useMemo } from 'react';
import { useJobsQuery } from '../api';
import { useCohortsQuery } from '../../cohorts/api';
import { CohortJobCard, SystemJobCard } from '../components';
import type { Cohort, Job } from '../../../types';

// System job stages (not associated with cohorts)
const SYSTEM_STAGES = ['restore', 'backup'];

/**
 * Separate jobs into cohort jobs and system jobs.
 */
const separateJobs = (jobs: Job[] | undefined): { cohortJobs: Job[]; systemJobs: Job[] } => {
  if (!jobs) return { cohortJobs: [], systemJobs: [] };

  const cohortJobs: Job[] = [];
  const systemJobs: Job[] = [];

  jobs.forEach((job) => {
    if (SYSTEM_STAGES.includes(job.stageId)) {
      systemJobs.push(job);
    } else {
      cohortJobs.push(job);
    }
  });

  return { cohortJobs, systemJobs };
};

/**
 * Group jobs by cohort ID.
 */
const groupJobsByCohort = (jobs: Job[]): Map<number, Job[]> => {
  const map = new Map<number, Job[]>();

  jobs.forEach((job) => {
    const cohortId = job.cohortId;
    if (cohortId != null) {
      if (!map.has(cohortId)) {
        map.set(cohortId, []);
      }
      map.get(cohortId)!.push(job);
    }
  });

  // Sort jobs within each cohort by submittedAt descending
  map.forEach((cohortJobs) => {
    cohortJobs.sort((a, b) => (a.submittedAt > b.submittedAt ? -1 : 1));
  });

  return map;
};

/**
 * Sort cohorts by activity:
 * 1. Cohorts with running/queued jobs first
 * 2. Then by most recent job activity
 * 3. Then by cohort update time
 */
const sortCohortsByActivity = (
  cohorts: Cohort[] | undefined,
  jobsByCohort: Map<number, Job[]>
): Cohort[] => {
  if (!cohorts) return [];

  return [...cohorts].sort((a, b) => {
    const aJobs = jobsByCohort.get(a.id) ?? [];
    const bJobs = jobsByCohort.get(b.id) ?? [];

    // Check for active jobs
    const aHasActive = aJobs.some((j) => ['running', 'queued', 'paused'].includes(j.status));
    const bHasActive = bJobs.some((j) => ['running', 'queued', 'paused'].includes(j.status));

    if (aHasActive && !bHasActive) return -1;
    if (!aHasActive && bHasActive) return 1;

    // Then by most recent job
    const aLatestJob = aJobs[0]?.submittedAt ?? '';
    const bLatestJob = bJobs[0]?.submittedAt ?? '';

    if (aLatestJob && bLatestJob) {
      if (aLatestJob > bLatestJob) return -1;
      if (aLatestJob < bLatestJob) return 1;
    } else if (aLatestJob) {
      return -1;
    } else if (bLatestJob) {
      return 1;
    }

    // Then by cohort update time
    return a.updated_at > b.updated_at ? -1 : 1;
  });
};

export const JobsPage = () => {
  const { data: jobs, isLoading: jobsLoading } = useJobsQuery();
  const { data: cohorts, isLoading: cohortsLoading } = useCohortsQuery();

  const isLoading = jobsLoading || cohortsLoading;

  // Separate system jobs from cohort jobs
  const { cohortJobs, systemJobs } = useMemo(() => separateJobs(jobs), [jobs]);

  // Group cohort jobs by cohort ID
  const jobsByCohort = useMemo(() => groupJobsByCohort(cohortJobs), [cohortJobs]);

  // Sort cohorts by activity
  const sortedCohorts = useMemo(
    () => sortCohortsByActivity(cohorts, jobsByCohort),
    [cohorts, jobsByCohort]
  );

  // Count total active jobs
  const activeJobCount = useMemo(
    () => jobs?.filter((j) => ['running', 'queued', 'paused'].includes(j.status)).length ?? 0,
    [jobs]
  );

  // Check if there are active system jobs
  const hasActiveSystemJob = useMemo(
    () => systemJobs.some((j) => ['running', 'queued', 'paused'].includes(j.status)),
    [systemJobs]
  );

  return (
    <Stack gap="lg" p="md">
      {/* Page Header */}
      <Stack gap={4}>
        <Title order={2} fw={600} c="var(--nils-text-primary)">
          Job Center
        </Title>
        <Text size="sm" c="var(--nils-text-secondary)">
          Monitor and manage pipeline processing tasks
          {activeJobCount > 0 && (
            <Text span c="var(--nils-accent-primary)" fw={500}>
              {' '}
              · {activeJobCount} active job{activeJobCount !== 1 ? 's' : ''}
            </Text>
          )}
        </Text>
      </Stack>

      {/* Loading State */}
      {isLoading && (
        <Box
          py="xl"
          style={{
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            minHeight: '200px',
          }}
        >
          <Loader size="md" color="var(--nils-accent-primary)" />
        </Box>
      )}

      {/* System Jobs Card (backup/restore) - shown at top if there are system jobs */}
      {!isLoading && systemJobs.length > 0 && (
        <SystemJobCard
          jobs={systemJobs}
          defaultExpanded={hasActiveSystemJob}
        />
      )}

      {/* Empty State - No cohorts */}
      {!isLoading && sortedCohorts.length === 0 && systemJobs.length === 0 && (
        <Box
          py="xl"
          style={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            minHeight: '300px',
            backgroundColor: 'var(--nils-bg-secondary)',
            borderRadius: 'var(--nils-radius-lg)',
            border: '1px solid var(--nils-border-subtle)',
          }}
        >
          <Box
            mb="md"
            style={{
              width: 48,
              height: 48,
              borderRadius: 'var(--nils-radius-md)',
              backgroundColor: 'var(--nils-bg-tertiary)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <IconFolderOff size={24} color="var(--nils-text-tertiary)" />
          </Box>
          <Text fw={600} size="md" c="var(--nils-text-primary)" mb={4}>
            No jobs yet
          </Text>
          <Text size="sm" c="var(--nils-text-secondary)" ta="center" maw={320}>
            Create a cohort to start processing data. Jobs will appear here when you run pipeline
            stages or system operations.
          </Text>
        </Box>
      )}

      {/* Cohort Cards */}
      {!isLoading &&
        sortedCohorts.map((cohort, index) => (
          <CohortJobCard
            key={cohort.id}
            cohort={cohort}
            jobs={jobsByCohort.get(cohort.id) ?? []}
            defaultExpanded={index < 3} // Expand first 3 by default
          />
        ))}
    </Stack>
  );
};
