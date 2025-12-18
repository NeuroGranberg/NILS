import {
  Card,
  Group,
  Loader,
  SimpleGrid,
  Stack,
  Text,
  Title,
  Timeline,
} from '@mantine/core';
import { IconAlertTriangle, IconChecks, IconGauge, IconLoader } from '@tabler/icons-react';
import { useMemo } from 'react';
import { useCohortsQuery } from '../../cohorts/api';
import { useJobsQuery } from '../../jobs/api';
import { PipelineStepper } from '../../shared/components/PipelineStepper';
import { formatDateTime, formatPercent } from '../../../utils/formatters';

export const DashboardPage = () => {
  const { data: cohorts, isLoading: cohortsLoading } = useCohortsQuery();
  const { data: jobs, isLoading: jobsLoading } = useJobsQuery();

  const stats = useMemo(() => {
    const totalCohorts = cohorts?.length ?? 0;
    const runningStages =
      cohorts?.reduce((acc, cohort) => acc + cohort.stages.filter((stage) => stage.status === 'running').length, 0) ?? 0;
    const completedCohorts =
      cohorts?.filter((cohort) => cohort.stages.every((stage) => stage.status === 'completed')).length ?? 0;

    return {
      totalCohorts,
      runningStages,
      completedCohorts,
    };
  }, [cohorts]);

  const recentActivity = useMemo(() => {
    if (!cohorts) return [];

    return cohorts
      .flatMap((cohort) =>
        cohort.stages.flatMap((stage) =>
          stage.runs.map((run) => ({
            id: `${run.id}`,
            cohortName: cohort.name,
            stageName: stage.title,
            status: run.status,
            startedAt: run.startedAt,
          })),
        ),
      )
      .sort((a, b) => (a.startedAt > b.startedAt ? -1 : 1))
      .slice(0, 6);
  }, [cohorts]);

  if (cohortsLoading || jobsLoading) {
    return (
      <Group justify="center" py="xl">
        <Loader />
      </Group>
    );
  }

  return (
    <Stack gap="xl" p="md">
      <Group justify="space-between" align="flex-end">
        <div>
          <Title order={2}>Pipeline overview</Title>
          <Text c="dimmed" size="sm">
            Monitor the progress of each dataset and keep an eye on long-running stages.
          </Text>
        </div>
      </Group>

      <SimpleGrid cols={{ base: 1, sm: 3 }} spacing="md">
        <Card withBorder padding="lg" radius="md">
          <Group align="center" gap="sm">
            <IconGauge size={28} />
            <Stack gap={2}>
              <Text size="sm" c="dimmed">
                Cohorts onboarded
              </Text>
              <Text fw={600} size="lg">
                {stats.totalCohorts}
              </Text>
            </Stack>
          </Group>
        </Card>
        <Card withBorder padding="lg" radius="md">
          <Group align="center" gap="sm">
            <IconLoader size={28} />
            <Stack gap={2}>
              <Text size="sm" c="dimmed">
                Stages in progress
              </Text>
              <Text fw={600} size="lg">
                {stats.runningStages}
              </Text>
            </Stack>
          </Group>
        </Card>
        <Card withBorder padding="lg" radius="md">
          <Group align="center" gap="sm">
            <IconChecks size={28} />
            <Stack gap={2}>
              <Text size="sm" c="dimmed">
                Completed pipelines
              </Text>
              <Text fw={600} size="lg">
                {stats.completedCohorts}
              </Text>
            </Stack>
          </Group>
        </Card>
      </SimpleGrid>

      {cohorts && cohorts.length > 0 && (
        <Card withBorder radius="md" padding="lg">
          <Stack gap="lg">
            <Group justify="space-between" align="center">
              <div>
                <Text fw={600}>Current focus</Text>
                <Text size="sm" c="dimmed">
                  {cohorts[0].name} — {formatPercent(cohorts[0].completion_percentage)} complete
                </Text>
              </div>
            </Group>
            <PipelineStepper stages={cohorts[0].stages} />
          </Stack>
        </Card>
      )}

      <SimpleGrid cols={{ base: 1, md: 2 }} spacing="lg">
        <Card withBorder radius="md" padding="lg">
          <Stack gap="md">
            <Text fw={600}>Recent activity</Text>
            <Timeline active={recentActivity.length} bulletSize={26} lineWidth={2}>
              {recentActivity.map((item) => (
                <Timeline.Item
                  key={item.id}
                  title={`${item.cohortName} — ${item.stageName}`}
                  bullet={item.status === 'failed' ? <IconAlertTriangle size={16} /> : undefined}
                  color={item.status === 'failed' ? 'red' : 'blue'}
                >
                  <Text size="sm" c="dimmed">
                    Started {formatDateTime(item.startedAt)}
                  </Text>
                  <Text size="xs" c="dimmed">
                    Status: {item.status.toUpperCase()}
                  </Text>
                </Timeline.Item>
              ))}
              {recentActivity.length === 0 && (
                <Timeline.Item title="No recorded activity yet" bullet={<IconGauge size={16} />}>
                  <Text size="sm" c="dimmed">
                    Run a stage to start capturing the audit trail.
                  </Text>
                </Timeline.Item>
              )}
            </Timeline>
          </Stack>
        </Card>

        <Card withBorder radius="md" padding="lg">
          <Stack gap="md">
            <Group justify="space-between">
              <Text fw={600}>Job queue</Text>
              <Text size="sm" c="dimmed">
                {jobs?.length ?? 0} tracked jobs
              </Text>
            </Group>
            <Stack gap="sm">
              {jobs?.map((job) => (
                <Card key={job.id} withBorder padding="sm" radius="md">
                  <Group justify="space-between" align="center">
                    <Stack gap={0}>
                      <Text size="sm" fw={500}>
                        {job.cohortName}
                      </Text>
                      <Text size="xs" c="dimmed">
                        Stage: {job.stageId} · Status: {job.status.toUpperCase()}
                      </Text>
                    </Stack>
                    <Text size="sm">{job.progress}%</Text>
                  </Group>
                </Card>
              ))}
              {(!jobs || jobs.length === 0) && (
                <Text size="sm" c="dimmed">
                  No background jobs in the queue.
                </Text>
              )}
            </Stack>
          </Stack>
        </Card>
      </SimpleGrid>
    </Stack>
  );
};
