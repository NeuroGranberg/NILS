/**
 * Cohort job card component.
 * Expandable card showing cohort pipeline status and job history.
 */

import { useState, useEffect } from 'react';
import {
  ActionIcon,
  Badge,
  Box,
  Collapse,
  Divider,
  Group,
  Stack,
  Text,
  Tooltip,
} from '@mantine/core';
import { IconChevronDown, IconChevronUp, IconExternalLink, IconFolder } from '@tabler/icons-react';
import { Link } from 'react-router-dom';
import type { Cohort, Job } from '../../../types';
import { MiniPipelineStepper } from './MiniPipelineStepper';
import { JobHistoryTable } from './JobHistoryTable';

// Helper to format relative time
const formatRelativeTime = (dateString: string): string => {
  const date = new Date(dateString);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMs / 3600000);
  const diffDays = Math.floor(diffMs / 86400000);

  if (diffMins < 1) return 'just now';
  if (diffMins < 60) return `${diffMins}m ago`;
  if (diffHours < 24) return `${diffHours}h ago`;
  if (diffDays < 7) return `${diffDays}d ago`;
  return date.toLocaleDateString();
};

// Helper to truncate path
const truncatePath = (path: string, maxLength = 50): string => {
  if (path.length <= maxLength) return path;
  const parts = path.split('/');
  if (parts.length <= 3) return path;
  return `.../${parts.slice(-3).join('/')}`;
};

interface CohortJobCardProps {
  cohort: Cohort;
  jobs: Job[];
  defaultExpanded?: boolean;
}

export const CohortJobCard = ({ cohort, jobs, defaultExpanded }: CohortJobCardProps) => {
  // Check for active jobs
  const hasActiveJob = jobs.some((j) => ['running', 'queued', 'paused'].includes(j.status));
  const hasFailedJob = jobs.some((j) => j.status === 'failed');
  const runningJob = jobs.find((j) => j.status === 'running');

  // Auto-expand if has active jobs, otherwise use default or localStorage
  const storageKey = `job-center-expanded-${cohort.id}`;
  const getInitialExpanded = () => {
    if (hasActiveJob) return true;
    if (defaultExpanded !== undefined) return defaultExpanded;
    const stored = localStorage.getItem(storageKey);
    return stored !== null ? stored === 'true' : true; // Default to expanded
  };

  const [expanded, setExpanded] = useState(getInitialExpanded);

  // Persist expanded state
  useEffect(() => {
    localStorage.setItem(storageKey, String(expanded));
  }, [expanded, storageKey]);

  // Auto-expand when a job becomes active
  useEffect(() => {
    if (hasActiveJob && !expanded) {
      setExpanded(true);
    }
  }, [hasActiveJob, expanded]);

  const toggleExpanded = () => setExpanded((prev) => !prev);

  // Determine card border color based on status
  const getBorderColor = () => {
    if (hasActiveJob) return 'var(--nils-accent-primary)';
    if (hasFailedJob) return 'var(--nils-error)';
    return 'var(--nils-border-subtle)';
  };

  return (
    <Box
      style={{
        backgroundColor: 'var(--nils-bg-secondary)',
        borderRadius: 'var(--nils-radius-lg)',
        border: `1px solid ${getBorderColor()}`,
        overflow: 'hidden',
        transition: 'border-color 0.2s ease',
      }}
    >
      {/* Header - always visible, clickable to expand/collapse */}
      <Box
        p="md"
        style={{
          cursor: 'pointer',
          userSelect: 'none',
        }}
        onClick={toggleExpanded}
      >
        <Group justify="space-between" wrap="nowrap">
          <Stack gap={4} style={{ minWidth: 0, flex: 1 }}>
            <Group gap="xs" wrap="nowrap">
              <IconFolder size={20} color="var(--nils-text-secondary)" style={{ flexShrink: 0 }} />
              <Text fw={600} size="md" c="var(--nils-text-primary)" truncate>
                {cohort.name}
              </Text>
              {hasActiveJob && (
                <Badge color="blue" size="xs" variant="light">
                  Active
                </Badge>
              )}
              {hasFailedJob && !hasActiveJob && (
                <Badge color="red" size="xs" variant="light">
                  Failed
                </Badge>
              )}
              {jobs.length > 0 && (
                <Badge color="gray" size="xs" variant="outline">
                  {jobs.length} job{jobs.length !== 1 ? 's' : ''}
                </Badge>
              )}
            </Group>
            <Group gap="md" wrap="nowrap">
              <Tooltip label={cohort.source_path}>
                <Text size="xs" c="var(--nils-text-tertiary)" truncate style={{ maxWidth: 300 }}>
                  {truncatePath(cohort.source_path)}
                </Text>
              </Tooltip>
              <Text size="xs" c="var(--nils-text-tertiary)">
                Updated {formatRelativeTime(cohort.updated_at)}
              </Text>
            </Group>
          </Stack>

          <Group gap="sm" wrap="nowrap">
            {/* Running job indicator */}
            {runningJob && (
              <Box
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 6,
                  padding: '4px 10px',
                  backgroundColor: 'rgba(88, 166, 255, 0.15)',
                  borderRadius: 'var(--nils-radius-sm)',
                }}
              >
                <Box
                  style={{
                    width: 8,
                    height: 8,
                    borderRadius: '50%',
                    backgroundColor: 'var(--nils-stage-running)',
                    animation: 'pulse 2s infinite',
                  }}
                />
                <Text size="xs" fw={500} c="var(--nils-stage-running)" tt="capitalize">
                  {runningJob.stageId}{runningJob.stepId ? ` - ${runningJob.stepId.replace(/_/g, ' ')}` : ''} {runningJob.progress}%
                </Text>
              </Box>
            )}

            {/* Link to cohort detail */}
            <Tooltip label="Go to cohort">
              <ActionIcon
                component={Link}
                to={`/cohorts/${cohort.id}`}
                variant="subtle"
                size="sm"
                onClick={(e: React.MouseEvent) => e.stopPropagation()}
              >
                <IconExternalLink size={16} />
              </ActionIcon>
            </Tooltip>

            {/* Expand/collapse toggle */}
            <ActionIcon variant="subtle" size="sm">
              {expanded ? <IconChevronUp size={18} /> : <IconChevronDown size={18} />}
            </ActionIcon>
          </Group>
        </Group>
      </Box>

      {/* Mini Pipeline Stepper - always visible */}
      <Box px="md" pb={expanded ? 0 : 'md'}>
        <MiniPipelineStepper stages={cohort.stages} />
      </Box>

      {/* Collapsible Job History */}
      <Collapse in={expanded}>
        <Divider color="var(--nils-border-subtle)" />
        <Box p="md">
          <Text size="xs" fw={600} c="var(--nils-text-tertiary)" mb="sm" tt="uppercase" style={{ letterSpacing: '0.05em' }}>
            Job History
          </Text>
          <JobHistoryTable jobs={jobs} compact />
        </Box>
      </Collapse>
    </Box>
  );
};
