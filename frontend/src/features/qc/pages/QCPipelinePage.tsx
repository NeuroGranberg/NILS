/**
 * QC Pipeline main page.
 * Shows cohort selector and QC module selection (Axes, Body Part, Contrast).
 */

import { useState, useEffect } from 'react';
import {
  Box,
  Loader,
  SimpleGrid,
  Stack,
  Text,
  Title,
  Group,
  Paper,
  ThemeIcon,
  Badge,
  ActionIcon,
  Button,
} from '@mantine/core';
import {
  IconShieldCheck,
  IconTarget,
  IconBrain,
  IconDroplet,
  IconArrowLeft,
  IconDatabase,
} from '@tabler/icons-react';
import { useLocation } from 'react-router-dom';
import { useCohortsQuery } from '../../cohorts/api';
import { useQCStore } from '../store';
import { QCCohortCard } from '../components/QCCohortCard';
import { QCViewerPage } from './QCViewerPage';
import { AxesQCPage } from './AxesQCPage';

// QC Module definitions
type QCModule = 'axes' | 'body_part' | 'contrast';

interface QCModuleMeta {
  id: QCModule;
  title: string;
  description: string;
  icon: React.ReactNode;
  color: string;
  implemented: boolean;
}

const QC_MODULES: QCModuleMeta[] = [
  {
    id: 'axes',
    title: 'Axes QC',
    description: 'Review classification axes (base, technique, modifier, provenance, construct)',
    icon: <IconTarget size={24} />,
    color: 'blue',
    implemented: true,
  },
  {
    id: 'body_part',
    title: 'Body Part QC',
    description: 'Validate body part detection using geometry and aspect ratio',
    icon: <IconBrain size={24} />,
    color: 'orange',
    implemented: false,
  },
  {
    id: 'contrast',
    title: 'Contrast QC',
    description: 'Cross-stack comparison for PRE/POST contrast determination',
    icon: <IconDroplet size={24} />,
    color: 'pink',
    implemented: false,
  },
];

// QC Module Card component
interface QCModuleCardProps {
  module: QCModuleMeta;
  onClick: () => void;
}

const QCModuleCard = ({ module, onClick }: QCModuleCardProps) => {
  return (
    <Paper
      p="md"
      radius="md"
      withBorder
      style={{
        cursor: module.implemented ? 'pointer' : 'not-allowed',
        opacity: module.implemented ? 1 : 0.6,
        transition: 'all 0.2s ease',
      }}
      onClick={module.implemented ? onClick : undefined}
    >
      <Group gap="md" wrap="nowrap">
        <ThemeIcon size="xl" radius="md" color={module.color} variant="light">
          {module.icon}
        </ThemeIcon>
        <Stack gap={4} style={{ flex: 1 }}>
          <Group gap="xs">
            <Text fw={600} size="md">
              {module.title}
            </Text>
            {!module.implemented && (
              <Badge size="xs" color="gray" variant="outline">
                Coming Soon
              </Badge>
            )}
          </Group>
          <Text size="sm" c="dimmed">
            {module.description}
          </Text>
        </Stack>
      </Group>
    </Paper>
  );
};

export const QCPipelinePage = () => {
  // Local state for view mode
  const [viewMode, setViewMode] = useState<'cohort_select' | 'module_select' | 'qc' | 'viewer'>(
    'cohort_select'
  );
  const [selectedModule, setSelectedModule] = useState<QCModule | null>(null);
  
  // Track navigation key to detect when user clicks sidebar link while already on /qc
  const location = useLocation();
  const [navigationKey, setNavigationKey] = useState(location.key);

  const { data: cohorts, isLoading: cohortsLoading } = useCohortsQuery();
  const { selectedCohortId, setSelectedCohort, reset: resetStore } = useQCStore();

  // Reset to cohort selection when user navigates to /qc from sidebar
  useEffect(() => {
    if (location.key !== navigationKey) {
      setNavigationKey(location.key);
      // Reset to initial state when navigation key changes (user clicked sidebar)
      setViewMode('cohort_select');
      setSelectedModule(null);
      resetStore();
    }
  }, [location.key, navigationKey, resetStore]);

  // Derived state to get the full object of the selected cohort
  const selectedCohort = cohorts?.find((c) => c.id === selectedCohortId);

  const handleCohortSelect = (cohortId: number) => {
    setSelectedCohort(cohortId);
    setViewMode('module_select');
  };

  const handleViewerClick = (cohortId: number) => {
    setSelectedCohort(cohortId);
    setViewMode('viewer');
  };

  const handleModuleSelect = (module: QCModule) => {
    setSelectedModule(module);
    setViewMode('qc');
  };

  const handleBackToCohorts = () => {
    setSelectedCohort(null);
    setSelectedModule(null);
    setViewMode('cohort_select');
  };

  const handleBackToModules = () => {
    setSelectedModule(null);
    setViewMode('module_select');
  };

  // --------------------------------------------------------------------------
  // View: Cohort Selection (Grid of Cards)
  // --------------------------------------------------------------------------
  if (viewMode === 'cohort_select' || !selectedCohortId || !selectedCohort) {
    return (
      <Stack gap="lg">
        <Group>
          <IconShieldCheck size={32} color="var(--nils-accent-primary)" />
          <Stack gap={0}>
            <Title order={2} c="var(--nils-text-primary)">
              QC Pipeline
            </Title>
            <Text size="sm" c="var(--nils-text-tertiary)">
              Select a cohort to begin quality control or explore data
            </Text>
          </Stack>
        </Group>

        {cohortsLoading ? (
          <Box p="xl" style={{ display: 'flex', justifyContent: 'center' }}>
            <Loader size="lg" />
          </Box>
        ) : !cohorts || cohorts.length === 0 ? (
          <Box p="xl" style={{ textAlign: 'center' }}>
            <Text c="var(--nils-text-secondary)">No active cohorts found.</Text>
          </Box>
        ) : (
          <SimpleGrid cols={{ base: 1, sm: 2, lg: 3, xl: 4 }} spacing="lg">
            {cohorts.map((cohort) => (
              <QCCohortCard
                key={cohort.id}
                cohort={cohort}
                onQCClick={() => handleCohortSelect(cohort.id)}
                onViewerClick={() => handleViewerClick(cohort.id)}
              />
            ))}
          </SimpleGrid>
        )}
      </Stack>
    );
  }

  // --------------------------------------------------------------------------
  // View: QC Module Selection
  // --------------------------------------------------------------------------
  if (viewMode === 'module_select') {
    return (
      <Stack gap="lg">
        <Group justify="space-between">
          <Group gap="md">
            <ActionIcon variant="subtle" size="lg" onClick={handleBackToCohorts}>
              <IconArrowLeft size={24} />
            </ActionIcon>
            <Stack gap={0}>
              <Title order={2} c="var(--nils-text-primary)">
                {selectedCohort.name}
              </Title>
              <Text size="sm" c="var(--nils-text-tertiary)">
                Select a QC module to begin review
              </Text>
            </Stack>
          </Group>
          <Button
            variant="light"
            color="violet"
            leftSection={<IconDatabase size={16} />}
            onClick={() => setViewMode('viewer')}
          >
            Open Viewer
          </Button>
        </Group>

        <SimpleGrid cols={{ base: 1, md: 2, lg: 3 }} spacing="md">
          {QC_MODULES.map((module) => (
            <QCModuleCard
              key={module.id}
              module={module}
              onClick={() => handleModuleSelect(module.id)}
            />
          ))}
        </SimpleGrid>
      </Stack>
    );
  }

  // --------------------------------------------------------------------------
  // View: QC Module (Axes, Body Part, or Contrast)
  // --------------------------------------------------------------------------
  if (viewMode === 'qc' && selectedModule) {
    // Currently only Axes is implemented
    if (selectedModule === 'axes') {
      return <AxesQCPage cohort={selectedCohort} onBack={handleBackToModules} />;
    }

    // Placeholder for unimplemented modules
    return (
      <Stack gap="lg">
        <Group gap="md">
          <ActionIcon variant="subtle" size="lg" onClick={handleBackToModules}>
            <IconArrowLeft size={24} />
          </ActionIcon>
          <Title order={2} c="var(--nils-text-primary)">
            {QC_MODULES.find((m) => m.id === selectedModule)?.title}
          </Title>
        </Group>
        <Paper p="xl" withBorder style={{ textAlign: 'center' }}>
          <Stack align="center" gap="md">
            <ThemeIcon size={64} radius="xl" color="gray" variant="light">
              {selectedModule === 'body_part' ? (
                <IconBrain size={32} />
              ) : (
                <IconDroplet size={32} />
              )}
            </ThemeIcon>
            <Text size="lg" fw={500}>
              Coming Soon
            </Text>
            <Text c="dimmed">
              {selectedModule === 'body_part'
                ? 'Body Part QC will allow you to validate body part detection using geometry and aspect ratio analysis.'
                : 'Contrast QC will enable cross-stack comparison for PRE/POST contrast determination.'}
            </Text>
            <Button variant="light" onClick={handleBackToModules}>
              Back to Modules
            </Button>
          </Stack>
        </Paper>
      </Stack>
    );
  }

  // --------------------------------------------------------------------------
  // View: QC Viewer
  // --------------------------------------------------------------------------
  if (viewMode === 'viewer') {
    return <QCViewerPage cohort={selectedCohort} onBack={handleBackToCohorts} />;
  }

  return null;
};
