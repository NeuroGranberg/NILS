import {
  Alert,
  Badge,
  Button,
  Divider,
  Group,
  Stack,
  Stepper,
  Text,
  TextInput,
  Textarea,
} from '@mantine/core';
import { notifications } from '@mantine/notifications';
import { IconAlertTriangle, IconFileExport } from '@tabler/icons-react';
import { useCallback, useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useResolveManifest, useRunExport } from '../api';
import { ManifestInput } from './ManifestInput';
import { ManifestPreview } from './ManifestPreview';
import { ExportConfigForm } from './ExportConfigForm';
import type { ExportConfig, ResolveResponse } from '../types';
import { DEFAULT_EXPORT_CONFIG } from '../types';

interface NewExportFlowProps {
  onComplete: () => void;
  onCancel: () => void;
}

const sanitizeSuffix = (name: string): string =>
  name.toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');

export const NewExportFlow = ({ onComplete, onCancel }: NewExportFlowProps) => {
  const navigate = useNavigate();
  const [activeStep, setActiveStep] = useState(0);
  const [exportName, setExportName] = useState('');
  const [exportDescription, setExportDescription] = useState('');
  const [resolveResult, setResolveResult] = useState<ResolveResponse | null>(null);
  const [config, setConfig] = useState<ExportConfig>({ ...DEFAULT_EXPORT_CONFIG });
  const [outputPath, setOutputPath] = useState('');
  const [rootNamesManuallyEdited, setRootNamesManuallyEdited] = useState(false);

  const manifestTextRef = useRef('');
  const resolveMutation = useResolveManifest();
  const runExportMutation = useRunExport();

  useEffect(() => {
    if (rootNamesManuallyEdited) return;
    const suffix = sanitizeSuffix(exportName);
    if (suffix) {
      const layout = config.layout;
      setConfig((prev) => ({
        ...prev,
        bidsDcmRootName: layout === 'bids' ? `bids-dcm-${suffix}` : prev.bidsDcmRootName,
        bidsNiftiRootName: layout === 'bids' ? `bids-nifti-${suffix}` : prev.bidsNiftiRootName,
        flatDcmRootName: layout === 'flat' ? `flat-dcm-${suffix}` : prev.flatDcmRootName,
        flatNiftiRootName: layout === 'flat' ? `flat-nifti-${suffix}` : prev.flatNiftiRootName,
      }));
    } else {
      setConfig((prev) => ({
        ...prev,
        bidsDcmRootName: 'bids-dcm',
        bidsNiftiRootName: 'bids-nifti',
        flatDcmRootName: 'flat-dcm',
        flatNiftiRootName: 'flat-nifti',
      }));
    }
  }, [exportName, config.layout, rootNamesManuallyEdited]);

  const handleResolve = useCallback(
    (content: string) => {
      manifestTextRef.current = content;
      resolveMutation.mutate(content, {
        onSuccess: (result) => {
          setResolveResult(result);
          if (result.total_stacks > 0) {
            setActiveStep(1);
          } else {
            notifications.show({
              title: 'No stacks resolved',
              message: 'The manifest did not match any data in the database.',
              color: 'yellow',
            });
          }
        },
        onError: (error) => {
          notifications.show({
            title: 'Resolution failed',
            message: error.message,
            color: 'red',
          });
        },
      });
    },
    [resolveMutation],
  );

  const handleConfigChange = useCallback(
    <K extends keyof ExportConfig>(key: K, value: ExportConfig[K]) => {
      const rootKeys: (keyof ExportConfig)[] = [
        'bidsDcmRootName', 'bidsNiftiRootName', 'flatDcmRootName', 'flatNiftiRootName',
      ];
      if (rootKeys.includes(key)) {
        setRootNamesManuallyEdited(true);
      }
      setConfig((prev) => ({ ...prev, [key]: value }));
    },
    [],
  );

  const handleRunExport = useCallback(() => {
    if (!resolveResult || !outputPath.trim() || !exportName.trim()) return;

    runExportMutation.mutate(
      {
        stack_ids: resolveResult.resolved_stack_ids,
        detected_cohorts: resolveResult.detected_cohorts,
        output_path: outputPath,
        export_name: exportName.trim(),
        export_description: exportDescription.trim(),
        manifest_text: manifestTextRef.current,
        resolve_summary: {
          total_subjects: resolveResult.total_subjects,
          total_sessions: resolveResult.total_sessions,
        },
        config,
      },
      {
        onSuccess: (result) => {
          const job = result.job;
          notifications.show({
            title: 'Export started',
            message: 'BIDS export job is running in the background.',
            color: 'green',
          });
          onComplete();
          navigate(`/export/${job.id}`);
        },
        onError: (error) => {
          notifications.show({
            title: 'Export failed',
            message: error.message,
            color: 'red',
          });
        },
      },
    );
  }, [resolveResult, outputPath, exportName, exportDescription, config, runExportMutation, onComplete, navigate]);

  return (
    <Stack gap="md">
      <Stepper
        active={activeStep}
        onStepClick={(step) => {
          if (step < activeStep) setActiveStep(step);
        }}
        size="sm"
      >
        {/* Step 1: Name + Manifest */}
        <Stepper.Step label="Selection" description="Name and define what to export">
          <Stack gap="md" mt="md">
            <TextInput
              label="Export name"
              description="A short identifier for this export (used in folder names)"
              placeholder="spms-nonconverters"
              value={exportName}
              onChange={(e) => setExportName(e.currentTarget.value)}
              required
            />
            <Textarea
              label="Description"
              description="Optional notes about this export"
              placeholder="SPMS non-converters for longitudinal analysis"
              minRows={2}
              value={exportDescription}
              onChange={(e) => setExportDescription(e.currentTarget.value)}
            />

            <Divider label="Selection Manifest" labelPosition="center" />

            <ManifestInput
              onResolve={handleResolve}
              isResolving={resolveMutation.isPending}
              disabled={!exportName.trim()}
            />
            {!exportName.trim() && (
              <Text size="xs" c="var(--nils-text-tertiary)">
                Enter an export name above before resolving the manifest.
              </Text>
            )}
            {resolveMutation.isError && (
              <Alert icon={<IconAlertTriangle size={16} />} color="red" variant="light">
                {resolveMutation.error?.message ?? 'Failed to resolve manifest'}
              </Alert>
            )}
          </Stack>
        </Stepper.Step>

        {/* Step 2: Preview + Configure */}
        <Stepper.Step label="Configure" description="Review and set options">
          <Stack gap="md" mt="md">
            {resolveResult && (
              <ManifestPreview
                result={resolveResult}
                subjectIdentifierSource={config.subjectIdentifierSource}
                onSubjectIdentifierChange={(value) => handleConfigChange('subjectIdentifierSource', value)}
              />
            )}

            <Divider label="Export Configuration" labelPosition="center" />

            {resolveResult?.detected_cohorts && resolveResult.detected_cohorts.length > 0 && (
              <Group gap="xs">
                <Text size="sm" c="var(--nils-text-secondary)">Source cohort:</Text>
                {resolveResult.detected_cohorts.map((name) => (
                  <Badge key={name} variant="light">{name}</Badge>
                ))}
              </Group>
            )}

            <ExportConfigForm
              config={config}
              onChange={handleConfigChange}
              outputPath={outputPath}
              onOutputPathChange={setOutputPath}
            />

            <Group justify="flex-end" mt="md">
              <Button variant="default" onClick={() => setActiveStep(0)}>
                Back
              </Button>
              <Button variant="default" onClick={onCancel}>
                Cancel
              </Button>
              <Button
                onClick={handleRunExport}
                loading={runExportMutation.isPending}
                disabled={
                  !resolveResult ||
                  resolveResult.total_stacks === 0 ||
                  !outputPath.trim() ||
                  !resolveResult.detected_cohorts?.length
                }
                leftSection={<IconFileExport size={16} />}
              >
                Run Export ({resolveResult?.total_stacks ?? 0} stacks)
              </Button>
            </Group>
          </Stack>
        </Stepper.Step>
      </Stepper>
    </Stack>
  );
};
