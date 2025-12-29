/**
 * BIDS Stage Form Component
 *
 * Configuration form for BIDS export with organized sections
 * using shared UI components for consistency.
 */

import {
  Badge,
  Card,
  Checkbox,
  Group,
  NumberInput,
  Progress,
  SegmentedControl,
  Select,
  Stack,
  Switch,
  Text,
  TextInput,
} from '@mantine/core';
import type { JobSummary } from '../../types';
import { JOB_STATUS_CONFIG } from '../../constants/status';
import { SectionCard, FormFieldGroup } from '../shared/components/SectionCard';
import { formatDateTime } from '../../utils/formatters';

export interface BidsConfig {
  layout?: string;
  overwriteMode?: string;
  outputModes?: string[];
  outputMode?: string;
  bidsDcmRootName?: string;
  bidsNiftiRootName?: string;
  flatDcmRootName?: string;
  flatNiftiRootName?: string;
  copyWorkers?: number;
  convertWorkers?: number;
  subjectIdentifierSource?: string | number;
  includeIntents?: string[];
  includeProvenance?: string[];
}

interface BidsStageFormProps {
  config: BidsConfig;
  onChange: (key: keyof BidsConfig, value: unknown) => void;
  subjectIdentifierOptions: Array<{ value: string; label: string }>;
  intentOptions: Array<{ value: string; label: string }>;
  provenanceOptions: Array<{ value: string; label: string }>;
  defaultIntentSelection: string[];
  defaultProvenanceSelection: string[];
  activeBidsJob?: JobSummary;
  lastBidsJob?: JobSummary;
}

const bidsRunningStatuses = ['running', 'queued', 'paused'];

export const BidsStageForm = ({
  config,
  onChange,
  subjectIdentifierOptions,
  intentOptions,
  provenanceOptions,
  defaultIntentSelection,
  defaultProvenanceSelection,
  activeBidsJob,
  lastBidsJob,
}: BidsStageFormProps) => {
  // Derive values from config
  const rawIncludeIntents = config.includeIntents;
  const includeIntentsVal =
    rawIncludeIntents && rawIncludeIntents.length === 0
      ? defaultIntentSelection
      : rawIncludeIntents ?? defaultIntentSelection;

  const rawIncludeProvenance = config.includeProvenance;
  const includeProvVal =
    rawIncludeProvenance && rawIncludeProvenance.length === 0
      ? defaultProvenanceSelection
      : rawIncludeProvenance ?? defaultProvenanceSelection;

  const showProvenanceSelection = includeIntentsVal.includes('anat');
  const copyWorkersVal = Number(config.copyWorkers ?? 8);
  const convertWorkersVal = Number(config.convertWorkers ?? 8);
  const bidsDcmRootNameVal = String(config.bidsDcmRootName ?? 'bids-dcm');
  const bidsNiftiRootNameVal = String(config.bidsNiftiRootName ?? 'bids-nifti');

  const rawFlatDcmRoot = config.flatDcmRootName;
  const rawFlatNiftiRoot = config.flatNiftiRootName;
  const flatDcmRootNameVal =
    rawFlatDcmRoot === 'dcm-flat' ? 'flat-dcm' : String(rawFlatDcmRoot ?? 'flat-dcm');
  const flatNiftiRootNameVal =
    rawFlatNiftiRoot === 'nii-flat' ? 'flat-nifti' : String(rawFlatNiftiRoot ?? 'flat-nifti');

  const layoutVal = String(config.layout ?? 'bids');
  const layoutLabel = layoutVal === 'flat' ? 'Flat' : 'BIDS';

  const outputModesVal =
    config.outputModes ?? (config.outputMode ? [String(config.outputMode)] : ['dcm']);
  const exportDicom = outputModesVal.includes('dcm');
  const niftiModeVal = outputModesVal.find((mode) => mode === 'nii' || mode === 'nii.gz') as
    | 'nii'
    | 'nii.gz'
    | undefined;

  const overwriteRaw = config.overwriteMode ?? 'skip';
  const overwriteModeVal = ['skip', 'clean', 'overwrite'].includes(overwriteRaw) ? overwriteRaw : 'skip';

  const subjectIdentifierSourceVal = String(config.subjectIdentifierSource ?? 'subject_code');

  const applyOutputModes = (nextDicom: boolean, nextNifti: 'nii' | 'nii.gz' | null) => {
    const next: string[] = [];
    if (nextDicom) next.push('dcm');
    if (nextNifti) next.push(nextNifti);
    const safeNext = next.length ? next : ['dcm'];
    onChange('outputModes', safeNext);
    onChange('outputMode', nextNifti ?? (nextDicom ? 'dcm' : 'dcm'));
  };

  const renderBidsJobCard = (job: JobSummary, title: string) => {
    const statusConfig = JOB_STATUS_CONFIG[job.status];
    return (
      <Card withBorder padding="md" radius="md">
        <Stack gap="xs">
          <Group justify="space-between" align="center">
            <Text fw={600}>{title}</Text>
            <Badge color={statusConfig?.mantineColor ?? 'gray'}>{job.status.toUpperCase()}</Badge>
          </Group>
          <Progress value={job.progress ?? 0} size="lg" radius="md" transitionDuration={200} />
          <Group justify="space-between">
            <Text size="xs" c="dimmed">
              Started {job.startedAt ? formatDateTime(job.startedAt) : formatDateTime(job.submittedAt)}
            </Text>
            <Text size="xs" c="dimmed">
              Job #{job.id}
            </Text>
          </Group>
          {job.finishedAt && (
            <Text size="xs" c="dimmed">
              Finished {formatDateTime(job.finishedAt)}
            </Text>
          )}
          {job.errorMessage && (
            <Text size="xs" c="red">
              Error: {job.errorMessage}
            </Text>
          )}
        </Stack>
      </Card>
    );
  };

  // Show active job if running
  if (activeBidsJob && bidsRunningStatuses.includes(activeBidsJob.status)) {
    return (
      <Stack gap="md">
        {renderBidsJobCard(activeBidsJob, 'BIDS export in progress')}
        {activeBidsJob.status === 'paused' && (
          <Text size="xs" c="dimmed">
            Job paused. Manage actions from the Jobs page.
          </Text>
        )}
      </Stack>
    );
  }

  return (
    <Stack gap="md">
      {/* Layout & Mode Section */}
      <SectionCard title="Layout & Output" description="Choose export layout and file formats">
        <Stack gap="sm">
          <SegmentedControl
            data={[
              { label: 'BIDS layout', value: 'bids' },
              { label: 'Flat layout', value: 'flat' },
            ]}
            value={layoutVal}
            onChange={(value) => onChange('layout', value ?? 'bids')}
          />
          <SegmentedControl
            value={overwriteModeVal}
            onChange={(value) => onChange('overwriteMode', value ?? 'skip')}
            data={[
              { label: 'Skip existing', value: 'skip' },
              { label: 'Clean', value: 'clean' },
              { label: 'Overwrite', value: 'overwrite' },
            ]}
          />
        </Stack>
      </SectionCard>

      {/* Export Formats Section */}
      <SectionCard title="Export Formats" description="Select output formats and directories">
        <Stack gap="sm">
          <Group gap="md" align="center">
            <Switch
              label="Export DICOM"
              checked={exportDicom}
              onChange={(event) => applyOutputModes(event.currentTarget.checked, niftiModeVal ?? null)}
            />
            {exportDicom && (
              <TextInput
                label={`${layoutLabel} DICOM root (under derivatives)`}
                value={layoutVal === 'flat' ? flatDcmRootNameVal : bidsDcmRootNameVal}
                style={{ flex: 1 }}
                onChange={(event) =>
                  onChange(
                    layoutVal === 'flat' ? 'flatDcmRootName' : 'bidsDcmRootName',
                    event.currentTarget.value,
                  )
                }
              />
            )}
          </Group>
          <Group gap="md" align="center">
            <Switch
              label="Export NIfTI"
              checked={Boolean(niftiModeVal)}
              onChange={(event) =>
                applyOutputModes(exportDicom, event.currentTarget.checked ? (niftiModeVal ?? 'nii.gz') : null)
              }
            />
            {Boolean(niftiModeVal) && (
              <TextInput
                label={`${layoutLabel} NIfTI root (under derivatives)`}
                value={layoutVal === 'flat' ? flatNiftiRootNameVal : bidsNiftiRootNameVal}
                style={{ flex: 1 }}
                onChange={(event) =>
                  onChange(
                    layoutVal === 'flat' ? 'flatNiftiRootName' : 'bidsNiftiRootName',
                    event.currentTarget.value,
                  )
                }
              />
            )}
          </Group>
          {Boolean(niftiModeVal) && (
            <SegmentedControl
              data={[
                { label: 'NIfTI (.nii)', value: 'nii' },
                { label: 'Compressed (.nii.gz)', value: 'nii.gz' },
              ]}
              value={niftiModeVal}
              onChange={(value) => applyOutputModes(exportDicom, value as 'nii' | 'nii.gz')}
            />
          )}
        </Stack>
      </SectionCard>

      {/* Subject Naming Section */}
      <SectionCard title="Subject Naming" description="Configure how subjects are named in the export">
        <Select
          label="Subject identifier"
          description="Choose which identifier to use for subject naming (sub-*)"
          value={subjectIdentifierSourceVal}
          data={subjectIdentifierOptions}
          onChange={(value) => {
            const parsed = value === 'subject_code' ? 'subject_code' : Number(value);
            onChange('subjectIdentifierSource', parsed);
          }}
        />
      </SectionCard>

      {/* Intent Selection Section */}
      <SectionCard title="Include Intents" description="Select which directory types to include in the export">
        <Stack gap="sm">
          <Checkbox.Group
            value={includeIntentsVal}
            onChange={(value) => onChange('includeIntents', value)}
          >
            <Group mt="xs">
              {intentOptions.map((option) => (
                <Checkbox key={option.value} value={option.value} label={option.label} />
              ))}
            </Group>
          </Checkbox.Group>
          {showProvenanceSelection && (
            <FormFieldGroup
              label="Include provenance (anat only)"
              description="Leave empty to include all provenance types"
              indent
            >
              <Checkbox.Group
                value={includeProvVal}
                onChange={(value) => onChange('includeProvenance', value)}
              >
                <Group mt="xs">
                  {provenanceOptions.map((option) => (
                    <Checkbox key={option.value} value={option.value} label={option.label} />
                  ))}
                </Group>
              </Checkbox.Group>
            </FormFieldGroup>
          )}
        </Stack>
      </SectionCard>

      {/* Performance Section */}
      <SectionCard title="Performance" description="Configure worker counts for parallel processing">
        <Group grow>
          <NumberInput
            label="Copy workers (DICOM)"
            min={1}
            value={copyWorkersVal}
            onChange={(value) => onChange('copyWorkers', Number(value ?? 8))}
          />
          <NumberInput
            label="Convert workers (NIfTI)"
            min={1}
            value={convertWorkersVal}
            onChange={(value) => onChange('convertWorkers', Number(value ?? 8))}
          />
        </Group>
      </SectionCard>

      {/* Last Job Card */}
      {lastBidsJob && renderBidsJobCard(lastBidsJob, 'Last BIDS export')}
    </Stack>
  );
};
