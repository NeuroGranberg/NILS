/**
 * Models tab — global model registry and training.
 *
 * Features:
 * - List all registered models with classes, accuracy, sample count
 * - Select a model for the current cohort
 * - Train new models from the global pool with label remapping
 * - Push cohort samples to the global pool
 * - Set default model / delete models
 */
import {
  ActionIcon,
  Badge,
  Button,
  Checkbox,
  Group,
  Loader,
  Modal,
  Paper,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Textarea,
  Tooltip,
} from '@mantine/core';
import {
  IconCheck,
  IconPlayerPlay,
  IconStar,
  IconStarFilled,
  IconTrash,
  IconUpload,
} from '@tabler/icons-react';
import { useState } from 'react';

import { useSelectModelMutation } from './api';
import {
  useDeleteModelMutation,
  useModelsQuery,
  usePoolSummaryQuery,
  usePushToPoolMutation,
  useSetDefaultModelMutation,
  useTrainModelMutation,
} from './globalApi';
import type { BodyPartModelEntry, TrainModelPayload } from './types';

interface ModelsTabProps {
  cohortId: number;
  selectedModelId: number | null;
  hasTrainingSamples: boolean;
}

export const ModelsTab = ({
  cohortId,
  selectedModelId,
  hasTrainingSamples,
}: ModelsTabProps) => {
  const { data: models, isLoading: modelsLoading } = useModelsQuery();
  const { data: poolSummary, isLoading: poolLoading } = usePoolSummaryQuery();

  const selectModelMutation = useSelectModelMutation(cohortId);
  const pushToPoolMutation = usePushToPoolMutation();
  const setDefaultMutation = useSetDefaultModelMutation();
  const deleteMutation = useDeleteModelMutation();

  const [trainModalOpen, setTrainModalOpen] = useState(false);

  if (modelsLoading || poolLoading) {
    return <Loader size="sm" />;
  }

  const poolLabels = Object.keys(poolSummary?.by_label ?? {});

  return (
    <Stack gap="md">
      {/* ── Global pool summary ─────────────────────────── */}
      <Paper withBorder p="md" radius="md">
        <Group justify="space-between" align="center">
          <div>
            <Text fw={600} size="sm">
              Global Sample Pool
            </Text>
            <Text size="xs" c="dimmed">
              {poolSummary?.total ?? 0} total samples across all cohorts
            </Text>
            {poolLabels.length > 0 && (
              <Group gap={4} mt={4}>
                {poolLabels.map((label) => (
                  <Badge key={label} size="sm" variant="light">
                    {label}: {poolSummary!.by_label[label]}
                  </Badge>
                ))}
              </Group>
            )}
          </div>
          <Button
            variant="light"
            leftSection={<IconUpload size={14} />}
            loading={pushToPoolMutation.isPending}
            disabled={!hasTrainingSamples}
            onClick={() => pushToPoolMutation.mutate({ cohort_id: cohortId })}
          >
            Push cohort samples to pool
          </Button>
        </Group>
      </Paper>

      {/* ── Model registry ──────────────────────────────── */}
      <Paper withBorder p="md" radius="md">
        <Group justify="space-between" align="center" mb="sm">
          <Text fw={600} size="sm">
            Model Registry
          </Text>
          <Button
            variant="filled"
            color="violet"
            leftSection={<IconPlayerPlay size={14} />}
            onClick={() => setTrainModalOpen(true)}
            disabled={(poolSummary?.total ?? 0) < 10}
          >
            Train new model
          </Button>
        </Group>

        {!models || models.length === 0 ? (
          <Text size="sm" c="dimmed">
            No models registered yet. Push samples to the global pool and
            train one.
          </Text>
        ) : (
          <Table striped highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Name</Table.Th>
                <Table.Th>Classes</Table.Th>
                <Table.Th>Remap</Table.Th>
                <Table.Th>Accuracy</Table.Th>
                <Table.Th>Samples</Table.Th>
                <Table.Th>Trained</Table.Th>
                <Table.Th>Actions</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {models.map((model) => (
                <ModelRow
                  key={model.id}
                  model={model}
                  isSelected={model.id === selectedModelId}
                  onSelect={() =>
                    selectModelMutation.mutate({ model_id: model.id })
                  }
                  onSetDefault={() => setDefaultMutation.mutate(model.id)}
                  onDelete={() => deleteMutation.mutate(model.id)}
                  selectLoading={selectModelMutation.isPending}
                />
              ))}
            </Table.Tbody>
          </Table>
        )}

        {selectedModelId && (
          <Button
            variant="subtle"
            color="gray"
            size="xs"
            mt="xs"
            onClick={() => selectModelMutation.mutate({ model_id: null })}
          >
            Clear model selection (use legacy per-cohort classifier)
          </Button>
        )}
      </Paper>

      {/* ── Train model modal ───────────────────────────── */}
      <TrainModelModal
        opened={trainModalOpen}
        onClose={() => setTrainModalOpen(false)}
        poolLabels={poolLabels}
        poolSummary={poolSummary?.by_label ?? {}}
      />
    </Stack>
  );
};

// ---------------------------------------------------------------------------
// Model row
// ---------------------------------------------------------------------------

const ModelRow = ({
  model,
  isSelected,
  onSelect,
  onSetDefault,
  onDelete,
  selectLoading,
}: {
  model: BodyPartModelEntry;
  isSelected: boolean;
  onSelect: () => void;
  onSetDefault: () => void;
  onDelete: () => void;
  selectLoading: boolean;
}) => {
  const remapEntries = Object.entries(model.label_remap);
  return (
    <Table.Tr>
      <Table.Td>
        <Group gap={4}>
          <Text size="sm" fw={isSelected ? 700 : 400}>
            {model.name}
          </Text>
          {model.is_default && (
            <Badge size="xs" color="yellow" variant="filled">
              default
            </Badge>
          )}
          {isSelected && (
            <Badge size="xs" color="blue" variant="filled">
              selected
            </Badge>
          )}
        </Group>
      </Table.Td>
      <Table.Td>
        <Group gap={4}>
          {model.classes.map((c) => (
            <Badge key={c} size="xs" variant="light">
              {c}
            </Badge>
          ))}
        </Group>
      </Table.Td>
      <Table.Td>
        {remapEntries.length > 0 ? (
          <Text size="xs" c="dimmed">
            {remapEntries.map(([from, to]) => `${from}→${to}`).join(', ')}
          </Text>
        ) : (
          <Text size="xs" c="dimmed">
            —
          </Text>
        )}
      </Table.Td>
      <Table.Td>
        <Text size="sm">
          {model.accuracy != null
            ? `${(model.accuracy * 100).toFixed(1)}%`
            : '—'}
        </Text>
      </Table.Td>
      <Table.Td>
        <Text size="sm">{model.n_samples}</Text>
      </Table.Td>
      <Table.Td>
        <Text size="xs" c="dimmed">
          {new Date(model.trained_at).toLocaleDateString()}
        </Text>
      </Table.Td>
      <Table.Td>
        <Group gap={4}>
          {!isSelected && (
            <Tooltip label="Use this model for Apply">
              <Button
                size="compact-xs"
                variant="light"
                color="blue"
                loading={selectLoading}
                onClick={onSelect}
              >
                Select
              </Button>
            </Tooltip>
          )}
          <Tooltip label={model.is_default ? 'Already default' : 'Set as default'}>
            <ActionIcon
              size="sm"
              variant="subtle"
              color="yellow"
              onClick={onSetDefault}
              disabled={model.is_default}
            >
              {model.is_default ? <IconStarFilled size={14} /> : <IconStar size={14} />}
            </ActionIcon>
          </Tooltip>
          <Tooltip label="Delete model">
            <ActionIcon
              size="sm"
              variant="subtle"
              color="red"
              onClick={onDelete}
            >
              <IconTrash size={14} />
            </ActionIcon>
          </Tooltip>
        </Group>
      </Table.Td>
    </Table.Tr>
  );
};

// ---------------------------------------------------------------------------
// Train model modal
// ---------------------------------------------------------------------------

const TrainModelModal = ({
  opened,
  onClose,
  poolLabels,
  poolSummary,
}: {
  opened: boolean;
  onClose: () => void;
  poolLabels: string[];
  poolSummary: Record<string, number>;
}) => {
  const trainMutation = useTrainModelMutation();

  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [selectedClasses, setSelectedClasses] = useState<Set<string>>(new Set());
  const [remap, setRemap] = useState<Record<string, string>>({});
  const [estimatorKind, setEstimatorKind] = useState<string>('logreg');
  const [usePca, setUsePca] = useState(true);

  // Labels not selected as classes can be remapped
  const unselectedLabels = poolLabels.filter((l) => !selectedClasses.has(l));

  // Compute effective sample counts after remap
  const effectiveCounts: Record<string, number> = {};
  for (const cls of selectedClasses) {
    effectiveCounts[cls] = poolSummary[cls] ?? 0;
  }
  for (const [from, to] of Object.entries(remap)) {
    if (to && selectedClasses.has(to)) {
      effectiveCounts[to] = (effectiveCounts[to] ?? 0) + (poolSummary[from] ?? 0);
    }
  }

  const totalEffective = Object.values(effectiveCounts).reduce((a, b) => a + b, 0);
  const canTrain =
    name.trim().length > 0 && selectedClasses.size >= 2 && totalEffective >= 10;

  const handleTrain = () => {
    const payload: TrainModelPayload = {
      name: name.trim(),
      classes: Array.from(selectedClasses),
      label_remap: Object.fromEntries(
        Object.entries(remap).filter(([, v]) => v && selectedClasses.has(v)),
      ),
      description: description.trim() || undefined,
      estimator_kind: estimatorKind,
      use_pca: usePca,
    };
    trainMutation.mutate(payload, {
      onSuccess: () => {
        onClose();
        setName('');
        setDescription('');
        setSelectedClasses(new Set());
        setRemap({});
        setEstimatorKind('logreg');
        setUsePca(true);
      },
    });
  };

  const classOptions = Array.from(selectedClasses).map((c) => ({
    value: c,
    label: c,
  }));

  return (
    <Modal
      opened={opened}
      onClose={onClose}
      title="Train New Model"
      size="lg"
      centered
    >
      <Stack gap="sm">
        <TextInput
          label="Model name"
          placeholder="e.g. 2-class (Brain/Spine)"
          value={name}
          onChange={(e) => setName(e.currentTarget.value)}
          required
        />
        <Textarea
          label="Description (optional)"
          placeholder="Notes about this model..."
          value={description}
          onChange={(e) => setDescription(e.currentTarget.value)}
          autosize
          minRows={1}
          maxRows={3}
        />

        <Text size="sm" fw={600}>
          Target classes
        </Text>
        <Text size="xs" c="dimmed">
          Select which classes this model should predict. Labels in the pool
          not selected here can be remapped to a selected class below.
        </Text>
        <Group gap="xs">
          {poolLabels.map((label) => (
            <Checkbox
              key={label}
              label={`${label} (${poolSummary[label] ?? 0})`}
              checked={selectedClasses.has(label)}
              onChange={(e) => {
                const next = new Set(selectedClasses);
                if (e.currentTarget.checked) {
                  next.add(label);
                  // Remove from remap if it was there
                  const nextRemap = { ...remap };
                  delete nextRemap[label];
                  setRemap(nextRemap);
                } else {
                  next.delete(label);
                }
                setSelectedClasses(next);
              }}
            />
          ))}
        </Group>

        {unselectedLabels.length > 0 && selectedClasses.size >= 1 && (
          <>
            <Text size="sm" fw={600} mt="sm">
              Label remapping
            </Text>
            <Text size="xs" c="dimmed">
              Map pool labels that aren't selected as classes to one of
              your target classes. Unmapped labels will be excluded from
              training.
            </Text>
            {unselectedLabels.map((label) => (
              <Group key={label} gap="xs" align="center">
                <Badge variant="light" size="sm">
                  {label} ({poolSummary[label] ?? 0})
                </Badge>
                <Text size="xs">→</Text>
                <Select
                  size="xs"
                  placeholder="exclude"
                  clearable
                  data={classOptions}
                  value={remap[label] ?? null}
                  onChange={(val) =>
                    setRemap({ ...remap, [label]: val ?? '' })
                  }
                  style={{ width: 160 }}
                />
              </Group>
            ))}
          </>
        )}

        {/* ── Pipeline configuration ────────────────────── */}
        <Text size="sm" fw={600} mt="sm">
          Pipeline
        </Text>
        <Group gap="md" align="flex-end">
          <Select
            label="Estimator"
            size="xs"
            data={[
              { value: 'logreg', label: 'Logistic Regression' },
              { value: 'rf', label: 'Random Forest' },
              { value: 'svm', label: 'SVM (Platt-calibrated)' },
            ]}
            value={estimatorKind}
            onChange={(val) => {
              setEstimatorKind(val ?? 'logreg');
              // RF typically doesn't benefit from PCA
              if (val === 'rf') setUsePca(false);
            }}
            style={{ width: 200 }}
          />
          <Checkbox
            label="Use PCA"
            checked={usePca}
            onChange={(e) => setUsePca(e.currentTarget.checked)}
          />
        </Group>
        <Text size="xs" c="dimmed">
          Pipeline: StandardScaler → {usePca ? 'PCA → ' : ''}{estimatorKind === 'rf' ? 'RandomForest' : estimatorKind === 'svm' ? 'SVM+Platt' : 'LogisticRegression'}
          {' · '}Hyperparameters auto-tuned via CV.
        </Text>

        {selectedClasses.size >= 2 && (
          <Paper withBorder p="xs" radius="sm">
            <Text size="xs" fw={600}>
              Effective training data after remap:
            </Text>
            <Group gap={4} mt={4}>
              {Object.entries(effectiveCounts).map(([cls, count]) => (
                <Badge
                  key={cls}
                  size="sm"
                  variant="light"
                  color={count < 5 ? 'red' : 'green'}
                >
                  {cls}: {count}
                </Badge>
              ))}
              <Badge size="sm" variant="outline">
                Total: {totalEffective}
              </Badge>
            </Group>
          </Paper>
        )}

        <Group justify="flex-end" mt="md">
          <Button variant="default" onClick={onClose}>
            Cancel
          </Button>
          <Button
            color="violet"
            leftSection={<IconPlayerPlay size={14} />}
            loading={trainMutation.isPending}
            disabled={!canTrain}
            onClick={handleTrain}
          >
            Train
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
};
