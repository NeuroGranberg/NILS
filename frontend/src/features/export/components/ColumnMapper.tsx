import {
  Alert,
  Badge,
  Box,
  Group,
  Paper,
  Select,
  Stack,
  Table,
  Text,
  Title,
} from '@mantine/core';
import { IconArrowRight, IconCheck, IconInfoCircle } from '@tabler/icons-react';
import { useCallback, useEffect, useMemo, useState } from 'react';

export interface ColumnMapping {
  subjectCode: string | null;
  studyDate: string | null;
  stackId: string | null;
}

interface ColumnMapperProps {
  columns: string[];
  sampleData: Record<string, string>[];
  onMappingChange: (mapping: ColumnMapping) => void;
  initialMapping?: ColumnMapping;
}

// Patterns for auto-detecting column purposes
const SUBJECT_PATTERNS = [
  /^subject[_-]?code$/i,
  /^subject[_-]?id$/i,
  /^patient[_-]?id$/i,
  /^patient[_-]?code$/i,
  /^subj$/i,
  /^sub$/i,
  /^id$/i,
];

const DATE_PATTERNS = [
  /^study[_-]?date$/i,
  /^session[_-]?date$/i,
  /^scan[_-]?date$/i,
  /^mri[_-]?date$/i,
  /^date$/i,
  /^session$/i,
];

const STACK_PATTERNS = [
  /^series[_-]?stack[_-]?id$/i,
  /^stack[_-]?id$/i,
  /^flair[_-]?stack[_-]?id$/i,
  /^series[_-]?id$/i,
  /^stack$/i,
];

function detectColumn(columns: string[], patterns: RegExp[]): string | null {
  for (const col of columns) {
    for (const pattern of patterns) {
      if (pattern.test(col)) {
        return col;
      }
    }
  }
  return null;
}

function autoDetectMapping(columns: string[]): ColumnMapping {
  return {
    subjectCode: detectColumn(columns, SUBJECT_PATTERNS),
    studyDate: detectColumn(columns, DATE_PATTERNS),
    stackId: detectColumn(columns, STACK_PATTERNS),
  };
}

export const ColumnMapper = ({
  columns,
  sampleData,
  onMappingChange,
  initialMapping,
}: ColumnMapperProps) => {
  const autoMapping = useMemo(() => autoDetectMapping(columns), [columns]);

  const [mapping, setMapping] = useState<ColumnMapping>(
    initialMapping ?? autoMapping
  );

  // Update parent when mapping changes
  useEffect(() => {
    onMappingChange(mapping);
  }, [mapping, onMappingChange]);

  // Update mapping when columns change (new file uploaded)
  useEffect(() => {
    const newAutoMapping = autoDetectMapping(columns);
    setMapping(newAutoMapping);
  }, [columns]);

  const handleChange = useCallback(
    (field: keyof ColumnMapping, value: string | null) => {
      setMapping((prev) => ({ ...prev, [field]: value }));
    },
    []
  );

  const columnOptions = useMemo(
    () => [
      { value: '', label: '-- Not mapped --' },
      ...columns.map((col) => ({ value: col, label: col })),
    ],
    [columns]
  );

  const isComplete = mapping.subjectCode !== null;

  const mappingFields = [
    {
      key: 'subjectCode' as const,
      label: 'Subject Code',
      description: 'Patient/subject identifier',
      required: true,
    },
    {
      key: 'studyDate' as const,
      label: 'Session Date',
      description: 'MRI scan date (optional)',
      required: false,
    },
    {
      key: 'stackId' as const,
      label: 'Stack ID',
      description: 'Series stack ID (optional)',
      required: false,
    },
  ];

  return (
    <Paper withBorder p="md">
      <Stack gap="md">
        <Group justify="space-between" align="flex-start">
          <div>
            <Title order={5}>Column Mapping</Title>
            <Text size="sm" c="dimmed">
              Map your CSV columns to NILS fields
            </Text>
          </div>
          {isComplete ? (
            <Badge color="green" leftSection={<IconCheck size={12} />}>
              Ready
            </Badge>
          ) : (
            <Badge color="yellow">Subject code required</Badge>
          )}
        </Group>

        <Alert icon={<IconInfoCircle size={16} />} color="blue" variant="light">
          <Text size="sm">
            <strong>Subject Code</strong> is required. Session Date and Stack ID
            are optional - if omitted, all sessions/stacks for each subject will
            be included.
          </Text>
        </Alert>

        {/* Mapping selectors */}
        <Table>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>NILS Field</Table.Th>
              <Table.Th></Table.Th>
              <Table.Th>Your CSV Column</Table.Th>
              <Table.Th>Sample Value</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {mappingFields.map((field) => {
              const selectedCol = mapping[field.key];
              const sampleValue =
                selectedCol && sampleData[0] ? sampleData[0][selectedCol] : '-';

              return (
                <Table.Tr key={field.key}>
                  <Table.Td>
                    <Group gap="xs">
                      <Text size="sm" fw={500}>
                        {field.label}
                      </Text>
                      {field.required && (
                        <Text c="red" size="xs">
                          *
                        </Text>
                      )}
                    </Group>
                    <Text size="xs" c="dimmed">
                      {field.description}
                    </Text>
                  </Table.Td>
                  <Table.Td>
                    <IconArrowRight size={16} color="gray" />
                  </Table.Td>
                  <Table.Td>
                    <Select
                      size="xs"
                      data={columnOptions}
                      value={selectedCol ?? ''}
                      onChange={(val) =>
                        handleChange(field.key, val || null)
                      }
                      placeholder="Select column..."
                      clearable
                      searchable
                      w={180}
                    />
                  </Table.Td>
                  <Table.Td>
                    <Text size="xs" c="dimmed" ff="monospace">
                      {sampleValue || '-'}
                    </Text>
                  </Table.Td>
                </Table.Tr>
              );
            })}
          </Table.Tbody>
        </Table>

        {/* Preview of unmapped columns */}
        {columns.length > 0 && (
          <Box>
            <Text size="xs" c="dimmed" mb="xs">
              Other columns in your CSV (will be ignored):
            </Text>
            <Group gap="xs">
              {columns
                .filter(
                  (col) =>
                    col !== mapping.subjectCode &&
                    col !== mapping.studyDate &&
                    col !== mapping.stackId
                )
                .map((col) => (
                  <Badge key={col} variant="light" color="gray" size="sm">
                    {col}
                  </Badge>
                ))}
              {columns.filter(
                (col) =>
                  col !== mapping.subjectCode &&
                  col !== mapping.studyDate &&
                  col !== mapping.stackId
              ).length === 0 && (
                <Text size="xs" c="dimmed" fs="italic">
                  None
                </Text>
              )}
            </Group>
          </Box>
        )}
      </Stack>
    </Paper>
  );
};
