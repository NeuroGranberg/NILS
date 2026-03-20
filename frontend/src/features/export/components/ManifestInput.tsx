import {
  Button,
  FileInput,
  Group,
  Stack,
  Tabs,
  Text,
  Textarea,
} from '@mantine/core';
import { IconFileUpload } from '@tabler/icons-react';
import { useCallback, useMemo, useState } from 'react';
import { SectionCard } from '../../shared/components/SectionCard';
import { ColumnMapper, type ColumnMapping } from './ColumnMapper';

interface ManifestInputProps {
  onResolve: (content: string, mapping?: ColumnMapping) => void;
  isResolving: boolean;
  disabled?: boolean;
}

const CSV_TEMPLATE = `subject_code,study_date,series_stack_id
PAT001,,
PAT002,2024-03-15,
PAT003,2024-01-10,101`;

const JSON_TEMPLATE = `[
  { "subject_code": "PAT001" },
  { "subject_code": "PAT002", "study_date": "2024-03-15" },
  { "subject_code": "PAT003", "study_date": "2024-01-10", "series_stack_ids": [101, 205] }
]`;

function parseCSVHeaders(text: string): { columns: string[]; rows: Record<string, string>[] } {
  const lines = text.trim().split('\n');
  if (lines.length === 0) return { columns: [], rows: [] };

  // Parse header line
  const headerLine = lines[0];
  const columns = headerLine.split(',').map((h) => h.trim().replace(/^["']|["']$/g, ''));

  // Parse data rows (up to 5 for preview)
  const rows: Record<string, string>[] = [];
  for (let i = 1; i < Math.min(lines.length, 6); i++) {
    const values = lines[i].split(',').map((v) => v.trim().replace(/^["']|["']$/g, ''));
    const row: Record<string, string> = {};
    columns.forEach((col, idx) => {
      row[col] = values[idx] ?? '';
    });
    rows.push(row);
  }

  return { columns, rows };
}

function isCSV(text: string): boolean {
  const trimmed = text.trim();
  // JSON starts with [ or {
  if (trimmed.startsWith('[') || trimmed.startsWith('{')) {
    return false;
  }
  // Check if first line looks like CSV headers
  const firstLine = trimmed.split('\n')[0];
  return firstLine.includes(',');
}

function hasCSVColumns(columns: string[]): boolean {
  return columns.length > 0;
}

function applyMapping(
  text: string,
  mapping: ColumnMapping
): string {
  if (!mapping.subjectCode) return text;

  const lines = text.trim().split('\n');
  if (lines.length === 0) return text;

  // Parse original headers
  const originalHeaders = lines[0].split(',').map((h) => h.trim().replace(/^["']|["']$/g, ''));

  // Create new headers based on mapping
  const newHeaders = originalHeaders.map((h) => {
    if (h === mapping.subjectCode) return 'subject_code';
    if (h === mapping.studyDate) return 'study_date';
    if (h === mapping.stackId) return 'series_stack_id';
    return h;
  });

  // Reconstruct CSV with new headers
  const newLines = [newHeaders.join(','), ...lines.slice(1)];
  return newLines.join('\n');
}

export const ManifestInput = ({ onResolve, isResolving, disabled }: ManifestInputProps) => {
  const [text, setText] = useState('');
  const [activeTab, setActiveTab] = useState<string | null>('paste');
  const [fileName, setFileName] = useState<string | null>(null);
  const [columnMapping, setColumnMapping] = useState<ColumnMapping>({
    subjectCode: null,
    studyDate: null,
    stackId: null,
  });

  // Parse CSV to get columns and sample data
  const csvInfo = useMemo(() => {
    if (!text.trim() || !isCSV(text)) {
      return { columns: [], rows: [], showMapper: false };
    }
    const { columns, rows } = parseCSVHeaders(text);
    return { columns, rows, showMapper: hasCSVColumns(columns) };
  }, [text]);

  const handleFileChange = useCallback((file: File | null) => {
    if (!file) return;
    setFileName(file.name);
    const reader = new FileReader();
    reader.onload = (e) => {
      const content = e.target?.result;
      if (typeof content === 'string') {
        setText(content);
      }
    };
    reader.readAsText(file);
  }, []);

  const handleResolve = useCallback(() => {
    if (!text.trim()) return;

    let contentToResolve = text.trim();

    // If CSV with custom columns, apply mapping
    if (csvInfo.showMapper && columnMapping.subjectCode) {
      contentToResolve = applyMapping(text, columnMapping);
    }

    onResolve(contentToResolve, columnMapping);
  }, [text, csvInfo.showMapper, columnMapping, onResolve]);

  const canResolve = useMemo(() => {
    if (!text.trim()) return false;
    // If showing mapper, need at least subject code mapped
    if (csvInfo.showMapper) {
      return columnMapping.subjectCode !== null;
    }
    return true;
  }, [text, csvInfo.showMapper, columnMapping.subjectCode]);

  return (
    <SectionCard
      title="Selection Manifest"
      description="Specify which subjects, sessions, and stacks to export. Upload a CSV/JSON file or paste content directly."
    >
      <Tabs value={activeTab} onChange={setActiveTab}>
        <Tabs.List>
          <Tabs.Tab value="paste">Paste</Tabs.Tab>
          <Tabs.Tab value="upload">Upload File</Tabs.Tab>
        </Tabs.List>

        <Tabs.Panel value="paste" pt="md">
          <Stack gap="sm">
            <Textarea
              placeholder={`Paste CSV or JSON manifest here...\n\nCSV example:\n${CSV_TEMPLATE}\n\nJSON example:\n${JSON_TEMPLATE}`}
              minRows={10}
              maxRows={20}
              autosize
              value={text}
              onChange={(e) => {
                setText(e.currentTarget.value);
                setFileName(null);
              }}
              styles={{
                input: {
                  fontFamily: 'monospace',
                  fontSize: 'var(--mantine-font-size-xs)',
                },
              }}
            />
            <Group gap="xs">
              <Button
                size="xs"
                variant="subtle"
                onClick={() => {
                  setText(CSV_TEMPLATE);
                  setFileName(null);
                }}
              >
                Load CSV template
              </Button>
              <Button
                size="xs"
                variant="subtle"
                onClick={() => {
                  setText(JSON_TEMPLATE);
                  setFileName(null);
                }}
              >
                Load JSON template
              </Button>
            </Group>
          </Stack>
        </Tabs.Panel>

        <Tabs.Panel value="upload" pt="md">
          <Stack gap="sm">
            <FileInput
              label="Select manifest file"
              description="Supported formats: .csv, .json, .txt"
              placeholder="Click to browse..."
              accept=".csv,.json,.txt"
              leftSection={<IconFileUpload size={16} />}
              onChange={handleFileChange}
            />
            {fileName && text && (
              <>
                <Text size="sm" c="var(--nils-text-secondary)">
                  Loaded: <strong>{fileName}</strong>
                </Text>
                <Textarea
                  value={text}
                  readOnly
                  minRows={5}
                  maxRows={10}
                  autosize
                  styles={{
                    input: {
                      fontFamily: 'monospace',
                      fontSize: 'var(--mantine-font-size-xs)',
                    },
                  }}
                />
              </>
            )}
          </Stack>
        </Tabs.Panel>
      </Tabs>

      {/* Column Mapper - always shown for CSV so user can verify/correct auto-detection */}
      {csvInfo.showMapper && csvInfo.columns.length > 0 && (
        <Stack gap="md" mt="md">
          <ColumnMapper
            columns={csvInfo.columns}
            sampleData={csvInfo.rows}
            onMappingChange={setColumnMapping}
          />
        </Stack>
      )}

      <Group justify="flex-end" mt="md">
        <Button
          onClick={handleResolve}
          loading={isResolving}
          disabled={!canResolve || disabled}
        >
          Resolve & Preview
        </Button>
      </Group>
    </SectionCard>
  );
};
