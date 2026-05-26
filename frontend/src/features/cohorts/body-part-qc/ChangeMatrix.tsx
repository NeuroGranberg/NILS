/**
 * ChangeMatrix — compact heatmap-style summary of the latest run's
 * before→after relabel counts. Rows are the prior label, columns the
 * new label. Diagonal cells (= unchanged) are intentionally rendered
 * as a faint grey so the eye lands on relabels.
 */
import { Box, Group, Stack, Table, Text, Tooltip } from '@mantine/core';
import { useMemo } from 'react';

interface ChangeMatrixProps {
  /** ``state.summary.change_matrix``: prior → new → count. */
  matrix: Record<string, Record<string, number>>;
  categories: string[];
}

export const ChangeMatrix = ({ matrix, categories }: ChangeMatrixProps) => {
  // Discover row labels (priors) from the data, including "(none)".
  const priorLabels = useMemo(() => {
    const set = new Set<string>(Object.keys(matrix));
    return Array.from(set).sort((a, b) => {
      // Pin "(none)" to the top so it's easy to scan.
      if (a === '(none)') return -1;
      if (b === '(none)') return 1;
      return a.localeCompare(b);
    });
  }, [matrix]);

  const cols = categories;

  // Find the max non-diagonal cell so we can normalize the heat.
  const max = useMemo(() => {
    let m = 0;
    for (const [from, row] of Object.entries(matrix)) {
      for (const [to, n] of Object.entries(row)) {
        if (from !== to && n > m) m = n;
      }
    }
    return Math.max(1, m);
  }, [matrix]);

  if (priorLabels.length === 0) {
    return (
      <Text size="sm" c="dimmed">
        No relabels in the latest run.
      </Text>
    );
  }

  const cellColor = (from: string, to: string, n: number): string => {
    if (n === 0) return 'transparent';
    if (from === to) return 'rgba(150, 150, 150, 0.10)';
    const intensity = Math.min(1, n / max);
    // Mantine violet, alpha-modulated.
    return `rgba(121, 80, 242, ${0.15 + 0.6 * intensity})`;
  };

  return (
    <Stack gap="xs" data-testid="body-part-change-matrix">
      <Group gap="xs" align="baseline">
        <Text size="sm" fw={600}>
          Change matrix
        </Text>
        <Text size="xs" c="dimmed">
          rows = prior · columns = new
        </Text>
      </Group>
      <Box style={{ overflowX: 'auto' }}>
        <Table withTableBorder withColumnBorders verticalSpacing={4} style={{ minWidth: 320 }}>
          <Table.Thead>
            <Table.Tr>
              <Table.Th />
              {cols.map((c) => (
                <Table.Th key={c} ta="center">
                  <Text size="xs" fw={600}>
                    {c}
                  </Text>
                </Table.Th>
              ))}
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {priorLabels.map((from) => (
              <Table.Tr key={from}>
                <Table.Td>
                  <Text size="xs" fw={500}>
                    {from}
                  </Text>
                </Table.Td>
                {cols.map((to) => {
                  const n = matrix[from]?.[to] ?? 0;
                  const cell = (
                    <Box
                      style={{
                        background: cellColor(from, to, n),
                        textAlign: 'center',
                        padding: '6px 4px',
                        minWidth: 48,
                      }}
                    >
                      <Text
                        size="xs"
                        c={n > 0 ? undefined : 'dimmed'}
                        fw={from !== to && n > 0 ? 600 : 400}
                      >
                        {n}
                      </Text>
                    </Box>
                  );
                  return (
                    <Table.Td key={to} p={0}>
                      {n > 0 ? (
                        <Tooltip
                          label={`${from} → ${to}: ${n}`}
                          openDelay={200}
                          withinPortal
                        >
                          {cell}
                        </Tooltip>
                      ) : (
                        cell
                      )}
                    </Table.Td>
                  );
                })}
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      </Box>
    </Stack>
  );
};
