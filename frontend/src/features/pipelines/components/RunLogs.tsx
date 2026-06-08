/**
 * RunLogs - a mono log viewer for the accumulated log lines produced by
 * useRunLogs (cursor-accumulated). Auto-scrolls to the bottom as new lines
 * arrive; shows a dimmed empty state when there is nothing yet.
 */
import { useEffect, useRef } from 'react';
import { ScrollArea, Text } from '@mantine/core';

interface RunLogsProps {
  lines: string[];
}

export const RunLogs = ({ lines }: RunLogsProps) => {
  const viewportRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const vp = viewportRef.current;
    if (vp) {
      vp.scrollTo({ top: vp.scrollHeight });
    }
  }, [lines]);

  if (lines.length === 0) {
    return (
      <Text size="sm" c="dimmed">
        No logs yet.
      </Text>
    );
  }

  return (
    <ScrollArea.Autosize
      mah={320}
      viewportRef={viewportRef}
      style={{
        backgroundColor: 'var(--nils-bg-tertiary)',
        borderRadius: 'var(--nils-radius-sm)',
        border: '1px solid var(--nils-border-subtle)',
      }}
    >
      <pre
        style={{
          margin: 0,
          padding: 'var(--nils-space-sm)',
          fontFamily: 'monospace',
          fontSize: 'var(--mantine-font-size-xs)',
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-word',
          color: 'var(--nils-text-secondary)',
        }}
      >
        {lines.join('\n')}
      </pre>
    </ScrollArea.Autosize>
  );
};
