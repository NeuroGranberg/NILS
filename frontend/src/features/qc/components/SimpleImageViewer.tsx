/**
 * Simple PNG-based DICOM Viewer
 *
 * A fallback viewer that uses server-side rendering to PNG.
 * Works with all DICOM transfer syntaxes since the server handles decoding.
 */

import { useEffect, useState, useCallback, useRef } from 'react';
import { Box, Group, Text, Loader, ActionIcon, Tooltip, Slider, Stack, Paper } from '@mantine/core';
import {
  IconPlayerPlay,
  IconPlayerPause,
  IconPlayerSkipBack,
  IconPlayerSkipForward,
  IconPhotoOff,
  IconAdjustments,
} from '@tabler/icons-react';

interface SimpleImageViewerProps {
  /** Series Instance UID to display */
  seriesUid: string;
  /** Stack index for multi-stack series */
  stackIndex?: number;
  /** Initial slice to display */
  initialSlice?: number;
  /** Callback when slice changes */
  onSliceChange?: (sliceIndex: number, total: number) => void;
  /** Compact mode for smaller display */
  compact?: boolean;
  /** Height constraint */
  maxHeight?: number;
}

export const SimpleImageViewer = ({
  seriesUid,
  stackIndex = 0,
  initialSlice,
  onSliceChange,
  compact = false,
  maxHeight = 400,
}: SimpleImageViewerProps) => {
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [instanceIds, setInstanceIds] = useState<number[]>([]);
  const [currentSlice, setCurrentSlice] = useState(0);
  const [isPlaying, setIsPlaying] = useState(false);
  const [imageUrl, setImageUrl] = useState<string | null>(null);

  const playIntervalRef = useRef<number | null>(null);

  // Fetch instance IDs
  useEffect(() => {
    const fetchInstances = async () => {
      try {
        setIsLoading(true);
        setError(null);

        const response = await fetch(`/api/qc/dicom/${seriesUid}/instances?stack_index=${stackIndex}`);
        if (!response.ok) {
          throw new Error('Failed to fetch instances');
        }

        const data = await response.json();
        const ids: number[] = data.instance_ids || [];

        if (ids.length === 0) {
          throw new Error('No instances found');
        }

        setInstanceIds(ids);
        const startSlice = initialSlice ?? Math.floor(ids.length / 2);
        setCurrentSlice(startSlice);
        setIsLoading(false);
      } catch (err) {
        console.error('[SimpleImageViewer] Failed to fetch instances:', err);
        setError(err instanceof Error ? err.message : 'Failed to load viewer');
        setIsLoading(false);
      }
    };

    fetchInstances();
  }, [seriesUid, stackIndex, initialSlice]);

  // Update image URL when slice changes
  useEffect(() => {
    if (instanceIds.length > 0 && currentSlice >= 0 && currentSlice < instanceIds.length) {
      const instanceId = instanceIds[currentSlice];
      setImageUrl(`/api/qc/dicom/image/${instanceId}`);
      onSliceChange?.(currentSlice, instanceIds.length);
    }
  }, [instanceIds, currentSlice, onSliceChange]);

  // Handle slice navigation
  const goToSlice = useCallback(
    (index: number) => {
      const clampedIndex = Math.max(0, Math.min(instanceIds.length - 1, index));
      setCurrentSlice(clampedIndex);
    },
    [instanceIds.length]
  );

  // Handle mouse wheel for slice scrolling
  const handleWheel = useCallback(
    (event: React.WheelEvent) => {
      event.preventDefault();
      const delta = event.deltaY > 0 ? 1 : -1;
      setCurrentSlice((prev) => {
        const next = Math.max(0, Math.min(instanceIds.length - 1, prev + delta));
        return next;
      });
    },
    [instanceIds.length]
  );

  // Auto-play functionality
  useEffect(() => {
    if (isPlaying && instanceIds.length > 1) {
      playIntervalRef.current = window.setInterval(() => {
        setCurrentSlice((prev) => {
          const next = prev >= instanceIds.length - 1 ? 0 : prev + 1;
          return next;
        });
      }, 150); // ~7 fps for smooth but not too fast playback
    } else {
      if (playIntervalRef.current) {
        clearInterval(playIntervalRef.current);
        playIntervalRef.current = null;
      }
    }

    return () => {
      if (playIntervalRef.current) {
        clearInterval(playIntervalRef.current);
      }
    };
  }, [isPlaying, instanceIds.length]);

  // Loading state
  if (isLoading) {
    return (
      <Box
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          height: compact ? 200 : maxHeight,
          backgroundColor: '#000',
          borderRadius: 'var(--mantine-radius-sm)',
        }}
      >
        <Loader size="lg" color="white" />
      </Box>
    );
  }

  // Error state
  if (error) {
    return (
      <Box
        style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          gap: 8,
          height: compact ? 150 : 200,
          backgroundColor: '#1a1a1a',
          borderRadius: 'var(--mantine-radius-sm)',
        }}
      >
        <IconPhotoOff size={32} color="var(--mantine-color-gray-6)" />
        <Text size="sm" c="dimmed">
          {error}
        </Text>
      </Box>
    );
  }

  return (
    <Stack gap={compact ? 'xs' : 'sm'}>
      {/* Image container */}
      <Paper
        p={0}
        radius="sm"
        onWheel={handleWheel}
        style={{
          backgroundColor: '#000',
          overflow: 'hidden',
          position: 'relative',
        }}
      >
        <Box
          style={{
            width: '100%',
            height: maxHeight,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          {imageUrl && (
            <img
              src={imageUrl}
              alt={`Slice ${currentSlice + 1} of ${instanceIds.length}`}
              style={{
                maxWidth: '100%',
                maxHeight: '100%',
                objectFit: 'contain',
              }}
            />
          )}
        </Box>

        {/* Slice info overlay */}
        <Box
          style={{
            position: 'absolute',
            top: 8,
            left: 8,
            backgroundColor: 'rgba(0,0,0,0.7)',
            padding: '4px 8px',
            borderRadius: 4,
            pointerEvents: 'none',
          }}
        >
          <Text size="xs" c="white" ff="monospace">
            {currentSlice + 1} / {instanceIds.length}
          </Text>
        </Box>

        {/* Controls hint */}
        <Box
          style={{
            position: 'absolute',
            bottom: 8,
            left: 8,
            backgroundColor: 'rgba(0,0,0,0.7)',
            padding: '4px 8px',
            borderRadius: 4,
            pointerEvents: 'none',
          }}
        >
          <Text size="xs" c="dimmed" ff="monospace">
            <IconAdjustments size={10} style={{ display: 'inline', marginRight: 4 }} />
            Scroll: slices | Slider: navigate
          </Text>
        </Box>
      </Paper>

      {/* Controls */}
      {!compact && (
        <Group justify="space-between">
          {/* Playback controls */}
          <Group gap="xs">
            <Tooltip label="First slice (Home)">
              <ActionIcon
                variant="subtle"
                onClick={() => goToSlice(0)}
                disabled={currentSlice === 0}
                size="sm"
              >
                <IconPlayerSkipBack size={16} />
              </ActionIcon>
            </Tooltip>
            <Tooltip label={isPlaying ? 'Pause (Space)' : 'Play (Space)'}>
              <ActionIcon
                variant="subtle"
                onClick={() => setIsPlaying(!isPlaying)}
                disabled={instanceIds.length <= 1}
                size="sm"
              >
                {isPlaying ? <IconPlayerPause size={16} /> : <IconPlayerPlay size={16} />}
              </ActionIcon>
            </Tooltip>
            <Tooltip label="Last slice (End)">
              <ActionIcon
                variant="subtle"
                onClick={() => goToSlice(instanceIds.length - 1)}
                disabled={currentSlice === instanceIds.length - 1}
                size="sm"
              >
                <IconPlayerSkipForward size={16} />
              </ActionIcon>
            </Tooltip>
          </Group>
        </Group>
      )}

      {/* Slice slider */}
      {instanceIds.length > 1 && (
        <Slider
          value={currentSlice}
          onChange={goToSlice}
          min={0}
          max={instanceIds.length - 1}
          step={1}
          label={(value) => `${value + 1}/${instanceIds.length}`}
          size={compact ? 'xs' : 'sm'}
          styles={{
            track: {
              backgroundColor: 'var(--mantine-color-dark-5)',
            },
          }}
        />
      )}
    </Stack>
  );
};
