/**
 * Cornerstone.js DICOM Viewer Component
 *
 * High-performance DICOM viewer using client-side rendering with Cornerstone.js.
 * Features:
 * - GPU-accelerated WebGL rendering
 * - Mouse wheel slice scrolling
 * - Window/level adjustment (left click + drag)
 * - Zoom (right click + drag)
 * - Pan (middle click + drag)
 */

import { useEffect, useRef, useState, useCallback, useMemo } from 'react';
import { Box, Group, Text, Loader, ActionIcon, Tooltip, Slider, Stack, Paper } from '@mantine/core';
import {
  IconPlayerPlay,
  IconPlayerPause,
  IconPlayerSkipBack,
  IconPlayerSkipForward,
  IconZoomIn,
  IconZoomOut,
  IconZoomReset,
  IconPhotoOff,
  IconAdjustments,
} from '@tabler/icons-react';
import {
  initCornerstone,
  createViewportToolGroup,
  destroyToolGroup,
  RenderingEngine,
  Enums,
} from '../utils/cornerstoneInit';
import type { Types } from '@cornerstonejs/core';
import { imageLoader } from '@cornerstonejs/core';

interface CornerstoneViewerProps {
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
  maxHeight?: number | string;
}

export const CornerstoneViewer = ({
  seriesUid,
  stackIndex = 0,
  initialSlice,
  onSliceChange,
  compact = false,
  maxHeight = 400,
}: CornerstoneViewerProps) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const renderingEngineRef = useRef<RenderingEngine | null>(null);
  const imageIdsRef = useRef<string[]>([]);

  // Use stable viewport ID based on series/stack (enables effective cache reuse)
  const viewportId = useMemo(
    () => `viewport-${seriesUid}-${stackIndex}`,
    [seriesUid, stackIndex]
  );
  const toolGroupId = useMemo(() => `toolGroup-${viewportId}`, [viewportId]);
  const renderingEngineId = useMemo(() => `renderingEngine-${viewportId}`, [viewportId]);

  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [currentSlice, setCurrentSlice] = useState(0);
  const [totalSlices, setTotalSlices] = useState(0);
  const [isPlaying, setIsPlaying] = useState(false);
  const [zoom, setZoom] = useState(1);

  const playIntervalRef = useRef<number | null>(null);

  // Build image IDs from the DICOM file endpoint
  const buildImageIds = useCallback(async () => {
    try {
      // Fetch instance list from our API
      const response = await fetch(`/api/qc/dicom/${seriesUid}/instances?stack_index=${stackIndex}`);
      if (!response.ok) {
        throw new Error('Failed to fetch instances');
      }

      const data = await response.json();
      const instanceIds: number[] = data.instance_ids || [];

      if (instanceIds.length === 0) {
        throw new Error('No instances found');
      }

      // Build Cornerstone image IDs using our raw DICOM endpoint
      // Format: wadouri:/api/qc/dicom/file/{instance_id}
      const ids = instanceIds.map(
        (id) => `wadouri:${window.location.origin}/api/qc/dicom/file/${id}`
      );

      return ids;
    } catch (err) {
      console.error('[CornerstoneViewer] Failed to build image IDs:', err);
      throw err;
    }
  }, [seriesUid, stackIndex]);

  // Prefetch adjacent slices for smooth scrolling
  const prefetchAdjacentSlices = useCallback((currentIdx: number, allIds: string[]) => {
    if (!allIds.length) return;

    const prefetchRange = 5; // Prefetch ±5 slices
    for (let offset = -prefetchRange; offset <= prefetchRange; offset++) {
      const targetIdx = currentIdx + offset;
      if (targetIdx >= 0 && targetIdx < allIds.length && offset !== 0) {
        // Silent prefetch - don't await, just trigger loading
        imageLoader.loadImage(allIds[targetIdx]).catch(() => {
          // Ignore prefetch errors - they're non-critical
        });
      }
    }
  }, []);

  // Initialize Cornerstone and set up viewport
  useEffect(() => {
    let mounted = true;

    const setup = async () => {
      if (!containerRef.current) return;

      try {
        setIsLoading(true);
        setError(null);

        // Initialize Cornerstone
        await initCornerstone();

        // Build image IDs
        const ids = await buildImageIds();
        if (!mounted) return;

        setTotalSlices(ids.length);

        // Create rendering engine
        const renderingEngine = new RenderingEngine(renderingEngineId);
        renderingEngineRef.current = renderingEngine;

        // Enable element for stack viewport
        renderingEngine.enableElement({
          viewportId: viewportId,
          element: containerRef.current,
          type: Enums.ViewportType.STACK,
        });

        // Set up tools
        createViewportToolGroup(toolGroupId, viewportId, renderingEngineId);

        // Get viewport and load images
        const viewport = renderingEngine.getViewport(viewportId) as Types.IStackViewport;

        // Determine initial slice (middle if not specified)
        const startSlice = initialSlice ?? Math.floor(ids.length / 2);
        setCurrentSlice(startSlice);

        // Set the stack with timeout
        const stackPromise = viewport.setStack(ids, startSlice);
        const timeoutPromise = new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error('Image loading timeout after 30s')), 30000)
        );

        await Promise.race([stackPromise, timeoutPromise]);
        viewport.render();

        // Store image IDs for prefetching
        imageIdsRef.current = ids;

        // Prefetch adjacent slices for smooth scrolling
        prefetchAdjacentSlices(startSlice, ids);

        // Listen for stack scroll events
        const element = containerRef.current;
        element.addEventListener('CORNERSTONE_STACK_NEW_IMAGE', ((event: CustomEvent) => {
          if (!mounted) return;
          const newIndex = event.detail?.imageIdIndex ?? 0;
          setCurrentSlice(newIndex);
          onSliceChange?.(newIndex, ids.length);

          // Prefetch slices around the new position
          prefetchAdjacentSlices(newIndex, imageIdsRef.current);
        }) as EventListener);

        setIsLoading(false);
      } catch (err) {
        if (!mounted) return;
        console.error('[CornerstoneViewer] Setup failed:', err);
        setError(err instanceof Error ? err.message : 'Failed to load viewer');
        setIsLoading(false);
      }
    };

    setup();

    return () => {
      mounted = false;

      // Clean up
      if (playIntervalRef.current) {
        clearInterval(playIntervalRef.current);
      }

      destroyToolGroup(toolGroupId);

      if (renderingEngineRef.current) {
        renderingEngineRef.current.destroy();
        renderingEngineRef.current = null;
      }
    };
  }, [seriesUid, stackIndex, initialSlice, buildImageIds, prefetchAdjacentSlices, onSliceChange, viewportId, toolGroupId, renderingEngineId]);

  // Handle slice navigation
  const goToSlice = useCallback(
    (index: number) => {
      if (!renderingEngineRef.current) return;

      const clampedIndex = Math.max(0, Math.min(totalSlices - 1, index));
      const viewport = renderingEngineRef.current.getViewport(viewportId) as Types.IStackViewport;

      if (viewport) {
        viewport.setImageIdIndex(clampedIndex);
        setCurrentSlice(clampedIndex);
        onSliceChange?.(clampedIndex, totalSlices);
        // Prefetch around new position
        prefetchAdjacentSlices(clampedIndex, imageIdsRef.current);
      }
    },
    [totalSlices, onSliceChange, viewportId, prefetchAdjacentSlices]
  );

  // Auto-play functionality
  useEffect(() => {
    if (isPlaying && totalSlices > 1) {
      playIntervalRef.current = window.setInterval(() => {
        setCurrentSlice((prev) => {
          const next = prev >= totalSlices - 1 ? 0 : prev + 1;
          goToSlice(next);
          return next;
        });
      }, 100); // 10 fps
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
  }, [isPlaying, totalSlices, goToSlice]);

  // Zoom controls
  const handleZoomIn = useCallback(() => {
    if (!renderingEngineRef.current) return;
    const viewport = renderingEngineRef.current.getViewport(viewportId) as Types.IStackViewport;
    if (viewport) {
      const currentZoom = viewport.getZoom();
      viewport.setZoom(Math.min(5, currentZoom * 1.25));
      setZoom(viewport.getZoom());
      viewport.render();
    }
  }, [viewportId]);

  const handleZoomOut = useCallback(() => {
    if (!renderingEngineRef.current) return;
    const viewport = renderingEngineRef.current.getViewport(viewportId) as Types.IStackViewport;
    if (viewport) {
      const currentZoom = viewport.getZoom();
      viewport.setZoom(Math.max(0.25, currentZoom / 1.25));
      setZoom(viewport.getZoom());
      viewport.render();
    }
  }, [viewportId]);

  const handleZoomReset = useCallback(() => {
    if (!renderingEngineRef.current) return;
    const viewport = renderingEngineRef.current.getViewport(viewportId) as Types.IStackViewport;
    if (viewport) {
      viewport.resetCamera();
      setZoom(1);
      viewport.render();
    }
  }, [viewportId]);

  // Error state - only show if not loading (error during load shows loading spinner)
  if (error && !isLoading) {
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
    <Stack gap={compact ? 'xs' : 'sm'} style={{ height: maxHeight === '100%' ? '100%' : 'auto' }}>
      {/* Cornerstone viewport container - ALWAYS render so ref is available */}
      <Paper
        p={0}
        radius="sm"
        style={{
          backgroundColor: '#000',
          overflow: 'hidden',
          position: 'relative',
          flex: maxHeight === '100%' ? 1 : undefined,
          height: maxHeight === '100%' ? undefined : maxHeight,
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        {/* The actual viewport element - must always exist for Cornerstone */}
        <Box
          ref={containerRef}
          tabIndex={0}
          style={{
            width: '100%',
            height: '100%',
            flex: 1,
            outline: 'none',
          }}
        />

        {/* Loading overlay */}
        {isLoading && (
          <Box
            style={{
              position: 'absolute',
              top: 0,
              left: 0,
              right: 0,
              bottom: 0,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              backgroundColor: '#000',
            }}
          >
            <Loader size="lg" color="white" />
          </Box>
        )}

        {/* Slice info overlay - high z-index to appear above HUDs */}
        <Box
          style={{
            position: 'absolute',
            top: 8,
            left: '50%',
            transform: 'translateX(-50%)',
            pointerEvents: 'none',
            zIndex: 15,
            fontFamily: '"JetBrains Mono", "Fira Code", "SF Mono", "Consolas", monospace',
          }}
        >
          <Text size="14px" c="#00ccff" fw={700} style={{ textShadow: '0 1px 3px rgba(0,0,0,0.9), 0 0 12px rgba(0,204,255,0.4)', letterSpacing: '0.05em' }}>
            {currentSlice + 1} / {totalSlices}
          </Text>
        </Box>

        {/* Zoom indicator */}
        {Math.abs(zoom - 1) > 0.01 && (
          <Box
            style={{
              position: 'absolute',
              top: 8,
              right: 8,
              backgroundColor: 'rgba(0,0,0,0.7)',
              padding: '4px 8px',
              borderRadius: 4,
              pointerEvents: 'none',
            }}
          >
            <Text size="xs" c="white" ff="monospace">
              {Math.round(zoom * 100)}%
            </Text>
          </Box>
        )}

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
            <IconAdjustments size={10} style={{ display: 'inline', marginRight: 4, verticalAlign: 'middle' }} />
            Scroll: slices | L-click: W/L | R-click: zoom
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
                disabled={totalSlices <= 1}
                size="sm"
              >
                {isPlaying ? <IconPlayerPause size={16} /> : <IconPlayerPlay size={16} />}
              </ActionIcon>
            </Tooltip>
            <Tooltip label="Last slice (End)">
              <ActionIcon
                variant="subtle"
                onClick={() => goToSlice(totalSlices - 1)}
                disabled={currentSlice === totalSlices - 1}
                size="sm"
              >
                <IconPlayerSkipForward size={16} />
              </ActionIcon>
            </Tooltip>
          </Group>

          {/* Zoom controls */}
          <Group gap="xs">
            <Tooltip label="Zoom out">
              <ActionIcon variant="subtle" onClick={handleZoomOut} size="sm">
                <IconZoomOut size={16} />
              </ActionIcon>
            </Tooltip>
            <Tooltip label="Reset zoom">
              <ActionIcon
                variant="subtle"
                onClick={handleZoomReset}
                disabled={Math.abs(zoom - 1) < 0.01}
                size="sm"
              >
                <IconZoomReset size={16} />
              </ActionIcon>
            </Tooltip>
            <Tooltip label="Zoom in">
              <ActionIcon variant="subtle" onClick={handleZoomIn} size="sm">
                <IconZoomIn size={16} />
              </ActionIcon>
            </Tooltip>
          </Group>
        </Group>
      )}

      {/* Slice slider */}
      {totalSlices > 1 && (
        <Slider
          value={currentSlice}
          onChange={goToSlice}
          min={0}
          max={totalSlices - 1}
          step={1}
          label={(value) => `${value + 1}/${totalSlices}`}
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
