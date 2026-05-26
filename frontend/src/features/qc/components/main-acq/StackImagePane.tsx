/**
 * Image-only "pane" for a single stack.
 *
 * Extracted from BundleStackTile so it can be reused by the cohort Main QC
 * Session Pick modal (and any future viewer that wants the same wheel-scroll
 * + ±5 prefetch UX without the MAIN/PRE/POST buttons).
 *
 * Props are deliberately minimal: just the addressing info plus an optional
 * shared progress (for synchronised scrolling across tiles in a bundle).
 * Each consumer can keep its own progress state if it doesn't need sync.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Box, Loader, Text } from '@mantine/core';

interface FrameEntry {
  instance_id: number;
  frame: number;
}

export interface StackImagePaneProps {
  seriesInstanceUid: string;
  stackIndex: number;
  /** 0..1; the displayed slice = round(progress * (frames-1)). */
  progress: number;
  /** Called when the user wheel-scrolls. Throttled to RAF. */
  onProgressChange: (next: number) => void;
  /** CSS aspect-ratio of the container. Defaults to 1/1. */
  aspectRatio?: string;
  /** Optional small overlay text shown top-right (e.g. acquisition time). */
  topRightOverlay?: string | null;
  /** alt-text base; the slice index is appended. */
  altLabel?: string;
}

const buildImageUrl = (entry: FrameEntry, isMultiFrame: boolean): string => {
  const base = `/api/qc/dicom/image/${entry.instance_id}`;
  return isMultiFrame ? `${base}?frame=${entry.frame}` : base;
};

const prefetchAround = (
  frames: FrameEntry[],
  center: number,
  isMultiFrame: boolean,
  range = 5,
) => {
  for (let offset = -range; offset <= range; offset++) {
    const idx = center + offset;
    if (idx >= 0 && idx < frames.length && offset !== 0) {
      const img = new Image();
      img.src = buildImageUrl(frames[idx], isMultiFrame);
    }
  }
};

export const StackImagePane = ({
  seriesInstanceUid,
  stackIndex,
  progress,
  onProgressChange,
  aspectRatio = '1 / 1',
  topRightOverlay,
  altLabel,
}: StackImagePaneProps) => {
  const [frames, setFrames] = useState<FrameEntry[]>([]);
  const [isMultiFrame, setIsMultiFrame] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const rafRef = useRef<number | null>(null);

  // Fetch the frame list (handles classic + multi-frame Enhanced DICOM).
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetch(
      `/api/qc/dicom/${seriesInstanceUid}/instances?stack_index=${stackIndex}`,
    )
      .then((r) =>
        r.ok ? r.json() : Promise.reject(new Error('Failed to load instances')),
      )
      .then((data) => {
        if (cancelled) return;
        const framesArr: FrameEntry[] = data.frames ?? [];
        const instanceIds: number[] = data.instance_ids ?? [];
        if (framesArr.length > 0) {
          const uniqueIds = new Set(framesArr.map((f) => f.instance_id)).size;
          setIsMultiFrame(framesArr.length > uniqueIds);
          setFrames(framesArr);
        } else {
          setIsMultiFrame(false);
          setFrames(instanceIds.map((id: number) => ({ instance_id: id, frame: 0 })));
        }
        setLoading(false);
      })
      .catch((e) => {
        if (cancelled) return;
        setError(e?.message || 'Failed to load');
        setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [seriesInstanceUid, stackIndex]);

  // Prefetch around middle slice on initial load.
  useEffect(() => {
    if (frames.length > 1) {
      const midIdx = Math.round(0.5 * (frames.length - 1));
      prefetchAround(frames, midIdx, isMultiFrame);
    }
  }, [frames, isMultiFrame]);

  const total = frames.length;
  const sliceIdx =
    total > 0
      ? Math.min(total - 1, Math.max(0, Math.round(progress * (total - 1))))
      : 0;

  const imageUrl = useMemo(
    () => (total > 0 ? buildImageUrl(frames[sliceIdx], isMultiFrame) : null),
    [frames, sliceIdx, isMultiFrame, total],
  );

  const throttledProgressChange = useCallback(
    (nextProgress: number) => {
      if (rafRef.current !== null) return;
      rafRef.current = requestAnimationFrame(() => {
        onProgressChange(nextProgress);
        rafRef.current = null;
      });
    },
    [onProgressChange],
  );

  useEffect(
    () => () => {
      if (rafRef.current !== null) cancelAnimationFrame(rafRef.current);
    },
    [],
  );

  // Native non-passive wheel listener so preventDefault() works.
  useEffect(() => {
    const el = containerRef.current;
    if (!el || total <= 1) return;

    const handler = (event: WheelEvent) => {
      event.preventDefault();
      event.stopPropagation();
      const delta = event.deltaY > 0 ? 1 : -1;
      const nextSlice = Math.min(total - 1, Math.max(0, sliceIdx + delta));
      const nextProgress = nextSlice / (total - 1 || 1);
      throttledProgressChange(nextProgress);
      prefetchAround(frames, nextSlice, isMultiFrame);
    };

    el.addEventListener('wheel', handler, { passive: false });
    return () => el.removeEventListener('wheel', handler);
  }, [total, sliceIdx, frames, isMultiFrame, throttledProgressChange]);

  return (
    <Box
      ref={containerRef}
      style={{
        backgroundColor: '#000',
        borderRadius: 4,
        overflow: 'hidden',
        position: 'relative',
        aspectRatio,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
      }}
    >
      {loading && <Loader size="sm" color="white" />}
      {error && (
        <Text size="xs" c="red.4">
          {error}
        </Text>
      )}
      {!loading && !error && imageUrl && (
        <img
          src={imageUrl}
          alt={`${altLabel ?? 'stack'} slice ${sliceIdx + 1}`}
          style={{ maxWidth: '100%', maxHeight: '100%', objectFit: 'contain' }}
          draggable={false}
        />
      )}
      {!loading && total > 0 && (
        <Box
          style={{
            position: 'absolute',
            top: 4,
            left: 6,
            backgroundColor: 'rgba(0,0,0,0.55)',
            padding: '1px 6px',
            borderRadius: 3,
            pointerEvents: 'none',
          }}
        >
          <Text size="xs" c="white" ff="monospace">
            {sliceIdx + 1}/{total}
          </Text>
        </Box>
      )}
      {topRightOverlay && (
        <Box
          style={{
            position: 'absolute',
            top: 4,
            right: 6,
            backgroundColor: 'rgba(0,0,0,0.55)',
            padding: '1px 6px',
            borderRadius: 3,
            pointerEvents: 'none',
          }}
        >
          <Text size="xs" c="dimmed" ff="monospace">
            {topRightOverlay}
          </Text>
        </Box>
      )}
    </Box>
  );
};
