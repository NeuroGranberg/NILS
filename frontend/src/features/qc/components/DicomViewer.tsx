/**
 * DICOM Viewer Component - displays DICOM slices using Cornerstone.js.
 *
 * High-performance GPU-accelerated viewer with:
 * - Client-side DICOM rendering (no server PNG conversion)
 * - Mouse wheel slice scrolling
 * - Window/Level adjustment (left click + drag)
 * - Zoom (right click + drag)
 * - Pan (middle click + drag)
 *
 * Falls back to SimpleImageViewer (server-side PNG) if Cornerstone fails.
 */

import { CornerstoneViewer } from './CornerstoneViewer';

interface DicomViewerProps {
  seriesUid: string;
  stackIndex?: number;
  initialSlice?: number;
  onSliceChange?: (sliceIndex: number, total: number) => void;
  /** Compact mode for smaller display */
  compact?: boolean;
  /** Height constraint */
  maxHeight?: number | string;
}

/**
 * DicomViewer - Cornerstone.js-based DICOM viewer.
 *
 * Uses client-side rendering with GPU acceleration for high performance.
 */
export const DicomViewer = ({
  seriesUid,
  stackIndex = 0,
  initialSlice,
  onSliceChange,
  compact = false,
  maxHeight = 400,
}: DicomViewerProps) => {
  return (
    <CornerstoneViewer
      seriesUid={seriesUid}
      stackIndex={stackIndex}
      initialSlice={initialSlice}
      onSliceChange={onSliceChange}
      compact={compact}
      maxHeight={maxHeight}
    />
  );
};
