/**
 * Single shared deferred tooltip for the Cohort Main QC heatmap.
 *
 * Each cell mounts only as a plain <div> with onMouseEnter/Leave handlers
 * (zero portal/Floating UI cost per cell). The provider keeps ONE floating
 * panel rendered to document.body and shows/hides it as the user hovers.
 *
 * This replaces the per-cell Mantine <Tooltip> which was the dominant
 * render cost on cohorts with thousands of cells.
 */
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from 'react';
import { createPortal } from 'react-dom';

interface TooltipState {
  content: ReactNode;
  x: number;
  y: number;
}

interface HeatmapTooltipApi {
  show: (content: ReactNode, anchorRect: DOMRect) => void;
  hide: () => void;
}

const HeatmapTooltipContext = createContext<HeatmapTooltipApi | null>(null);

const SHOW_DELAY_MS = 250;
const TOOLTIP_OFFSET_Y = 6;

export const useHeatmapTooltip = (): HeatmapTooltipApi => {
  const ctx = useContext(HeatmapTooltipContext);
  if (!ctx) {
    // Soft fallback so cells can render even without the provider mounted.
    return { show: () => undefined, hide: () => undefined };
  }
  return ctx;
};

interface ProviderProps {
  children: ReactNode;
}

export const HeatmapTooltipProvider = ({ children }: ProviderProps) => {
  const [state, setState] = useState<TooltipState | null>(null);
  const showTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const clearShowTimer = () => {
    if (showTimerRef.current) {
      clearTimeout(showTimerRef.current);
      showTimerRef.current = null;
    }
  };

  const show = useCallback((content: ReactNode, anchorRect: DOMRect) => {
    clearShowTimer();
    showTimerRef.current = setTimeout(() => {
      // Position centered horizontally above the anchor; flip below if too high.
      const x = anchorRect.left + anchorRect.width / 2;
      const y = anchorRect.bottom + TOOLTIP_OFFSET_Y;
      setState({ content, x, y });
    }, SHOW_DELAY_MS);
  }, []);

  const hide = useCallback(() => {
    clearShowTimer();
    setState(null);
  }, []);

  // Hide on scroll / resize so the tooltip doesn't stick to the wrong place.
  useEffect(() => {
    const onScrollOrResize = () => hide();
    window.addEventListener('scroll', onScrollOrResize, true);
    window.addEventListener('resize', onScrollOrResize);
    return () => {
      window.removeEventListener('scroll', onScrollOrResize, true);
      window.removeEventListener('resize', onScrollOrResize);
    };
  }, [hide]);

  // Cleanup pending timer on unmount.
  useEffect(() => () => clearShowTimer(), []);

  const api: HeatmapTooltipApi = { show, hide };

  return (
    <HeatmapTooltipContext.Provider value={api}>
      {children}
      {state &&
        typeof document !== 'undefined' &&
        createPortal(
          <div
            role="tooltip"
            style={{
              position: 'fixed',
              left: state.x,
              top: state.y,
              transform: 'translate(-50%, 0)',
              background: 'var(--nils-bg-primary, #fff)',
              color: 'var(--nils-text-primary, #111)',
              border: '1px solid var(--nils-border, rgba(0,0,0,0.15))',
              borderRadius: 4,
              padding: '6px 8px',
              fontSize: 12,
              lineHeight: 1.3,
              maxWidth: 320,
              boxShadow: '0 6px 16px rgba(0,0,0,0.18)',
              pointerEvents: 'none',
              zIndex: 10000,
            }}
          >
            {state.content}
          </div>,
          document.body,
        )}
    </HeatmapTooltipContext.Provider>
  );
};
