/**
 * React hook for connecting to the sorting pipeline SSE stream.
 */

import { useCallback, useEffect, useRef, useState } from 'react';
import type {
  StepState,
  SSEStepStartEvent,
  SSEStepProgressEvent,
  SSEStepCompleteEvent,
  SSEStepErrorEvent,
  SSEPipelineCompleteEvent,
  SSEPipelineErrorEvent,
} from '../types';

export interface UseSortingStreamResult {
  /** Current state of each step */
  steps: Record<string, StepState>;
  /** Currently running step ID */
  currentStep: string | null;
  /** Whether the pipeline has completed */
  isComplete: boolean;
  /** Whether there was an error */
  hasError: boolean;
  /** Error message if any */
  errorMessage: string | null;
  /** Pipeline summary when complete */
  summary: SSEPipelineCompleteEvent['summary'] | null;
  /** Whether we're connected to the stream */
  isConnected: boolean;
  /** Disconnect from the stream */
  disconnect: () => void;
}

/**
 * Hook to connect to the sorting pipeline SSE stream.
 *
 * @param streamUrl - The SSE endpoint URL (e.g., /api/cohorts/1/stages/sort/stream/42)
 * @param enabled - Whether to connect to the stream
 */
export const useSortingStream = (
  streamUrl: string | null,
  enabled: boolean = true,
): UseSortingStreamResult => {
  const [steps, setSteps] = useState<Record<string, StepState>>({});
  const [currentStep, setCurrentStep] = useState<string | null>(null);
  const [isComplete, setIsComplete] = useState(false);
  const [hasError, setHasError] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [summary, setSummary] = useState<SSEPipelineCompleteEvent['summary'] | null>(null);
  const [isConnected, setIsConnected] = useState(false);

  const eventSourceRef = useRef<EventSource | null>(null);

  const disconnect = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
      setIsConnected(false);
    }
  }, []);

  useEffect(() => {
    if (!streamUrl || !enabled) {
      disconnect();
      return;
    }

    // Reset state when connecting
    setSteps({});
    setCurrentStep(null);
    setIsComplete(false);
    setHasError(false);
    setErrorMessage(null);
    setSummary(null);

    const eventSource = new EventSource(streamUrl);
    eventSourceRef.current = eventSource;

    eventSource.onopen = () => {
      console.log('[SSE] Connected to:', streamUrl);
      setIsConnected(true);
    };

    eventSource.onerror = (error) => {
      console.error('SSE connection error:', error);
      setHasError(true);
      setErrorMessage('Connection to server lost');
      setIsConnected(false);
      eventSource.close();
    };

    // Handle step_start event
    eventSource.addEventListener('step_start', (event) => {
      try {
        const data: SSEStepStartEvent = JSON.parse(event.data);
        console.log('[SSE] step_start:', data.step_id, data.step_title);
        setCurrentStep(data.step_id);
        setSteps((prev) => ({
          ...prev,
          [data.step_id]: {
            status: 'running',
            progress: 0,
            message: `Starting ${data.step_title}...`,
          },
        }));
      } catch (e) {
        console.error('Failed to parse step_start event:', e);
      }
    });

    // Handle step_progress event
    eventSource.addEventListener('step_progress', (event) => {
      try {
        const data: SSEStepProgressEvent = JSON.parse(event.data);
        console.log('[SSE] step_progress:', data.step_id, data.progress, data.metrics);
        setSteps((prev) => ({
          ...prev,
          [data.step_id]: {
            ...prev[data.step_id],
            status: 'running',
            progress: data.progress,
            message: data.message || data.current_action || prev[data.step_id]?.message,
            metrics: data.metrics || prev[data.step_id]?.metrics,
          },
        }));
      } catch (e) {
        console.error('Failed to parse step_progress event:', e);
      }
    });

    // Handle step_complete event
    eventSource.addEventListener('step_complete', (event) => {
      try {
        const data: SSEStepCompleteEvent = JSON.parse(event.data);
        setSteps((prev) => ({
          ...prev,
          [data.step_id]: {
            status: 'complete',
            progress: 100,
            message: 'Complete',
            metrics: data.metrics || prev[data.step_id]?.metrics,
          },
        }));
      } catch (e) {
        console.error('Failed to parse step_complete event:', e);
      }
    });

    // Handle step_error event
    eventSource.addEventListener('step_error', (event) => {
      try {
        const data: SSEStepErrorEvent = JSON.parse(event.data);
        setSteps((prev) => ({
          ...prev,
          [data.step_id]: {
            status: 'error',
            progress: prev[data.step_id]?.progress || 0,
            message: `Error: ${data.error}`,
            error: data.error,
            metrics: data.metrics || prev[data.step_id]?.metrics,
          },
        }));
        setHasError(true);
        setErrorMessage(data.error);
      } catch (e) {
        console.error('Failed to parse step_error event:', e);
      }
    });

    // Handle pipeline_complete event
    eventSource.addEventListener('pipeline_complete', (event) => {
      try {
        const data: SSEPipelineCompleteEvent = JSON.parse(event.data);
        setIsComplete(true);
        setSummary(data.summary || null);
        setCurrentStep(null);
        eventSource.close();
        setIsConnected(false);
      } catch (e) {
        console.error('Failed to parse pipeline_complete event:', e);
      }
    });

    // Handle pipeline_error event
    eventSource.addEventListener('pipeline_error', (event) => {
      try {
        const data: SSEPipelineErrorEvent = JSON.parse(event.data);
        setHasError(true);
        setErrorMessage(data.error);
        setCurrentStep(null);
        eventSource.close();
        setIsConnected(false);
      } catch (e) {
        console.error('Failed to parse pipeline_error event:', e);
      }
    });

    // Handle pipeline_cancelled event
    eventSource.addEventListener('pipeline_cancelled', () => {
      setHasError(true);
      setErrorMessage('Pipeline was cancelled');
      setCurrentStep(null);
      eventSource.close();
      setIsConnected(false);
    });

    return () => {
      eventSource.close();
      setIsConnected(false);
    };
  }, [streamUrl, enabled, disconnect]);

  return {
    steps,
    currentStep,
    isComplete,
    hasError,
    errorMessage,
    summary,
    isConnected,
    disconnect,
  };
};

export default useSortingStream;
