import type { StageSummary } from '../../../types';

export const findSuggestedActiveIndex = (stages: StageSummary[]): number => {
  const runningIndex = stages.findIndex((stage) => ['running', 'pending', 'paused'].includes(stage.status));
  const failureIndex = stages.findIndex((stage) => stage.status === 'failed');

  if (runningIndex !== -1) return runningIndex;
  if (failureIndex !== -1) return failureIndex;

  for (let i = stages.length - 1; i >= 0; i -= 1) {
    if (stages[i].status === 'completed') {
      return i;
    }
  }

  return 0;
};
