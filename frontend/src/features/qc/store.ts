/**
 * Zustand store for QC Pipeline local state.
 */

import { create } from 'zustand';

interface QCStore {
  // Selected state
  selectedCohortId: number | null;

  // Actions
  setSelectedCohort: (cohortId: number | null) => void;
  reset: () => void;
}

const initialState = {
  selectedCohortId: null,
};

export const useQCStore = create<QCStore>((set) => ({
  ...initialState,

  setSelectedCohort: (cohortId) =>
    set({
      selectedCohortId: cohortId,
    }),

  reset: () => set(initialState),
}));
