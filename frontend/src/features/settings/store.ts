import { create } from 'zustand';
import { persist } from 'zustand/middleware';

export type ThemeOption = 'system' | 'light' | 'dark';

interface AppSettings {
  dataRoot: string;
  outputRoot: string;
  enableGpu: boolean;
  defaultConcurrency: number;
  theme: ThemeOption;
  llmProvider: 'none' | 'openai' | 'ollama';
}

interface AppSettingsStore {
  settings: AppSettings;
  updateSetting: <K extends keyof AppSettings>(key: K, value: AppSettings[K]) => void;
}

const DEFAULT_SETTINGS: AppSettings = {
  dataRoot: '/data',
  outputRoot: '/outputs',
  enableGpu: false,
  defaultConcurrency: 4,
  theme: 'system',
  llmProvider: 'none',
};

export const useAppSettingsStore = create<AppSettingsStore>()(
  persist(
    (set) => ({
      settings: DEFAULT_SETTINGS,
      updateSetting: (key, value) =>
        set((state) => ({
          settings: {
            ...state.settings,
            [key]: value,
          },
        })),
    }),
    {
      name: 'neuro-toolkit-settings',
    },
  ),
);
