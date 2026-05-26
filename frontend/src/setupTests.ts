import '@testing-library/jest-dom/vitest';

if (!globalThis.window.matchMedia) {
  globalThis.window.matchMedia = (query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => false,
  });
}

// jsdom doesn't ship ResizeObserver; Mantine's ScrollArea / Modal need it.
if (!globalThis.ResizeObserver) {
  // @ts-expect-error -- polyfill
  globalThis.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
}
