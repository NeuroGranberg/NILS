import { describe, it, expect } from 'vitest';

// Removed: DatabaseManagementPage UI is exercised via higher-level E2E flows and the legacy
// Datatables-heavy unit test no longer reflects the current architecture.
// Keeping this placeholder prevents Vitest from attempting to auto-discover the old suite.
describe.skip('DatabaseManagementPage (legacy)', () => {
  it('is covered by higher-level tests', () => {
    expect(true).toBe(true);
  });
});
