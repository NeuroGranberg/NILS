import { describe, expect, it } from 'vitest';
import { buildDefaultExtractConfig } from '../defaults';

describe('buildDefaultExtractConfig', () => {
  it('returns expected baseline values', () => {
    const config = buildDefaultExtractConfig();

    expect(config.extensionMode).toBe('all');
    expect(config.maxWorkers).toBe(4);
    expect(config.batchSize).toBe(100);
    expect(config.queueSize).toBe(10);
    expect(config.seriesWorkersPerSubject).toBe(1);
    expect(config.duplicatePolicy).toBe('skip');
    expect(config.resume).toBe(true);
    expect(config.resumeByPath).toBe(true);
    expect(config.subjectIdTypeId).toBeNull();
    expect(config.subjectCodeCsv).toBeNull();
    expect(config.subjectCodeSeed).toBe('');
    expect(config.adaptiveBatchingEnabled).toBe(false);
    expect(config.adaptiveTargetTxMs).toBe(200);
    expect(config.adaptiveMinBatchSize).toBe(50);
    expect(config.adaptiveMaxBatchSize).toBe(1000);
  });
});
