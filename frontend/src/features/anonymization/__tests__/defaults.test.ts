import { describe, expect, it } from 'vitest';
import { buildAnonymizeConfigFromExisting, buildDefaultAnonymizeConfig } from '../../anonymization/defaults';
import { TIME_AND_DATE_CODES } from '../../anonymization/constants';

describe('buildDefaultAnonymizeConfig', () => {
  it('uses uppercase cohort name and appends placeholders when needed', () => {
    const config = buildDefaultAnonymizeConfig({ cohortName: 'Geneuro Prospective', sourcePath: '/data/gp' });
    expect(config.patientIdPrefixTemplate).toBe('GENEUROPROSPECTIVEXXXX');
    expect(config.folderFallbackTemplate).toBe('GENEUROPROSPECTIVE');
    expect(config.deterministicPattern).toBe('GENEUROPROSPECTIVEXXXX');
    expect(config.deterministicSalt).toBe('geneuro-prospective-salt');
    expect(config.sourceRoot).toBe('/data/gp');
    expect(config.metadataFilename).toBe('metadata_audit.xlsx');
    expect(config.outputFormat).toBe('encrypted_excel');
    expect(config.scrubbedTagCodes.length).toBeGreaterThan(0);
    expect(config.scrubbedTagCodes.some((code) => TIME_AND_DATE_CODES.includes(code))).toBe(false);
    expect(config.renamePatientFolders).toBe(false);
    expect(config.auditResumePerLeaf).toBe(true);
  });

  it('falls back to default prefix when cohort name missing', () => {
    const config = buildDefaultAnonymizeConfig({ cohortName: '', sourcePath: '/data' });
    expect(config.patientIdPrefixTemplate).toBe('COHORTXXXX');
    expect(config.folderFallbackTemplate).toBe('COHORT');
    expect(config.deterministicPattern).toBe('COHORTXXXX');
    expect(config.deterministicSalt).toBe('cohort-salt');
    expect(config.renamePatientFolders).toBe(false);
    expect(config.auditResumePerLeaf).toBe(true);
  });
});

describe('buildAnonymizeConfigFromExisting', () => {
  it('merges existing values with defaults', () => {
    const existing = {
      outputFormat: 'csv' as const,
      metadataFilename: 'custom.csv',
      scrubbedTagCodes: ['0010,0010'],
      auditResumePerLeaf: false,
    };
    const config = buildAnonymizeConfigFromExisting(existing, { cohortName: 'Test', sourcePath: '/data/test' });
    expect(config.metadataFilename).toBe('custom.csv');
    expect(config.outputFormat).toBe('csv');
    expect(config.scrubbedTagCodes).toContain('0010,0010');
    expect(config.auditResumePerLeaf).toBe(false);
  });

  it('normalizes filename extension to match format', () => {
    const config = buildAnonymizeConfigFromExisting(
      {
        outputFormat: 'csv',
        metadataFilename: 'report.xlsx',
      },
      { cohortName: 'Normalizer', sourcePath: '/data/norm' },
    );

    expect(config.metadataFilename).toBe('report.csv');
    expect(config.outputFormat).toBe('csv');
    expect(config.scrubbedTagCodes.length).toBeGreaterThan(0);
  });
});
