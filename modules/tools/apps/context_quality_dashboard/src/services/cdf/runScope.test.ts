import { describe, expect, it } from 'vitest';

import { ALL_DIMENSION_IDS } from '../../domain/dimensions';

import { coversDimension, parseRunCoverage } from './runScope';

describe(parseRunCoverage.name, () => {
  it('reads a marker without a scope as a run of every dimension and no configured metrics', () => {
    // Arrange / Act
    const coverage = parseRunCoverage(null);

    // Assert
    expect([...coverage.dimensions].sort()).toEqual([...ALL_DIMENSION_IDS].sort());
    expect(coverage.configured).toBe(false);
    expect(parseRunCoverage('  ')).toEqual(coverage);
  });

  it('reads all and all+configured', () => {
    expect(parseRunCoverage('all').dimensions.size).toBe(ALL_DIMENSION_IDS.length);
    expect(parseRunCoverage('all').configured).toBe(false);
    expect(parseRunCoverage('all+configured').dimensions.size).toBe(ALL_DIMENSION_IDS.length);
    expect(parseRunCoverage('all+configured').configured).toBe(true);
  });

  it('reads a list of dimensions, with or without the configured metrics', () => {
    expect([...parseRunCoverage('time-series,files').dimensions].sort()).toEqual(['files', 'time-series']);
    expect(parseRunCoverage('time-series,files').configured).toBe(false);
    expect(parseRunCoverage('files+configured').configured).toBe(true);
  });

  it('reads configured as a run of no dimension', () => {
    const coverage = parseRunCoverage('configured');

    expect(coverage.dimensions.size).toBe(0);
    expect(coverage.configured).toBe(true);
  });

  it('never reads a scope it does not know as a full run', () => {
    expect(parseRunCoverage('everything-else').dimensions.size).toBe(0);
    expect(parseRunCoverage('files,nope').dimensions.has('files')).toBe(true);
    expect(parseRunCoverage('files,nope').dimensions.size).toBe(1);
  });
});

describe(coversDimension.name, () => {
  it('says whether a run measured a dimension', () => {
    const coverage = parseRunCoverage('files');

    expect(coversDimension(coverage, 'files')).toBe(true);
    expect(coversDimension(coverage, 'asset-hierarchy')).toBe(false);
  });
});
