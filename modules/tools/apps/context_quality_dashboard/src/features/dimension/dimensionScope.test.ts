import { describe, expect, it } from 'vitest';

import {
  decodeDimensionsScopeValue,
  dimensionsScopeSearchValue,
  encodeDimensionsScopeValue,
} from './dimensionScope';

describe(encodeDimensionsScopeValue.name, () => {
  it('should round-trip built-in and configured scope values', () => {
    const builtin = encodeDimensionsScopeValue({ kind: 'builtin', dimensionId: 'asset-hierarchy' });
    expect(decodeDimensionsScopeValue(builtin)).toEqual({
      kind: 'builtin',
      dimensionId: 'asset-hierarchy',
    });

    const groupId = 'cdf_cdm:CogniteCore:v1/CogniteAsset';
    const configured = encodeDimensionsScopeValue({
      kind: 'configured',
      groupId,
      modelId: 'cdf_cdm:CogniteCore:v1',
    });
    expect(decodeDimensionsScopeValue(configured, 'cdf_cdm:CogniteCore:v1')).toEqual({
      kind: 'configured',
      groupId,
      modelId: 'cdf_cdm:CogniteCore:v1',
    });
  });
});

describe(dimensionsScopeSearchValue.name, () => {
  it('should combine labels for cmdk filtering', () => {
    expect(
      dimensionsScopeSearchValue({
        modelLabel: 'CogniteCore',
        viewLabel: 'CogniteAsset',
      })
    ).toBe('cognitecore cogniteasset');
  });
});
