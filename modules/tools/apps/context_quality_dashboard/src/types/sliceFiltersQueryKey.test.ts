import { describe, expect, it } from 'vitest';

import { sliceFiltersQueryKey } from './sliceFiltersQueryKey';

describe(sliceFiltersQueryKey.name, () => {
  it('serializes active filter values in a stable order', () => {
    expect(
      sliceFiltersQueryKey({
        facilityId: 'demo-north',
        divisionId: 'prod-a',
      })
    ).toEqual(['demo-north', null, 'prod-a', null, null]);
  });

  it('includes the facilities of a chosen location, in a stable order', () => {
    expect(sliceFiltersQueryKey({ facilityIds: ['b', 'a'] })).toEqual([null, 'a,b', null, null, null]);
  });

  it('uses null for unset filters', () => {
    expect(sliceFiltersQueryKey({})).toEqual([null, null, null, null, null]);
  });
});
