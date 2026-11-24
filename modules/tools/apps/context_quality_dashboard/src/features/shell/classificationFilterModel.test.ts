import { describe, expect, it } from 'vitest';

import {
  type Classified,
  classificationKindLabel,
  classificationKinds,
  classificationValues,
  matchesClassification,
} from './classificationFilterModel';

const ITEMS: Classified[] = [
  { classes: { assetType: 'Pump', assetClass: 'Rotating' } },
  { classes: { assetType: 'Pump' } },
  { classes: { assetType: 'Valve', fileCategory: 'Pid' } },
  { classes: {} },
];

describe(classificationKinds.name, () => {
  it('lists the kinds any item carries, in overview order', () => {
    expect(classificationKinds(ITEMS)).toEqual(['assetClass', 'assetType', 'fileCategory']);
  });

  it('returns nothing when no item is classified', () => {
    expect(classificationKinds([{ classes: {} }])).toEqual([]);
  });

  it('places unknown kinds after the known ones', () => {
    expect(classificationKinds([{ classes: { zone: 'A', assetType: 'Pump' } }])).toEqual(['assetType', 'zone']);
  });
});

describe(classificationValues.name, () => {
  it('counts items per value, largest first', () => {
    expect(classificationValues(ITEMS, 'assetType')).toEqual([
      { value: 'Pump', count: 2 },
      { value: 'Valve', count: 1 },
    ]);
  });

  it('ignores items without that kind', () => {
    expect(classificationValues(ITEMS, 'fileCategory')).toEqual([{ value: 'Pid', count: 1 }]);
  });
});

describe(matchesClassification.name, () => {
  it('matches everything until both kind and value are chosen', () => {
    expect(matchesClassification({ classes: {} }, {})).toBe(true);
    expect(matchesClassification({ classes: {} }, { kind: 'assetType' })).toBe(true);
  });

  it('keeps only items with the chosen value', () => {
    const filter = { kind: 'assetType', value: 'Pump' };

    expect(matchesClassification({ classes: { assetType: 'Pump' } }, filter)).toBe(true);
    expect(matchesClassification({ classes: { assetType: 'Valve' } }, filter)).toBe(false);
    expect(matchesClassification({ classes: {} }, filter)).toBe(false);
  });
});

describe(classificationKindLabel.name, () => {
  it('uses the translation when one exists', () => {
    const t = (key: string) => (key === 'overview.classification.kind.assetType' ? 'Asset type' : key);

    expect(classificationKindLabel('assetType', t)).toBe('Asset type');
  });

  it('falls back to the kind written as words', () => {
    expect(classificationKindLabel('siteZone', (key: string) => key)).toBe('Site zone');
  });
});
