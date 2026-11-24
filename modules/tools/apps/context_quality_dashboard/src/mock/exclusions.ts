import type { ExclusionBucket, ExclusionRule } from '../types';

export const SYSTEM_EXCLUSION_BUCKETS: readonly ExclusionBucket[] = [
  {
    kind: 'missing-upstream-target',
    labelKey: 'exclusion.missingUpstreamTarget',
    count: 3,
    source: 'system',
  },
  {
    kind: 'invalid-ocr',
    labelKey: 'exclusion.invalidOcr',
    count: 2,
    source: 'system',
  },
  {
    kind: 'synthetic-timeseries',
    labelKey: 'exclusion.syntheticTimeseries',
    count: 1,
    source: 'system',
  },
  {
    kind: 'historical-retired',
    labelKey: 'exclusion.historicalRetired',
    count: 1,
    source: 'system',
  },
  {
    kind: 'location-centric-files',
    labelKey: 'exclusion.locationCentricFiles',
    count: 1,
    source: 'system',
  },
];

export const DEFAULT_EXCLUSION_RULES: readonly ExclusionRule[] = [
  {
    id: 'user-exclude-png',
    kind: 'file-extension',
    label: 'Exclude .png images',
    pattern: '.png',
    enabled: true,
    source: 'user',
    excludedCount: 2,
  },
  {
    id: 'user-exclude-calc',
    kind: 'tag-pattern',
    label: 'Exclude CALC-* tags',
    pattern: 'CALC-*',
    enabled: true,
    source: 'user',
    excludedCount: 1,
  },
  {
    id: 'user-exclude-virtual',
    kind: 'tag-pattern',
    label: 'Exclude VIRTUAL-* tags',
    pattern: 'VIRTUAL-*',
    enabled: true,
    source: 'user',
    excludedCount: 1,
  },
  {
    id: 'user-exclude-decommissioned',
    kind: 'lifecycle-status',
    label: 'Exclude decommissioned equipment',
    pattern: 'decommissioned',
    enabled: true,
    source: 'user',
    excludedCount: 1,
  },
];

export const EXCLUSION_PREVIEW_DELTAS: Readonly<Record<string, number>> = {
  '.jpg': 3,
  '.png': 4,
  'CALC-*': 5,
  'VIRTUAL-*': 4,
  decommissioned: 1,
};
