import type { ExclusionBucket, ExclusionBucketKind } from '../../types';

import type { ExclusionRow } from './metricRows';

type BucketDefinition = { kind: ExclusionBucketKind; labelKey: string; source: 'system' | 'user' };

const BUCKETS: Readonly<Record<string, BucketDefinition>> = {
  'missing-upstream-target': { kind: 'missing-upstream-target', labelKey: 'exclusion.missingUpstreamTarget', source: 'system' },
  'invalid-ocr': { kind: 'invalid-ocr', labelKey: 'exclusion.invalidOcr', source: 'system' },
  'synthetic-timeseries': { kind: 'synthetic-timeseries', labelKey: 'exclusion.syntheticTimeseries', source: 'system' },
  'historical-retired': { kind: 'historical-retired', labelKey: 'exclusion.historicalRetired', source: 'system' },
  'location-centric-files': { kind: 'location-centric-files', labelKey: 'exclusion.locationCentricFiles', source: 'system' },
  'user:file-extension': { kind: 'user-file-extension', labelKey: 'exclusion.userFileExtension', source: 'user' },
  'user:tag-pattern': { kind: 'user-tag-pattern', labelKey: 'exclusion.userTagPattern', source: 'user' },
  'user:decommissioned': { kind: 'user-decommissioned-equipment', labelKey: 'exclusion.userDecommissionedEquipment', source: 'user' },
};

export function buildExclusionBuckets(rows: readonly ExclusionRow[]): ExclusionBucket[] {
  const counts = new Map<string, number>();
  for (const row of rows) {
    counts.set(row.exclusionCategory, (counts.get(row.exclusionCategory) ?? 0) + row.excludedCount);
  }
  const buckets: ExclusionBucket[] = [];
  for (const [category, count] of counts) {
    const definition = BUCKETS[category];
    if (definition !== undefined) {
      buckets.push({ ...definition, count });
    }
  }
  return buckets;
}

export function titleCase(id: string): string {
  return id
    .replace(/^user:/, '')
    .split(/[-_\s]+/)
    .filter((part) => part.length > 0)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

export function buildUserExclusionSummary(
  rows: readonly ExclusionRow[]
): { id: string; label: string; count: number }[] {
  return buildExclusionBuckets(rows)
    .filter((bucket) => bucket.source === 'user')
    .map((bucket) => ({ id: bucket.kind, label: titleCase(bucket.kind), count: bucket.count }));
}
