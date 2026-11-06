import { describe, expect, it } from 'vitest';

import { translate } from '../../i18n/messages';
import type { IssueDto } from '../../types';


import {
  buildIssuePatternRows,
  buildIssueTypeFilterOptions,
  mergeIssuePatternFilters,
} from './issuePatternPresentation';

describe(buildIssuePatternRows.name, () => {
  it('groups issues by dimension and kind into pattern rows', () => {
    const rows = buildIssuePatternRows([
      issue({ id: '1', kind: 'orphan-asset', ownerId: 'sap-data-steward' }),
      issue({ id: '2', kind: 'orphan-asset', ownerId: 'sap-data-steward' }),
      issue({ id: '3', kind: 'type-gap', ownerId: 'data-engineering' }),
    ]);

    expect(rows).toHaveLength(2);
    const orphan = rows.find((row) => row.kind === 'orphan-asset');
    expect(orphan?.affectedCount).toBe(2);
    expect(orphan?.nameKey).toBe('issues.kind.orphan-asset');
    expect(orphan?.issueIds).toEqual(['1', '2']);
  });

  it('sorts groups by affected count, highest first', () => {
    const rows = buildIssuePatternRows([
      issue({ id: '1', kind: 'type-gap' }),
      issue({ id: '2', kind: 'orphan-asset' }),
      issue({ id: '3', kind: 'orphan-asset' }),
      issue({ id: '4', kind: 'orphan-asset' }),
    ]);

    expect(rows.map((row) => row.kind)).toEqual(['orphan-asset', 'type-gap']);
  });

  it('describes the group by what its kind means, not by one of its items', () => {
    const rows = buildIssuePatternRows([
      issue({ id: '1', kind: 'orphan-timeseries', dimensionId: 'time-series', detail: 'prefix PI' }),
      issue({ id: '2', kind: 'orphan-timeseries', dimensionId: 'time-series', detail: 'prefix TS' }),
    ]);

    expect(rows[0].dimensionId).toBe('time-series');
    expect(rows[0].descriptionKey).toBe('issues.kindDescription.orphan-timeseries');
  });

  it('picks the most common owner within a group', () => {
    const rows = buildIssuePatternRows([
      issue({ id: '1', kind: 'orphan-asset', ownerId: 'sap-data-steward' }),
      issue({ id: '2', kind: 'orphan-asset', ownerId: 'sap-data-steward' }),
      issue({ id: '3', kind: 'orphan-asset', ownerId: 'data-engineering' }),
    ]);

    expect(rows[0].ownerId).toBe('sap-data-steward');
  });

  it('returns an empty list when there are no issues', () => {
    expect(buildIssuePatternRows([])).toEqual([]);
  });
});

describe(buildIssueTypeFilterOptions.name, () => {
  const t = (key: string) => translate(key, 'en');

  it('should list only issue kinds for the selected dimension', () => {
    const rows = buildIssuePatternRows([
      issue({ id: '1', kind: 'unlinked-3d', dimensionId: 'model-3d' }),
      issue({ id: '2', kind: 'bbox-gap', dimensionId: 'model-3d' }),
      issue({ id: '3', kind: 'orphan-timeseries', dimensionId: 'time-series' }),
    ]);

    const options = buildIssueTypeFilterOptions(rows, 'model-3d', t).map((option) => option.label);
    expect(options).toEqual(['Unlinked 3D', 'Bounding box gap']);
  });

  it('should list every kind when no dimension is selected', () => {
    const rows = buildIssuePatternRows([
      issue({ id: '1', kind: 'unlinked-3d', dimensionId: 'model-3d' }),
      issue({ id: '2', kind: 'orphan-timeseries', dimensionId: 'time-series' }),
    ]);

    expect(buildIssueTypeFilterOptions(rows, undefined, t)).toHaveLength(2);
  });
});

describe(mergeIssuePatternFilters.name, () => {
  it('should clear the issue type when the dimension changes', () => {
    const rows = buildIssuePatternRows([
      issue({ id: '1', kind: 'orphan-timeseries', dimensionId: 'time-series' }),
      issue({ id: '2', kind: 'unlinked-3d', dimensionId: 'model-3d' }),
    ]);

    const next = mergeIssuePatternFilters(
      { dimensionId: 'time-series', kind: 'orphan-timeseries' },
      { dimensionId: 'model-3d', kind: 'orphan-timeseries', search: '' },
      rows
    );

    expect(next.kind).toBeUndefined();
  });
});

function issue(overrides: Partial<IssueDto> & { id: string }): IssueDto {
  return {
    id: overrides.id,
    kind: overrides.kind ?? 'orphan-asset',
    dimensionId: overrides.dimensionId ?? 'asset-hierarchy',
    metricId: overrides.metricId ?? 'hierarchyCompletionRate',
    entityId: overrides.entityId ?? `entity-${overrides.id}`,
    entityLabel: overrides.entityLabel ?? `Entity ${overrides.id}`,
    tier: overrides.tier ?? 3,
    facilityId: overrides.facilityId ?? 'facility-1',
    ownerId: overrides.ownerId ?? 'sap-data-steward',
    detail: overrides.detail ?? 'Asset has no parent in the hierarchy.',
    actionKey: overrides.actionKey ?? 'issues.action.review',
    classes: overrides.classes ?? {},
  };
}
