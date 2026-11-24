import type { RunWarningDto, SliceFilters, Tier } from '../../types';

import type { PropertyBag } from './recordsClient';
import { parseRunCoverage, type RunCoverage } from './runScope';
import { parseRunWarnings } from './runWarnings';

export type AggregateRow = {
  runId: string;
  runTimestamp: string;
  metricId: string;
  dimensionId: string;
  reportingUnitId: string;
  facilityId: string | null;
  divisionId: string | null;
  disciplineId: string | null;
  ownerId: string | null;
  /** Null when the backend assigned no tier (unmapped or unlinked entities). Never read as tier 3. */
  tier: Tier | null;
  subKey: string | null;
  eligibleCount: number;
  passingCount: number;
  issueCount: number;
  valueSum: number;
  /** The backend wrote this row because nothing was eligible for the metric: it has no value, not a value of 0. */
  notApplicable: boolean;
};

export type ExclusionRow = {
  dimensionId: string;
  exclusionCategory: string;
  facilityId: string | null;
  divisionId: string | null;
  disciplineId: string | null;
  excludedCount: number;
};

export type DrilldownRow = {
  metricId: string;
  dimensionId: string;
  entityId: string;
  entityLabel: string;
  detail: string;
  facilityId: string | null;
  divisionId: string | null;
  disciplineId: string | null;
  ownerId: string | null;
  /** Null when the backend assigned no tier. Never defaulted to a tier. */
  tier: Tier | null;
  /** Classification label per kind for this entity, e.g. { assetType: 'Pump' }. */
  classes: Record<string, string>;
};

export type ClassificationRow = {
  /** The dimension the classification counts for. Absent on rows written before the producer named it. */
  dimensionId?: string;
  kind: string;
  classificationId: string;
  classificationName: string;
  facilityId: string | null;
  divisionId: string | null;
  disciplineId: string | null;
  ownerId: string | null;
  eligibleCount: number;
  passingCount: number;
};

/** A metric the user named in the config file. The names are read from the row, never from a fixed list. */
export type ExtraLinkRow = {
  /** `space:externalId:version` of the data model. */
  model: string;
  /** The model's externalId, for display. */
  modelLabel: string;
  view: string;
  property: string;
  facilityId: string | null;
  divisionId: string | null;
  disciplineId: string | null;
  ownerId: string | null;
  eligibleCount: number;
  passingCount: number;
  notApplicable: boolean;
};

/** An instance of a configured view whose link is not set: one line of a configured metric's drill-down. */
export type ExtraLinkGapRow = {
  model: string;
  modelLabel: string;
  view: string;
  property: string;
  /** The instance space the row was counted in. */
  unit: string;
  facilityId: string | null;
  divisionId: string | null;
  disciplineId: string | null;
  ownerId: string | null;
  entityId: string;
  entityLabel: string;
  detail: string;
};

/** A location filter the run wrote as a scope: the facilities it covers, and its parent when it has one. */
export type LocationRow = {
  id: string;
  name: string;
  parentId: string | null;
  facilityIds: string[];
};

export function parseLocationRow(bag: PropertyBag): LocationRow | null {
  const id = str(bag, 'entityId');
  const cells = str(bag, 'cellsJson');
  if (id === null || cells === null) {
    return null;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(cells);
  } catch {
    return null;
  }
  const facilityIds =
    typeof parsed === 'object' && parsed !== null && 'facilityIds' in parsed && Array.isArray(parsed.facilityIds)
      ? parsed.facilityIds.filter((item): item is string => typeof item === 'string')
      : null;
  if (facilityIds === null) {
    return null;
  }
  return { id, name: str(bag, 'entityLabel') ?? id, parentId: str(bag, 'detail'), facilityIds };
}

export type ExtraLinkHistoryRow = ExtraLinkRow & {
  runId: string;
  runTimestamp: string;
};

export type RunRow = {
  runId: string;
  runTimestamp: string;
  /** The warnings the run marker carries; empty for a run that has none (all runs written before they existed). */
  warnings: RunWarningDto[];
  /** What the run measured (the marker's `scope`). A run written before scopes existed measured every dimension. */
  coverage: RunCoverage;
};

export type RowTotals = {
  eligible: number;
  passing: number;
  issue: number;
  valueSum: number;
};

function str(bag: PropertyBag, key: string): string | null {
  const value = bag[key];
  return typeof value === 'string' ? value : null;
}

function num(bag: PropertyBag, key: string): number {
  const value = bag[key];
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

function parseClasses(value: unknown): Record<string, string> {
  if (typeof value !== 'string') {
    return {};
  }
  try {
    const parsed: unknown = JSON.parse(value);
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      return {};
    }
    return Object.fromEntries(
      Object.entries(parsed).filter((entry): entry is [string, string] => typeof entry[1] === 'string')
    );
  } catch {
    return {};
  }
}

function toTier(value: unknown): Tier | null {
  return value === 1 || value === 2 || value === 3 ? value : null;
}

export function parseAggregateRow(bag: PropertyBag): AggregateRow | null {
  const runId = str(bag, 'runId');
  const runTimestamp = str(bag, 'runTimestamp');
  const metricId = str(bag, 'metricId');
  const dimensionId = str(bag, 'dimensionId');
  const reportingUnitId = str(bag, 'reportingUnitId');
  if (
    runId === null ||
    runTimestamp === null ||
    metricId === null ||
    dimensionId === null ||
    reportingUnitId === null
  ) {
    return null;
  }
  return {
    runId,
    runTimestamp,
    metricId,
    dimensionId,
    reportingUnitId,
    facilityId: str(bag, 'facilityId'),
    divisionId: str(bag, 'divisionId'),
    disciplineId: str(bag, 'disciplineId'),
    ownerId: str(bag, 'ownerId'),
    tier: toTier(bag.tier),
    subKey: str(bag, 'subKey'),
    eligibleCount: num(bag, 'eligibleCount'),
    passingCount: num(bag, 'passingCount'),
    issueCount: num(bag, 'issueCount'),
    valueSum: num(bag, 'valueSum'),
    notApplicable: bag.notApplicable === true,
  };
}

export function parseExclusionRow(bag: PropertyBag): ExclusionRow | null {
  const dimensionId = str(bag, 'dimensionId');
  const exclusionCategory = str(bag, 'exclusionCategory');
  if (dimensionId === null || exclusionCategory === null) {
    return null;
  }
  return {
    dimensionId,
    exclusionCategory,
    facilityId: str(bag, 'facilityId'),
    divisionId: str(bag, 'divisionId'),
    disciplineId: str(bag, 'disciplineId'),
    excludedCount: num(bag, 'excludedCount'),
  };
}

export function parseClassificationRow(bag: PropertyBag): ClassificationRow | null {
  const kind = str(bag, 'classificationKind');
  const classificationId = str(bag, 'classificationId');
  if (kind === null || classificationId === null) {
    return null;
  }
  const dimensionId = str(bag, 'dimensionId');
  return {
    ...(dimensionId === null ? {} : { dimensionId }),
    kind,
    classificationId,
    classificationName: str(bag, 'classificationName') ?? classificationId,
    facilityId: str(bag, 'facilityId'),
    divisionId: str(bag, 'divisionId'),
    disciplineId: str(bag, 'disciplineId'),
    ownerId: str(bag, 'ownerId'),
    eligibleCount: num(bag, 'eligibleCount'),
    passingCount: num(bag, 'passingCount'),
  };
}

/** `dimensionId` of these rows is `space:externalId:version/View`, `metricId` the property. */
export function parseExtraLinkRow(bag: PropertyBag): ExtraLinkRow | null {
  const property = str(bag, 'metricId');
  const dimensionId = str(bag, 'dimensionId');
  const split = dimensionId?.lastIndexOf('/') ?? -1;
  if (property === null || dimensionId === null || split <= 0 || split === dimensionId.length - 1) {
    return null;
  }
  const model = dimensionId.slice(0, split);
  return {
    model,
    modelLabel: model.split(':')[1] ?? model,
    view: dimensionId.slice(split + 1),
    property,
    facilityId: str(bag, 'facilityId'),
    divisionId: str(bag, 'divisionId'),
    disciplineId: str(bag, 'disciplineId'),
    ownerId: str(bag, 'ownerId'),
    eligibleCount: num(bag, 'eligibleCount'),
    passingCount: num(bag, 'passingCount'),
    notApplicable: bag.notApplicable === true,
  };
}

export function parseExtraLinkHistoryRow(bag: PropertyBag): ExtraLinkHistoryRow | null {
  const parsed = parseExtraLinkRow(bag);
  const runId = str(bag, 'runId');
  const runTimestamp = str(bag, 'runTimestamp');
  if (parsed === null || runId === null || runTimestamp === null) {
    return null;
  }
  return { ...parsed, runId, runTimestamp };
}

/** Same naming as `parseExtraLinkRow`: `dimensionId` is `space:externalId:version/View`, `metricId` the property. */
export function parseExtraLinkGapRow(bag: PropertyBag): ExtraLinkGapRow | null {
  const property = str(bag, 'metricId');
  const dimensionId = str(bag, 'dimensionId');
  const entityId = str(bag, 'entityId');
  const unit = str(bag, 'reportingUnitId');
  const split = dimensionId?.lastIndexOf('/') ?? -1;
  if (
    property === null ||
    dimensionId === null ||
    entityId === null ||
    unit === null ||
    split <= 0 ||
    split === dimensionId.length - 1
  ) {
    return null;
  }
  const model = dimensionId.slice(0, split);
  return {
    model,
    modelLabel: model.split(':')[1] ?? model,
    view: dimensionId.slice(split + 1),
    property,
    unit,
    facilityId: str(bag, 'facilityId'),
    divisionId: str(bag, 'divisionId'),
    disciplineId: str(bag, 'disciplineId'),
    ownerId: str(bag, 'ownerId'),
    entityId,
    entityLabel: str(bag, 'entityLabel') ?? entityId,
    detail: str(bag, 'detail') ?? '',
  };
}

export function parseDrilldownRow(bag: PropertyBag): DrilldownRow | null {
  const metricId = str(bag, 'metricId');
  const dimensionId = str(bag, 'dimensionId');
  const entityId = str(bag, 'entityId');
  if (metricId === null || dimensionId === null || entityId === null) {
    return null;
  }
  return {
    metricId,
    dimensionId,
    entityId,
    entityLabel: str(bag, 'entityLabel') ?? entityId,
    detail: str(bag, 'detail') ?? '',
    facilityId: str(bag, 'facilityId'),
    divisionId: str(bag, 'divisionId'),
    disciplineId: str(bag, 'disciplineId'),
    ownerId: str(bag, 'ownerId'),
    tier: toTier(bag.tier),
    classes: parseClasses(bag.cellsJson),
  };
}

export function parseRunRow(bag: PropertyBag): RunRow | null {
  const runId = str(bag, 'runId');
  const runTimestamp = str(bag, 'runTimestamp');
  if (runId === null || runTimestamp === null) {
    return null;
  }
  const warnings = parseRunWarnings(bag.warnings);
  const coverage = parseRunCoverage(str(bag, 'scope'));
  return { runId, runTimestamp, warnings, coverage };
}

type Sliceable = {
  facilityId: string | null;
  divisionId: string | null;
  disciplineId: string | null;
  ownerId?: string | null;
};

/** A row matches when every active filter equals the row's value. Rows with no value never match an active filter. */
export function matchesRowSlice(row: Sliceable, filters: SliceFilters): boolean {
  if (filters.facilityId !== undefined && row.facilityId !== filters.facilityId) {
    return false;
  }
  if (filters.facilityIds !== undefined && (row.facilityId === null || !filters.facilityIds.includes(row.facilityId))) {
    return false;
  }
  if (filters.divisionId !== undefined && row.divisionId !== filters.divisionId) {
    return false;
  }
  if (filters.disciplineId !== undefined && row.disciplineId !== filters.disciplineId) {
    return false;
  }
  if (filters.ownerId !== undefined && (row.ownerId ?? null) !== filters.ownerId) {
    return false;
  }
  return true;
}

export function sumRows(rows: readonly AggregateRow[]): RowTotals {
  return rows.reduce<RowTotals>(
    (totals, row) => ({
      eligible: totals.eligible + row.eligibleCount,
      passing: totals.passing + row.passingCount,
      issue: totals.issue + row.issueCount,
      valueSum: totals.valueSum + row.valueSum,
    }),
    { eligible: 0, passing: 0, issue: 0, valueSum: 0 }
  );
}
