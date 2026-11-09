import type { PropertyBag, RecordsClient, RecordsPage, RecordsQuery } from './recordsClient';

/** In-memory records client: exact-match filtering with an optional page limit to exercise truncation. */
export class FakeRecordsClient implements RecordsClient {
  readonly queries: RecordsQuery[] = [];
  private readonly rows: readonly PropertyBag[];
  private readonly pageLimit: number;

  constructor(rows: readonly PropertyBag[], pageLimit = Number.POSITIVE_INFINITY) {
    this.rows = rows;
    this.pageLimit = pageLimit;
  }

  async query(query: RecordsQuery): Promise<RecordsPage> {
    this.queries.push(query);
    const matches = this.rows.filter(
      (row) =>
        Object.entries(query.equals).every(([property, value]) => row[property] === value) &&
        (query.absent ?? []).every((property) => row[property] === undefined)
    );
    const limit = Math.min(query.limit ?? Number.POSITIVE_INFINITY, this.pageLimit);
    return {
      rows: matches.slice(0, limit),
      truncated: matches.length >= limit,
    };
  }
}

export const RUN_OLD = 'run-old';
export const RUN_NEW = 'run-new';
export const RUN_INCOMPLETE = 'run-incomplete';

export function runRow(
  runId: string,
  runTimestamp: string,
  status = 'complete',
  extra: Partial<Record<string, string | number>> = {}
): PropertyBag {
  return { rowKind: 'run', runId, runTimestamp, status, ...extra };
}

type AggregateInput = {
  runId?: string;
  runTimestamp?: string;
  metricId: string;
  dimensionId: string;
  facilityId?: string;
  divisionId?: string;
  disciplineId?: string;
  ownerId?: string;
  /** A number is written as the row's tier; `null` writes no tier, as the backend does for an untiered row. */
  tier?: number | null;
  subKey?: string;
  eligible: number;
  passing: number;
  valueSum?: number;
  notApplicable?: boolean;
};

export function aggregateRow(input: AggregateInput): PropertyBag {
  return {
    rowKind: 'aggregate',
    runId: input.runId ?? RUN_NEW,
    runTimestamp: input.runTimestamp ?? '2026-10-12T06:00:00.000Z',
    metricId: input.metricId,
    dimensionId: input.dimensionId,
    reportingUnitId: `unit-${input.facilityId ?? 'x'}`,
    facilityId: input.facilityId,
    divisionId: input.divisionId,
    disciplineId: input.disciplineId,
    ownerId: input.ownerId,
    tier: input.tier === undefined ? 1 : (input.tier ?? undefined),
    subKey: input.subKey,
    eligibleCount: input.eligible,
    passingCount: input.passing,
    issueCount: input.eligible - input.passing,
    valueSum: input.valueSum,
    notApplicable: input.notApplicable === true ? true : undefined,
  };
}

/** The unit-grain row the backend writes for a metric with nothing eligible: counts of 0, `notApplicable`, no tier. */
export function notApplicableRow(metricId: string, dimensionId: string, extra: Partial<AggregateInput> = {}): PropertyBag {
  return aggregateRow({ metricId, dimensionId, tier: null, eligible: 0, passing: 0, notApplicable: true, ...extra });
}

export function drilldownRow(
  metricId: string,
  dimensionId: string,
  entityId: string,
  extra: Partial<Record<string, string | number>> = {}
): PropertyBag {
  return {
    rowKind: 'drilldown',
    runId: RUN_NEW,
    runTimestamp: '2026-10-12T06:00:00.000Z',
    metricId,
    dimensionId,
    entityId,
    entityLabel: `Label ${entityId}`,
    detail: 'detail text',
    facilityId: 'north',
    ownerId: 'sap-steward',
    tier: 1,
    ...extra,
  };
}

export function exclusionRow(
  dimensionId: string,
  exclusionCategory: string,
  excludedCount: number,
  facilityId = 'north'
): PropertyBag {
  return { rowKind: 'exclusion', runId: RUN_NEW, dimensionId, exclusionCategory, excludedCount, facilityId };
}

type ClassificationInput = {
  kind: string;
  id: string;
  name?: string;
  facilityId?: string;
  divisionId?: string;
  disciplineId?: string;
  ownerId?: string;
  tier?: number;
  eligible: number;
  passing: number;
};

export function classificationRow(input: ClassificationInput): PropertyBag {
  return {
    rowKind: 'classification',
    runId: RUN_NEW,
    runTimestamp: '2026-10-12T06:00:00.000Z',
    classificationKind: input.kind,
    classificationId: input.id,
    classificationName: input.name ?? input.id,
    dimensionId: 'asset-hierarchy',
    reportingUnitId: `unit-${input.facilityId ?? 'x'}`,
    facilityId: input.facilityId,
    divisionId: input.divisionId,
    disciplineId: input.disciplineId,
    ownerId: input.ownerId,
    tier: input.tier ?? 1,
    eligibleCount: input.eligible,
    passingCount: input.passing,
    issueCount: input.eligible - input.passing,
  };
}

/** A drill-down row the backend wrote without a tier. */
export function untieredDrilldownRow(...args: Parameters<typeof drilldownRow>): PropertyBag {
  return Object.fromEntries(Object.entries(drilldownRow(...args)).filter(([key]) => key !== 'tier'));
}

type ExtraLinkInput = {
  model?: string;
  view: string;
  property: string;
  facilityId?: string;
  eligible: number;
  passing: number;
  notApplicable?: boolean;
};

/** A row for a metric the user named in the config file: the property is the metric, the model and view the dimension. */
export function extraLinkRow(input: ExtraLinkInput): PropertyBag {
  return {
    rowKind: 'extraLink',
    runId: RUN_NEW,
    runTimestamp: '2026-10-12T06:00:00.000Z',
    metricId: input.property,
    dimensionId: `${input.model ?? 'cdf_cdm:CogniteCore:v1'}/${input.view}`,
    reportingUnitId: input.facilityId ?? 'north',
    facilityId: input.facilityId ?? 'north',
    eligibleCount: input.eligible,
    passingCount: input.passing,
    issueCount: input.eligible - input.passing,
    notApplicable: input.notApplicable === true ? true : undefined,
  };
}

type ExtraLinkGapInput = {
  model?: string;
  view: string;
  property: string;
  facilityId?: string;
  entityId: string;
  entityLabel?: string;
};

/** An instance of a configured view whose link is not set. */
export function locationRow(id: string, name: string, facilityIds: string[], parent?: string): PropertyBag {
  return {
    rowKind: 'location',
    runId: RUN_NEW,
    runTimestamp: '2026-10-12T06:00:00.000Z',
    entityId: id,
    entityLabel: name,
    ...(parent === undefined ? {} : { detail: parent }),
    cellsJson: JSON.stringify({ facilityIds, spaces: facilityIds.map((facility) => `sp_${facility}`) }),
  };
}

export function extraLinkGapRow(input: ExtraLinkGapInput): PropertyBag {
  return {
    rowKind: 'extraLinkGap',
    runId: RUN_NEW,
    runTimestamp: '2026-10-12T06:00:00.000Z',
    metricId: input.property,
    dimensionId: `${input.model ?? 'cdf_cdm:CogniteCore:v1'}/${input.view}`,
    reportingUnitId: input.facilityId ?? 'north',
    facilityId: input.facilityId ?? 'north',
    entityId: input.entityId,
    entityLabel: input.entityLabel ?? input.entityId,
    detail: 'link not set',
  };
}
