import type {
  DimensionId,
  MetricId,
  NotApplicableReasonDto,
  QualityIssueKind,
  RunWarningsDto,
} from '../types';

/**
 * Demo-only fixtures for the metric states the backend can now report. They exist so these states can be looked at
 * locally in explicit mock mode (which always shows the Demo banner); nothing here says anything about a deployed backend.
 */

/** Entities with no tier, as a count for one dimension in one facility. They never carry a division, discipline or owner filter value. */
export type UntieredPool = {
  dimensionId: DimensionId;
  facilityId: string;
  eligibleCount: number;
  passingCount: number;
};

export type UntieredIssue = {
  id: string;
  kind: QualityIssueKind;
  dimensionId: DimensionId;
  metricId: MetricId;
  entityId: string;
  entityLabel: string;
  facilityId: string;
  ownerId: string;
  detail: string;
};

export type MockStateFixtures = {
  untieredPools: readonly UntieredPool[];
  untieredIssues: readonly UntieredIssue[];
  /** Metrics the demo reports as not applicable, with the reason the run's warnings give. */
  notApplicableMetrics: Partial<Record<MetricId, NotApplicableReasonDto>>;
  runWarnings: RunWarningsDto;
};

export const MOCK_STATE_FIXTURES: MockStateFixtures = {
  untieredPools: [
    { dimensionId: 'asset-hierarchy', facilityId: 'demo-north', eligibleCount: 14, passingCount: 9 },
    { dimensionId: 'equipment-asset', facilityId: 'demo-central', eligibleCount: 11, passingCount: 8 },
    { dimensionId: 'time-series', facilityId: 'demo-south', eligibleCount: 20, passingCount: 12 },
  ],
  untieredIssues: [
    {
      id: 'issue-untiered-orphan-1',
      kind: 'orphan-asset',
      dimensionId: 'asset-hierarchy',
      metricId: 'orphansByTier',
      entityId: 'untiered-asset-1',
      entityLabel: 'UNTIERED-ASSET-001',
      facilityId: 'demo-north',
      ownerId: 'sap-steward',
      detail: 'Asset has no parent in the hierarchy. Its type is not mapped to a tier.',
    },
    {
      id: 'issue-untiered-orphan-2',
      kind: 'orphan-asset',
      dimensionId: 'asset-hierarchy',
      metricId: 'orphansByTier',
      entityId: 'untiered-asset-2',
      entityLabel: 'UNTIERED-ASSET-002',
      facilityId: 'demo-north',
      ownerId: 'sap-steward',
      detail: 'Asset has no parent in the hierarchy. It has no asset type.',
    },
    {
      id: 'issue-untiered-typegap-1',
      kind: 'type-gap',
      dimensionId: 'asset-hierarchy',
      metricId: 'typeCoverageGaps',
      entityId: 'untiered-asset-3',
      entityLabel: 'UNTIERED-ASSET-003',
      facilityId: 'demo-north',
      ownerId: 'sap-steward',
      detail: 'Asset is missing type classification.',
    },
    {
      id: 'issue-untiered-unmatched-1',
      kind: 'unmatched-equipment',
      dimensionId: 'equipment-asset',
      metricId: 'unmatchedWithCandidates',
      entityId: 'untiered-equipment-1',
      entityLabel: 'UNTIERED-EQUIPMENT-001',
      facilityId: 'demo-central',
      ownerId: 'sap-steward',
      detail: 'Equipment is not linked to an asset, so it has no tier.',
    },
  ],
  notApplicableMetrics: {
    monitoringCoverage: { kind: 'mapping-empty', mapping: 'tierByTypeCode' },
    freshness: { kind: 'all-excluded', mapping: null },
    asset3dCoverage: { kind: 'no-eligible-entities', mapping: null },
  },
  runWarnings: {
    runTimestamp: '2026-08-31T22:00:00.000Z',
    warnings: [
      {
        code: 'mapping-empty',
        message: 'tierByTypeCode is empty: no asset gets a tier.',
        subject: 'tierByTypeCode',
        reason: null,
        counts: {},
        codes: [],
      },
      {
        code: 'tiering-off',
        message: 'tierByTypeCode is set but tieringEnabled is false, so it is ignored and no asset has a tier.',
        subject: 'tierByTypeCode',
        reason: null,
        counts: {},
        codes: [],
      },
      {
        code: 'unmapped-asset-types',
        message: 'Some assets have no type code, or one that tierByTypeCode does not map; they have no tier.',
        subject: null,
        reason: null,
        counts: { untyped: 12, unmapped: 31 },
        codes: ['air_cooler', 'flare', 'skid'],
      },
      {
        code: 'optional-source-unavailable',
        message: 'The view cdf_idm.CogniteNotification does not exist in this project.',
        subject: 'cdf_idm.CogniteNotification',
        reason: null,
        counts: {},
        codes: [],
      },
      {
        code: 'no-source-entities',
        message: 'No objects3d were found in any facility space.',
        subject: 'objects3d',
        reason: null,
        counts: {},
        codes: [],
      },
      {
        code: 'metric-not-applicable',
        message: 'Nothing was eligible to measure.',
        subject: 'monitoringCoverage',
        reason: 'mapping-empty:tierByTypeCode',
        counts: { units: 3 },
        codes: [],
      },
      {
        code: 'metric-not-applicable',
        message: 'Nothing was eligible to measure.',
        subject: 'freshness',
        reason: 'all-excluded',
        counts: { units: 1 },
        codes: [],
      },
      {
        code: 'drilldown-truncated',
        message: 'The drill-down list was cut at the configured limit.',
        subject: 'orphansByTier',
        reason: null,
        counts: { limit: 500, dropped: 130, units: 2 },
        codes: [],
      },
      {
        code: 'parents-not-loaded',
        message: 'Some assets have a parent that is not among the loaded assets.',
        subject: null,
        reason: null,
        counts: { assets: 3 },
        codes: [],
      },
      {
        code: 'export-failed',
        message: 'The unlinked items of one part of the run could not be exported.',
        subject: 'unlinked-items',
        reason: null,
        counts: {},
        codes: [],
      },
      {
        code: 'location-unsupported',
        message: '1 location filter(s) cannot be used as a scope.',
        subject: null,
        reason: 'no-instance-spaces',
        counts: { locations: 1 },
        codes: [],
      },
      {
        code: 'locations-unavailable',
        message: 'The location filters could not be read.',
        subject: 'locationfilters',
        reason: null,
        counts: {},
        codes: [],
      },
      {
        code: 'free-text',
        message: 'Nightly sync finished late; some inputs may be older than usual.',
        subject: null,
        reason: null,
        counts: {},
        codes: [],
      },
      {
        code: 'warnings-truncated',
        message: 'More warnings than fit; see a dry run.',
        subject: null,
        reason: null,
        counts: { dropped: 4 },
        codes: [],
      },
    ],
  },
};
