import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it } from 'vitest';

import { ProductShell } from '../../app/ProductShell';
import type { AppServices } from '../../services/createServices';
import { createMockServices } from '../../services/mock/createMockServices';
import { renderOverview } from '../../test/renderOverview';
import { withQualityService } from '../../test/withQualityService';
import type { AggregateMetricRowDto, DimensionDto, DrilldownDto, TierScore } from '../../types';

const TIERS: TierScore[] = [
  { tier: 1, scorePercent: 90, targetPercent: 95, scored: true },
  { tier: 2, scorePercent: 80, targetPercent: 60, scored: true },
  { tier: 3, scorePercent: null, targetPercent: 0, scored: false },
];

function metric(partial: Partial<AggregateMetricRowDto> & { metricId: AggregateMetricRowDto['metricId'] }): AggregateMetricRowDto {
  return {
    labelKey: `metric.${partial.metricId}`,
    direction: 'critical',
    eligibleCount: 10,
    passingCount: 9,
    issueCount: 1,
    valuePercent: 90,
    tier: 'all',
    extended: false,
    hasDrilldown: false,
    actionKey: 'action.none',
    status: 'good',
    notApplicable: false,
    notApplicableReason: null,
    ...partial,
  };
}

function notApplicableMetric(
  metricId: AggregateMetricRowDto['metricId'],
  reason: AggregateMetricRowDto['notApplicableReason']
): AggregateMetricRowDto {
  return metric({ metricId, eligibleCount: 0, passingCount: 0, issueCount: 0, valuePercent: null, status: 'not-applicable', notApplicable: true, notApplicableReason: reason });
}

function dimension(partial: Partial<DimensionDto>): DimensionDto {
  return {
    dimensionId: 'equipment-asset',
    nameKey: 'dimension.equipment-asset',
    enabled: true,
    optional: false,
    status: 'good',
    entityCount: 10,
    tierScores: TIERS,
    untiered: null,
    scoreBasis: 'tiered',
    overallPercent: 85,
    aggregateRows: [],
    excludedBuckets: [],
    userExclusionSummary: [],
    insightIds: [],
    extendedMetricsAvailable: false,
    ...partial,
  };
}

function servicesWith(dto: DimensionDto, drilldown?: DrilldownDto): AppServices {
  return withQualityService(createMockServices({ latency: 0 }), {
    getDimension: () => Promise.resolve(dto),
    ...(drilldown === undefined ? {} : { getDrilldown: () => Promise.resolve(drilldown) }),
  });
}

function renderDimension(dto: DimensionDto, extraState: Record<string, unknown> = {}, drilldown?: DrilldownDto) {
  return renderOverview(
    <ProductShell hostInitialState={JSON.stringify({ page: 'dimensions', dimensionId: 'equipment-asset', ...extraState })} />,
    { services: servicesWith(dto, drilldown) }
  );
}

describe('dimension page states', () => {
  beforeEach(() => {
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('shows a dimension with nothing eligible as Not applicable, with no score and no healthy status', async () => {
    renderDimension(
      dimension({
        status: 'not-applicable',
        entityCount: 0,
        overallPercent: null,
        tierScores: [],
        aggregateRows: [
          notApplicableMetric('associationRate', null),
          notApplicableMetric('serialMfrCompleteness', null),
        ],
      })
    );

    expect(await screen.findByTestId('dimension-not-applicable')).toHaveTextContent('Nothing was eligible to measure');
    expect(screen.getAllByText('Not applicable').length).toBeGreaterThan(0);
    expect(screen.queryByText('Good')).not.toBeInTheDocument();
    expect(screen.queryByText('No data for this scope')).not.toBeInTheDocument();
    const pageTitle = screen.getByRole('heading', { name: 'Equipment-Asset' });
    expect(within(pageTitle).queryByText(/%/)).not.toBeInTheDocument();
  });

  it('shows each metric with nothing eligible as Not applicable and gives the reason the run reported', async () => {
    renderDimension(
      dimension({
        status: 'not-applicable',
        entityCount: 0,
        overallPercent: null,
        tierScores: [],
        aggregateRows: [
          notApplicableMetric('monitoringCoverage', { kind: 'mapping-empty', mapping: 'tierByTypeCode' }),
          notApplicableMetric('associationRate', { kind: 'all-excluded', mapping: null }),
          notApplicableMetric('completenessByManufacturer', { kind: 'no-eligible-entities', mapping: null }),
          notApplicableMetric('serialMfrCompleteness', { kind: 'all-excluded', mapping: null }),
        ],
      })
    );

    const cell = async (metricId: string) => within(await screen.findByTestId(`metric-not-applicable-${metricId}`));
    expect((await cell('monitoringCoverage')).getByText('tierByTypeCode is not configured')).toBeInTheDocument();
    expect(screen.queryByTestId('metric-not-applicable-associationRate')).not.toBeInTheDocument();
    expect((await cell('completenessByManufacturer')).getByText('No entities were eligible')).toBeInTheDocument();
    expect((await cell('serialMfrCompleteness')).getByText('Every entity was excluded')).toBeInTheDocument();
  });

  describe('orphan rate', () => {
    const hierarchy = (aggregateRows: AggregateMetricRowDto[]) =>
      dimension({ dimensionId: 'asset-hierarchy', nameKey: 'dimension.asset-hierarchy', aggregateRows });

    it('shows the number of orphans out of all assets instead of a percentage', async () => {
      renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'dimensions', dimensionId: 'asset-hierarchy' })} />, {
        services: servicesWith(
          hierarchy([metric({ metricId: 'orphanRate', eligibleCount: 2010, passingCount: 2010, issueCount: 0, valuePercent: 100 })])
        ),
      });

      const card = within(await screen.findByTestId('metric-card-orphanRate'));
      expect(card.getByText('0 orphans of 2010 assets')).toBeInTheDocument();
      expect(card.getByTestId('metric-coverage-badge')).toHaveTextContent('100% coverage');
      expect(card.queryByText('Affected count', { exact: false })).not.toBeInTheDocument();
    });

    it('counts orphans when there are some, and says "orphan" for one', async () => {
      renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'dimensions', dimensionId: 'asset-hierarchy' })} />, {
        services: servicesWith(
          hierarchy([
            metric({ metricId: 'orphanRate', eligibleCount: 50, passingCount: 38, issueCount: 12, valuePercent: 76, status: 'weak' }),
            metric({ metricId: 'hierarchyCompletionRate', eligibleCount: 40, passingCount: 39, issueCount: 1, valuePercent: 98 }),
          ])
        ),
      });

      expect(await within(await screen.findByTestId('metric-card-orphanRate')).findByText('12 orphans of 50 assets')).toBeInTheDocument();
      expect(screen.queryByTestId('metric-card-hierarchyCompletionRate')).not.toBeInTheDocument();
    });

    it('says "1 orphan of 3 assets" for a single orphan', async () => {
      renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'dimensions', dimensionId: 'asset-hierarchy' })} />, {
        services: servicesWith(
          hierarchy([metric({ metricId: 'orphanRate', eligibleCount: 3, passingCount: 2, issueCount: 1, valuePercent: 67, status: 'weak' })])
        ),
      });

      expect(await within(await screen.findByTestId('metric-card-orphanRate')).findByText('1 orphan of 3 assets')).toBeInTheDocument();
    });

    it('keeps Not applicable when nothing was eligible', async () => {
      renderOverview(<ProductShell hostInitialState={JSON.stringify({ page: 'dimensions', dimensionId: 'asset-hierarchy' })} />, {
        services: servicesWith(hierarchy([notApplicableMetric('orphanRate', null)])),
      });

      expect(within(await screen.findByTestId('metric-card-orphanRate')).getByTestId('metric-not-applicable-orphanRate')).toHaveTextContent('Not applicable');
    });
  });

  it('says how many annotations there are instead of a share of them', async () => {
    renderDimension(
      dimension({
        dimensionId: 'file-annotation',
        aggregateRows: [
          metric({ metricId: 'annotationCount', eligibleCount: 2198, passingCount: 2198, issueCount: 0, valuePercent: 100 }),
        ],
      }),
      { dimensionId: 'file-annotation' }
    );

    expect(await screen.findByTestId('dimension-readiness')).toHaveTextContent('2,198 annotations');
    expect(screen.queryByTestId('dimension-coverage-percent')).not.toBeInTheDocument();
    expect(screen.queryByText(/contextualised/)).not.toBeInTheDocument();
  });

  it('names the parts of a metric after what it counts, and keeps Contextualised for a link metric', async () => {
    renderDimension(
      dimension({
        dimensionId: 'file-annotation',
        aggregateRows: [
          metric({ metricId: 'annotationCount', eligibleCount: 2198, passingCount: 2198, issueCount: 0, valuePercent: 100 }),
          metric({ metricId: 'approvalMix', eligibleCount: 2198, passingCount: 1363, issueCount: 835, valuePercent: 62 }),
          metric({ metricId: 'confidenceAverage', eligibleCount: 2198, passingCount: 2198, issueCount: 0, valuePercent: 82 }),
          metric({ metricId: 'tsToAssetRate', eligibleCount: 10, passingCount: 9, issueCount: 1, valuePercent: 90 }),
        ],
      }),
      { dimensionId: 'file-annotation' }
    );

    const approval = within(await screen.findByTestId('metric-card-approvalMix'));
    expect(approval.getByText('Approved')).toBeInTheDocument();
    expect(approval.getByText('Not approved')).toBeInTheDocument();
    expect(approval.getByText('62% approved')).toBeInTheDocument();
    expect(approval.queryByText('Contextualised')).not.toBeInTheDocument();

    const confidence = within(screen.getByTestId('metric-card-confidenceAverage'));
    expect(confidence.getByTestId('metric-count-confidenceAverage')).toHaveTextContent('82%');
    expect(confidence.getByText('average confidence')).toBeInTheDocument();
    expect(confidence.getByText('Annotations scored')).toBeInTheDocument();
    expect(confidence.getByTestId('average-gauge-fill')).toHaveStyle({ width: '82%' });
    expect(confidence.queryByText('Contextualised')).not.toBeInTheDocument();
    expect(screen.queryByTestId('metric-coverage-badge-confidenceAverage')).not.toBeInTheDocument();

    const link = within(screen.getByTestId('metric-card-tsToAssetRate'));
    expect(link.getByText('Contextualised')).toBeInTheDocument();
    expect(link.getByText('90% coverage')).toBeInTheDocument();
  });

  it('opens the main metric drill-down from the summary card, and offers none when the metric has no list', async () => {
    const user = userEvent.setup();
    renderDimension(
      dimension({
        aggregateRows: [metric({ metricId: 'associationRate', hasDrilldown: true })],
      })
    );

    await user.click(await screen.findByTestId('dimension-summary-open-drilldown'));

    expect(await screen.findByTestId('drilldown-sidesheet')).toBeInTheDocument();
  });

  it('shows no summary drill-down button when the main metric has no entity list', async () => {
    renderDimension(
      dimension({
        aggregateRows: [metric({ metricId: 'associationRate', hasDrilldown: false })],
      })
    );

    await screen.findByTestId('dimension-summary-card');
    expect(screen.queryByTestId('dimension-summary-open-drilldown')).not.toBeInTheDocument();
  });

  it('keeps a measured zero as 0% beside a not-applicable metric', async () => {
    renderDimension(
      dimension({
        status: 'critical',
        overallPercent: 0,
        aggregateRows: [
          metric({ metricId: 'associationRate', eligibleCount: 12, passingCount: 0, issueCount: 12, valuePercent: 0, status: 'critical' }),
          metric({
            metricId: 'completenessByManufacturer',
            eligibleCount: 12,
            passingCount: 0,
            issueCount: 12,
            valuePercent: 0,
            status: 'critical',
          }),
          notApplicableMetric('serialMfrCompleteness', null),
        ],
      })
    );

    const measured = await screen.findByTestId('metric-card-completenessByManufacturer');
    expect(within(measured).getByText('0 / 12')).toBeInTheDocument();
    expect(within(measured).queryAllByText('Not applicable')).toHaveLength(0);
    expect(within(screen.getByTestId('metric-card-serialMfrCompleteness')).getAllByText('Not applicable')).not.toHaveLength(0);
  });

  it('adds an explicit row for entities with no tier, never labelled as a tier', async () => {
    renderDimension(
      dimension({
        untiered: { scorePercent: 50, eligibleCount: 20, passingCount: 10 },
        aggregateRows: [metric({ metricId: 'associationRate' })],
      })
    );

    const row = await screen.findByTestId('tier-row-untiered');
    expect(row).toHaveTextContent('No tier assigned');
    expect(row).toHaveTextContent('10 / 20');
    expect(row).not.toHaveTextContent(/Tier 3/);
    expect(screen.queryByTestId('dimension-pooled-note')).not.toBeInTheDocument();
  });

  it('explains a pooled score when no tier 1 or 2 entity was measured', async () => {
    renderDimension(
      dimension({
        scoreBasis: 'pooled-fallback',
        untiered: { scorePercent: 50, eligibleCount: 40, passingCount: 20 },
        overallPercent: 50,
        tierScores: TIERS.map((tier) => ({ ...tier, scorePercent: null })),
        aggregateRows: [metric({ metricId: 'associationRate' })],
      })
    );

    expect(await screen.findByTestId('dimension-pooled-note')).toHaveTextContent(
      'No tier 1 or 2 entities were measured, so this score covers all entities together.'
    );
    expect(screen.getByTestId('tier-row-untiered')).toHaveTextContent('20 / 40');
  });

  it('says a partial drill-down list is not the full list', async () => {
    const drilldown: DrilldownDto = {
      metricId: 'unmatchedWithCandidates',
      dimensionId: 'equipment-asset',
      labelKey: 'metric.unmatchedWithCandidates',
      actionKey: 'action.none',
      ownerId: null,
      columnKeys: ['entity', 'tier', 'detail'],
      partial: { limit: 200 },
      rows: [{ id: 'a', tier: 1, ownerId: '', classes: {}, cells: { entity: 'Pump A', tier: '1', detail: 'd' } }],
    };
    renderDimension(
      dimension({ aggregateRows: [metric({ metricId: 'unmatchedWithCandidates', hasDrilldown: true })] }),
      { metricId: 'unmatchedWithCandidates' },
      drilldown
    );

    expect(await screen.findByTestId('drilldown-partial')).toHaveTextContent('This is not the full list.');
    expect(screen.getByTestId('drilldown-partial')).toHaveTextContent('first 200 items');
  });

  it('shows Unassigned, not 3, for a drill-down row without a tier', async () => {
    const drilldown: DrilldownDto = {
      metricId: 'unmatchedWithCandidates',
      dimensionId: 'equipment-asset',
      labelKey: 'metric.unmatchedWithCandidates',
      actionKey: 'action.none',
      ownerId: null,
      columnKeys: ['entity', 'tier', 'detail'],
      partial: null,
      rows: [
        { id: 'a', tier: 1, ownerId: '', classes: {}, cells: { entity: 'Pump A', tier: '1', detail: 'd' } },
        { id: 'b', tier: null, ownerId: '', classes: {}, cells: { entity: 'Loose B', tier: '', detail: 'd' } },
      ],
    };
    renderDimension(
      dimension({ aggregateRows: [metric({ metricId: 'unmatchedWithCandidates', hasDrilldown: true })] }),
      { metricId: 'unmatchedWithCandidates' },
      drilldown
    );

    const table = await screen.findByTestId('drilldown-table');
    await waitFor(() => {
      expect(within(table).getByText('Loose B')).toBeInTheDocument();
    });
    const loose = within(screen.getByTestId('drilldown-row-b'));
    expect(loose.getByText('Unassigned')).toBeInTheDocument();
    expect(loose.queryByText('3')).not.toBeInTheDocument();
    expect(within(screen.getByTestId('drilldown-row-a')).getByText('1')).toBeInTheDocument();
  });
});

describe('a scope with no rows at all', () => {
  it('still reads as no data, not as not applicable', async () => {
    renderDimension(dimension({ status: 'not-applicable', entityCount: 0, overallPercent: null, tierScores: [], aggregateRows: [] }));

    expect(await screen.findByText('No data for this scope')).toBeInTheDocument();
  });
});
