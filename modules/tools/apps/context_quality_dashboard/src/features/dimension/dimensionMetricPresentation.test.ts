import { describe, expect, it } from 'vitest';

import { translate } from '../../i18n/messages';
import type { AggregateMetricRowDto, DimensionDto } from '../../types';

import {
  aggregateRowsForMetricCards,
  canOpenMetricDrilldown,
  formatMetricReadiness,
  headlineMetricRow,
  metricDescriptionKey,
  metricDrilldownDisabledTooltipMessage,
  metricCardDrilldownButtonClassName,
  metricDrilldownUnavailableDetail,
  showsMetricDrilldownUnavailableCallout,
} from './dimensionMetricPresentation';

describe(metricDescriptionKey.name, () => {
  it('should use a stable i18n key per metric id', () => {
    expect(metricDescriptionKey('orphanRate')).toBe('dimension.metric.description.orphanRate');
    expect(metricDescriptionKey('woCompletionRate')).toBe('dimension.metric.description.woCompletionRate');
  });
});

describe(formatMetricReadiness.name, () => {
  it('formats passing and eligible counts instead of a percentage', () => {
    const text = formatMetricReadiness(sampleMetric(), (key) => translate(key, 'en'));
    expect(text).toBe('39 / 40');
  });

  it('keeps orphan rate as an orphan count phrase', () => {
    const text = formatMetricReadiness(
      sampleMetric({ metricId: 'orphanRate', issueCount: 12, eligibleCount: 50, passingCount: 38 }),
      (key) => translate(key, 'en')
    );
    expect(text).toBe('12 orphans of 50 assets');
  });
});

describe(aggregateRowsForMetricCards.name, () => {
  it('omits the primary headline metric from the metrics grid', () => {
    const rows = aggregateRowsForMetricCards('asset-hierarchy', [
      sampleMetric(),
      sampleMetric({ metricId: 'orphanRate', labelKey: 'metric.orphanRate' }),
    ]);
    expect(rows.map((row) => row.metricId)).toEqual(['orphanRate']);
  });
});

describe(headlineMetricRow.name, () => {
  it('picks the primary metric for the dimension', () => {
    const row = headlineMetricRow(sampleDimension());
    expect(row?.metricId).toBe('hierarchyCompletionRate');
  });
});

describe(canOpenMetricDrilldown.name, () => {
  it('should allow drilldown when the metric has a list and is applicable', () => {
    expect(canOpenMetricDrilldown(sampleMetric({ hasDrilldown: true }))).toBe(true);
  });

  it('should block drilldown when there is no entity list', () => {
    expect(canOpenMetricDrilldown(sampleMetric({ hasDrilldown: false }))).toBe(false);
  });

  it('should block drilldown when the metric is not applicable', () => {
    expect(
      canOpenMetricDrilldown(
        sampleMetric({ hasDrilldown: true, notApplicable: true, notApplicableReason: { kind: 'no-eligible-entities', mapping: null } })
      )
    ).toBe(false);
  });
});

describe(metricDrilldownUnavailableDetail.name, () => {
  const t = (key: string) => translate(key, 'en');

  it('should explain when there is no entity drilldown', () => {
    expect(metricDrilldownUnavailableDetail(sampleMetric({ hasDrilldown: false }), t)).toBe(
      'This metric has no entity drilldown.'
    );
  });

  it('should omit detail when the metric is not applicable', () => {
    expect(
      metricDrilldownUnavailableDetail(
        sampleMetric({
          notApplicable: true,
          notApplicableReason: { kind: 'mapping-empty', mapping: 'tierByTypeCode' },
        }),
        t
      )
    ).toBeNull();
  });
});

describe(metricDrilldownDisabledTooltipMessage.name, () => {
  const t = (key: string) => translate(key, 'en');

  it('should return null when drilldown can be opened', () => {
    expect(metricDrilldownDisabledTooltipMessage(sampleMetric({ hasDrilldown: true }), t)).toBeNull();
  });

  it('should return the no-entity-list message when applicable but not drillable', () => {
    expect(metricDrilldownDisabledTooltipMessage(sampleMetric({ hasDrilldown: false }), t)).toBe(
      'This metric has no entity drilldown.'
    );
  });
});

describe(showsMetricDrilldownUnavailableCallout.name, () => {
  it('should show the callout only for applicable metrics without a drilldown list', () => {
    expect(showsMetricDrilldownUnavailableCallout(sampleMetric({ hasDrilldown: false }))).toBe(true);
    expect(showsMetricDrilldownUnavailableCallout(sampleMetric({ hasDrilldown: true }))).toBe(false);
    expect(showsMetricDrilldownUnavailableCallout(sampleMetric({ notApplicable: true }))).toBe(false);
  });
});

describe(metricCardDrilldownButtonClassName.name, () => {
  it('should use foreground emphasis when drilldown can open', () => {
    expect(metricCardDrilldownButtonClassName(true)).toContain('text-foreground');
    expect(metricCardDrilldownButtonClassName(true)).toContain('font-medium');
  });

  it('should use muted styling when drilldown cannot open', () => {
    expect(metricCardDrilldownButtonClassName(false)).toContain('text-muted-foreground');
  });
});

function sampleMetric(partial: Partial<AggregateMetricRowDto> = {}): AggregateMetricRowDto {
  return {
    metricId: 'hierarchyCompletionRate',
    labelKey: 'metric.hierarchyCompletionRate',
    direction: 'critical',
    eligibleCount: 40,
    passingCount: 39,
    issueCount: 1,
    valuePercent: 98,
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

function sampleDimension(): DimensionDto {
  return {
    dimensionId: 'asset-hierarchy',
    nameKey: 'dimension.asset-hierarchy',
    enabled: true,
    optional: false,
    status: 'weak',
    entityCount: 40,
    tierScores: [],
    untiered: null,
    scoreBasis: 'tiered',
    overallPercent: 98,
    aggregateRows: [sampleMetric()],
    excludedBuckets: [],
    userExclusionSummary: [],
    insightIds: [],
    extendedMetricsAvailable: false,
  };
}
