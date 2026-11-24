import { describe, expect, it } from 'vitest';

import { DEFAULT_APP_CONFIG } from '../../mock/config';
import { MOCK_DATASET } from '../../mock/dataset';

import {
  buildQualityIssues,
  computeTierScores,
  filterIssues,
  getEnabledDimensions,
} from './aggregation';
import {
  isEligibleTimeSeries,
  isSyntheticTimeSeries,
  isUserExcludedEquipment,
  matchesTagPattern,
} from './exclusionLogic';

describe('metric aggregation', () => {
  it('computes tier scores for asset hierarchy', () => {
    const { tierScores, overallPercent } = computeTierScores(
      MOCK_DATASET,
      DEFAULT_APP_CONFIG,
      'asset-hierarchy',
      {}
    );

    expect(tierScores).toHaveLength(3);
    expect(tierScores[0]?.tier).toBe(1);
    expect(overallPercent).toBeGreaterThan(0);
    expect(overallPercent).toBeLessThanOrEqual(100);
  });

  it('changes issue counts when facility slice is applied', () => {
    const issues = buildQualityIssues(MOCK_DATASET, DEFAULT_APP_CONFIG);
    const allOrphans = filterIssues(issues, {}, 'time-series', 'orphansByPrefix');
    const habshanOrphans = filterIssues(
      issues,
      { facilityId: 'demo-north' },
      'time-series',
      'orphansByPrefix'
    );

    expect(habshanOrphans.length).toBeLessThan(allOrphans.length);
    expect(habshanOrphans.every((issue) => issue.facilityId === 'demo-north')).toBe(true);
  });
});

describe('issue filtering', () => {
  it('includes inferred tag gaps but excludes synthetic time series from orphan prefix issues', () => {
    const issues = buildQualityIssues(MOCK_DATASET, DEFAULT_APP_CONFIG);
    const orphanIssues = filterIssues(issues, {}, 'time-series', 'orphansByPrefix');
    const inferredIssues = filterIssues(issues, {}, 'time-series', 'inferredTagsWithoutMatch');

    expect(orphanIssues.some((issue) => issue.entityLabel.includes('CALC-'))).toBe(false);
    expect(orphanIssues.some((issue) => issue.entityLabel.includes('VIRTUAL-'))).toBe(false);
    expect(inferredIssues.some((issue) => issue.entityLabel === 'PI-44102.PV')).toBe(true);
  });
});

describe('exclusion rule application', () => {
  it('matches tag patterns for user exclusion rules', () => {
    expect(matchesTagPattern('CALC-LOAD-01', 'CALC-*')).toBe(true);
    expect(matchesTagPattern('TI-12.PV', 'CALC-*')).toBe(false);
  });

  it('treats synthetic and user-excluded time series as ineligible', () => {
    const calcSeries = MOCK_DATASET.timeSeries.find((ts) => ts.externalId === 'CALC-LOAD-01');
    const physicalSeries = MOCK_DATASET.timeSeries.find((ts) => ts.externalId === 'PI-COMP3-001.PV');

    expect(calcSeries).toBeDefined();
    expect(physicalSeries).toBeDefined();
    expect(isSyntheticTimeSeries(calcSeries!)).toBe(true);
    expect(isEligibleTimeSeries(calcSeries!, DEFAULT_APP_CONFIG)).toBe(false);
    expect(isEligibleTimeSeries(physicalSeries!, DEFAULT_APP_CONFIG)).toBe(true);
  });

  it('excludes decommissioned equipment when lifecycle rule is enabled', () => {
    const decommissioned = MOCK_DATASET.equipment.find((eq) => eq.status === 'decommissioned');
    expect(decommissioned).toBeDefined();
    expect(isUserExcludedEquipment(decommissioned!, DEFAULT_APP_CONFIG)).toBe(true);
  });
});

describe('dimension visibility flags', () => {
  it('hides optional dimensions when config flags are off', () => {
    const disabledConfig = {
      ...DEFAULT_APP_CONFIG,
      model3dAvailable: false,
      maintenanceDimensionEnabled: false,
      failureTaxonomyAvailable: false,
      fileAnnotationAcceleratorAvailable: false,
      lifecycleMetadataAvailable: false,
    };

    const enabled = getEnabledDimensions(disabledConfig);
    expect(enabled).not.toContain('model-3d');
    expect(enabled).not.toContain('maintenance');
    expect(enabled).toContain('asset-hierarchy');
    expect(enabled).toContain('files');
  });
});
