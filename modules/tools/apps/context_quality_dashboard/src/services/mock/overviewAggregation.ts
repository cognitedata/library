import {
  ALL_DIMENSION_IDS,
  OPTIONAL_DIMENSION_IDS,
  computeDimensionStatus,
  computePortfolioAggregatePercent,
  findWeakestTier,
  isOptionalDimension,
} from '../../domain/dimensions';
import type { MockDataset } from '../../mock/dataset';
import type {
  AppConfigDto,
  DimensionId,
  MetricId,
  OverviewTrendDto,
  SliceFilters,
  Tier,
  TierScore,
} from '../../types';
import { TIER_TARGETS } from '../../types';


import { computeTierScores, getEnabledDimensions, sliceFilter } from './aggregation';
import { buildTrendPoints } from './trends';

export function countScopedEntities(dataset: MockDataset, filters: SliceFilters): number {
  return (
    sliceFilter(dataset.assets, filters).length +
    sliceFilter(dataset.equipment, filters).length +
    sliceFilter(dataset.timeSeries, filters).length +
    sliceFilter(dataset.maintenance, filters).length +
    sliceFilter(dataset.fileAnnotations, filters).length +
    sliceFilter(dataset.model3d, filters).length +
    sliceFilter(dataset.files, filters).length
  );
}

export function computePortfolioTierScores(
  dataset: MockDataset,
  config: AppConfigDto,
  filters: SliceFilters
): TierScore[] {
  const enabledDimensions = getEnabledDimensions(config);

  return ([1, 2, 3] as Tier[]).map((tier) => {
    if (enabledDimensions.length === 0) {
      return {
        tier,
        scorePercent: null,
        targetPercent: TIER_TARGETS[tier - 1]?.targetPercent ?? 0,
        scored: false,
      };
    }

    const scores = enabledDimensions
      .map((dimensionId) => {
        const { tierScores } = computeTierScores(dataset, config, dimensionId, filters);
        const tierScore = tierScores.find((entry) => entry.tier === tier);
        return tierScore?.scorePercent;
      })
      .filter((score): score is number => score !== null && score !== undefined);

    if (scores.length === 0) {
      return {
        tier,
        scorePercent: null,
        targetPercent: TIER_TARGETS[tier - 1]?.targetPercent ?? 0,
        scored: tier !== 3,
      };
    }

    const average = Math.round(scores.reduce((sum, score) => sum + score, 0) / scores.length);
    return {
      tier,
      scorePercent: average,
      targetPercent: TIER_TARGETS.find((entry) => entry.tier === tier)?.targetPercent ?? 0,
      scored: tier !== 3,
    };
  });
}

export function countDimensionEntities(
  dataset: MockDataset,
  dimensionId: DimensionId,
  filters: SliceFilters
): number {
  switch (dimensionId) {
    case 'asset-hierarchy':
      return sliceFilter(dataset.assets, filters).length;
    case 'equipment-asset':
      return sliceFilter(dataset.equipment, filters).length;
    case 'time-series':
      return sliceFilter(dataset.timeSeries, filters).length;
    case 'maintenance':
      return sliceFilter(dataset.maintenance, filters).length;
    case 'file-annotation':
      return sliceFilter(dataset.fileAnnotations, filters).length;
    case 'model-3d':
      return sliceFilter(dataset.model3d, filters).length;
    case 'files':
      return sliceFilter(dataset.files, filters).length;
  }
}

function averagePercents(values: readonly number[]): number {
  return Math.round(values.reduce((sum, value) => sum + value, 0) / values.length);
}

export function computeOverallTrend(
  snapshots: MockDataset['snapshots'],
  metricIds: readonly MetricId[],
  filters: SliceFilters
): OverviewTrendDto | null {
  const currentValues: number[] = [];
  const previousValues: number[] = [];

  for (const metricId of metricIds) {
    const points = buildTrendPoints(snapshots, metricId, '30d', filters);
    const current = points[points.length - 1];
    const previous = points[points.length - 2];
    if (current === undefined || previous === undefined) {
      continue;
    }
    currentValues.push(current.valuePercent);
    previousValues.push(previous.valuePercent);
  }

  if (currentValues.length === 0) {
    return null;
  }

  const currentPercent = averagePercents(currentValues);
  const previousPercent = averagePercents(previousValues);
  return {
    currentPercent,
    previousPercent,
    changePts: currentPercent - previousPercent,
  };
}

export {
  ALL_DIMENSION_IDS,
  OPTIONAL_DIMENSION_IDS,
  computeDimensionStatus,
  computePortfolioAggregatePercent,
  findWeakestTier,
  isOptionalDimension,
};
