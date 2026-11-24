import type { SnapshotPointDto } from '../../types';

import { trendGlyph } from './overviewLabels';

export function contextualisedCountAtPercent(total: number, valuePercent: number): number {
  return Math.round((total * valuePercent) / 100);
}

export function formatDimensionTrendPointChange(
  points: readonly SnapshotPointDto[],
  index: number,
  t: (key: string) => string
): string {
  if (index === 0) {
    return t('overview.trend.pointChangeNone');
  }
  const deltaPercent = points[index].valuePercent - points[index - 1].valuePercent;
  if (deltaPercent === 0) {
    return t('overview.trend.pointChangeNone');
  }
  const sign = deltaPercent > 0 ? '+' : '−';
  const direction: 'up' | 'down' = deltaPercent > 0 ? 'up' : 'down';
  return `${trendGlyph(direction)} ${sign}${Math.abs(deltaPercent)}% ${t('overview.trend.pointChangeSuffix')}`;
}

export function pickAxisLabelIndexes(length: number, maxLabels: number): number[] {
  if (length <= maxLabels) {
    return Array.from({ length }, (_, index) => index);
  }
  const step = (length - 1) / (maxLabels - 1);
  return Array.from({ length: maxLabels }, (_, index) => Math.round(index * step));
}

export function formatAxisDate(iso: string): string {
  return new Date(iso).toLocaleDateString('en-US', {
    timeZone: 'UTC',
    month: 'short',
    day: 'numeric',
  });
}

export function formatTooltipDate(iso: string): string {
  return new Date(iso).toLocaleDateString('en-US', {
    timeZone: 'UTC',
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
}
