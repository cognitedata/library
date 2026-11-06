import type { DimensionQualityStatus } from '../../types';

import type { CriticalityLevel } from './overviewPresentation';

export type QualityTone = 'critical' | 'weak' | 'good';

export function statusLabelKey(status: DimensionQualityStatus): string {
  switch (status) {
    case 'good':
      return 'overview.good';
    case 'weak':
      return 'overview.weak';
    case 'critical':
      return 'overview.critical';
    case 'disabled':
      return 'overview.disabled';
    case 'not-applicable':
      return 'overview.notApplicable';
  }
}

export function criticalityLabelKey(criticality: CriticalityLevel): string {
  return `overview.criticality.${criticality}`;
}

export function criticalityMeaningKey(criticality: CriticalityLevel): string {
  return `overview.criticalityMeaning.${criticality}`;
}

export function criticalityGroupLabelKey(criticality: CriticalityLevel): string {
  return `overview.criticalityGroup.${criticality}`;
}

export function badgeVariantForCriticality(
  criticality: CriticalityLevel
): 'error' | 'warning' | 'secondary' {
  if (criticality === 'critical') {
    return 'error';
  }
  if (criticality === 'important') {
    return 'warning';
  }
  return 'secondary';
}

export function badgeVariantForStatus(
  status: DimensionQualityStatus
): 'error' | 'secondary' {
  return status === 'critical' ? 'error' : 'secondary';
}

export function qualityToneFromGap(
  gapPts: number | null,
  overallPercent: number | null
): QualityTone {
  if (gapPts !== null) {
    if (gapPts < -30) {
      return 'critical';
    }
    if (gapPts < 0) {
      return 'weak';
    }
    return 'good';
  }
  if (overallPercent === null) {
    return 'weak';
  }
  if (overallPercent < 50) {
    return 'critical';
  }
  if (overallPercent < 80) {
    return 'weak';
  }
  return 'good';
}

export function qualityTextClassName(tone: QualityTone): string {
  if (tone === 'critical') {
    return 'text-destructive-foreground';
  }
  if (tone === 'weak') {
    return 'text-warning-foreground';
  }
  return 'text-success-foreground';
}

export function qualityProgressClassName(tone: QualityTone): string {
  if (tone === 'critical') {
    return '[&_[data-slot=progress-indicator]]:!bg-destructive-foreground';
  }
  if (tone === 'weak') {
    return '[&_[data-slot=progress-indicator]]:!bg-warning-foreground';
  }
  return '[&_[data-slot=progress-indicator]]:!bg-success-foreground';
}

export function formatSignedPts(gapPts: number, ptsLabel: string): string {
  if (gapPts === 0) {
    return `0 ${ptsLabel}`;
  }
  const sign = gapPts > 0 ? '+' : '−';
  return `${sign}${Math.abs(gapPts)} ${ptsLabel}`;
}

export function trendGlyph(direction: 'up' | 'down' | 'stable'): string {
  if (direction === 'up') {
    return '↑';
  }
  if (direction === 'down') {
    return '↓';
  }
  return '→';
}
