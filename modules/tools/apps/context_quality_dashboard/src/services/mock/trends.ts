import type {
  MetricId,
  SliceFilters,
  SnapshotPointDto,
  SnapshotScopeDto,
  TrendWindow,
  TrendSnapshot,
} from '../../types';

const WINDOW_DAYS: Record<TrendWindow, number> = {
  '7d': 7,
  '30d': 30,
  '90d': 90,
};

const FACILITY_OFFSETS: Record<string, number> = {
  'demo-north': -2,
  'demo-central': 1,
  'demo-south': -1,
};

function toSnapshotScope(filters: SliceFilters): SnapshotScopeDto {
  return {
    facilityId: filters.facilityId,
    divisionId: filters.divisionId,
    disciplineId: filters.disciplineId,
    ownerId: filters.ownerId,
  };
}

function scopeOffset(filters: SliceFilters): number {
  let offset = 0;
  if (filters.facilityId !== undefined) {
    offset += FACILITY_OFFSETS[filters.facilityId] ?? 0;
  }
  if (filters.divisionId === 'prod-b') {
    offset += 1;
  }
  if (filters.disciplineId === 'instrumentation') {
    offset -= 1;
  }
  if (filters.ownerId === 'instrumentation-lead') {
    offset -= 1;
  }
  return offset;
}

function adjustValue(valuePercent: number, filters: SliceFilters): number {
  const adjusted = valuePercent + scopeOffset(filters);
  return Math.max(0, Math.min(100, adjusted));
}

export function buildTrendPoints(
  snapshots: readonly TrendSnapshot[],
  metricId: MetricId,
  window: TrendWindow,
  filters: SliceFilters = {},
  referenceDateIso = '2026-09-01T00:00:00.000Z'
): SnapshotPointDto[] {
  const referenceMs = Date.parse(referenceDateIso);
  const windowMs = WINDOW_DAYS[window] * 86_400_000;
  const scope = toSnapshotScope(filters);

  return snapshots
    .filter((snapshot) => snapshot.metricId === metricId)
    .filter((snapshot) => referenceMs - Date.parse(snapshot.capturedAt) <= windowMs)
    .map((snapshot) => ({
      metricId,
      capturedAt: snapshot.capturedAt,
      valuePercent: adjustValue(snapshot.valuePercent, filters),
      scope,
      tier: snapshot.tier,
    }))
    .sort((left, right) => Date.parse(left.capturedAt) - Date.parse(right.capturedAt));
}

export function filterAlertsBySlice<T extends {
  facilityId: string | null;
  ownerId: string | null;
}>(
  alerts: readonly T[],
  filters: { facilityId?: string; ownerId?: string }
): T[] {
  return alerts.filter((alert) => {
    if (filters.facilityId !== undefined && alert.facilityId !== filters.facilityId) {
      return false;
    }
    if (filters.ownerId !== undefined && alert.ownerId !== filters.ownerId) {
      return false;
    }
    return true;
  });
}
