import type { SliceFilters } from './filters';

export function sliceFiltersQueryKey(filters: SliceFilters): (string | null)[] {
  return [
    filters.facilityId ?? null,
    filters.facilityIds === undefined ? null : [...filters.facilityIds].sort().join(','),
    filters.divisionId ?? null,
    filters.disciplineId ?? null,
    filters.ownerId ?? null,
  ];
}
