export type SliceFilters = {
  facilityId?: string;
  /** The location chosen in the filter bar: a named group of facilities. */
  locationId?: string;
  /** The facilities of the chosen location, resolved from the run's location rows. A row must be in one of them. */
  facilityIds?: readonly string[];
  divisionId?: string;
  disciplineId?: string;
  ownerId?: string;
};

export function matchesSlice(
  entity: {
    facilityId: string;
    divisionId: string;
    disciplineId: string;
    ownerId: string;
  },
  filters: SliceFilters
): boolean {
  if (filters.facilityId !== undefined && entity.facilityId !== filters.facilityId) {
    return false;
  }
  if (filters.facilityIds !== undefined && !filters.facilityIds.includes(entity.facilityId)) {
    return false;
  }
  if (filters.divisionId !== undefined && entity.divisionId !== filters.divisionId) {
    return false;
  }
  if (filters.disciplineId !== undefined && entity.disciplineId !== filters.disciplineId) {
    return false;
  }
  if (filters.ownerId !== undefined && entity.ownerId !== filters.ownerId) {
    return false;
  }
  return true;
}
