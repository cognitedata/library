import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';

import { useAppState } from '../../app/state/useAppState';
import { useCatalogService } from '../../services/useServices';
import type { SliceFilters } from '../../types';

/**
 * The filters the pages read the backend with: the ones in the app state, plus the facilities of the chosen location.
 * A location is a name for a group of facilities, so the rows of those facilities are the rows of the location.
 * While the locations are loading, a chosen location covers no facility rather than every one.
 */
export function useScopedFilters(): SliceFilters {
  const { state } = useAppState();
  const catalogService = useCatalogService();
  const { locationId } = state.filters;
  const locations = useQuery({
    queryKey: ['catalog', 'locations'],
    queryFn: () => catalogService.getLocations(),
    enabled: locationId !== undefined,
  });

  return useMemo(() => {
    if (locationId === undefined) {
      return state.filters;
    }
    const chosen = locations.data?.find((location) => location.id === locationId);
    if (locations.data !== undefined && chosen === undefined) {
      const { locationId: _unknown, ...rest } = state.filters; // a location the run no longer offers is no filter
      return rest;
    }
    return { ...state.filters, facilityIds: chosen?.facilityIds ?? [] };
  }, [state.filters, locationId, locations.data]);
}
