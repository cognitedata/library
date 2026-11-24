import { useQuery } from '@tanstack/react-query';
import { useCallback, useMemo } from 'react';

import type { FilterKey, PageId } from '../../app/state/types';
import { useAppState } from '../../app/state/useAppState';
import { useThemeMode } from '../../app/useThemeMode';
import { useLanguage } from '../../i18n/useLanguage';
import { useCatalogService } from '../../services/useServices';
import type { Location, SliceFilters, TrendWindow } from '../../types';

import { useScopedFilters } from './useScopedFilters';

export type FilterOption = {
  id: string;
  label: string;
};

export type AppShellViewModel = {
  page: PageId;
  filters: SliceFilters;
  trendWindow: TrendWindow;
  pageLabel: string;
  showFilters: boolean;
  showFilterFields: boolean;
  spaces: FilterOption[];
  /** The locations to choose from, parents before their children (a child's label is indented). Empty when there are none. */
  locations: FilterOption[];
  isCatalogLoading: boolean;
  language: 'en' | 'ja';
  themeMode: 'light' | 'dark';
  setPage: (page: PageId) => void;
  setFilter: (key: FilterKey, value?: string) => void;
  setTrendWindow: (window: TrendWindow) => void;
  setLanguage: (language: 'en' | 'ja') => void;
  setTheme: (mode: 'light' | 'dark') => void;
  t: (key: string) => string;
};

export function useAppShellViewModel(): AppShellViewModel {
  const { state, dispatch } = useAppState();
  const filters = useScopedFilters();
  const { t, language, setLanguage } = useLanguage();
  const { mode, setTheme } = useThemeMode();
  const catalogService = useCatalogService();
  const spacesQuery = useQuery({
    queryKey: ['catalog', 'facilities'],
    queryFn: () => catalogService.getFacilities(),
  });

  const locationsQuery = useQuery({
    queryKey: ['catalog', 'locations'],
    queryFn: () => catalogService.getLocations(),
  });
  const locations = useMemo(() => locationOptions(locationsQuery.data ?? []), [locationsQuery.data]);

  const spaces = useMemo(
    () =>
      (spacesQuery.data ?? []).map((facility) => ({
        id: facility.id,
        label: facility.name,
      })),
    [spacesQuery.data]
  );

  const setPage = useCallback(
    (page: PageId) => {
      dispatch({ type: 'setPage', page });
    },
    [dispatch]
  );

  const setFilter = useCallback(
    (key: FilterKey, value?: string) => {
      dispatch({ type: 'setFilter', key, value });
    },
    [dispatch]
  );

  const setTrendWindow = useCallback(
    (trendWindow: TrendWindow) => {
      dispatch({ type: 'setTrendWindow', trendWindow });
    },
    [dispatch]
  );

  const pageLabel = t(`page.${state.page}`);

  return {
    page: state.page,
    filters,
    trendWindow: state.trendWindow,
    pageLabel,
    showFilters: state.page !== 'configuration' && state.page !== 'issues',
    showFilterFields: state.page !== 'issues' && state.page !== 'dimensions',
    spaces,
    locations,
    isCatalogLoading: spacesQuery.isLoading,
    language,
    themeMode: mode,
    setPage,
    setFilter,
    setTrendWindow,
    setLanguage,
    setTheme,
    t,
  };
}

/** Roots first, each followed by its children, so the select reads as a hierarchy. A parent that is not offered makes its children roots. */
export function locationOptions(locations: readonly Location[]): FilterOption[] {
  const offered = new Set(locations.map((location) => location.id));
  const byName = (a: Location, b: Location) => a.name.localeCompare(b.name);
  const children = (parentId: string | null) =>
    locations
      .filter((location) => (location.parentId !== null && offered.has(location.parentId) ? location.parentId : null) === parentId)
      .sort(byName);
  const walk = (parentId: string | null, depth: number, seen: ReadonlySet<string>): FilterOption[] =>
    children(parentId).flatMap((location) =>
      seen.has(location.id)
        ? []
        : [
            { id: location.id, label: `${depth === 0 ? '' : `${'\u00A0\u00A0'.repeat(depth)}↳ `}${location.name}` },
            ...walk(location.id, depth + 1, new Set([...seen, location.id])),
          ]
    );
  return walk(null, 0, new Set());
}
