import { useCallback, useState } from "react";

export function useFilterState<T extends Record<string, string>>(initialState: T) {
  const [filters, setFilters] = useState<T>(initialState);

  const setFilter = useCallback(<K extends keyof T>(key: K, value: T[K]) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
  }, []);

  const resetFilters = useCallback(() => {
    setFilters(initialState);
  }, [initialState]);

  return {
    filters,
    setFilter,
    setFilters,
    resetFilters,
  };
}
