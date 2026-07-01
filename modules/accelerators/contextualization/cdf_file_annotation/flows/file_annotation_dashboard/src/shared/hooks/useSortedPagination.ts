import { useEffect, useMemo, useState } from "react";

export type SortDirection = "asc" | "desc";

interface UseSortedPaginationOptions<T, F extends string> {
  items: T[];
  compare: (a: T, b: T, sortField: F) => number;
  resetToken?: string;
  initialPageSize?: number;
}

interface UseSortedPaginationResult<T, F extends string> {
  sortField: F | null;
  sortDirection: SortDirection;
  currentPage: number;
  pageSize: number;
  totalPages: number;
  startIndex: number;
  sortedItems: T[];
  pagedItems: T[];
  rangeLabel: string;
  setPageSize: (value: number) => void;
  setCurrentPage: (value: number | ((prev: number) => number)) => void;
  toggleSort: (field: F) => void;
}

export function useSortedPagination<T, F extends string>({
  items,
  compare,
  resetToken,
  initialPageSize = 50,
}: UseSortedPaginationOptions<T, F>): UseSortedPaginationResult<T, F> {
  const [sortField, setSortField] = useState<F | null>(null);
  const [sortDirection, setSortDirection] = useState<SortDirection>("asc");
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(initialPageSize);

  const sortedItems = useMemo(() => {
    if (!sortField) return items;

    const result = [...items];
    result.sort((a, b) => {
      const comparison = compare(a, b, sortField);
      return sortDirection === "desc" ? -comparison : comparison;
    });

    return result;
  }, [items, compare, sortField, sortDirection]);

  const totalPages = useMemo(() => {
    return Math.max(1, Math.ceil(sortedItems.length / pageSize));
  }, [sortedItems.length, pageSize]);

  useEffect(() => {
    if (currentPage > totalPages) {
      setCurrentPage(totalPages);
    }
  }, [currentPage, totalPages]);

  useEffect(() => {
    setCurrentPage(1);
  }, [sortField, sortDirection, pageSize, resetToken]);

  const startIndex = (currentPage - 1) * pageSize;

  const pagedItems = useMemo(() => {
    return sortedItems.slice(startIndex, startIndex + pageSize);
  }, [sortedItems, startIndex, pageSize]);

  const rangeLabel = useMemo(() => {
    if (sortedItems.length === 0) return "0 of 0";
    const start = startIndex + 1;
    const end = Math.min(startIndex + pageSize, sortedItems.length);
    return `${start}-${end} of ${sortedItems.length}`;
  }, [sortedItems.length, startIndex, pageSize]);

  const toggleSort = (field: F) => {
    if (sortField !== field) {
      setSortField(field);
      setSortDirection("asc");
      return;
    }

    if (sortDirection === "asc") {
      setSortDirection("desc");
      return;
    }

    setSortField(null);
    setSortDirection("asc");
  };

  return {
    sortField,
    sortDirection,
    currentPage,
    pageSize,
    totalPages,
    startIndex,
    sortedItems,
    pagedItems,
    rangeLabel,
    setPageSize,
    setCurrentPage,
    toggleSort,
  };
}
