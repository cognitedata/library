/**
 * Reusable pagination hook.
 */

import { useCallback, useMemo, useState } from "react";

import { getAppConfig } from "@/config/appConfig";

export function getDefaultPageSize(): number {
  return getAppConfig().limits.defaultPageSize;
}

interface UsePaginationOptions<T> {
  items: T[];
  pageSize?: number;
  initialPage?: number;
}

interface UsePaginationResult<T> {
  currentPage: number;
  totalPages: number;
  totalItems: number;
  pageSize: number;
  paginatedItems: T[];
  hasNextPage: boolean;
  hasPreviousPage: boolean;
  goToPage: (page: number) => void;
  nextPage: () => void;
  previousPage: () => void;
  setPageSize: (size: number) => void;
  resetPagination: () => void;
}

export function usePagination<T>({
  items,
  pageSize: initialPageSize = getDefaultPageSize(),
  initialPage = 1,
}: UsePaginationOptions<T>): UsePaginationResult<T> {
  const [currentPage, setCurrentPage] = useState(initialPage);
  const [pageSize, setPageSizeState] = useState(initialPageSize);

  const totalItems = items.length;
  const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));

  // Reset to page 1 if current page exceeds total pages
  const validCurrentPage = useMemo(() => {
    return Math.min(Math.max(1, currentPage), totalPages);
  }, [currentPage, totalPages]);

  const paginatedItems = useMemo(() => {
    const startIndex = (validCurrentPage - 1) * pageSize;
    const endIndex = startIndex + pageSize;
    return items.slice(startIndex, endIndex);
  }, [items, validCurrentPage, pageSize]);

  const hasNextPage = validCurrentPage < totalPages;
  const hasPreviousPage = validCurrentPage > 1;

  const goToPage = useCallback(
    (page: number) => {
      const validPage = Math.min(Math.max(1, page), totalPages);
      setCurrentPage(validPage);
    },
    [totalPages],
  );

  const nextPage = useCallback(() => {
    if (hasNextPage) {
      setCurrentPage((prev) => prev + 1);
    }
  }, [hasNextPage]);

  const previousPage = useCallback(() => {
    if (hasPreviousPage) {
      setCurrentPage((prev) => prev - 1);
    }
  }, [hasPreviousPage]);

  const setPageSize = useCallback((size: number) => {
    setPageSizeState(Math.max(1, size));
    setCurrentPage(1); // Reset to first page when page size changes
  }, []);

  const resetPagination = useCallback(() => {
    setCurrentPage(initialPage);
    setPageSizeState(initialPageSize);
  }, [initialPage, initialPageSize]);

  return {
    currentPage: validCurrentPage,
    totalPages,
    totalItems,
    pageSize,
    paginatedItems,
    hasNextPage,
    hasPreviousPage,
    goToPage,
    nextPage,
    previousPage,
    setPageSize,
    resetPagination,
  };
}
