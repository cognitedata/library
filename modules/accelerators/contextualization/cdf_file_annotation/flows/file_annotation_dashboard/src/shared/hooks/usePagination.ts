import { useCallback, useEffect, useMemo, useState } from "react";

interface UsePaginationOptions<T> {
  items: T[];
  initialPageSize?: number;
  resetToken?: string;
}

export function usePagination<T>({
  items,
  initialPageSize = 50,
  resetToken,
}: UsePaginationOptions<T>) {
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(initialPageSize);

  const totalPages = useMemo(() => {
    return Math.max(1, Math.ceil(items.length / pageSize));
  }, [items.length, pageSize]);

  useEffect(() => {
    setCurrentPage((prev) => Math.min(prev, totalPages));
  }, [totalPages]);

  useEffect(() => {
    setCurrentPage(1);
  }, [resetToken, pageSize]);

  const pagedItems = useMemo(() => {
    const startIndex = (currentPage - 1) * pageSize;
    return items.slice(startIndex, startIndex + pageSize);
  }, [items, currentPage, pageSize]);

  const pageRangeLabel = useMemo(() => {
    if (items.length === 0) {
      return "0 of 0";
    }
    const startIndex = (currentPage - 1) * pageSize + 1;
    const endIndex = Math.min(currentPage * pageSize, items.length);
    return `${startIndex}-${endIndex} of ${items.length}`;
  }, [items.length, currentPage, pageSize]);

  const goToPreviousPage = useCallback(() => {
    setCurrentPage((prev) => Math.max(1, prev - 1));
  }, []);

  const goToNextPage = useCallback(() => {
    setCurrentPage((prev) => Math.min(totalPages, prev + 1));
  }, [totalPages]);

  return {
    currentPage,
    pageSize,
    totalPages,
    pagedItems,
    pageRangeLabel,
    setCurrentPage,
    setPageSize,
    goToPreviousPage,
    goToNextPage,
  };
}
