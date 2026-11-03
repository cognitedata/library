/**
 * List pagination helpers.
 */

export interface PaginationResult<T> {
  items: T[];
  totalPages: number;
  currentPage: number;
  totalItems: number;
}

/**
 * Paginate a list of items
 */
export function paginateItems<T>(
  items: T[],
  page = 1,
  pageSize = 50,
): PaginationResult<T> {
  const totalItems = items.length;
  const totalPages = Math.ceil(totalItems / pageSize) || 1;

  const currentPage = Math.max(1, Math.min(page, totalPages));

  const startIdx = (currentPage - 1) * pageSize;
  const endIdx = startIdx + pageSize;

  return {
    items: items.slice(startIdx, endIdx),
    totalPages,
    currentPage,
    totalItems,
  };
}
