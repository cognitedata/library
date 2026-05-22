export type GridRow = Record<string, unknown>;

export function sqlPageCount(totalItems: number, pageSize: number): number {
  if (totalItems <= 0) return 1;
  return Math.max(1, Math.ceil(totalItems / pageSize));
}

export function sqlPageItems(items: GridRow[], pageIndex: number, pageSize: number): GridRow[] {
  const start = pageIndex * pageSize;
  return items.slice(start, start + pageSize);
}

export function clampSqlPageIndex(pageIndex: number, totalItems: number, pageSize: number): number {
  const maxPage = sqlPageCount(totalItems, pageSize) - 1;
  return Math.max(0, Math.min(pageIndex, maxPage));
}
