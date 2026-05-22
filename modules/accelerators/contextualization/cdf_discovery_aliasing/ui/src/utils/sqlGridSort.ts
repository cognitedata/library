import type { GridRow } from "./sqlPagination";

export type GridSort = { column: string; direction: "asc" | "desc" };

export function nextGridSort(prev: GridSort | null, column: string): GridSort {
  if (prev?.column === column) {
    return { column, direction: prev.direction === "asc" ? "desc" : "asc" };
  }
  return { column, direction: "asc" };
}

function compareCell(a: unknown, b: unknown): number {
  if (a == null && b == null) return 0;
  if (a == null) return -1;
  if (b == null) return 1;
  if (typeof a === "number" && typeof b === "number") return a - b;
  return String(a).localeCompare(String(b), undefined, { numeric: true });
}

export function sortGridRows(items: GridRow[], sort: GridSort): GridRow[] {
  const { column, direction } = sort;
  const out = [...items];
  out.sort((ra, rb) => {
    const c = compareCell(ra[column], rb[column]);
    return direction === "asc" ? c : -c;
  });
  return out;
}
