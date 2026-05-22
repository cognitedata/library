import type { GridRow } from "../types/explorerNodes";

export type SortDirection = "asc" | "desc";

export type GridSort = {
  column: string;
  direction: SortDirection;
};

function sortRank(value: unknown): number | string | null {
  if (value == null) return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed !== "" && Number.isFinite(Number(trimmed))) return Number(trimmed);
    return trimmed.toLowerCase();
  }
  try {
    return JSON.stringify(value).toLowerCase();
  } catch {
    return String(value).toLowerCase();
  }
}

export function compareGridRows(a: GridRow, b: GridRow, column: string, direction: SortDirection): number {
  const av = sortRank(a[column]);
  const bv = sortRank(b[column]);
  if (av == null && bv == null) return 0;
  if (av == null) return 1;
  if (bv == null) return -1;
  let cmp = 0;
  if (typeof av === "number" && typeof bv === "number") {
    cmp = av - bv;
  } else {
    cmp = String(av).localeCompare(String(bv), undefined, { numeric: true, sensitivity: "base" });
  }
  return direction === "asc" ? cmp : -cmp;
}

export function sortGridRows(items: GridRow[], sort: GridSort): GridRow[] {
  if (!sort.column || items.length < 2) return items;
  return [...items].sort((a, b) => compareGridRows(a, b, sort.column, sort.direction));
}

export function nextGridSort(current: GridSort | null, column: string): GridSort | null {
  if (!current || current.column !== column) return { column, direction: "asc" };
  if (current.direction === "asc") return { column, direction: "desc" };
  return null;
}
