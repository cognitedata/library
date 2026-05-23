import type { GridRow } from "../types/discoveryNodes";
import { formatGridCell } from "./gridFormat";

export function filterGridRows(
  items: GridRow[],
  columns: string[],
  query: string
): GridRow[] {
  const q = query.trim().toLowerCase();
  if (!q) return items;
  return items.filter((row) =>
    columns.some((col) => formatGridCell(row[col]).toLowerCase().includes(q))
  );
}
