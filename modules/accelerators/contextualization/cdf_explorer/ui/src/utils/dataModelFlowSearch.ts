import type { DataModelGraphView } from "../types/explorerNodes";

export function viewMatchesSearch(view: DataModelGraphView, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  const haystack = [
    view.external_id,
    view.name,
    view.space,
    view.version,
    view.id,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return haystack.includes(q);
}

export function filterViewsBySearch(views: DataModelGraphView[], query: string): DataModelGraphView[] {
  const q = query.trim();
  if (!q) return views;
  return views.filter((v) => viewMatchesSearch(v, q));
}
