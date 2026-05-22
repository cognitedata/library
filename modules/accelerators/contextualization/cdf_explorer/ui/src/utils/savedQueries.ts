import type { SavedQuery, SqlDocumentTab, TreeNode } from "../types/explorerNodes";

const SAVED_QUERY_TAB_PREFIX = "sql:saved:";

export function sqlSavedQueryTabId(savedQueryId: string): string {
  return `${SAVED_QUERY_TAB_PREFIX}${savedQueryId}`;
}

export function savedQueryIdFromTabId(tabId: string): string | null {
  if (!tabId.startsWith(SAVED_QUERY_TAB_PREFIX)) return null;
  const id = tabId.slice(SAVED_QUERY_TAB_PREFIX.length);
  return id.trim() ? id : null;
}

export function slugifySavedQueryId(name: string): string {
  let slug = name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  if (!slug) slug = "query";
  if (!/^[a-z]/.test(slug)) slug = `q_${slug}`;
  return slug.slice(0, 128);
}

export function uniqueSavedQueryId(base: string, existingIds: ReadonlySet<string>): string {
  if (!existingIds.has(base)) return base;
  let n = 2;
  while (existingIds.has(`${base}_${n}`)) n += 1;
  return `${base}_${n}`;
}

export function createSqlTabFromSavedQuery(q: SavedQuery): SqlDocumentTab {
  return {
    kind: "sql",
    id: sqlSavedQueryTabId(q.id),
    label: q.name,
    query: q.query,
    limit: q.limit,
    convertToString: q.convert_to_string,
    savedQueryId: q.id,
    result: null,
    loading: false,
    error: null,
    pageSize: 100,
    pageIndex: 0,
    selectedRowIndex: null,
  };
}

export function savedQueryFromNode(node: TreeNode): SavedQuery | null {
  if (node.kind !== "saved_query" || !node.meta) return null;
  const id = typeof node.meta.saved_query_id === "string" ? node.meta.saved_query_id : null;
  const name = typeof node.meta.name === "string" ? node.meta.name : node.label;
  const query = typeof node.meta.query === "string" ? node.meta.query : null;
  if (!id || !query) return null;
  return {
    id,
    name: name.trim() || id,
    query,
    limit: typeof node.meta.limit === "number" ? node.meta.limit : 100,
    convert_to_string:
      typeof node.meta.convert_to_string === "boolean" ? node.meta.convert_to_string : true,
  };
}

export function savedQueryEntryFromSqlTab(tab: SqlDocumentTab, name: string, id: string): SavedQuery {
  return {
    id,
    name: name.trim() || id,
    query: tab.query,
    limit: tab.limit,
    convert_to_string: tab.convertToString,
  };
}
