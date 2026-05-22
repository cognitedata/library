import type { OpenTarget, SqlDocumentTab } from "../types/explorerNodes";
import { openTargetKey, tabLabelForTarget } from "../types/explorerNodes";
import { sqlQueryForOpenTarget } from "./sqlQuerySeed";
import { DEFAULT_PAGE_SIZE } from "./pagination";

export const SQL_WORKSPACE_TAB_ID = "sql:workspace";

export const DEFAULT_SQL_QUERY =
  "SELECT * FROM cdf_nodes('cdf_cdm', 'CogniteAsset', 'v1')";

export function createSqlTab(opts?: {
  id?: string;
  label?: string;
  query?: string;
}): SqlDocumentTab {
  return {
    kind: "sql",
    id: opts?.id ?? `sql:${Date.now()}`,
    label: opts?.label ?? "SQL Query",
    query: opts?.query ?? DEFAULT_SQL_QUERY,
    limit: 100,
    convertToString: true,
    result: null,
    loading: false,
    error: null,
    pageSize: DEFAULT_PAGE_SIZE,
    pageIndex: 0,
    selectedRowIndex: null,
  };
}

export function sqlTabLabelFromQuery(query: string, fallback: string): string {
  const line = query.trim().split(/\s+/).slice(0, 6).join(" ");
  if (!line) return fallback;
  return line.length > 48 ? `${line.slice(0, 45)}…` : line;
}

/** Stable document tab id for an explorer ``open_target``. */
export function sqlTabKeyForOpenTarget(target: OpenTarget): string {
  return `sql:${openTargetKey(target)}`;
}

export function createSqlTabForOpenTarget(
  target: OpenTarget,
  label?: string
): SqlDocumentTab | null {
  const query = sqlQueryForOpenTarget(target);
  if (!query) return null;
  return createSqlTab({
    id: sqlTabKeyForOpenTarget(target),
    label: label?.trim() || tabLabelForTarget(target),
    query,
  });
}
