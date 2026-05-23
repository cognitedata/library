import type { FileContentRef, OpenTarget, SqlDocumentTab } from "../types/explorerNodes";
import { openTargetKey, tabLabelForTarget } from "../types/explorerNodes";
import { fileContentTabLabel } from "./queryableFileFromRow";
import { sqlQueryForOpenTarget } from "./sqlQuerySeed";
import { DEFAULT_PAGE_SIZE } from "./pagination";

export const SQL_WORKSPACE_TAB_ID = "sql:workspace";

export const DEFAULT_SQL_QUERY =
  "SELECT * FROM cdf_nodes('cdf_cdm', 'CogniteAsset', 'v1')";

export const DEFAULT_FILE_CONTENT_QUERY = "SELECT * FROM data";

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
    engine: "cdf",
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

export function fileContentTabKey(ref: FileContentRef): string {
  const key =
    ref.file_id != null
      ? String(ref.file_id)
      : encodeURIComponent(ref.external_id ?? "unknown");
  return `sql:file:${ref.format}:${key}`;
}

export function createFileContentSqlTab(ref: FileContentRef, label?: string): SqlDocumentTab {
  return {
    ...createSqlTab({
      id: fileContentTabKey(ref),
      label: label?.trim() || fileContentTabLabel(ref),
      query: DEFAULT_FILE_CONTENT_QUERY,
    }),
    engine: "file_content",
    fileContent: ref,
  };
}
