import type {
  DataModelGraph,
  DataModelRef,
  FunctionDetail,
  SavedQuery,
  SavedWorkspace,
  TransformationDetail,
  WorkflowGraph,
  WorkflowRef,
} from "./types/explorerNodes";

const API = "";

export type ConnectionInfo = {
  project: string;
  base_url: string;
  auth_mode: string;
};

export async function fetchConnection(): Promise<ConnectionInfo> {
  const r = await fetch(`${API}/api/connection`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<ConnectionInfo>;
}

export type ExplorerConfig = {
  stars: { node_ids: string[] };
  workspace: SavedWorkspace;
  saved_queries: { queries: SavedQuery[] };
};

export async function fetchExplorerConfig(): Promise<ExplorerConfig> {
  const r = await fetch(`${API}/api/cdf/explorer/config`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<ExplorerConfig>;
}

export async function saveExplorerStars(nodeIds: string[]): Promise<ExplorerConfig> {
  const r = await fetch(`${API}/api/cdf/explorer/config/stars`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ node_ids: nodeIds }),
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<ExplorerConfig>;
}

export async function saveExplorerSavedQueries(
  queries: SavedQuery[]
): Promise<{ saved_queries: { queries: SavedQuery[] } }> {
  const r = await fetch(`${API}/api/cdf/explorer/config/saved-queries`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ queries }),
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ saved_queries: { queries: SavedQuery[] } }>;
}

export async function saveExplorerWorkspace(workspace: SavedWorkspace): Promise<{ workspace: SavedWorkspace }> {
  const r = await fetch(`${API}/api/cdf/explorer/config/workspace`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(workspace),
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ workspace: SavedWorkspace }>;
}

export async function fetchTreeChildren(nodeId: string, signal?: AbortSignal) {
  const r = await fetch(
    `${API}/api/cdf/explorer/children?${new URLSearchParams({ node_id: nodeId })}`,
    { signal }
  );
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{
    nodes: import("./types/explorerNodes").TreeNode[];
    stars?: string[];
  }>;
}

export type SqlRunResult = {
  columns: string[];
  items: Record<string, unknown>[];
  schema: { name?: string | null; type?: string | null }[];
  row_count: number;
};

export async function fetchDataModelGraph(ref: DataModelRef): Promise<DataModelGraph> {
  const params = new URLSearchParams({
    space: ref.space,
    external_id: ref.external_id,
    version: ref.version,
  });
  const r = await fetch(`${API}/api/cdf/data-modeling/data-model/graph?${params}`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<DataModelGraph>;
}

export async function fetchTransformationDetail(transformationId: number): Promise<TransformationDetail> {
  const params = new URLSearchParams({ id: String(transformationId) });
  const r = await fetch(`${API}/api/cdf/transformations/detail?${params}`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<TransformationDetail>;
}

export async function fetchWorkflowGraph(ref: WorkflowRef): Promise<WorkflowGraph> {
  const params = new URLSearchParams({ external_id: ref.external_id });
  if (ref.version?.trim()) params.set("version", ref.version.trim());
  const r = await fetch(`${API}/api/cdf/workflows/graph?${params}`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<WorkflowGraph>;
}

export async function fetchFunctionDetail(functionId: string): Promise<FunctionDetail> {
  const params = new URLSearchParams({ id: functionId });
  const r = await fetch(`${API}/api/cdf/functions/detail?${params}`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<FunctionDetail>;
}

export async function runSqlQuery(body: {
  query: string;
  limit?: number;
  source_limit?: number;
  convert_to_string?: boolean;
  timeout?: number;
}): Promise<SqlRunResult> {
  const r = await fetch(`${API}/api/cdf/sql/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const errBody = await r.json().catch(() => ({}));
    throw new Error(String((errBody as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<SqlRunResult>;
}
