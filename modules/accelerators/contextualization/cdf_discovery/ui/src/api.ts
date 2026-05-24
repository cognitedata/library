import type {
  DataModelGraph,
  DataModelRef,
  FunctionDetail,
  SavedQuery,
  SavedWorkspace,
  TransformationDetail,
  WorkflowGraph,
  WorkflowRef,
} from "./types/discoveryNodes";

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

export type DiscoveryConfig = {
  stars: { node_ids: string[] };
  workspace: SavedWorkspace;
  saved_queries: { queries: SavedQuery[] };
};

export async function fetchDiscoveryConfig(): Promise<DiscoveryConfig> {
  const r = await fetch(`${API}/api/cdf/discovery/config`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<DiscoveryConfig>;
}

export async function saveDiscoveryStars(nodeIds: string[]): Promise<DiscoveryConfig> {
  const r = await fetch(`${API}/api/cdf/discovery/config/stars`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ node_ids: nodeIds }),
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<DiscoveryConfig>;
}

export async function saveDiscoverySavedQueries(
  queries: SavedQuery[]
): Promise<{ saved_queries: { queries: SavedQuery[] } }> {
  const r = await fetch(`${API}/api/cdf/discovery/config/saved-queries`, {
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

export async function saveDiscoveryWorkspace(workspace: SavedWorkspace): Promise<{ workspace: SavedWorkspace }> {
  const r = await fetch(`${API}/api/cdf/discovery/config/workspace`, {
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
    `${API}/api/cdf/discovery/children?${new URLSearchParams({ node_id: nodeId })}`,
    { signal }
  );
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{
    nodes: import("./types/discoveryNodes").TreeNode[];
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

export async function fetchContainerDetail(args: {
  space: string;
  externalId: string;
}): Promise<Record<string, unknown>> {
  const params = new URLSearchParams({
    space: args.space,
    external_id: args.externalId,
  });
  const r = await fetch(`${API}/api/cdf/data-modeling/container/detail?${params}`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<Record<string, unknown>>;
}

export async function fetchNodeDetail(args: {
  space: string;
  externalId: string;
}): Promise<Record<string, unknown>> {
  const params = new URLSearchParams({
    space: args.space,
    external_id: args.externalId,
  });
  const r = await fetch(`${API}/api/cdf/data-modeling/node/detail?${params}`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<Record<string, unknown>>;
}

export async function fetchEdgeDetail(args: {
  space: string;
  externalId: string;
}): Promise<Record<string, unknown>> {
  const params = new URLSearchParams({
    space: args.space,
    external_id: args.externalId,
  });
  const r = await fetch(`${API}/api/cdf/data-modeling/edge/detail?${params}`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<Record<string, unknown>>;
}

export async function runSqlQuery(
  body: {
    query: string;
    limit?: number;
    source_limit?: number;
    convert_to_string?: boolean;
    timeout?: number;
  },
  opts?: { signal?: AbortSignal }
): Promise<SqlRunResult> {
  const r = await fetch(`${API}/api/cdf/sql/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: opts?.signal,
  });
  if (!r.ok) {
    const errBody = await r.json().catch(() => ({}));
    throw new Error(String((errBody as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<SqlRunResult>;
}

export async function runFileContentSqlQuery(
  body: {
    query: string;
    limit?: number;
    format: "parquet" | "csv" | "json";
    file_id?: number;
    file_external_id?: string;
    file_instance_space?: string;
    convert_to_string?: boolean;
  },
  opts?: { signal?: AbortSignal }
): Promise<SqlRunResult> {
  const r = await fetch(`${API}/api/cdf/file-content/sql/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: opts?.signal,
  });
  if (!r.ok) {
    const errBody = await r.json().catch(() => ({}));
    throw new Error(String((errBody as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<SqlRunResult>;
}

export type FileDownloadRef = {
  file_id?: number;
  external_id?: string;
  instance_space?: string;
  name?: string;
};

function fileDownloadQuery(ref: FileDownloadRef): string {
  const params = new URLSearchParams();
  const instanceSpace = ref.instance_space?.trim();
  const externalId = ref.external_id?.trim();
  if (instanceSpace && externalId) {
    params.set("file_instance_space", instanceSpace);
    params.set("file_external_id", externalId);
  } else if (ref.file_id != null) {
    params.set("file_id", String(ref.file_id));
    if (externalId) params.set("file_external_id", externalId);
  } else if (externalId) {
    params.set("file_external_id", externalId);
  }
  return params.toString();
}

export async function headFileDownload(
  ref: FileDownloadRef,
  opts?: { signal?: AbortSignal }
): Promise<{ sizeBytes: number | undefined; filename: string }> {
  const qs = fileDownloadQuery(ref);
  if (!qs) throw new Error("file_id or file_external_id is required");
  const r = await fetch(`${API}/api/cdf/files/download?${qs}`, {
    method: "HEAD",
    signal: opts?.signal,
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return {
    sizeBytes: (() => {
      const raw = r.headers.get("Content-Length");
      if (raw == null || raw.trim() === "") return undefined;
      const n = Number(raw.trim());
      return Number.isFinite(n) && n >= 0 ? Math.trunc(n) : undefined;
    })(),
    filename: r.headers.get("X-File-Name")?.trim() || ref.name?.trim() || "download",
  };
}

export async function downloadFileBlob(
  ref: FileDownloadRef,
  opts?: { signal?: AbortSignal }
): Promise<Blob> {
  const qs = fileDownloadQuery(ref);
  if (!qs) throw new Error("file_id or file_external_id is required");
  const r = await fetch(`${API}/api/cdf/files/download?${qs}`, {
    method: "GET",
    signal: opts?.signal,
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.blob();
}
