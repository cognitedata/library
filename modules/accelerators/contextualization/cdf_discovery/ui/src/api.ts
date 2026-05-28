import type {
  DataModelGraph,
  DataModelRef,
  FunctionDetail,
  SavedQuery,
  SavedWorkspace,
  TransformationDetail,
  TransformationListItem,
  WorkflowGraph,
  WorkflowRef,
} from "./types/discoveryNodes";
import type { TransformCanvasDocument, TransformPipelineDocument } from "./types/transformCanvas";

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

export async function fetchTransformationList(
  limit = 500
): Promise<{ items: TransformationListItem[] }> {
  const params = new URLSearchParams({ limit: String(limit) });
  const r = await fetch(`${API}/api/cdf/transformations/list?${params}`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ items: TransformationListItem[] }>;
}

export async function fetchTransformationDetail(
  transformationIdOrOpts: number | { id?: number; externalId?: string }
): Promise<TransformationDetail> {
  const params = new URLSearchParams();
  if (typeof transformationIdOrOpts === "number") {
    params.set("id", String(transformationIdOrOpts));
  } else if (transformationIdOrOpts.id != null) {
    params.set("id", String(transformationIdOrOpts.id));
  } else if (transformationIdOrOpts.externalId?.trim()) {
    params.set("external_id", transformationIdOrOpts.externalId.trim());
  } else {
    throw new Error("fetchTransformationDetail requires id or externalId");
  }
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

export async function fetchTransformTemplates(): Promise<{
  templates: Array<{ id: string; label: string }>;
}> {
  const r = await fetch(`${API}/api/transform/templates`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ templates: Array<{ id: string; label: string }> }>;
}

export async function createTransformPipeline(body: {
  id: string;
  label: string;
  template_id?: string;
}): Promise<{ pipeline: TransformPipelineDocument }> {
  const r = await fetch(`${API}/api/transform/pipelines`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const resBody = await r.json().catch(() => ({}));
    throw new Error(String((resBody as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ pipeline: TransformPipelineDocument }>;
}

export async function fetchTransformPipelineByWorkflow(
  workflowExternalId: string
): Promise<{
  pipeline_id: string;
  scope_suffix?: string;
  pipeline: TransformPipelineDocument;
  match: string;
}> {
  const params = new URLSearchParams({ external_id: workflowExternalId });
  const r = await fetch(`${API}/api/transform/pipelines/by-workflow?${params}`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{
    pipeline_id: string;
    scope_suffix?: string;
    pipeline: TransformPipelineDocument;
    match: string;
  }>;
}

export async function importTransformPipelineFromWorkflow(body: {
  workflow_external_id: string;
  version?: string;
  pipeline_id?: string;
  label?: string;
}): Promise<{
  created: boolean;
  pipeline_id: string;
  scope_suffix?: string;
  pipeline: TransformPipelineDocument;
  match: string;
}> {
  const r = await fetch(`${API}/api/transform/pipelines/import-from-workflow`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const resBody = await r.json().catch(() => ({}));
    throw new Error(String((resBody as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{
    created: boolean;
    pipeline_id: string;
    scope_suffix?: string;
    pipeline: TransformPipelineDocument;
    match: string;
  }>;
}

export async function fetchTransformWorkflowYaml(
  path: string
): Promise<{ path: string; content: string }> {
  const params = new URLSearchParams({ path });
  const r = await fetch(`${API}/api/transform/workflow-yaml?${params}`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ path: string; content: string }>;
}

export async function saveTransformWorkflowYaml(path: string, content: string): Promise<void> {
  const params = new URLSearchParams({ path });
  const r = await fetch(`${API}/api/transform/workflow-yaml?${params}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ content }),
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
}

const LEGACY_UNSCOPED_SCOPE = "all";

function normalizePipelineScopeSuffix(scopeSuffix?: string | null): string {
  const s = String(scopeSuffix ?? "").trim();
  return s === LEGACY_UNSCOPED_SCOPE ? "" : s;
}

function transformPipelineScopeQuery(scopeSuffix = ""): string {
  const scope = normalizePipelineScopeSuffix(scopeSuffix);
  return scope ? `?scope_suffix=${encodeURIComponent(scope)}` : "";
}

export async function fetchTransformPipeline(
  pipelineId: string,
  scopeSuffix = ""
): Promise<{ pipeline: TransformPipelineDocument }> {
  const r = await fetch(
    `${API}/api/transform/pipelines/${encodeURIComponent(pipelineId)}${transformPipelineScopeQuery(scopeSuffix)}`
  );
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ pipeline: TransformPipelineDocument }>;
}

export async function saveTransformPipelineAsTemplate(
  pipelineId: string,
  body: { template_id: string; label?: string; canvas?: TransformCanvasDocument },
  scopeSuffix = ""
): Promise<{ template: Record<string, unknown> }> {
  const r = await fetch(
    `${API}/api/transform/pipelines/${encodeURIComponent(pipelineId)}/save-as-template${transformPipelineScopeQuery(scopeSuffix)}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }
  );
  if (!r.ok) {
    const resBody = await r.json().catch(() => ({}));
    throw new Error(String((resBody as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ template: Record<string, unknown> }>;
}

export async function saveTransformPipelineAsPipeline(
  pipelineId: string,
  body: { id: string; label: string; canvas?: TransformCanvasDocument },
  scopeSuffix = ""
): Promise<{ pipeline: TransformPipelineDocument }> {
  const r = await fetch(
    `${API}/api/transform/pipelines/${encodeURIComponent(pipelineId)}/save-as-pipeline${transformPipelineScopeQuery(scopeSuffix)}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }
  );
  if (!r.ok) {
    const resBody = await r.json().catch(() => ({}));
    throw new Error(String((resBody as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ pipeline: TransformPipelineDocument }>;
}

export async function saveTransformTemplateAsTemplate(
  templateId: string,
  body: { template_id: string; label?: string; canvas?: TransformCanvasDocument }
): Promise<{ template: Record<string, unknown> }> {
  const r = await fetch(
    `${API}/api/transform/templates/${encodeURIComponent(templateId)}/save-as-template`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }
  );
  if (!r.ok) {
    const resBody = await r.json().catch(() => ({}));
    throw new Error(String((resBody as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ template: Record<string, unknown> }>;
}

export async function saveTransformTemplateAsPipeline(
  templateId: string,
  body: { id: string; label: string; canvas?: TransformCanvasDocument }
): Promise<{ pipeline: TransformPipelineDocument }> {
  const r = await fetch(
    `${API}/api/transform/templates/${encodeURIComponent(templateId)}/save-as-pipeline`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }
  );
  if (!r.ok) {
    const resBody = await r.json().catch(() => ({}));
    throw new Error(String((resBody as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ pipeline: TransformPipelineDocument }>;
}

export async function deleteTransformPipeline(pipelineId: string): Promise<void> {
  const r = await fetch(`${API}/api/transform/pipelines/${encodeURIComponent(pipelineId)}`, {
    method: "DELETE",
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
}

export async function renameTransformPipeline(
  pipelineId: string,
  label: string,
  scopeSuffix = ""
): Promise<{ pipeline: TransformPipelineDocument }> {
  const r = await fetch(
    `${API}/api/transform/pipelines/${encodeURIComponent(pipelineId)}/label${transformPipelineScopeQuery(scopeSuffix)}`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ label }),
    }
  );
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ pipeline: TransformPipelineDocument }>;
}

export async function saveTransformPipelineCanvas(
  pipelineId: string,
  canvas: TransformCanvasDocument,
  scopeSuffix = ""
): Promise<void> {
  const r = await fetch(
    `${API}/api/transform/pipelines/${encodeURIComponent(pipelineId)}/canvas${transformPipelineScopeQuery(scopeSuffix)}`,
    {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ canvas }),
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
}

export type TransformBuildPairing = {
  pipeline_id: string;
  workflow_base: string;
  workflow_version: string;
  workflow_external_id: string;
  trigger_external_id: string;
  pairings: Array<{
    scope_suffix: string;
    workflow_external_id: string;
    trigger_external_id: string;
    workflow_version: string;
  }>;
};

export async function fetchTransformBuildPairing(
  pipelineId: string,
  scopeSuffix = ""
): Promise<TransformBuildPairing> {
  const params = new URLSearchParams();
  const scope = normalizePipelineScopeSuffix(scopeSuffix);
  if (scope) params.set("scope_suffix", scope);
  const q = params.toString();
  const r = await fetch(
    `${API}/api/transform/pipelines/${encodeURIComponent(pipelineId)}/build-pairing?${q}`
  );
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<TransformBuildPairing>;
}

export async function validateTransformPipeline(
  pipelineId: string,
  scopeSuffix = "",
  canvas?: Record<string, unknown>
): Promise<{ ok: boolean; warnings?: string[]; errors?: string[] }> {
  const r = await fetch(
    `${API}/api/transform/pipelines/${encodeURIComponent(pipelineId)}/validate${transformPipelineScopeQuery(scopeSuffix)}`,
    {
      method: "POST",
      headers: canvas ? { "Content-Type": "application/json" } : undefined,
      body: canvas ? JSON.stringify({ canvas }) : undefined,
    }
  );
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ ok: boolean; warnings?: string[]; errors?: string[] }>;
}

export type TransformBuildResult = {
  ok: boolean;
  pipeline_id?: string;
  template_id?: string;
  stdout?: string;
  stderr?: string;
  task_count?: number;
};

export async function buildTransformPipeline(
  pipelineId: string,
  scopeSuffix = ""
): Promise<TransformBuildResult> {
  const scopeQ = transformPipelineScopeQuery(scopeSuffix);
  const url = `${API}/api/transform/workflows/${encodeURIComponent(pipelineId)}/build${scopeQ}`;
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({}),
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<TransformBuildResult>;
}

export type TransformCdfCliResult = {
  ok: boolean;
  exit_code: number;
  stdout: string;
  stderr: string;
  pipeline_id?: string;
  scope_suffix?: string;
};

export async function deployTransformPipelineCdf(
  pipelineId: string,
  scopeSuffix = "",
  options: {
    dryRun?: boolean;
    skipBuild?: boolean;
    allowUnresolvedPlaceholders?: boolean;
  } = {}
): Promise<TransformCdfCliResult> {
  const scopeQ = transformPipelineScopeQuery(scopeSuffix);
  const r = await fetch(
    `${API}/api/transform/pipelines/${encodeURIComponent(pipelineId)}/deploy-cdf${scopeQ}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        dry_run: options.dryRun ?? false,
        skip_build: options.skipBuild ?? false,
        allow_unresolved_placeholders: options.allowUnresolvedPlaceholders ?? true,
      }),
    }
  );
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<TransformCdfCliResult>;
}

export async function runTransformPipelineCdf(
  pipelineId: string,
  scopeSuffix = "",
  options: {
    dryRun?: boolean;
    instanceSpace?: string;
    timeoutSeconds?: number;
  } = {}
): Promise<TransformCdfCliResult> {
  const scopeQ = transformPipelineScopeQuery(scopeSuffix);
  const r = await fetch(
    `${API}/api/transform/pipelines/${encodeURIComponent(pipelineId)}/cdf-run${scopeQ}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        dry_run: options.dryRun ?? false,
        instance_space: options.instanceSpace?.trim() || undefined,
        timeout_seconds: options.timeoutSeconds,
      }),
    }
  );
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<TransformCdfCliResult>;
}

export async function buildTransformTemplate(
  templateId: string
): Promise<TransformBuildResult> {
  const path = `${API}/api/transform/templates/${encodeURIComponent(templateId)}/build`;
  const r = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({}),
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<TransformBuildResult>;
}

export async function runTransformPipelineLocal(
  pipelineId: string,
  dryRun = false,
  incrementalChangeProcessing = true
): Promise<{
  ok: boolean;
  detail?: string;
  run_id?: string;
  task_summaries?: Record<string, unknown>;
}> {
  const r = await fetch(`${API}/api/transform/pipelines/${encodeURIComponent(pipelineId)}/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      dry_run: dryRun,
      incremental_change_processing: incrementalChangeProcessing,
    }),
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{
    ok: boolean;
    detail?: string;
    run_id?: string;
    task_summaries?: Record<string, unknown>;
  }>;
}

export async function fetchTransformTemplate(
  templateId: string
): Promise<{ template: Record<string, unknown> }> {
  const r = await fetch(`${API}/api/transform/templates/${encodeURIComponent(templateId)}`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ template: Record<string, unknown> }>;
}

export async function renameTransformTemplate(
  templateId: string,
  label: string
): Promise<{ template: Record<string, unknown> }> {
  const r = await fetch(`${API}/api/transform/templates/${encodeURIComponent(templateId)}/label`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ label }),
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ template: Record<string, unknown> }>;
}

export async function deleteTransformTemplate(templateId: string): Promise<void> {
  const r = await fetch(`${API}/api/transform/templates/${encodeURIComponent(templateId)}`, {
    method: "DELETE",
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
}

export async function saveTransformTemplateCanvas(
  templateId: string,
  canvas: TransformCanvasDocument
): Promise<void> {
  const r = await fetch(`${API}/api/transform/templates/${encodeURIComponent(templateId)}/canvas`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ canvas }),
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
}

export async function validateTransformTemplate(
  templateId: string,
  canvas?: Record<string, unknown>
): Promise<{ ok: boolean; warnings?: string[]; errors?: string[] }> {
  const r = await fetch(`${API}/api/transform/templates/${encodeURIComponent(templateId)}/validate`, {
    method: "POST",
    headers: canvas ? { "Content-Type": "application/json" } : undefined,
    body: canvas ? JSON.stringify({ canvas }) : undefined,
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ ok: boolean; warnings?: string[]; errors?: string[] }>;
}
