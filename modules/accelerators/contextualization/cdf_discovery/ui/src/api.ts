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
import type {
  TransformCanvasDocument,
  TransformPipelineDocument,
  TransformWorkflowDocument,
} from "./types/transformCanvas";
import type {
  MonitorWorkflowDetail as MonitorWorkflowStateDetail,
  MonitorWorkflowListItem as MonitorWorkflowStateListItem,
  MonitorScheduleItem,
  MonitorWorkflowSummary as MonitorWorkflowStateSummary,
} from "./types/monitorWorkflowState";

const API = "";
const CONNECTION_TIMEOUT_MS = 15000;

export type ConnectionInfo = {
  project: string;
  base_url: string;
  auth_mode: string;
};

export async function fetchConnection(): Promise<ConnectionInfo> {
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), CONNECTION_TIMEOUT_MS);
  let r: Response;
  try {
    r = await fetch(`${API}/api/connection`, { signal: controller.signal });
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new Error(`connection timeout after ${CONNECTION_TIMEOUT_MS}ms`);
    }
    throw error;
  } finally {
    window.clearTimeout(timeoutId);
  }
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

export async function fetchMonitorWorkflowStateSummary(
  runLimit = 200
): Promise<MonitorWorkflowStateSummary> {
  const params = new URLSearchParams({ run_limit: String(runLimit) });
  const r = await fetch(`${API}/api/monitor/workflow-state/summary?${params}`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<MonitorWorkflowStateSummary>;
}

export async function fetchMonitorWorkflowStates(
  runLimit = 200
): Promise<{ workflows: MonitorWorkflowStateListItem[] }> {
  const params = new URLSearchParams({ run_limit: String(runLimit) });
  const r = await fetch(`${API}/api/monitor/workflow-state/workflows?${params}`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ workflows: MonitorWorkflowStateListItem[] }>;
}

export async function fetchMonitorWorkflowStateDetail(
  workflowId: string,
  runsLimit = 20
): Promise<MonitorWorkflowStateDetail> {
  const params = new URLSearchParams({ runs_limit: String(runsLimit) });
  const r = await fetch(
    `${API}/api/monitor/workflow-state/workflows/${encodeURIComponent(workflowId)}?${params}`
  );
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<MonitorWorkflowStateDetail>;
}

export async function fetchMonitorSchedules(
  lookbackDays = 7
): Promise<{ lookback_days: number; schedules: MonitorScheduleItem[] }> {
  const params = new URLSearchParams({ lookback_days: String(lookbackDays) });
  const r = await fetch(`${API}/api/monitor/schedules?${params}`);
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ lookback_days: number; schedules: MonitorScheduleItem[] }>;
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

async function createTransformPipeline(body: {
  id: string;
  label: string;
  template_id?: string;
}): Promise<{ pipeline: TransformPipelineDocument }> {
  const r = await fetch(`${API}/api/transform/workflows`, {
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

export async function createTransformWorkflow(body: {
  id: string;
  label: string;
  template_id?: string;
}): Promise<{ workflow: TransformWorkflowDocument }> {
  const out = await createTransformPipeline(body);
  return { workflow: out.pipeline };
}

async function fetchTransformPipelineByWorkflow(
  workflowExternalId: string
): Promise<{
  pipeline_id: string;
  scope_suffix?: string;
  pipeline: TransformPipelineDocument;
  match: string;
}> {
  const params = new URLSearchParams({ external_id: workflowExternalId });
  const r = await fetch(`${API}/api/transform/workflows/by-workflow?${params}`);
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

export async function fetchTransformWorkflowByWorkflow(
  workflowExternalId: string
): Promise<{
  workflow_id: string;
  scope_suffix?: string;
  workflow: TransformWorkflowDocument;
  match: string;
}> {
  const out = await fetchTransformPipelineByWorkflow(workflowExternalId);
  return {
    workflow_id: out.pipeline_id,
    scope_suffix: out.scope_suffix,
    workflow: out.pipeline,
    match: out.match,
  };
}

async function importTransformPipelineFromWorkflow(body: {
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
  const r = await fetch(`${API}/api/transform/workflows/import-from-workflow`, {
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

export async function importTransformWorkflowFromWorkflow(body: {
  workflow_external_id: string;
  version?: string;
  workflow_id?: string;
  label?: string;
}): Promise<{
  created: boolean;
  workflow_id: string;
  scope_suffix?: string;
  workflow: TransformWorkflowDocument;
  match: string;
}> {
  const out = await importTransformPipelineFromWorkflow({
    workflow_external_id: body.workflow_external_id,
    version: body.version,
    pipeline_id: body.workflow_id,
    label: body.label,
  });
  return {
    created: out.created,
    workflow_id: out.pipeline_id,
    scope_suffix: out.scope_suffix,
    workflow: out.pipeline,
    match: out.match,
  };
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

function normalizePipelineScopeSuffix(scopeSuffix?: string | null): string {
  return String(scopeSuffix ?? "").trim();
}

function transformPipelineScopeQuery(scopeSuffix = ""): string {
  const scope = normalizePipelineScopeSuffix(scopeSuffix);
  return scope ? `?scope_suffix=${encodeURIComponent(scope)}` : "";
}

async function fetchTransformPipeline(
  workflowId: string,
  scopeSuffix = ""
): Promise<{ pipeline: TransformPipelineDocument }> {
  const r = await fetch(
    `${API}/api/transform/workflows/${encodeURIComponent(workflowId)}${transformPipelineScopeQuery(scopeSuffix)}`
  );
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<{ pipeline: TransformPipelineDocument }>;
}

export async function fetchTransformWorkflow(
  workflowId: string,
  scopeSuffix = ""
): Promise<{ workflow: TransformWorkflowDocument }> {
  const out = await fetchTransformPipeline(workflowId, scopeSuffix);
  return { workflow: out.pipeline };
}

async function saveTransformPipelineAsTemplate(
  workflowId: string,
  body: { template_id: string; label?: string; canvas?: TransformCanvasDocument },
  scopeSuffix = ""
): Promise<{ template: Record<string, unknown> }> {
  const r = await fetch(
    `${API}/api/transform/workflows/${encodeURIComponent(workflowId)}/save-as-template${transformPipelineScopeQuery(scopeSuffix)}`,
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

export async function saveTransformWorkflowAsTemplate(
  workflowId: string,
  body: { template_id: string; label?: string; canvas?: TransformCanvasDocument },
  scopeSuffix = ""
): Promise<{ template: Record<string, unknown> }> {
  return saveTransformPipelineAsTemplate(workflowId, body, scopeSuffix);
}

async function saveTransformPipelineAsPipeline(
  workflowId: string,
  body: { id: string; label: string; canvas?: TransformCanvasDocument },
  scopeSuffix = ""
): Promise<{ pipeline: TransformPipelineDocument }> {
  const r = await fetch(
    `${API}/api/transform/workflows/${encodeURIComponent(workflowId)}/save-as-pipeline${transformPipelineScopeQuery(scopeSuffix)}`,
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

export async function saveTransformWorkflowAsWorkflow(
  workflowId: string,
  body: { id: string; label: string; canvas?: TransformCanvasDocument },
  scopeSuffix = ""
): Promise<{ workflow: TransformWorkflowDocument }> {
  const out = await saveTransformPipelineAsPipeline(workflowId, body, scopeSuffix);
  return { workflow: out.pipeline };
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

async function saveTransformTemplateAsPipeline(
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

export async function saveTransformTemplateAsWorkflow(
  templateId: string,
  body: { id: string; label: string; canvas?: TransformCanvasDocument }
): Promise<{ workflow: TransformWorkflowDocument }> {
  const out = await saveTransformTemplateAsPipeline(templateId, body);
  return { workflow: out.pipeline };
}

async function deleteTransformPipeline(pipelineId: string): Promise<void> {
  const r = await fetch(`${API}/api/transform/workflows/${encodeURIComponent(pipelineId)}`, {
    method: "DELETE",
  });
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
}

export async function deleteTransformWorkflow(workflowId: string): Promise<void> {
  return deleteTransformPipeline(workflowId);
}

async function renameTransformPipeline(
  workflowId: string,
  label: string,
  scopeSuffix = ""
): Promise<{ pipeline: TransformPipelineDocument }> {
  const r = await fetch(
    `${API}/api/transform/workflows/${encodeURIComponent(workflowId)}/label${transformPipelineScopeQuery(scopeSuffix)}`,
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

export async function renameTransformWorkflow(
  workflowId: string,
  label: string,
  scopeSuffix = ""
): Promise<{ workflow: TransformWorkflowDocument }> {
  const out = await renameTransformPipeline(workflowId, label, scopeSuffix);
  return { workflow: out.pipeline };
}

async function saveTransformPipelineCanvas(
  workflowId: string,
  canvas: TransformCanvasDocument,
  scopeSuffix = ""
): Promise<void> {
  const r = await fetch(
    `${API}/api/transform/workflows/${encodeURIComponent(workflowId)}/canvas${transformPipelineScopeQuery(scopeSuffix)}`,
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

export async function saveTransformWorkflowCanvas(
  workflowId: string,
  canvas: TransformCanvasDocument,
  scopeSuffix = ""
): Promise<void> {
  return saveTransformPipelineCanvas(workflowId, canvas, scopeSuffix);
}

type TransformBuildPairing = {
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

export type TransformWorkflowBuildPairing = {
  workflow_id: string;
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

async function fetchTransformBuildPairing(
  workflowId: string,
  scopeSuffix = ""
): Promise<TransformBuildPairing> {
  const params = new URLSearchParams();
  const scope = normalizePipelineScopeSuffix(scopeSuffix);
  if (scope) params.set("scope_suffix", scope);
  const q = params.toString();
  const r = await fetch(
    `${API}/api/transform/workflows/${encodeURIComponent(workflowId)}/build-pairing?${q}`
  );
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(String((body as { detail?: string }).detail ?? r.status));
  }
  return r.json() as Promise<TransformBuildPairing>;
}

export async function fetchTransformWorkflowBuildPairing(
  workflowId: string,
  scopeSuffix = ""
): Promise<TransformWorkflowBuildPairing> {
  const out = await fetchTransformBuildPairing(workflowId, scopeSuffix);
  return {
    workflow_id: out.pipeline_id,
    workflow_base: out.workflow_base,
    workflow_version: out.workflow_version,
    workflow_external_id: out.workflow_external_id,
    trigger_external_id: out.trigger_external_id,
    pairings: out.pairings,
  };
}

async function validateTransformPipeline(
  workflowId: string,
  scopeSuffix = "",
  canvas?: Record<string, unknown>
): Promise<{ ok: boolean; warnings?: string[]; errors?: string[] }> {
  const r = await fetch(
    `${API}/api/transform/workflows/${encodeURIComponent(workflowId)}/validate${transformPipelineScopeQuery(scopeSuffix)}`,
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

export async function validateTransformWorkflow(
  workflowId: string,
  scopeSuffix = "",
  canvas?: Record<string, unknown>
): Promise<{ ok: boolean; warnings?: string[]; errors?: string[] }> {
  return validateTransformPipeline(workflowId, scopeSuffix, canvas);
}

type TransformBuildResult = {
  ok: boolean;
  pipeline_id?: string;
  template_id?: string;
  stdout?: string;
  stderr?: string;
  task_count?: number;
};

export type TransformWorkflowBuildResult = {
  ok: boolean;
  workflow_id?: string;
  template_id?: string;
  stdout?: string;
  stderr?: string;
  task_count?: number;
};

async function buildTransformPipeline(
  workflowId: string,
  scopeSuffix = ""
): Promise<TransformBuildResult> {
  const scopeQ = transformPipelineScopeQuery(scopeSuffix);
  const url = `${API}/api/transform/workflows/${encodeURIComponent(workflowId)}/build${scopeQ}`;
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

export async function buildTransformWorkflow(
  workflowId: string,
  scopeSuffix = ""
): Promise<TransformWorkflowBuildResult> {
  const out = await buildTransformPipeline(workflowId, scopeSuffix);
  return {
    ok: out.ok,
    workflow_id: out.pipeline_id,
    template_id: out.template_id,
    stdout: out.stdout,
    stderr: out.stderr,
    task_count: out.task_count,
  };
}

type TransformCdfCliResult = {
  ok: boolean;
  exit_code: number;
  stdout: string;
  stderr: string;
  pipeline_id?: string;
  scope_suffix?: string;
};

export type TransformWorkflowCdfCliResult = {
  ok: boolean;
  exit_code: number;
  stdout: string;
  stderr: string;
  workflow_id?: string;
  scope_suffix?: string;
};

export type TransformStateResetTableResult = {
  raw_db: string;
  raw_table: string;
  status: "deleted" | "not_found" | "error";
  error?: string;
};

export type TransformStateResetResult = {
  pipeline_id: string;
  scope_suffix: string;
  raw_db: string;
  base_table_key: string;
  results: TransformStateResetTableResult[];
  ok: boolean;
};

async function deployTransformPipelineCdf(
  workflowId: string,
  scopeSuffix = "",
  options: {
    dryRun?: boolean;
    skipBuild?: boolean;
    allowUnresolvedPlaceholders?: boolean;
  } = {}
): Promise<TransformCdfCliResult> {
  const scopeQ = transformPipelineScopeQuery(scopeSuffix);
  const r = await fetch(
    `${API}/api/transform/workflows/${encodeURIComponent(workflowId)}/deploy-cdf${scopeQ}`,
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

export async function deployTransformWorkflowCdf(
  workflowId: string,
  scopeSuffix = "",
  options: {
    dryRun?: boolean;
    skipBuild?: boolean;
    allowUnresolvedPlaceholders?: boolean;
  } = {}
): Promise<TransformWorkflowCdfCliResult> {
  const out = await deployTransformPipelineCdf(workflowId, scopeSuffix, options);
  return {
    ok: out.ok,
    exit_code: out.exit_code,
    stdout: out.stdout,
    stderr: out.stderr,
    workflow_id: out.pipeline_id,
    scope_suffix: out.scope_suffix,
  };
}

async function runTransformPipelineCdf(
  workflowId: string,
  scopeSuffix = "",
  options: {
    dryRun?: boolean;
    instanceSpace?: string;
    timeoutSeconds?: number;
  } = {}
): Promise<TransformCdfCliResult> {
  const scopeQ = transformPipelineScopeQuery(scopeSuffix);
  const r = await fetch(
    `${API}/api/transform/workflows/${encodeURIComponent(workflowId)}/cdf-run${scopeQ}`,
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

export async function runTransformWorkflowCdf(
  workflowId: string,
  scopeSuffix = "",
  options: {
    dryRun?: boolean;
    instanceSpace?: string;
    timeoutSeconds?: number;
  } = {}
): Promise<TransformWorkflowCdfCliResult> {
  const out = await runTransformPipelineCdf(workflowId, scopeSuffix, options);
  return {
    ok: out.ok,
    exit_code: out.exit_code,
    stdout: out.stdout,
    stderr: out.stderr,
    workflow_id: out.pipeline_id,
    scope_suffix: out.scope_suffix,
  };
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

async function runTransformPipelineLocal(
  workflowId: string,
  dryRun = false,
  incrementalChangeProcessing = true
): Promise<{
  ok: boolean;
  detail?: string;
  run_id?: string;
  task_summaries?: Record<string, unknown>;
}> {
  const r = await fetch(`${API}/api/transform/workflows/${encodeURIComponent(workflowId)}/run`, {
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

export async function runTransformWorkflowLocal(
  workflowId: string,
  dryRun = false,
  incrementalChangeProcessing = true
): Promise<{
  ok: boolean;
  detail?: string;
  run_id?: string;
  task_summaries?: Record<string, unknown>;
}> {
  return runTransformPipelineLocal(workflowId, dryRun, incrementalChangeProcessing);
}

export async function resetTransformWorkflowState(
  workflowId: string,
  scopeSuffix = ""
): Promise<TransformStateResetResult> {
  const scopeQ = transformPipelineScopeQuery(scopeSuffix);
  const r = await fetch(
    `${API}/api/transform/workflows/${encodeURIComponent(workflowId)}/reset-state${scopeQ}`,
    { method: "POST" }
  );
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    const detail =
      typeof body?.detail === "string"
        ? body.detail
        : JSON.stringify(body?.detail ?? body ?? { status: r.status });
    throw new Error(detail);
  }
  return r.json() as Promise<TransformStateResetResult>;
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
