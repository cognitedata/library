import type { ConnectionInfo, RuntimeConfigSummary } from "./types/indexWorkspace";

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  const text = await res.text();
  let data: unknown = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }
  if (!res.ok) {
    const detail =
      typeof data === "object" && data !== null && "detail" in data
        ? String((data as { detail: unknown }).detail)
        : text || res.statusText;
    throw new Error(detail);
  }
  return data as T;
}

export type ConfigResponse = {
  path: string;
  yaml_text: string;
  runtime: RuntimeConfigSummary;
};

export type WorkspaceResponse = {
  workspace: {
    active_tab_id: string | null;
    tabs: Array<Record<string, unknown>>;
  };
};

export async function fetchHealth(): Promise<{ ok: boolean }> {
  return fetchJson("/api/health");
}

export async function fetchConnection(): Promise<ConnectionInfo> {
  return fetchJson("/api/connection");
}

export async function fetchConfig(): Promise<ConfigResponse> {
  return fetchJson("/api/inverted-index/config");
}

export async function saveConfig(yamlText: string): Promise<ConfigResponse> {
  return fetchJson("/api/inverted-index/config", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ yaml_text: yamlText }),
  });
}

export async function fetchWorkspace(): Promise<WorkspaceResponse> {
  return fetchJson("/api/inverted-index/workspace");
}

export async function saveWorkspace(workspace: WorkspaceResponse["workspace"]): Promise<WorkspaceResponse> {
  return fetchJson("/api/inverted-index/workspace", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(workspace),
  });
}

export async function buildMetadata(body: {
  dry_run: boolean;
  filter_updated_after?: string;
  batch_size?: number;
  progress_interval?: number;
}): Promise<Record<string, unknown>> {
  return fetchJson("/api/inverted-index/build/metadata", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function buildAnnotations(body: {
  dry_run: boolean;
  file_external_id?: string;
  detection_mode: "all" | "pattern" | "standard";
}): Promise<Record<string, unknown>> {
  return fetchJson("/api/inverted-index/build/annotations", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function queryIndex(body: {
  terms: string[];
  all_scopes: boolean;
  match_scope_keys: string[];
  source_types?: string[];
  min_confidence: number;
  reuse_only: boolean;
  hits_only: boolean;
}): Promise<unknown> {
  return fetchJson("/api/inverted-index/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function tagReuseAudit(body: {
  all_scopes: boolean;
  match_scope_keys: string[];
  min_scope_count: number;
  limit: number;
}): Promise<Record<string, unknown>> {
  return fetchJson("/api/inverted-index/tag-reuse-audit", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function runTargetDriven(body: {
  dry_run: boolean;
  instance_external_id?: string;
  incoming_view_key?: string;
  view_external_id?: string;
  instance_space: string;
  min_confidence: number;
  match_scope_keys: string[];
  scope_lookup_override: boolean;
  max_assets?: number;
}): Promise<Record<string, unknown>> {
  return fetchJson("/api/inverted-index/target-driven", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function scoreFile(body: {
  file_external_id: string;
  file_space: string;
  match_scope_key?: string;
}): Promise<Record<string, unknown>> {
  return fetchJson("/api/inverted-index/score", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function fileDeltas(body: {
  file_external_id: string;
  file_space: string;
  match_scope_key?: string;
}): Promise<Record<string, unknown>> {
  return fetchJson("/api/inverted-index/deltas", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export async function listByFile(body: {
  file_external_id: string;
  file_space: string;
  match_scope_key?: string;
  source_types?: string[];
  limit: number;
}): Promise<unknown[]> {
  return fetchJson("/api/inverted-index/list-by-file", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function parseCsvList(raw: string): string[] {
  return raw
    .split(/[,\n]/)
    .map((s) => s.trim())
    .filter(Boolean);
}

export function redactForDisplay(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(redactForDisplay);
  }
  if (value && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      if (k === "POSTINGS_JSON" || k === "postings_json") {
        out[k] = "[redacted]";
      } else {
        out[k] = redactForDisplay(v);
      }
    }
    return out;
  }
  return value;
}

/** Compact tag-reuse audit summary without the full duplicate table payload. */
export function formatTagReuseAuditResult(result: unknown): unknown | null {
  if (result == null) return null;
  if (typeof result !== "object" || Array.isArray(result)) return result;
  const record = result as Record<string, unknown>;
  if (record.cancelled === true) return null;

  const reuse = record.reuse_metrics;
  let reuseMetricsSummary: Record<string, unknown> | undefined;
  if (reuse && typeof reuse === "object" && !Array.isArray(reuse)) {
    const metrics = reuse as Record<string, unknown>;
    const byTerm = metrics.by_term;
    reuseMetricsSummary = {
      scopes_queried: metrics.scopes_queried,
      terms_queried: metrics.terms_queried,
      terms_with_hits: metrics.terms_with_hits,
      cross_scope_duplicate_count: metrics.cross_scope_duplicate_count,
      cross_scope_duplicate_rate: metrics.cross_scope_duplicate_rate,
      by_term_count: Array.isArray(byTerm) ? byTerm.length : 0,
    };
  }

  return {
    scopes_scanned: record.scopes_scanned,
    lookup_keys_scanned: record.lookup_keys_scanned,
    unique_terms_scanned: record.unique_terms_scanned,
    min_scope_count: record.min_scope_count,
    duration_sec: record.duration_sec,
    reuse_metrics: reuseMetricsSummary,
  };
}

/** Drop cancellation stubs and oversized dry-run detail arrays from operation summaries. */
export function formatBuildOperationResult(result: unknown): unknown | null {
  if (result == null) return null;
  if (typeof result !== "object" || Array.isArray(result)) return result;
  const record = result as Record<string, unknown>;
  if (record.cancelled === true) return null;
  if (Array.isArray(record.results)) {
    const { results, ...summary } = record;
    return { ...summary, dry_run_results_count: results.length };
  }
  return result;
}
