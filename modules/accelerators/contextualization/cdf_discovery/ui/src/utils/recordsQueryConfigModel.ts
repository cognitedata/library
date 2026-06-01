import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/jsonConfig";

export type RecordsQueryValidation = {
  ok: boolean;
  issues: MessageKey[];
};

export function readStreamExternalId(cfg: Record<string, unknown>): string {
  return String(cfg.stream_external_id ?? "").trim();
}

export function readReadMode(cfg: Record<string, unknown>): "sync" | "filter" {
  const raw = String(cfg.read_mode ?? "sync").trim().toLowerCase();
  return raw === "filter" ? "filter" : "sync";
}

export function readRecordsSources(cfg: Record<string, unknown>): JsonObject[] {
  const raw = cfg.sources;
  if (!Array.isArray(raw)) return [];
  return raw.filter((x) => x && typeof x === "object" && !Array.isArray(x)) as JsonObject[];
}

export function readRecordsFilter(cfg: Record<string, unknown>): JsonObject | null {
  const raw = cfg.filter;
  if (raw && typeof raw === "object" && !Array.isArray(raw)) return raw as JsonObject;
  if (typeof raw === "string" && raw.trim()) {
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) return parsed as JsonObject;
    } catch {
      return null;
    }
  }
  return null;
}

export function validateRecordsQueryConfig(cfg: Record<string, unknown>): RecordsQueryValidation {
  const issues: MessageKey[] = [];
  if (!readStreamExternalId(cfg)) {
    issues.push("transform.query.recordsErrorStreamRequired");
  }
  const batch = cfg.batch_size ?? cfg.limit;
  if (batch !== undefined && batch !== null && batch !== "") {
    const n = typeof batch === "number" ? batch : parseInt(String(batch), 10);
    if (!Number.isFinite(n) || n < 1 || n > 1000) {
      issues.push("transform.query.recordsErrorBatchSize");
    }
  }
  const readLimit = cfg.read_limit;
  if (readLimit !== undefined && readLimit !== null && readLimit !== "") {
    const n = typeof readLimit === "number" ? readLimit : parseInt(String(readLimit), 10);
    if (!Number.isFinite(n) || n < 1) {
      issues.push("transform.query.recordsErrorReadLimit");
    }
  }
  if (typeof cfg.filter === "string" && cfg.filter.trim()) {
    try {
      JSON.parse(cfg.filter);
    } catch {
      issues.push("transform.query.recordsErrorFilterJson");
    }
  }
  return { ok: issues.length === 0, issues };
}

export function recordsQuerySummary(cfg: Record<string, unknown>): string {
  const stream = readStreamExternalId(cfg);
  if (!stream) return "";
  const mode = readReadMode(cfg);
  const srcCount = readRecordsSources(cfg).length;
  const srcSuffix = srcCount > 0 ? ` · ${srcCount} source(s)` : "";
  return `${stream} · ${mode}${srcSuffix}`;
}
