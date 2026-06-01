import type { MessageKey } from "../i18n";

export type RecordsWriteMode = "ingest" | "upsert" | "delete";

export type RecordsSaveValidation = {
  ok: boolean;
  issues: MessageKey[];
};

export function readRecordsWriteMode(cfg: Record<string, unknown>): RecordsWriteMode {
  const raw = String(cfg.write_mode ?? "ingest").trim().toLowerCase();
  if (raw === "upsert" || raw === "delete") return raw;
  return "ingest";
}

export function validateRecordsSaveConfig(cfg: Record<string, unknown>): RecordsSaveValidation {
  const issues: MessageKey[] = [];
  const stream = String(cfg.stream_external_id ?? "").trim();
  if (!stream) issues.push("transform.save.recordsErrorStreamRequired");
  const mode = readRecordsWriteMode(cfg);
  if (!["ingest", "upsert", "delete"].includes(mode)) {
    issues.push("transform.save.recordsErrorWriteMode");
  }
  return { ok: issues.length === 0, issues };
}

export function recordsSaveSummary(cfg: Record<string, unknown>): string {
  const stream = String(cfg.stream_external_id ?? "").trim();
  if (!stream) return "";
  return `${stream} · ${readRecordsWriteMode(cfg)}`;
}
