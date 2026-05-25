import type { MessageKey } from "../../i18n";

export type LocalRunProgressRowCounts = {
  rows_read?: number;
  rows_written?: number;
  instances_written?: number;
  instances_listed?: number;
  instances_applied?: number;
  row_count?: number;
  rows_read_left?: number;
  rows_read_right?: number;
};

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

function intField(raw: Record<string, unknown>, key: string): number | undefined {
  const val = raw[key];
  if (typeof val === "number" && Number.isFinite(val)) return val;
  return undefined;
}

export function resolveReadCount(fields: LocalRunProgressRowCounts & Record<string, unknown>): number | undefined {
  const vals: number[] = [];
  for (const key of ["rows_read", "instances_listed"] as const) {
    const v = intField(fields, key);
    if (v != null) vals.push(v);
  }
  const left = intField(fields, "rows_read_left");
  const right = intField(fields, "rows_read_right");
  if (left != null || right != null) vals.push((left ?? 0) + (right ?? 0));
  if (!vals.length) return undefined;
  const positive = vals.filter((v) => v > 0);
  return positive.length ? Math.max(...positive) : vals[0];
}

export function resolveWriteCount(fields: LocalRunProgressRowCounts & Record<string, unknown>): number | undefined {
  const vals: number[] = [];
  for (const key of ["rows_written", "instances_written", "instances_applied", "row_count"] as const) {
    const v = intField(fields, key);
    if (v != null) vals.push(v);
  }
  if (!vals.length) return undefined;
  const positive = vals.filter((v) => v > 0);
  return positive.length ? Math.max(...positive) : vals[0];
}

export function rowCountFieldsFromEvent(
  ev: Record<string, unknown>
): LocalRunProgressRowCounts {
  const read = resolveReadCount(ev);
  const written = resolveWriteCount(ev);
  const out: LocalRunProgressRowCounts = {};
  if (read != null) out.rows_read = read;
  if (written != null) out.rows_written = written;
  return out;
}

export function localRunRowSuffix(ev: LocalRunProgressRowCounts, t: TFn): string {
  const parts: string[] = [];
  const read = resolveReadCount(ev);
  const written = resolveWriteCount(ev);
  if (read != null) parts.push(t("run.localTaskRowsRead", { count: read }));
  if (written != null) parts.push(t("run.localTaskRowsWritten", { count: written }));
  return parts.length ? ` — ${parts.join(", ")}` : "";
}

export function formatLocalRunDetail(
  taskSummaries: Record<string, unknown>,
  t: TFn,
  runId?: string
): string {
  const entries = Object.entries(taskSummaries).sort(([a], [b]) => a.localeCompare(b));
  if (!entries.length) {
    const id = runId?.trim();
    return id ? `run_id=${id}` : "";
  }
  const parts: string[] = [];
  for (const [taskId, raw] of entries) {
    const summary = (raw ?? {}) as Record<string, unknown>;
    const status = String(summary.status ?? "ok");
    const read = resolveReadCount(summary);
    const written = resolveWriteCount(summary);
    const countBits: string[] = [];
    if (read != null) countBits.push(t("run.localTaskRowsRead", { count: read }));
    if (written != null) countBits.push(t("run.localTaskRowsWritten", { count: written }));
    if (countBits.length) parts.push(`${taskId}: ${status} (${countBits.join(", ")})`);
    else parts.push(`${taskId}: ${status}`);
  }
  const prefix = runId?.trim() ? `run_id=${runId.trim()} · ` : "";
  return prefix + parts.join("; ");
}
