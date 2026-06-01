import type { MessageKey } from "../../i18n";
import type { TransformPipelineRunResult } from "../../types/transformTabRun";
import type { TransformCanvasDocument } from "../../types/transformCanvas";
import { collectStartCanvasNodeIdsOnAnyPathToTarget } from "./flowRunProgressEdges";
import type { CanvasNodeRunProgress } from "./canvasNodeRunProgress";
import {
  formatLocalRunDetail,
  localRunRowSuffix,
  rowCountFieldsFromEvent,
  type LocalRunProgressRowCounts,
} from "./localRunRowCounts";

export type TransformFlowRunProgress = {
  busy: boolean;
  executingCanvasNodeIds: readonly string[];
  runActiveCanvasNodeIds: readonly string[];
  runCompletedCanvasNodeIds: readonly string[];
  failedCanvasNodeIds: readonly string[];
  warningCanvasNodeIds: readonly string[];
  nodeProgressById: Readonly<Record<string, CanvasNodeRunProgress>>;
};

export type { CanvasNodeRunProgress } from "./canvasNodeRunProgress";

export type { TransformPipelineRunResult } from "../../types/transformTabRun";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type ProgressEv = {
  event?: string;
  task_id?: string;
  code?: number;
  level?: string;
  message?: string;
  function_external_id?: string;
  canvas_node_id?: string;
  pipeline_node_id?: string;
  status?: string;
  error?: string;
  duration_sec?: number;
  progress_current?: number;
  progress_total?: number;
  progress_label?: string;
} & LocalRunProgressRowCounts;

export type TransformRunStreamCallbacks = {
  onLogAppend: (chunk: string) => void;
  onProgress: (progress: TransformFlowRunProgress) => void;
  onComplete: (result: TransformPipelineRunResult) => void;
};

function emptyProgress(busy: boolean): TransformFlowRunProgress {
  return {
    busy,
    executingCanvasNodeIds: [],
    runActiveCanvasNodeIds: [],
    runCompletedCanvasNodeIds: [],
    failedCanvasNodeIds: [],
    warningCanvasNodeIds: [],
    nodeProgressById: {},
  };
}

function isCanvasHostedSubTask(ev: ProgressEv): boolean {
  const canvas = (ev.canvas_node_id ?? "").trim();
  const taskId = (ev.task_id ?? "").trim();
  return Boolean(canvas && taskId && taskId !== canvas);
}

function mergeNodeProgress(
  existing: CanvasNodeRunProgress | undefined,
  patch: Partial<CanvasNodeRunProgress>
): CanvasNodeRunProgress {
  const startedAtMs =
    patch.startedAtMs ??
    existing?.startedAtMs ??
    (patch.current != null || patch.total != null ? Date.now() : undefined);
  return {
    current: patch.current ?? existing?.current ?? 0,
    total: patch.total ?? existing?.total,
    label: patch.label ?? existing?.label,
    startedAtMs,
    elapsedMs: patch.elapsedMs ?? existing?.elapsedMs,
  };
}

function finalizeNodeProgress(
  existing: CanvasNodeRunProgress | undefined,
  rowFields: LocalRunProgressRowCounts,
  durationSec?: number
): CanvasNodeRunProgress {
  const read = rowFields.rows_read;
  const written = rowFields.rows_written;
  const count = written ?? read;
  const elapsedMs =
    typeof durationSec === "number" && Number.isFinite(durationSec)
      ? Math.max(0, Math.floor(durationSec * 1000))
      : existing?.startedAtMs != null
        ? Math.max(0, Date.now() - existing.startedAtMs)
        : existing?.elapsedMs;
  if (count != null && count > 0) {
    return {
      current: count,
      total: count,
      label: existing?.label,
      startedAtMs: existing?.startedAtMs,
      elapsedMs,
    };
  }
  if (existing?.total != null && existing.total > 0) {
    return {
      current: existing.total,
      total: existing.total,
      label: existing.label,
      startedAtMs: existing.startedAtMs,
      elapsedMs,
    };
  }
  if (existing && existing.current > 0) {
    return { ...existing, total: existing.total ?? existing.current, elapsedMs };
  }
  return { current: 1, total: 1, startedAtMs: existing?.startedAtMs, elapsedMs };
}

function nodeSuffixFor(ev: ProgressEv, taskId: string): string {
  const canvas = (ev.canvas_node_id ?? "").trim();
  if (canvas) return ` — ${canvas}`;
  const p = (ev.pipeline_node_id ?? "").trim();
  if (p && p !== taskId) return ` — ${p}`;
  return "";
}

export type TransformLocalRunTarget =
  | { kind: "pipeline"; id: string; scopeSuffix?: string }
  | { kind: "template"; id: string };

export async function streamTransformPipelineRun(
  target: TransformLocalRunTarget,
  options: { incrementalChangeProcessing: boolean; dryRun?: boolean; signal?: AbortSignal },
  canvasDoc: TransformCanvasDocument,
  t: TFn,
  callbacks: TransformRunStreamCallbacks
): Promise<void> {
  const apiBase =
    target.kind === "pipeline"
      ? `/api/transform/workflows/${encodeURIComponent(target.id)}`
      : `/api/transform/templates/${encodeURIComponent(target.id)}`;
  const executingCanvasNodes = new Set<string>();
  const runActiveCanvas = new Set<string>();
  const runCompletedCanvas = new Set<string>();
  const runFailedCanvas = new Set<string>();
  const runWarningCanvas = new Set<string>();
  const activeTasks = new Set<string>();
  const taskMeta = new Map<string, { fn?: string; node?: string }>();
  const taskSummaries: Record<string, unknown> = {};
  const nodeProgressById = new Map<string, CanvasNodeRunProgress>();

  const flushProgress = (busy: boolean) => {
    callbacks.onProgress({
      busy,
      executingCanvasNodeIds: [...executingCanvasNodes],
      runActiveCanvasNodeIds: [...runActiveCanvas],
      runCompletedCanvasNodeIds: [...runCompletedCanvas],
      failedCanvasNodeIds: [...runFailedCanvas],
      warningCanvasNodeIds: [...runWarningCanvas],
      nodeProgressById: Object.fromEntries(nodeProgressById),
    });
  };

  const appendExecutingLine = () => {
    if (activeTasks.size === 0) {
      callbacks.onLogAppend(`${t("run.localExecutingNow", { list: t("run.localExecutingNone") })}\n`);
      return;
    }
    const parts: string[] = [];
    for (const id of [...activeTasks].sort()) {
      const m = taskMeta.get(id);
      const fn = (m?.fn ?? "").trim();
      const node = (m?.node ?? "").trim();
      const label = fn || id;
      parts.push(node ? `${label} (${node})` : label);
    }
    callbacks.onLogAppend(`${t("run.localExecutingNow", { list: parts.join("; ") })}\n`);
  };

  flushProgress(true);
  nodeProgressById.clear();

  const scopeQ =
    target.kind === "pipeline"
      ? (() => {
          const scope = (target.scopeSuffix ?? "").trim();
          const normalized = scope === "all" ? "" : scope;
          return normalized ? `?scope_suffix=${encodeURIComponent(normalized)}` : "";
        })()
      : "";
  const res = await fetch(`${apiBase}/run-stream${scopeQ}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      dry_run: options.dryRun ?? false,
      incremental_change_processing: options.incrementalChangeProcessing,
    }),
    signal: options.signal,
  });

  if (res.status === 501) {
    callbacks.onLogAppend(`${t("flow.previewRunProgressUnsupported")}\n`);
    const fallback = await fetch(`${apiBase}/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
      dry_run: options.dryRun ?? false,
      incremental_change_processing: options.incrementalChangeProcessing,
    }),
    });
    if (!fallback.ok) {
      const errText = await fallback.text();
      throw new Error(errText || fallback.statusText);
    }
    const payload = (await fallback.json()) as TransformPipelineRunResult;
    callbacks.onLogAppend(`${payload.detail ?? (payload.ok ? t("transform.toolbar.runOk") : t("transform.toolbar.runFailed"))}\n`);
    callbacks.onComplete(payload);
    flushProgress(false);
    return;
  }

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(errText || res.statusText);
  }

  const reader = res.body?.getReader();
  if (!reader) throw new Error("No response body");

  const dec = new TextDecoder();
  let buf = "";
  let exitCode = -1;
  let cancelled = false;

  const handleLine = (line: string) => {
    if (!line.trim()) return;
    let ev: ProgressEv;
    try {
      ev = JSON.parse(line) as ProgressEv;
    } catch {
      return;
    }
    if (ev.event === "log" && typeof ev.message === "string") {
      const prefix = ev.level ? `[${ev.level}] ` : "";
      callbacks.onLogAppend(`${prefix}${ev.message}\n`);
      return;
    }
    if (ev.event === "task_progress") {
      const canvas = (ev.canvas_node_id ?? "").trim();
      if (canvas) {
        const current =
          typeof ev.progress_current === "number" && Number.isFinite(ev.progress_current)
            ? Math.max(0, Math.floor(ev.progress_current))
            : 0;
        const totalRaw = ev.progress_total;
        const total =
          typeof totalRaw === "number" && Number.isFinite(totalRaw) && totalRaw > 0
            ? Math.floor(totalRaw)
            : undefined;
        const label =
          typeof ev.progress_label === "string" && ev.progress_label.trim()
            ? ev.progress_label.trim()
            : undefined;
        nodeProgressById.set(
          canvas,
          mergeNodeProgress(nodeProgressById.get(canvas), {
            current,
            total,
            label,
          })
        );
        flushProgress(true);
      }
      return;
    }
    if (ev.event === "task_start" && ev.task_id) {
      const taskId = ev.task_id;
      const fn = (ev.function_external_id ?? "").trim();
      const canvas = (ev.canvas_node_id ?? "").trim();
      const pnode = (ev.pipeline_node_id ?? "").trim();
      const node = canvas || (pnode && pnode !== taskId ? pnode : "");
      taskMeta.set(taskId, { fn: fn || undefined, node: node || undefined });
      activeTasks.add(taskId);
      const startIds =
        canvas && canvasDoc.nodes.length > 0
          ? collectStartCanvasNodeIdsOnAnyPathToTarget(canvasDoc, canvas)
          : [];
      for (const sid of startIds) runCompletedCanvas.add(sid);
      if (canvas && !isCanvasHostedSubTask(ev)) {
        runFailedCanvas.delete(canvas);
        executingCanvasNodes.add(canvas);
        runActiveCanvas.add(canvas);
        nodeProgressById.set(
          canvas,
          mergeNodeProgress(nodeProgressById.get(canvas), { current: 0, startedAtMs: Date.now() })
        );
      }
      callbacks.onLogAppend(
        `${t("run.localTaskStart", {
          functionId: fn || taskId,
          taskId,
          nodeSuffix: nodeSuffixFor(ev, taskId),
        })}\n`
      );
      appendExecutingLine();
      flushProgress(true);
      return;
    }
    if (ev.event === "task_end" && ev.task_id) {
      const taskId = ev.task_id;
      const fn = (ev.function_external_id ?? "").trim();
      const canvas = (ev.canvas_node_id ?? "").trim();
      const statusRaw = typeof ev.status === "string" ? ev.status.trim().toLowerCase() : "";
      const failed = statusRaw === "failed";
      const warned = statusRaw === "completed_with_errors";
      const rowFields = rowCountFieldsFromEvent(ev);
      const durationSec =
        typeof ev.duration_sec === "number" && Number.isFinite(ev.duration_sec) ? ev.duration_sec : undefined;
      activeTasks.delete(taskId);
      taskMeta.delete(taskId);
      if (canvas && !isCanvasHostedSubTask(ev)) {
        executingCanvasNodes.delete(canvas);
        runActiveCanvas.delete(canvas);
        if (failed) {
          runFailedCanvas.add(canvas);
          runWarningCanvas.delete(canvas);
          runCompletedCanvas.delete(canvas);
          nodeProgressById.delete(canvas);
        } else if (warned) {
          runWarningCanvas.add(canvas);
          runFailedCanvas.delete(canvas);
          runCompletedCanvas.add(canvas);
          nodeProgressById.set(
            canvas,
            finalizeNodeProgress(nodeProgressById.get(canvas), rowFields, durationSec)
          );
        } else {
          runFailedCanvas.delete(canvas);
          runWarningCanvas.delete(canvas);
          runCompletedCanvas.add(canvas);
          nodeProgressById.set(
            canvas,
            finalizeNodeProgress(nodeProgressById.get(canvas), rowFields, durationSec)
          );
        }
      }
      taskSummaries[taskId] = {
        status: statusRaw || "succeeded",
        task_id: taskId,
        ...(canvas ? { canvas_node_id: canvas } : {}),
        ...(fn ? { function_external_id: fn } : {}),
        ...(ev.error ? { error: ev.error } : {}),
        ...(durationSec != null ? { duration_sec: durationSec } : {}),
        ...rowFields,
      };
      callbacks.onLogAppend(
        `${t("run.localTaskEnd", {
          functionId: fn || taskId,
          taskId,
          nodeSuffix: nodeSuffixFor(ev, taskId),
          rowSuffix: localRunRowSuffix({ ...ev, ...rowFields }, t),
        })}\n`
      );
      appendExecutingLine();
      flushProgress(true);
      return;
    }
    if (ev.event === "exit") {
      exitCode = ev.code ?? -1;
      if ((ev as { cancelled?: boolean }).cancelled) {
        cancelled = true;
      }
      callbacks.onLogAppend(`\n${t("run.localRunExitLine", { code: exitCode })}\n`);
      const stderr = String((ev as { stderr?: string }).stderr ?? "").trim();
      if (stderr) {
        callbacks.onLogAppend(`${stderr}\n`);
      }
      flushProgress(true);
    }
  };

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (value) buf += dec.decode(value, { stream: true });
      const parts = buf.split("\n");
      buf = parts.pop() ?? "";
      for (const line of parts) handleLine(line);
      if (done) break;
    }
    buf += dec.decode();
    for (const line of buf.split("\n")) handleLine(line);
  } catch (e) {
    if (options.signal?.aborted) {
      cancelled = true;
      callbacks.onLogAppend(`\n${t("run.localCancelled")}\n`);
    } else {
      throw e;
    }
  } finally {
    flushProgress(false);
    callbacks.onProgress({
      busy: false,
      executingCanvasNodeIds: [],
      runActiveCanvasNodeIds: [],
      runCompletedCanvasNodeIds: [],
      failedCanvasNodeIds: [...runFailedCanvas],
      warningCanvasNodeIds: [...runWarningCanvas],
      nodeProgressById: {},
    });
    const detail = cancelled
      ? t("run.localCancelled")
      : Object.keys(taskSummaries).length > 0
        ? formatLocalRunDetail(taskSummaries, t)
        : exitCode === 0
          ? t("transform.toolbar.runOk")
          : t("transform.toolbar.runFailed");
    callbacks.onComplete({
      ok: !cancelled && exitCode === 0,
      detail,
      task_summaries: taskSummaries,
    });
  }
}

export function initialTransformFlowRunProgress(): TransformFlowRunProgress {
  return emptyProgress(false);
}
