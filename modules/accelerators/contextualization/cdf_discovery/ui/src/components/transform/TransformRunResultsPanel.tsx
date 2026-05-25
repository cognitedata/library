import type { MessageKey } from "../../i18n";
import type { TransformCanvasDocument } from "../../types/transformCanvas";
import {
  formatTransformCanvasNodeLabelWithId,
  resolveTransformCanvasNodeForTask,
} from "../../utils/transformCanvasFlowSearch";
import type { TransformPipelineRunResult } from "../../types/transformTabRun";
import { resolveReadCount, resolveWriteCount } from "./localRunRowCounts";

type TaskSummary = Record<string, unknown>;

type Props = {
  t: (key: MessageKey, vars?: Record<string, string | number>) => string;
  canvas: TransformCanvasDocument;
  lastRun: TransformPipelineRunResult | null;
  /** Overrides default empty state when there is no local run yet. */
  emptyMessage?: string;
};

function taskStatusLabel(summary: TaskSummary | undefined): string {
  if (!summary) return "—";
  const status = String(summary.status ?? "ok");
  const reason = summary.reason != null ? String(summary.reason) : "";
  return reason ? `${status} (${reason})` : status;
}

function taskDurationLabel(summary: TaskSummary | undefined): string {
  const raw = summary?.duration_sec;
  if (typeof raw !== "number" || !Number.isFinite(raw)) return "—";
  return `${raw.toFixed(3)}s`;
}

function taskDetails(summary: TaskSummary | undefined, t: Props["t"]): string {
  if (!summary) return "";
  const parts: string[] = [];
  const read = resolveReadCount(summary);
  const written = resolveWriteCount(summary);
  if (read != null) parts.push(t("run.localTaskRowsRead", { count: read }));
  if (written != null) parts.push(t("run.localTaskRowsWritten", { count: written }));
  for (const key of ["transformation_external_id", "error"] as const) {
    const val = summary[key];
    if (val != null && String(val).trim()) parts.push(`${key}=${String(val)}`);
  }
  return parts.join("; ");
}

function canvasNodeDisplayForTask(
  canvas: TransformCanvasDocument,
  taskId: string,
  summary: TaskSummary | undefined
): string {
  const node = resolveTransformCanvasNodeForTask(canvas, taskId, summary);
  if (node) return formatTransformCanvasNodeLabelWithId(node);
  const fromSummary = summary?.canvas_node_id;
  if (fromSummary != null && String(fromSummary).trim()) return String(fromSummary).trim();
  return taskId;
}

export function TransformRunResultsPanel({ t, canvas, lastRun, emptyMessage }: Props) {
  const summaries = lastRun?.task_summaries ?? {};
  const rows = Object.entries(summaries).sort(([a], [b]) => a.localeCompare(b));

  return (
    <section className="transform-run-results" aria-label={t("transform.runResults.title")}>
      <header className="transform-run-results__header">
        <h3 className="transform-run-results__title">{t("transform.runResults.title")}</h3>
        <p className="transform-run-results__hint">{t("transform.runResults.hint")}</p>
      </header>
      {!lastRun ? (
        <p className="transform-run-results__empty">{emptyMessage ?? t("transform.runResults.empty")}</p>
      ) : (
        <>
          <dl className="transform-run-results__summary">
            <div>
              <dt>{t("transform.runResults.summaryStatus")}</dt>
              <dd>{lastRun.ok ? t("transform.runResults.statusOk") : t("transform.runResults.statusFailed")}</dd>
            </div>
            {lastRun.run_id ? (
              <div>
                <dt>{t("transform.runResults.summaryRunId")}</dt>
                <dd>{lastRun.run_id}</dd>
              </div>
            ) : null}
            {lastRun.detail ? (
              <div>
                <dt>{t("transform.runResults.summaryDetail")}</dt>
                <dd>{lastRun.detail}</dd>
              </div>
            ) : null}
          </dl>
          {rows.length === 0 ? (
            <p className="transform-run-results__empty">{t("transform.runResults.noTasks")}</p>
          ) : (
            <div className="disc-table-wrap">
              <table className="disc-table transform-run-results__table">
                <thead>
                  <tr>
                    <th>{t("transform.runResults.table.canvasNode")}</th>
                    <th>{t("transform.runResults.table.taskId")}</th>
                    <th>{t("transform.runResults.table.status")}</th>
                    <th>{t("transform.runResults.table.duration")}</th>
                    <th>{t("transform.runResults.table.details")}</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map(([taskId, raw]) => {
                    const summary = raw as TaskSummary;
                    return (
                      <tr key={taskId}>
                        <td>{canvasNodeDisplayForTask(canvas, taskId, summary)}</td>
                        <td>{taskId}</td>
                        <td>{taskStatusLabel(summary)}</td>
                        <td>{taskDurationLabel(summary)}</td>
                        <td>{taskDetails(summary, t)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </section>
  );
}
