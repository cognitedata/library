/**
 * How scope YAML is compiled into ``workflow.input.compiled_workflow`` (canvas DAG).
 * Mirrors ``cdf_fn_common.workflow_compile.canvas_dag``.
 */

export type CompileWorkflowDagMode = "auto" | "canvas";

export function normalizeCompileWorkflowDagMode(raw: unknown): CompileWorkflowDagMode {
  const s = String(raw ?? "")
    .trim()
    .toLowerCase();
  if (s === "legacy") return "auto";
  if (s === "canvas" || s === "auto") return s;
  return "auto";
}

/** Read mode from root ``compile_workflow_dag`` or ``workflow.compile_dag_mode``. */
export function readCompileWorkflowDagMode(doc: Record<string, unknown>): CompileWorkflowDagMode {
  const top = doc.compile_workflow_dag;
  if (top !== undefined && top !== null) return normalizeCompileWorkflowDagMode(top);
  const wf = doc.workflow;
  if (wf != null && typeof wf === "object" && !Array.isArray(wf)) {
    const m = (wf as Record<string, unknown>).compile_dag_mode;
    if (m !== undefined && m !== null) return normalizeCompileWorkflowDagMode(m);
  }
  return "auto";
}

/** Patch scope document: sets root ``compile_workflow_dag`` and removes duplicate ``workflow.compile_dag_mode`` when aligned. */
export function patchCompileWorkflowDagMode(
  doc: Record<string, unknown>,
  mode: CompileWorkflowDagMode
): Record<string, unknown> {
  const next: Record<string, unknown> = { ...doc, compile_workflow_dag: mode };
  const wf = next.workflow;
  if (wf != null && typeof wf === "object" && !Array.isArray(wf)) {
    const w = { ...(wf as Record<string, unknown>) };
    if ("compile_dag_mode" in w) delete w.compile_dag_mode;
    if (Object.keys(w).length === 0) {
      delete next.workflow;
    } else {
      next.workflow = w;
    }
  }
  return next;
}
