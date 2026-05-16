/**
 * Scope YAML ``compile_workflow_dag`` (canvas-only). Mirrors
 * ``cdf_fn_common.workflow_compile.canvas_dag._compile_dag_mode``.
 */

export type CompileWorkflowDagMode = "canvas";

export function normalizeCompileWorkflowDagMode(raw: unknown): CompileWorkflowDagMode {
  const s = String(raw ?? "")
    .trim()
    .toLowerCase();
  if (s === "" || s === "canvas") {
    return "canvas";
  }
  throw new Error(`Unsupported compile_workflow_dag value: ${String(raw)} (use "canvas" or omit)`);
}

/** Read mode from root ``compile_workflow_dag`` only. */
export function readCompileWorkflowDagMode(doc: Record<string, unknown>): CompileWorkflowDagMode {
  const top = doc.compile_workflow_dag;
  if (top === undefined || top === null) {
    return "canvas";
  }
  return normalizeCompileWorkflowDagMode(top);
}

/** Set root ``compile_workflow_dag`` to ``canvas`` and remove duplicate ``workflow.compile_dag_mode``. */
export function patchCompileWorkflowDagMode(
  doc: Record<string, unknown>,
  _mode: CompileWorkflowDagMode = "canvas"
): Record<string, unknown> {
  const next: Record<string, unknown> = { ...doc, compile_workflow_dag: "canvas" };
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
