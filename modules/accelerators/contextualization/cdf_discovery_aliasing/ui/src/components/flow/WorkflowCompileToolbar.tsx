import type { MessageKey } from "../../i18n";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  /** When true (e.g. local pipeline run in progress), toolbar is display-only. */
  readOnly?: boolean;
};

/** Workflow compile mode is fixed to canvas DAG (``compile_workflow_dag: canvas``). */
export function WorkflowCompileToolbar({ t, readOnly }: Props) {
  void readOnly;
  return (
    <div className="kea-flow-compile-toolbar" style={{ padding: "0.5rem 0.75rem", borderBottom: "1px solid var(--kea-border, #ddd)" }}>
      <div style={{ display: "flex", flexDirection: "column", gap: "0.25rem" }}>
        <span className="kea-hint" style={{ margin: 0, fontWeight: 600 }}>
          {t("flow.workflowCompileModeLabel")}
        </span>
        <div className="kea-select" style={{ padding: "0.35rem 0.5rem", width: "100%", maxWidth: "100%" }}>
          {t("flow.workflowCompileModeCanvas")}
        </div>
      </div>
      <p className="kea-hint" style={{ margin: "0.5rem 0 0", fontSize: "0.78rem", lineHeight: 1.35 }}>
        {t("flow.workflowCompileModeHint")}
      </p>
    </div>
  );
}
