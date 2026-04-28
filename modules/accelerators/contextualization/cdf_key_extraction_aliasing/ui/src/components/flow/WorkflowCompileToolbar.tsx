import type { ChangeEvent } from "react";
import type { MessageKey } from "../../i18n";
import {
  type CompileWorkflowDagMode,
  patchCompileWorkflowDagMode,
  readCompileWorkflowDagMode,
} from "../../utils/workflowCompileMode";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  workflowScopeDoc: Record<string, unknown>;
  onPatchWorkflowScope: (recipe: (doc: Record<string, unknown>) => Record<string, unknown>) => void;
  /** When true (e.g. local pipeline run in progress), compile mode cannot be changed. */
  readOnly?: boolean;
};

export function WorkflowCompileToolbar({ t, workflowScopeDoc, onPatchWorkflowScope, readOnly }: Props) {
  const mode = readCompileWorkflowDagMode(workflowScopeDoc);
  const onChange = (e: ChangeEvent<HTMLSelectElement>) => {
    const next = e.target.value as CompileWorkflowDagMode;
    onPatchWorkflowScope((d) => patchCompileWorkflowDagMode({ ...d }, next));
  };
  return (
    <div className="kea-flow-compile-toolbar" style={{ padding: "0.5rem 0.75rem", borderBottom: "1px solid var(--kea-border, #ddd)" }}>
      <label style={{ display: "flex", flexDirection: "column", gap: "0.25rem" }}>
        <span className="kea-hint" style={{ margin: 0, fontWeight: 600 }}>
          {t("flow.workflowCompileModeLabel")}
        </span>
        <select
          className="kea-select"
          style={{ width: "100%", maxWidth: "100%" }}
          value={mode}
          onChange={onChange}
          disabled={readOnly}
        >
          <option value="auto">{t("flow.workflowCompileModeAuto")}</option>
          <option value="canvas">{t("flow.workflowCompileModeCanvas")}</option>
        </select>
      </label>
      <p className="kea-hint" style={{ margin: "0.5rem 0 0", fontSize: "0.78rem", lineHeight: 1.35 }}>
        {t("flow.workflowCompileModeHint")}
      </p>
    </div>
  );
}
