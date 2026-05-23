import { useCallback, useMemo } from "react";
import type { MessageKey } from "../../i18n";
import type { JsonObject } from "../../types/scopeConfig";
import type { WorkflowCanvasDocument } from "../../types/workflowCanvas";
import { FieldPoliciesEditor } from "../FieldPoliciesEditor";
import { ViewQueryConfigFields } from "../ViewQueryConfigFields";
import { patchNodeConfig, readNodeConfig } from "../../utils/queriesCanvasUtils";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  canvas: WorkflowCanvasDocument;
  onChange: (next: WorkflowCanvasDocument) => void;
  nodeId: string;
  t: TFn;
  /** Default view space from module ``schema_space`` (same as view query editor). */
  schema_space?: string;
};

export function SaveNodeConfigFields({ canvas, onChange, nodeId, t, schema_space }: Props) {
  const node = useMemo(() => canvas.nodes.find((n) => n.id === nodeId) ?? null, [canvas.nodes, nodeId]);
  const cfg = useMemo(() => (node ? readNodeConfig(node) : {}), [node]);
  const isViewSave = node?.kind === "save_view";

  const fanIn = String(cfg.save_fan_in_mode ?? "none").trim() || "none";

  const patchCfg = useCallback(
    (p: JsonObject) => {
      const n = canvas.nodes.find((x) => x.id === nodeId);
      if (!n) return;
      const cur = readNodeConfig(n);
      onChange(patchNodeConfig(canvas, nodeId, { ...cur, ...p }));
    },
    [canvas, nodeId, onChange]
  );

  if (!node) {
    return <p className="discovery-hint">{t("flow.saveNodeMissing")}</p>;
  }

  return (
    <div className="discovery-loc-fields" style={{ maxWidth: "52rem" }}>
      {isViewSave ? (
        <ViewQueryConfigFields
          fieldKey={nodeId}
          value={cfg}
          schema_space={schema_space}
          variant="viewTarget"
          onChange={(next) => onChange(patchNodeConfig(canvas, nodeId, next))}
        />
      ) : null}
      <h3 className="discovery-section-title" style={{ marginTop: isViewSave ? "1.25rem" : 0 }}>
        {t("flow.saveNodeConfigTitle")}
      </h3>
      <label className="discovery-label discovery-label--block">
        {t("flow.saveFanInMode")}
        <select
          className="discovery-select"
          style={{ marginTop: "0.35rem" }}
          value={fanIn}
          onChange={(e) => patchCfg({ save_fan_in_mode: e.target.value })}
        >
          <option value="none">{t("flow.saveFanInNone")}</option>
          <option value="merge_per_instance">{t("flow.saveFanInMerge")}</option>
        </select>
      </label>
      <p className="discovery-hint" style={{ marginTop: "0.35rem" }}>
        {t("flow.saveFanInHint")}
      </p>

      <div style={{ marginTop: "1rem" }}>
        <FieldPoliciesEditor
          t={t}
          policies={cfg.save_field_policies}
          onChange={(policies) => patchCfg({ save_field_policies: policies })}
          omitWhenEmpty
        />
      </div>
    </div>
  );
}
