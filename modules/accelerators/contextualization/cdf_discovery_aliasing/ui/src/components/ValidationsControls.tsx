import { useEffect, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import type { WorkflowCanvasDocument } from "../types/workflowCanvas";
import {
  addValidationNode,
  listValidationNodeRefs,
  patchValidationNode,
  readValidationConfig,
  removeValidationNode,
  validationNodeContainerLabel,
  validationNodeListLabel,
  validationNodeLocationKey,
  type ValidationNodeRef,
} from "../utils/validationsCanvasUtils";
import { ValidationNodeConfigFields } from "./ValidationNodeConfigFields";

type Props = {
  canvas: WorkflowCanvasDocument;
  onChange: (next: WorkflowCanvasDocument) => void;
  initialNodeId?: string;
};

export function ValidationsControls({ canvas, onChange, initialNodeId }: Props) {
  const { t } = useAppSettings();
  const refs = listValidationNodeRefs(canvas);
  const [selectedKey, setSelectedKey] = useState<string | null>(
    refs[0] ? validationNodeLocationKey(refs[0]) : null
  );

  useEffect(() => {
    if (refs.length === 0) {
      setSelectedKey(null);
      return;
    }
    setSelectedKey((sel) => {
      if (sel && refs.some((r) => validationNodeLocationKey(r) === sel)) return sel;
      return validationNodeLocationKey(refs[0]!);
    });
  }, [refs]);

  useEffect(() => {
    if (!initialNodeId || refs.length === 0) return;
    const hit = refs.find((r) => r.node.id === initialNodeId);
    if (hit) {
      setSelectedKey(validationNodeLocationKey(hit));
    }
  }, [initialNodeId, refs]);

  const selected: ValidationNodeRef | null =
    refs.find((r) => validationNodeLocationKey(r) === selectedKey) ?? null;

  return (
    <div className="kea-source-views">
      <div className="kea-toolbar-inline">
        <h3 className="kea-section-title" style={{ margin: 0 }}>
          {t("validations.title")}
        </h3>
        <button
          type="button"
          className="kea-btn kea-btn--primary kea-btn--sm"
          onClick={() => {
            const { canvas: next, nodeId } = addValidationNode(canvas, {
              subgraphPath: selected?.subgraphPath,
            });
            onChange(next);
            const hit = listValidationNodeRefs(next).find((r) => r.node.id === nodeId);
            if (hit) setSelectedKey(validationNodeLocationKey(hit));
          }}
        >
          {t("validations.addNode")}
        </button>
      </div>

      <p className="kea-hint" style={{ marginTop: "0.35rem", marginBottom: "0.85rem" }}>
        {t("validations.canvasHint")}
      </p>

      <div className="kea-source-views-split">
        <aside className="kea-source-views-sidebar">
          <p className="kea-artifact-list-title">{t("validations.listTitle")}</p>
          <ul className="kea-source-views-list" role="listbox" aria-label={t("validations.listAriaLabel")}>
            {refs.map((ref) => {
              const key = validationNodeLocationKey(ref);
              const container = validationNodeContainerLabel(canvas, ref.subgraphPath);
              return (
              <li key={key} role="none">
                <button
                  type="button"
                  role="option"
                  aria-selected={selectedKey === key}
                  className={`kea-source-views-item${selectedKey === key ? " kea-source-views-item--active" : ""}`}
                  onClick={() => setSelectedKey(key)}
                >
                  {container ? (
                    <span className="kea-hint" style={{ display: "block", fontSize: "0.68rem", marginBottom: 2 }}>
                      {t("validations.insideSubgraph", { name: container })}
                    </span>
                  ) : null}
                  <span className="kea-hint" style={{ display: "block", fontSize: "0.68rem", marginBottom: 2 }}>
                    {t("flow.discoveryValidate")} · {ref.node.id}
                  </span>
                  {validationNodeListLabel(ref.node)}
                </button>
              </li>
            );
            })}
          </ul>
        </aside>

        <div className="kea-source-views-editor">
          {!selected ? (
            <p className="kea-hint">{t("validations.emptyEditor")}</p>
          ) : (
            <div className="kea-source-views-editor-inner">
              <div className="kea-toolbar-inline" style={{ marginBottom: "0.85rem" }}>
                <span className="kea-hint" style={{ margin: 0 }}>
                  {t("validations.stageLabel")} · {validationNodeListLabel(selected.node)}
                </span>
                <button
                  type="button"
                  className="kea-btn kea-btn--ghost kea-btn--sm"
                  onClick={() =>
                    onChange(
                      removeValidationNode(canvas, selected.node.id, selected.subgraphPath)
                    )
                  }
                >
                  {t("validations.removeNode")}
                </button>
              </div>
              <ValidationNodeConfigFields
                value={readValidationConfig(selected.node)}
                onChange={(cfg) =>
                  onChange(
                    patchValidationNode(
                      canvas,
                      selected.node.id,
                      cfg as JsonObject,
                      selected.subgraphPath
                    )
                  )
                }
              />
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
