import { useEffect, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import type { WorkflowCanvasDocument } from "../types/workflowCanvas";
import {
  listValidationNodeRefs,
  patchValidationNode,
  readValidationConfig,
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
  singleNode?: boolean;
};

export function ValidationsControls({ canvas, onChange, initialNodeId, singleNode }: Props) {
  const { t } = useAppSettings();
  const refs = listValidationNodeRefs(canvas);
  const [selectedKey, setSelectedKey] = useState<string | null>(() => {
    if (singleNode && initialNodeId) {
      const hit = refs.find((r) => r.node.id === initialNodeId);
      return hit ? validationNodeLocationKey(hit) : null;
    }
    return refs[0] ? validationNodeLocationKey(refs[0]) : null;
  });

  useEffect(() => {
    if (singleNode) {
      const hit = initialNodeId ? refs.find((r) => r.node.id === initialNodeId) : null;
      setSelectedKey(hit ? validationNodeLocationKey(hit) : null);
      return;
    }
    if (refs.length === 0) {
      setSelectedKey(null);
      return;
    }
    setSelectedKey((sel) => {
      if (sel && refs.some((r) => validationNodeLocationKey(r) === sel)) return sel;
      return validationNodeLocationKey(refs[0]!);
    });
  }, [refs, singleNode, initialNodeId]);

  useEffect(() => {
    if (singleNode || !initialNodeId || refs.length === 0) return;
    const hit = refs.find((r) => r.node.id === initialNodeId);
    if (hit) {
      setSelectedKey(validationNodeLocationKey(hit));
    }
  }, [initialNodeId, refs, singleNode]);

  const selected: ValidationNodeRef | null =
    refs.find((r) => validationNodeLocationKey(r) === selectedKey) ?? null;

  if (singleNode) {
    if (!selected) {
      return (
        <p className="kea-hint" style={{ marginTop: 0 }}>
          {t("flow.nodeEditorFocusedNodeMissing")}
        </p>
      );
    }
    return (
      <ValidationNodeConfigFields
        value={readValidationConfig(selected.node)}
        onChange={(cfg) =>
          onChange(
            patchValidationNode(canvas, selected.node.id, cfg as JsonObject, selected.subgraphPath)
          )
        }
      />
    );
  }

  return (
    <div className="kea-source-views">
      <div className="kea-toolbar-inline">
        <h3 className="kea-section-title" style={{ margin: 0 }}>
          {t("validations.title")}
        </h3>
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
              <p className="kea-hint" style={{ margin: "0 0 0.85rem" }}>
                {t("validations.stageLabel")} · {validationNodeListLabel(selected.node)}
              </p>
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
