import { useEffect, useState } from "react";
import type { MessageKey } from "../i18n";
import type { WorkflowCanvasDocument } from "../types/workflowCanvas";
import { patchNodeConfig, readNodeConfig } from "../utils/queriesCanvasUtils";
import { listMergeNodes, mergeNodeListLabel } from "../utils/mergesCanvasUtils";
import { MergeNodeConfigFields } from "./MergeNodeConfigFields";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  canvas: WorkflowCanvasDocument;
  onChange: (next: WorkflowCanvasDocument) => void;
  initialNodeId?: string;
  t: TFn;
  singleNode?: boolean;
};

export function MergesControls({ canvas, onChange, initialNodeId, t, singleNode }: Props) {
  const merges = listMergeNodes(canvas);
  const [selectedId, setSelectedId] = useState<string | null>(() => {
    if (singleNode && initialNodeId && merges.some((n) => n.id === initialNodeId)) {
      return initialNodeId;
    }
    return merges[0]?.id ?? null;
  });

  useEffect(() => {
    if (singleNode) {
      setSelectedId(
        initialNodeId && merges.some((n) => n.id === initialNodeId) ? initialNodeId : null
      );
      return;
    }
    if (merges.length === 0) {
      setSelectedId(null);
      return;
    }
    setSelectedId((sel) => {
      if (sel && merges.some((n) => n.id === sel)) return sel;
      return merges[0]?.id ?? null;
    });
  }, [merges, singleNode, initialNodeId]);

  useEffect(() => {
    if (singleNode || !initialNodeId || merges.length === 0) return;
    if (merges.some((n) => n.id === initialNodeId)) {
      setSelectedId(initialNodeId);
    }
  }, [initialNodeId, merges, singleNode]);

  const selected = merges.find((n) => n.id === selectedId) ?? null;

  if (singleNode) {
    if (!selected) {
      return (
        <p className="discovery-hint" style={{ marginTop: 0 }}>
          {t("flow.nodeEditorFocusedNodeMissing")}
        </p>
      );
    }
    return (
      <MergeNodeConfigFields
        t={t}
        value={readNodeConfig(selected)}
        onChange={(cfg) => onChange(patchNodeConfig(canvas, selected.id, cfg))}
      />
    );
  }

  return (
    <div className="discovery-source-views">
      <div className="discovery-toolbar-inline">
        <h3 className="discovery-section-title" style={{ margin: 0 }}>
          {t("merges.title")}
        </h3>
      </div>

      <p className="discovery-hint" style={{ marginTop: "0.35rem", marginBottom: "0.85rem" }}>
        {t("merges.canvasHint")}
      </p>

      <div className="discovery-source-views-split">
        <aside className="discovery-source-views-sidebar">
          <p className="discovery-artifact-list-title">{t("merges.listTitle")}</p>
          <ul className="discovery-source-views-list" role="listbox" aria-label={t("merges.listAriaLabel")}>
            {merges.map((n) => (
              <li key={n.id} role="none">
                <button
                  type="button"
                  role="option"
                  aria-selected={selectedId === n.id}
                  className={`discovery-source-views-item${selectedId === n.id ? " discovery-source-views-item--active" : ""}`}
                  onClick={() => setSelectedId(n.id)}
                >
                  <span className="discovery-hint" style={{ display: "block", fontSize: "0.68rem", marginBottom: 2 }}>
                    merge · {n.id}
                  </span>
                  {mergeNodeListLabel(n)}
                </button>
              </li>
            ))}
          </ul>
        </aside>

        <div className="discovery-source-views-editor">
          {!selected ? (
            <p className="discovery-hint">{t("merges.emptyEditor")}</p>
          ) : (
            <div className="discovery-source-views-editor-inner">
              <p className="discovery-hint" style={{ margin: "0 0 0.85rem" }}>
                {t("flow.discoveryMerge")} — {mergeNodeListLabel(selected)}
              </p>
              <MergeNodeConfigFields
                t={t}
                value={readNodeConfig(selected)}
                onChange={(cfg) => onChange(patchNodeConfig(canvas, selected.id, cfg))}
              />
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
