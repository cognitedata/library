import { useEffect, useState } from "react";
import type { MessageKey } from "../i18n";
import type { WorkflowCanvasDocument } from "../types/workflowCanvas";
import { patchNodeConfig, readNodeConfig } from "../utils/queriesCanvasUtils";
import { joinNodeListLabel, listJoinNodes } from "../utils/joinsCanvasUtils";
import { JoinNodeConfigFields } from "./JoinNodeConfigFields";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  canvas: WorkflowCanvasDocument;
  onChange: (next: WorkflowCanvasDocument) => void;
  initialNodeId?: string;
  t: TFn;
  singleNode?: boolean;
};

export function JoinsControls({ canvas, onChange, initialNodeId, t, singleNode }: Props) {
  const joins = listJoinNodes(canvas);
  const [selectedId, setSelectedId] = useState<string | null>(() => {
    if (singleNode && initialNodeId && joins.some((n) => n.id === initialNodeId)) {
      return initialNodeId;
    }
    return joins[0]?.id ?? null;
  });

  useEffect(() => {
    if (singleNode) {
      setSelectedId(
        initialNodeId && joins.some((n) => n.id === initialNodeId) ? initialNodeId : null
      );
      return;
    }
    if (joins.length === 0) {
      setSelectedId(null);
      return;
    }
    setSelectedId((sel) => {
      if (sel && joins.some((n) => n.id === sel)) return sel;
      return joins[0]?.id ?? null;
    });
  }, [joins, singleNode, initialNodeId]);

  useEffect(() => {
    if (singleNode || !initialNodeId || joins.length === 0) return;
    if (joins.some((n) => n.id === initialNodeId)) {
      setSelectedId(initialNodeId);
    }
  }, [initialNodeId, joins, singleNode]);

  const selected = joins.find((n) => n.id === selectedId) ?? null;

  if (singleNode) {
    if (!selected) {
      return (
        <p className="kea-hint" style={{ marginTop: 0 }}>
          {t("flow.nodeEditorFocusedNodeMissing")}
        </p>
      );
    }
    return (
      <JoinNodeConfigFields
        t={t}
        value={readNodeConfig(selected)}
        onChange={(cfg) => onChange(patchNodeConfig(canvas, selected.id, cfg))}
      />
    );
  }

  return (
    <div className="kea-source-views">
      <div className="kea-toolbar-inline">
        <h3 className="kea-section-title" style={{ margin: 0 }}>
          {t("joins.title")}
        </h3>
      </div>

      <p className="kea-hint" style={{ marginTop: "0.35rem", marginBottom: "0.85rem" }}>
        {t("joins.canvasHint")}
      </p>

      <div className="kea-source-views-split">
        <aside className="kea-source-views-sidebar">
          <p className="kea-artifact-list-title">{t("joins.listTitle")}</p>
          <ul className="kea-source-views-list" role="listbox" aria-label={t("joins.listAriaLabel")}>
            {joins.map((n) => (
              <li key={n.id} role="none">
                <button
                  type="button"
                  role="option"
                  aria-selected={selectedId === n.id}
                  className={`kea-source-views-item${selectedId === n.id ? " kea-source-views-item--active" : ""}`}
                  onClick={() => setSelectedId(n.id)}
                >
                  <span className="kea-hint" style={{ display: "block", fontSize: "0.68rem", marginBottom: 2 }}>
                    join · {n.id}
                  </span>
                  {joinNodeListLabel(n)}
                </button>
              </li>
            ))}
          </ul>
        </aside>

        <div className="kea-source-views-editor">
          {!selected ? (
            <p className="kea-hint">{t("joins.emptyEditor")}</p>
          ) : (
            <div className="kea-source-views-editor-inner">
              <p className="kea-hint" style={{ margin: "0 0 0.85rem" }}>
                {t("flow.discoveryJoin")} — {joinNodeListLabel(selected)}
              </p>
              <JoinNodeConfigFields
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