import { useEffect, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n/types";
import type { WorkflowCanvasDocument } from "../types/workflowCanvas";
import { listSaveNodes, saveNodeListLabel } from "../utils/queriesCanvasUtils";
import { SaveNodeConfigFields } from "./flow/SaveNodeConfigFields";

type Props = {
  canvas: WorkflowCanvasDocument;
  onChange: (next: WorkflowCanvasDocument) => void;
  /** Select this canvas node when opened from flow double-click. */
  initialNodeId?: string;
  /** Default view space from module ``schema_space`` (view save editor). */
  schema_space?: string;
};

function saveKindLabelKey(kind: string | undefined): MessageKey {
  switch (kind) {
    case "save_view":
      return "flow.discoveryViewSave";
    case "save_raw":
      return "flow.discoveryRawSave";
    case "save_classic":
      return "flow.discoveryClassicSave";
    default:
      return "flow.nodeEditorTitleSave";
  }
}

export function PersistenceControls({ canvas, onChange, initialNodeId, schema_space }: Props) {
  const { t } = useAppSettings();
  const nodes = listSaveNodes(canvas);
  const [selectedId, setSelectedId] = useState<string | null>(nodes[0]?.id ?? null);

  useEffect(() => {
    if (nodes.length === 0) {
      setSelectedId(null);
      return;
    }
    setSelectedId((sel) => {
      if (sel && nodes.some((n) => n.id === sel)) return sel;
      return nodes[0]?.id ?? null;
    });
  }, [nodes]);

  useEffect(() => {
    if (!initialNodeId || nodes.length === 0) return;
    if (nodes.some((n) => n.id === initialNodeId)) {
      setSelectedId(initialNodeId);
    }
  }, [initialNodeId, nodes]);

  const selected = nodes.find((n) => n.id === selectedId) ?? null;

  return (
    <div className="discovery-source-views">
      <div className="discovery-toolbar-inline">
        <h3 className="discovery-section-title" style={{ margin: 0 }}>
          {t("persistence.title")}
        </h3>
      </div>

      <p className="discovery-hint" style={{ marginTop: "0.35rem", marginBottom: "0.85rem" }}>
        {t("persistence.canvasHint")}
      </p>

      <div className="discovery-source-views-split">
        <aside className="discovery-source-views-sidebar">
          <p className="discovery-artifact-list-title">{t("persistence.listTitle")}</p>
          <ul className="discovery-source-views-list" role="listbox" aria-label={t("persistence.listAriaLabel")}>
            {nodes.map((n) => (
              <li key={n.id} role="none">
                <button
                  type="button"
                  role="option"
                  aria-selected={selectedId === n.id}
                  className={`discovery-source-views-item${selectedId === n.id ? " discovery-source-views-item--active" : ""}`}
                  onClick={() => setSelectedId(n.id)}
                >
                  <span className="discovery-hint" style={{ display: "block", fontSize: "0.68rem", marginBottom: 2 }}>
                    {t(saveKindLabelKey(n.kind))} · {n.id}
                  </span>
                  {saveNodeListLabel(n)}
                </button>
              </li>
            ))}
          </ul>
        </aside>

        <div className="discovery-source-views-editor">
          {!selected ? (
            <p className="discovery-hint">{t("persistence.emptyEditor")}</p>
          ) : (
            <div className="discovery-source-views-editor-inner">
              <div className="discovery-toolbar-inline" style={{ marginBottom: "0.85rem" }}>
                <span className="discovery-hint" style={{ margin: 0 }}>
                  {t("persistence.stageLabel")} · {t(saveKindLabelKey(selected.kind))} · {saveNodeListLabel(selected)}
                </span>
              </div>
              <SaveNodeConfigFields
                canvas={canvas}
                onChange={onChange}
                nodeId={selected.id}
                t={t}
                schema_space={schema_space}
              />
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
