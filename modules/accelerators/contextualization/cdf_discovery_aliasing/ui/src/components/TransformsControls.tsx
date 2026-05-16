import { useEffect, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { WorkflowCanvasDocument } from "../types/workflowCanvas";
import { type TransformHandlerId } from "../components/flow/handlerRegistry";
import { TransformHandlerSelect } from "./transforms/TransformHandlerSelect";
import {
  addTransformNode,
  listTransformNodes,
  patchTransformNode,
  readTransformConfig,
  isHandlerTypedTransformNode,
  readTransformHandlerId,
  removeTransformNode,
  transformNodeListLabel,
} from "../utils/transformsCanvasUtils";
import { TransformNodeConfigFields } from "./TransformNodeConfigFields";

type Props = {
  canvas: WorkflowCanvasDocument;
  onChange: (next: WorkflowCanvasDocument) => void;
  initialNodeId?: string;
};

export function TransformsControls({ canvas, onChange, initialNodeId }: Props) {
  const { t } = useAppSettings();
  const transforms = listTransformNodes(canvas);
  const [selectedId, setSelectedId] = useState<string | null>(transforms[0]?.id ?? null);
  const [addHandler, setAddHandler] = useState<TransformHandlerId>("regex_substitution");

  useEffect(() => {
    if (transforms.length === 0) {
      setSelectedId(null);
      return;
    }
    setSelectedId((sel) => {
      if (sel && transforms.some((n) => n.id === sel)) return sel;
      return transforms[0]?.id ?? null;
    });
  }, [transforms]);

  useEffect(() => {
    if (!initialNodeId || transforms.length === 0) return;
    if (transforms.some((n) => n.id === initialNodeId)) {
      setSelectedId(initialNodeId);
    }
  }, [initialNodeId, transforms]);

  const selected = transforms.find((n) => n.id === selectedId) ?? null;

  return (
    <div className="kea-source-views">
      <div className="kea-toolbar-inline">
        <h3 className="kea-section-title" style={{ margin: 0 }}>
          {t("transforms.title")}
        </h3>
        <label className="kea-label" style={{ margin: 0, display: "flex", alignItems: "center", gap: 8 }}>
          <span className="kea-hint" style={{ margin: 0 }}>
            {t("transforms.addHandler")}
          </span>
          <TransformHandlerSelect
            style={{ marginTop: 0, minWidth: "12rem" }}
            value={addHandler}
            onChange={(h) => setAddHandler(h as TransformHandlerId)}
            coreGroupLabel={t("transforms.handlerGroup.core")}
            eltGroupLabel={t("transforms.handlerGroup.elt")}
          />
        </label>
        <button
          type="button"
          className="kea-btn kea-btn--primary kea-btn--sm"
          onClick={() => {
            const { canvas: next, nodeId } = addTransformNode(canvas, addHandler);
            onChange(next);
            setSelectedId(nodeId);
          }}
        >
          {t("transforms.addTransform")}
        </button>
      </div>

      <p className="kea-hint" style={{ marginTop: "0.35rem", marginBottom: "0.85rem" }}>
        {t("transforms.canvasHint")}
      </p>

      <div className="kea-source-views-split">
        <aside className="kea-source-views-sidebar">
          <p className="kea-artifact-list-title">{t("transforms.listTitle")}</p>
          <ul className="kea-source-views-list" role="listbox" aria-label={t("transforms.listAriaLabel")}>
            {transforms.map((n) => (
              <li key={n.id} role="none">
                <button
                  type="button"
                  role="option"
                  aria-selected={selectedId === n.id}
                  className={`kea-source-views-item${selectedId === n.id ? " kea-source-views-item--active" : ""}`}
                  onClick={() => setSelectedId(n.id)}
                >
                  <span className="kea-hint" style={{ display: "block", fontSize: "0.68rem", marginBottom: 2 }}>
                    {readTransformHandlerId(n) || "transform"} · {n.id}
                  </span>
                  {transformNodeListLabel(n)}
                </button>
              </li>
            ))}
          </ul>
        </aside>

        <div className="kea-source-views-editor">
          {!selected ? (
            <p className="kea-hint">{t("transforms.emptyEditor")}</p>
          ) : (
            <div className="kea-source-views-editor-inner">
              <div className="kea-toolbar-inline" style={{ marginBottom: "0.85rem" }}>
                <span className="kea-hint" style={{ margin: 0 }}>
                  {readTransformHandlerId(selected) || t("flow.discoveryTransform")} —{" "}
                  {transformNodeListLabel(selected)}
                </span>
                <button
                  type="button"
                  className="kea-btn kea-btn--ghost kea-btn--sm"
                  onClick={() => onChange(removeTransformNode(canvas, selected.id))}
                >
                  {t("transforms.removeTransform")}
                </button>
              </div>
              <TransformNodeConfigFields
                value={readTransformConfig(selected)}
                handlerLocked={isHandlerTypedTransformNode(selected)}
                onChange={(cfg) =>
                  onChange(
                    patchTransformNode(
                      canvas,
                      selected.id,
                      cfg,
                      isHandlerTypedTransformNode(selected)
                        ? readTransformHandlerId(selected)
                        : String(cfg.handler_id ?? cfg.handler ?? "").trim() || undefined
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
