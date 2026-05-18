import { useEffect, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { WorkflowCanvasDocument } from "../types/workflowCanvas";
import {
  listTransformNodes,
  patchTransformNode,
  readTransformConfig,
  isHandlerTypedTransformNode,
  readTransformHandlerId,
  transformNodeListLabel,
} from "../utils/transformsCanvasUtils";
import { TransformNodeConfigFields } from "./TransformNodeConfigFields";

type Props = {
  canvas: WorkflowCanvasDocument;
  onChange: (next: WorkflowCanvasDocument) => void;
  initialNodeId?: string;
  singleNode?: boolean;
};

export function TransformsControls({ canvas, onChange, initialNodeId, singleNode }: Props) {
  const { t } = useAppSettings();
  const transforms = listTransformNodes(canvas);
  const [selectedId, setSelectedId] = useState<string | null>(() => {
    if (singleNode && initialNodeId && transforms.some((n) => n.id === initialNodeId)) {
      return initialNodeId;
    }
    return transforms[0]?.id ?? null;
  });

  useEffect(() => {
    if (singleNode) {
      setSelectedId(
        initialNodeId && transforms.some((n) => n.id === initialNodeId) ? initialNodeId : null
      );
      return;
    }
    if (transforms.length === 0) {
      setSelectedId(null);
      return;
    }
    setSelectedId((sel) => {
      if (sel && transforms.some((n) => n.id === sel)) return sel;
      return transforms[0]?.id ?? null;
    });
  }, [transforms, singleNode, initialNodeId]);

  useEffect(() => {
    if (singleNode || !initialNodeId || transforms.length === 0) return;
    if (transforms.some((n) => n.id === initialNodeId)) {
      setSelectedId(initialNodeId);
    }
  }, [initialNodeId, transforms, singleNode]);

  const selected = transforms.find((n) => n.id === selectedId) ?? null;

  if (singleNode) {
    if (!selected) {
      return (
        <p className="kea-hint" style={{ marginTop: 0 }}>
          {t("flow.nodeEditorFocusedNodeMissing")}
        </p>
      );
    }
    return (
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
    );
  }

  return (
    <div className="kea-source-views">
      <div className="kea-toolbar-inline">
        <h3 className="kea-section-title" style={{ margin: 0 }}>
          {t("transforms.title")}
        </h3>
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
              <p className="kea-hint" style={{ margin: "0 0 0.85rem" }}>
                {readTransformHandlerId(selected) || t("flow.discoveryTransform")} —{" "}
                {transformNodeListLabel(selected)}
              </p>
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
