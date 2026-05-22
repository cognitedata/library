import { useEffect, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { WorkflowCanvasDocument } from "../types/workflowCanvas";
import { readTransformHandlerId as readCfgHandlerId } from "../utils/transformHandlerTemplates";
import { materializeTransformSteps } from "../utils/transformNodeConfigModel";
import {
  listTransformNodes,
  patchTransformNode,
  readTransformConfig,
  isHandlerTypedTransformNode,
  readTransformHandlerId,
  transformNodeListLabel,
} from "../utils/transformsCanvasUtils";

function handlerIdForTransformConfig(cfg: Record<string, unknown>): string | undefined {
  const steps = materializeTransformSteps(cfg);
  const fromStep = steps.length > 0 ? readCfgHandlerId(steps[0]!) : "";
  const flat = readCfgHandlerId(cfg);
  const h = fromStep || flat;
  return h || undefined;
}
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
        <p className="discovery-hint" style={{ marginTop: 0 }}>
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
                : handlerIdForTransformConfig(cfg as Record<string, unknown>)
            )
          )
        }
      />
    );
  }

  return (
    <div className="discovery-source-views">
      <div className="discovery-toolbar-inline">
        <h3 className="discovery-section-title" style={{ margin: 0 }}>
          {t("transforms.title")}
        </h3>
      </div>

      <p className="discovery-hint" style={{ marginTop: "0.35rem", marginBottom: "0.85rem" }}>
        {t("transforms.canvasHint")}
      </p>

      <div className="discovery-source-views-split">
        <aside className="discovery-source-views-sidebar">
          <p className="discovery-artifact-list-title">{t("transforms.listTitle")}</p>
          <ul className="discovery-source-views-list" role="listbox" aria-label={t("transforms.listAriaLabel")}>
            {transforms.map((n) => (
              <li key={n.id} role="none">
                <button
                  type="button"
                  role="option"
                  aria-selected={selectedId === n.id}
                  className={`discovery-source-views-item${selectedId === n.id ? " discovery-source-views-item--active" : ""}`}
                  onClick={() => setSelectedId(n.id)}
                >
                  <span className="discovery-hint" style={{ display: "block", fontSize: "0.68rem", marginBottom: 2 }}>
                    {readTransformHandlerId(n) || "transform"} · {n.id}
                  </span>
                  {transformNodeListLabel(n)}
                </button>
              </li>
            ))}
          </ul>
        </aside>

        <div className="discovery-source-views-editor">
          {!selected ? (
            <p className="discovery-hint">{t("transforms.emptyEditor")}</p>
          ) : (
            <div className="discovery-source-views-editor-inner">
              <p className="discovery-hint" style={{ margin: "0 0 0.85rem" }}>
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
                        : handlerIdForTransformConfig(cfg as Record<string, unknown>)
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
