import { Panel, useReactFlow } from "@xyflow/react";
import type { MessageKey } from "../../i18n";
import {
  normalizeTransformCanvasEdgePathStyle,
  normalizeTransformCanvasHandleOrientation,
  type TransformCanvasEdgePathStyle,
  type TransformCanvasHandleOrientation,
} from "../../types/transformCanvas";
import type { AlignFlowSelectionMode } from "./alignSelectedNodes";
import { TransformFlowSelectionAlignButtons } from "./TransformFlowSelectionAlignButtons";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  readOnly?: boolean;
  handleOrientation: TransformCanvasHandleOrientation;
  onHandleOrientationChange: (next: TransformCanvasHandleOrientation) => void;
  edgePathStyle: TransformCanvasEdgePathStyle;
  onEdgePathStyleChange: (next: TransformCanvasEdgePathStyle) => void;
  onAutoLayout: () => void;
  alignDisabled?: boolean;
  onAlignSelection: (mode: AlignFlowSelectionMode) => void;
};

/** Zoom, fit, auto-layout, handle orientation, and selection alignment on the pipeline canvas. */
export function TransformFlowLayoutControls({
  t,
  readOnly = false,
  handleOrientation,
  onHandleOrientationChange,
  edgePathStyle,
  onEdgePathStyleChange,
  onAutoLayout,
  alignDisabled = true,
  onAlignSelection,
}: Props) {
  const { fitView } = useReactFlow();

  return (
    <Panel className="transform-flow-layout-panel" position="top-left" aria-label={t("transform.layout.aria")}>
      <TransformFlowSelectionAlignButtons
        t={t}
        disabled={alignDisabled}
        onAlign={onAlignSelection}
      />
      <button
        type="button"
        className="disc-btn disc-btn--sm transform-flow-layout-panel__btn"
        title={t("transform.layout.fitView")}
        aria-label={t("transform.layout.fitView")}
        onClick={() => fitView({ padding: 0.15, duration: 200 })}
      >
        {t("transform.layout.fitView")}
      </button>
      <button
        type="button"
        className="disc-btn disc-btn--sm transform-flow-layout-panel__btn"
        title={t("transform.layout.autoLayout")}
        aria-label={t("transform.layout.autoLayout")}
        onClick={onAutoLayout}
      >
        {t("transform.layout.autoLayout")}
      </button>
      <label className="transform-flow-layout-panel__orientation">
        <span className="transform-flow-layout-panel__orientation-label">
          {t("transform.layout.handleOrientation")}
        </span>
        <select
          className="gov-input transform-flow-layout-panel__select"
          value={handleOrientation}
          aria-label={t("transform.layout.handleOrientation")}
          disabled={readOnly}
          onChange={(e) =>
            onHandleOrientationChange(normalizeTransformCanvasHandleOrientation(e.target.value))
          }
        >
          <option value="lr">{t("transform.layout.handleOrientationLr")}</option>
          <option value="tb">{t("transform.layout.handleOrientationTb")}</option>
        </select>
      </label>
      <label className="transform-flow-layout-panel__orientation">
        <span className="transform-flow-layout-panel__orientation-label">
          {t("transform.layout.edgeStyle")}
        </span>
        <select
          className="gov-input transform-flow-layout-panel__select"
          value={edgePathStyle}
          aria-label={t("transform.layout.edgeStyle")}
          disabled={readOnly}
          onChange={(e) =>
            onEdgePathStyleChange(normalizeTransformCanvasEdgePathStyle(e.target.value))
          }
        >
          <option value="smoothstep">{t("transform.layout.edgeStyleSmoothStep")}</option>
          <option value="straight">{t("transform.layout.edgeStyleStraight")}</option>
          <option value="step">{t("transform.layout.edgeStyleStep")}</option>
          <option value="default">{t("transform.layout.edgeStyleBezier")}</option>
          <option value="simplebezier">{t("transform.layout.edgeStyleSimpleBezier")}</option>
        </select>
      </label>
    </Panel>
  );
}
