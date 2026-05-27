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
  onFitView: () => void;
  /** When false, hide align buttons (e.g. read-only pipeline preview). */
  showAlign?: boolean;
};

/** Fit view, auto-layout, handle orientation, edge style, and selection alignment for flow canvases. */
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
  onFitView,
  showAlign = true,
}: Props) {
  return (
    <div
      className="transform-flow-layout-panel transform-flow-layout-panel--inline"
      role="group"
      aria-label={t("transform.layout.aria")}
    >
      {showAlign ? (
        <TransformFlowSelectionAlignButtons
          t={t}
          disabled={alignDisabled}
          onAlign={onAlignSelection}
        />
      ) : null}
      <button
        type="button"
        className="disc-btn disc-btn--sm transform-flow-layout-panel__btn"
        title={t("transform.layout.fitView")}
        aria-label={t("transform.layout.fitView")}
        onClick={onFitView}
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
    </div>
  );
}
