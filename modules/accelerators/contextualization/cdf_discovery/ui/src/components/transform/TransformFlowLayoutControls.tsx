import type { MessageKey } from "../../i18n";
import {
  normalizeTransformCanvasEdgePathStyle,
  normalizeTransformCanvasHandleOrientation,
  type TransformCanvasEdgePathStyle,
  type TransformCanvasHandleOrientation,
} from "../../types/transformCanvas";
import {
  TRANSFORM_FLOW_LAYOUT_METHOD_OPTIONS,
  type FlowLayoutMethodOption,
} from "./transformFlowCanvasContextMenu";
import type { AlignFlowSelectionMode } from "./alignSelectedNodes";
import { TransformFlowSelectionAlignButtons } from "./TransformFlowSelectionAlignButtons";
import {
  FlowToolbarAutoLayoutIcon,
  FlowToolbarEdgeStyleIcon,
  FlowToolbarFitViewIcon,
  FlowToolbarGroup,
  FlowToolbarHandleLrIcon,
  FlowToolbarHandleTbIcon,
  FlowToolbarIconButton,
  FlowToolbarLayoutMethodIcon,
  FlowToolbarRedoIcon,
  FlowToolbarSelect,
  FlowToolbarSeparator,
  FlowToolbarUndoIcon,
} from "../flow/FlowToolbarIcons";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

export type TransformFlowLayoutControlsMode = "full" | "viewer";

type Props = {
  t: TFn;
  readOnly?: boolean;
  mode?: TransformFlowLayoutControlsMode;
  handleOrientation?: TransformCanvasHandleOrientation;
  onHandleOrientationChange?: (next: TransformCanvasHandleOrientation) => void;
  layoutMethod?: string;
  layoutMethodOptions?: FlowLayoutMethodOption[];
  layoutMethodLabelKey?: MessageKey;
  onLayoutMethodChange?: (next: string) => void;
  edgePathStyle?: TransformCanvasEdgePathStyle;
  onEdgePathStyleChange?: (next: TransformCanvasEdgePathStyle) => void;
  onAutoLayout: () => void;
  alignDisabled?: boolean;
  onAlignSelection?: (mode: AlignFlowSelectionMode) => void;
  onFitView: () => void;
  showAlign?: boolean;
  layoutAriaLabelKey?: MessageKey;
  canUndo?: boolean;
  canRedo?: boolean;
  onUndo?: () => void;
  onRedo?: () => void;
};

/** Grouped icon toolbar: history, align, view, and canvas appearance settings. */
export function TransformFlowLayoutControls({
  t,
  readOnly = false,
  mode = "full",
  handleOrientation = "lr",
  onHandleOrientationChange,
  layoutMethod,
  layoutMethodOptions = TRANSFORM_FLOW_LAYOUT_METHOD_OPTIONS,
  layoutMethodLabelKey = "transform.layout.method",
  onLayoutMethodChange,
  edgePathStyle = "smoothstep",
  onEdgePathStyleChange,
  onAutoLayout,
  alignDisabled = true,
  onAlignSelection,
  onFitView,
  showAlign = true,
  layoutAriaLabelKey = "transform.layout.aria",
  canUndo = false,
  canRedo = false,
  onUndo,
  onRedo,
}: Props) {
  const isViewer = mode === "viewer";
  const showHistory = !readOnly && onUndo != null && onRedo != null;
  const showAlignGroup = !isViewer && showAlign && onAlignSelection != null;
  const showAppearance =
    !isViewer &&
    onHandleOrientationChange != null &&
    onEdgePathStyleChange != null &&
    layoutMethod != null &&
    onLayoutMethodChange != null &&
    layoutMethodOptions.length > 0;

  return (
    <div
      className="transform-flow-layout-panel transform-flow-layout-panel--inline"
      role="toolbar"
      aria-label={t(layoutAriaLabelKey)}
    >
      {showHistory ? (
        <>
          <FlowToolbarGroup label={t("flow.toolbar.history")}>
            <FlowToolbarIconButton
              label={t("transform.flow.undo")}
              disabled={!canUndo}
              onClick={onUndo}
            >
              <FlowToolbarUndoIcon />
            </FlowToolbarIconButton>
            <FlowToolbarIconButton
              label={t("transform.flow.redo")}
              disabled={!canRedo}
              onClick={onRedo}
            >
              <FlowToolbarRedoIcon />
            </FlowToolbarIconButton>
          </FlowToolbarGroup>
          <FlowToolbarSeparator />
        </>
      ) : null}

      {showAlignGroup ? (
        <>
          <TransformFlowSelectionAlignButtons
            t={t}
            disabled={alignDisabled}
            onAlign={onAlignSelection}
          />
          <FlowToolbarSeparator />
        </>
      ) : null}

      <FlowToolbarGroup label={t("flow.toolbar.view")}>
        <FlowToolbarIconButton label={t("transform.layout.fitView")} onClick={onFitView}>
          <FlowToolbarFitViewIcon />
        </FlowToolbarIconButton>
        <FlowToolbarIconButton
          label={t("transform.layout.autoLayout")}
          disabled={readOnly}
          onClick={onAutoLayout}
        >
          <FlowToolbarAutoLayoutIcon />
        </FlowToolbarIconButton>
      </FlowToolbarGroup>

      {showAppearance ? (
        <>
          <FlowToolbarSeparator />
          <FlowToolbarGroup label={t("flow.toolbar.appearance")}>
            <FlowToolbarSelect
              label={t(layoutMethodLabelKey)}
              value={layoutMethod}
              disabled={readOnly}
              onChange={onLayoutMethodChange}
              icon={<FlowToolbarLayoutMethodIcon />}
            >
              {layoutMethodOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {t(option.labelKey)}
                </option>
              ))}
            </FlowToolbarSelect>
            <FlowToolbarIconButton
              label={t("transform.layout.handleOrientationLr")}
              active={handleOrientation === "lr"}
              disabled={readOnly}
              onClick={() => onHandleOrientationChange!("lr")}
            >
              <FlowToolbarHandleLrIcon />
            </FlowToolbarIconButton>
            <FlowToolbarIconButton
              label={t("transform.layout.handleOrientationTb")}
              active={handleOrientation === "tb"}
              disabled={readOnly}
              onClick={() => onHandleOrientationChange!("tb")}
            >
              <FlowToolbarHandleTbIcon />
            </FlowToolbarIconButton>
            <FlowToolbarSelect
              label={t("transform.layout.edgeStyle")}
              value={edgePathStyle}
              disabled={readOnly}
              onChange={(v) => onEdgePathStyleChange!(normalizeTransformCanvasEdgePathStyle(v))}
              icon={<FlowToolbarEdgeStyleIcon />}
            >
              <option value="smoothstep">{t("transform.layout.edgeStyleSmoothStep")}</option>
              <option value="straight">{t("transform.layout.edgeStyleStraight")}</option>
              <option value="step">{t("transform.layout.edgeStyleStep")}</option>
              <option value="default">{t("transform.layout.edgeStyleBezier")}</option>
              <option value="simplebezier">{t("transform.layout.edgeStyleSimpleBezier")}</option>
            </FlowToolbarSelect>
          </FlowToolbarGroup>
        </>
      ) : null}
    </div>
  );
}
