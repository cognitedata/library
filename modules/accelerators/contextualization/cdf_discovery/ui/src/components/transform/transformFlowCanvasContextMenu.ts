import type { MessageKey } from "../../i18n";
import {
  normalizeTransformCanvasEdgePathStyle,
  normalizeTransformCanvasHandleOrientation,
  type TransformCanvasEdgePathStyle,
  type TransformCanvasHandleOrientation,
  type TransformCanvasLayoutMethod,
} from "../../types/transformCanvas";
import { treeCtxMenuSeparator, type TreeCtxMenuItem } from "../governance/TreeContextMenu";
import type { AlignFlowSelectionMode } from "./alignSelectedNodes";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

export const TRANSFORM_LAYOUT_METHODS: TransformCanvasLayoutMethod[] = ["layered", "dagre"];

export type FlowLayoutMethodOption<V extends string = string> = {
  value: V;
  labelKey: MessageKey;
};

export const TRANSFORM_FLOW_LAYOUT_METHOD_OPTIONS: FlowLayoutMethodOption<TransformCanvasLayoutMethod>[] =
  [
    { value: "layered", labelKey: "transform.layout.methodLayered" },
    { value: "dagre", labelKey: "transform.layout.methodDagre" },
  ];

const HANDLE_ORIENTATION_LABEL: Record<TransformCanvasHandleOrientation, MessageKey> = {
  lr: "transform.layout.handleOrientationLr",
  tb: "transform.layout.handleOrientationTb",
};

const EDGE_PATH_STYLE_LABEL: Record<TransformCanvasEdgePathStyle, MessageKey> = {
  smoothstep: "transform.layout.edgeStyleSmoothStep",
  straight: "transform.layout.edgeStyleStraight",
  step: "transform.layout.edgeStyleStep",
  default: "transform.layout.edgeStyleBezier",
  simplebezier: "transform.layout.edgeStyleSimpleBezier",
};

const EDGE_PATH_STYLES: TransformCanvasEdgePathStyle[] = [
  "smoothstep",
  "straight",
  "step",
  "default",
  "simplebezier",
];

const ALIGN_MODES: { mode: AlignFlowSelectionMode; labelKey: MessageKey }[] = [
  { mode: "left", labelKey: "transform.align.left" },
  { mode: "centerHorizontal", labelKey: "transform.align.centerHorizontal" },
  { mode: "right", labelKey: "transform.align.right" },
  { mode: "top", labelKey: "transform.align.top" },
  { mode: "centerVertical", labelKey: "transform.align.centerVertical" },
  { mode: "bottom", labelKey: "transform.align.bottom" },
];

function checkedLabel(t: TFn, key: MessageKey, active: boolean): string {
  const label = t(key);
  return active ? `${label} ✓` : label;
}

export type FlowLayoutContextMenuOptions = {
  t: TFn;
  handleOrientation: TransformCanvasHandleOrientation;
  onHandleOrientationChange: (next: TransformCanvasHandleOrientation) => void;
  edgePathStyle: TransformCanvasEdgePathStyle;
  onEdgePathStyleChange: (next: TransformCanvasEdgePathStyle) => void;
  onFitView: () => void;
  onAutoLayout: () => void;
  layoutMethod?: string;
  layoutMethodOptions?: FlowLayoutMethodOption[];
  onLayoutMethodChange?: (next: string) => void;
};

/** Layout controls mirrored from the canvas toolbar (fit, auto-layout, method, handles, edges). */
export function flowLayoutContextMenuItems(opts: FlowLayoutContextMenuOptions): TreeCtxMenuItem[] {
  const {
    t,
    handleOrientation,
    onHandleOrientationChange,
    edgePathStyle,
    onEdgePathStyleChange,
    onFitView,
    onAutoLayout,
    layoutMethod,
    layoutMethodOptions = TRANSFORM_FLOW_LAYOUT_METHOD_OPTIONS,
    onLayoutMethodChange,
  } = opts;

  const items: TreeCtxMenuItem[] = [
    {
      id: "fit-view",
      label: t("transform.layout.fitView"),
      onSelect: onFitView,
    },
    {
      id: "auto-layout",
      label: t("transform.layout.autoLayout"),
      onSelect: onAutoLayout,
    },
  ];

  if (layoutMethod != null && onLayoutMethodChange && layoutMethodOptions.length > 0) {
    items.push(treeCtxMenuSeparator("sep-layout-method"));
    for (const option of layoutMethodOptions) {
      items.push({
        id: `layout-method-${option.value}`,
        label: checkedLabel(t, option.labelKey, layoutMethod === option.value),
        onSelect: () => onLayoutMethodChange(option.value),
      });
    }
  }

  items.push(treeCtxMenuSeparator("sep-handle-orientation"));
  for (const orientation of ["lr", "tb"] as const) {
    items.push({
      id: `handle-${orientation}`,
      label: checkedLabel(t, HANDLE_ORIENTATION_LABEL[orientation], handleOrientation === orientation),
      onSelect: () => onHandleOrientationChange(normalizeTransformCanvasHandleOrientation(orientation)),
    });
  }

  items.push(treeCtxMenuSeparator("sep-edge-style"));
  for (const style of EDGE_PATH_STYLES) {
    items.push({
      id: `edge-${style}`,
      label: checkedLabel(t, EDGE_PATH_STYLE_LABEL[style], edgePathStyle === style),
      onSelect: () => onEdgePathStyleChange(normalizeTransformCanvasEdgePathStyle(style)),
    });
  }

  return items;
}

export type TransformFlowAlignContextMenuOptions = {
  t: TFn;
  alignDisabled: boolean;
  onAlignSelection: (mode: AlignFlowSelectionMode) => void;
};

export function transformFlowAlignContextMenuItems(opts: TransformFlowAlignContextMenuOptions): TreeCtxMenuItem[] {
  const { t, alignDisabled, onAlignSelection } = opts;
  return ALIGN_MODES.map(({ mode, labelKey }) => ({
    id: `align-${mode}`,
    label: t(labelKey),
    disabled: alignDisabled,
    onSelect: () => onAlignSelection(mode),
  }));
}

export type TransformFlowPaneContextMenuOptions = FlowLayoutContextMenuOptions & {
  onCopy: () => void;
  onPaste: () => void;
  onAddNode: () => void;
  showAlign?: boolean;
  alignDisabled?: boolean;
  onAlignSelection?: (mode: AlignFlowSelectionMode) => void;
};

/** Full transform pipeline canvas background context menu. */
export function buildTransformFlowPaneContextMenuItems(opts: TransformFlowPaneContextMenuOptions): TreeCtxMenuItem[] {
  const {
    t,
    onCopy,
    onPaste,
    onAddNode,
    showAlign = true,
    alignDisabled = true,
    onAlignSelection,
    ...layoutOpts
  } = opts;

  const items: TreeCtxMenuItem[] = [
    {
      id: "copy",
      label: t("transform.flow.ctxMenuCopy"),
      onSelect: onCopy,
    },
    {
      id: "paste",
      label: t("transform.flow.ctxMenuPaste"),
      onSelect: onPaste,
    },
    treeCtxMenuSeparator("sep-edit"),
    {
      id: "add-node",
      label: t("transform.contextMenu.addNode"),
      onSelect: onAddNode,
    },
  ];

  if (showAlign && onAlignSelection) {
    items.push(treeCtxMenuSeparator("sep-align"));
    items.push(...transformFlowAlignContextMenuItems({ t, alignDisabled, onAlignSelection }));
  }

  items.push(treeCtxMenuSeparator("sep-layout"));
  items.push(...flowLayoutContextMenuItems({ t, ...layoutOpts }));

  return items;
}
