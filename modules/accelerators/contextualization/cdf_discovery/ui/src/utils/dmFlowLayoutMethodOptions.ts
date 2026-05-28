import type { MessageKey } from "../i18n";
import type { FlowLayoutMethodOption } from "../components/transform/transformFlowCanvasContextMenu";
import { DM_FLOW_LAYOUT_METHODS, type DmFlowLayoutMethod } from "./dataModelFlowLayout";

export const DM_FLOW_LAYOUT_METHOD_OPTIONS: FlowLayoutMethodOption<DmFlowLayoutMethod>[] = [
  { value: "dagre", labelKey: "dmViewer.layout.methodDagre" },
  { value: "grid", labelKey: "dmViewer.layout.methodGrid" },
];

export { DM_FLOW_LAYOUT_METHODS };
