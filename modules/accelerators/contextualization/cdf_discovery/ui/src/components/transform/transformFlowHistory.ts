import type { Edge, Node, Viewport } from "@xyflow/react";
import type {
  TransformCanvasEdgePathStyle,
  TransformCanvasHandleOrientation,
  TransformCanvasLayoutMethod,
} from "../../types/transformCanvas";
import type { TransformCanvasViewport } from "../../types/transformCanvasViewport";

export const TRANSFORM_FLOW_HISTORY_LIMIT = 50;

export type TransformFlowHistorySnapshot = {
  nodes: Node[];
  edges: Edge[];
  handleOrientation: TransformCanvasHandleOrientation;
  layoutMethod: TransformCanvasLayoutMethod;
  edgePathStyle: TransformCanvasEdgePathStyle;
  viewport: TransformCanvasViewport | null;
};

function cloneData(data: unknown): Record<string, unknown> {
  if (!data || typeof data !== "object") return {};
  return { ...(data as Record<string, unknown>) };
}

export function cloneTransformFlowHistorySnapshot(
  snap: TransformFlowHistorySnapshot
): TransformFlowHistorySnapshot {
  return {
    handleOrientation: snap.handleOrientation,
    layoutMethod: snap.layoutMethod,
    edgePathStyle: snap.edgePathStyle,
    viewport: snap.viewport ? { ...snap.viewport } : null,
    nodes: snap.nodes.map((n) => ({
      ...n,
      position: { ...n.position },
      data: cloneData(n.data),
      style: n.style ? { ...(n.style as Record<string, unknown>) } : n.style,
    })),
    edges: snap.edges.map((e) => ({
      ...e,
      data: cloneData(e.data),
    })),
  };
}

export function viewportToCanvasViewport(vp: Viewport): TransformCanvasViewport {
  return { x: vp.x, y: vp.y, zoom: vp.zoom };
}

export function canvasViewportToFlowViewport(vp: TransformCanvasViewport): Viewport {
  return { x: vp.x, y: vp.y, zoom: vp.zoom };
}

export function pushTransformFlowHistory(
  past: TransformFlowHistorySnapshot[],
  snapshot: TransformFlowHistorySnapshot
): TransformFlowHistorySnapshot[] {
  const next = [...past, cloneTransformFlowHistorySnapshot(snapshot)];
  if (next.length > TRANSFORM_FLOW_HISTORY_LIMIT) {
    return next.slice(next.length - TRANSFORM_FLOW_HISTORY_LIMIT);
  }
  return next;
}

/** Layout-only snapshot for read-only viewers (workflow / similar canvases). */
export type FlowLayoutHistorySnapshot = {
  nodes: Node[];
  viewport: TransformCanvasViewport | null;
};

export function cloneFlowLayoutHistorySnapshot(snap: FlowLayoutHistorySnapshot): FlowLayoutHistorySnapshot {
  return {
    viewport: snap.viewport ? { ...snap.viewport } : null,
    nodes: snap.nodes.map((n) => ({
      ...n,
      position: { ...n.position },
      data: cloneData(n.data),
      style: n.style ? { ...(n.style as Record<string, unknown>) } : n.style,
    })),
  };
}

export function pushFlowLayoutHistory(
  past: FlowLayoutHistorySnapshot[],
  snapshot: FlowLayoutHistorySnapshot
): FlowLayoutHistorySnapshot[] {
  const next = [...past, cloneFlowLayoutHistorySnapshot(snapshot)];
  if (next.length > TRANSFORM_FLOW_HISTORY_LIMIT) {
    return next.slice(next.length - TRANSFORM_FLOW_HISTORY_LIMIT);
  }
  return next;
}
