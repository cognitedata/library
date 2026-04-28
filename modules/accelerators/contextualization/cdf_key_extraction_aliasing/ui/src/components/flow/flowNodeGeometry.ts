import type { Node } from "@xyflow/react";
import { absoluteNodePosition } from "./flowParentGeometry";

function parseStyleDim(v: unknown): number | undefined {
  if (typeof v === "number" && Number.isFinite(v) && v > 0) return v;
  if (typeof v === "string" && v.trim().endsWith("px")) {
    const n = parseFloat(v);
    return Number.isFinite(n) && n > 0 ? n : undefined;
  }
  return undefined;
}

/** Flow-space width/height (measured / style / type defaults). */
export function nodeFlowSize(n: Node): { w: number; h: number } {
  const ext = n as Node & {
    width?: number;
    height?: number;
    measured?: { width?: number; height?: number };
  };
  const style = (n.style ?? {}) as Record<string, unknown>;
  const fromStyleW = parseStyleDim(style.width);
  const fromStyleH = parseStyleDim(style.height);
  if (fromStyleW != null && fromStyleH != null) return { w: fromStyleW, h: fromStyleH };
  const mw = ext.measured?.width;
  const mh = ext.measured?.height;
  if (typeof mw === "number" && mw > 0 && typeof mh === "number" && mh > 0) {
    return { w: mw, h: mh };
  }
  if (typeof ext.width === "number" && ext.width > 0 && typeof ext.height === "number" && ext.height > 0) {
    return { w: ext.width, h: ext.height };
  }
  if (n.type === "keaSubflow") return { w: 380, h: 260 };
  if (n.type === "keaSubflowGraphIn" || n.type === "keaSubflowGraphOut") return { w: 132, h: 72 };
  if (n.type === "keaSubgraph") return { w: 192, h: 112 };
  return { w: 200, h: 72 };
}

export type FlowRect = { x: number; y: number; w: number; h: number };

export function absoluteNodeRect(nodes: Node[], n: Node): FlowRect {
  const { x, y } = absoluteNodePosition(nodes, n.id);
  const { w, h } = nodeFlowSize(n);
  return { x, y, w, h };
}
