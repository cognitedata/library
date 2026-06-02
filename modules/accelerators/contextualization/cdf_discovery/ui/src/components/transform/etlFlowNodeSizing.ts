import type { Node } from "@xyflow/react";
import type { TransformCanvasNodeKind } from "../../types/transformCanvas";

export const ETL_NODE_MIN_WIDTH = 112;
export const ETL_NODE_MIN_HEIGHT = 48;
export const ETL_NODE_MAX_WIDTH = 260;
export const ETL_NODE_MAX_HEIGHT = 400;
export const ETL_NODE_RESIZE_MAX_WIDTH = 260;

export const ETL_DUAL_INPUT_MIN_WIDTH = 200;
export const ETL_DUAL_INPUT_MIN_HEIGHT = 80;

const DUAL_INPUT_KINDS: TransformCanvasNodeKind[] = ["workflow_fanout_plan", "file_annotation"];

export function isDualInputCanvasKind(kind: TransformCanvasNodeKind): boolean {
  return DUAL_INPUT_KINDS.includes(kind);
}

export function etlDualInputMinSize(): { width: number; height: number } {
  return { width: ETL_DUAL_INPUT_MIN_WIDTH, height: ETL_DUAL_INPUT_MIN_HEIGHT };
}

export function defaultEtlNodeSize(kind: TransformCanvasNodeKind): { width: number; height: number } {
  if (kind === "start" || kind === "end") {
    return { width: 148, height: 68 };
  }
  if (isDualInputCanvasKind(kind)) {
    return { width: 236, height: 116 };
  }
  return { width: 192, height: 96 };
}

export function maxEtlNodeWidth(kind: TransformCanvasNodeKind): number {
  return Math.max(defaultEtlNodeSize(kind).width, ETL_NODE_RESIZE_MAX_WIDTH);
}

export function parseFlowNodeDimension(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value) && value > 0) {
    return Math.round(value);
  }
  if (typeof value === "string" && value.trim().endsWith("px")) {
    const n = parseFloat(value);
    if (Number.isFinite(n) && n > 0) return Math.round(n);
  }
  return undefined;
}

export function readFlowNodeSize(
  node: { width?: number; height?: number; style?: unknown },
  kind: TransformCanvasNodeKind
): { width: number; height: number } {
  const style = (node.style ?? {}) as Record<string, unknown>;
  const width =
    parseFlowNodeDimension(node.width) ??
    parseFlowNodeDimension(style.width) ??
    defaultEtlNodeSize(kind).width;
  const height =
    parseFlowNodeDimension(node.height) ??
    parseFlowNodeDimension(style.height) ??
    defaultEtlNodeSize(kind).height;
  return { width, height };
}

export function flowNodeSizeStyle(width: number, height: number): { width: number; height: number } {
  return { width, height };
}

export function withEtlNodeDimensions(node: Node, kind: TransformCanvasNodeKind): Node {
  const size = readFlowNodeSize(node, kind);
  return {
    ...node,
    width: size.width,
    height: size.height,
    style: {
      ...(node.style as Record<string, unknown>),
      ...flowNodeSizeStyle(size.width, size.height),
    },
  };
}
