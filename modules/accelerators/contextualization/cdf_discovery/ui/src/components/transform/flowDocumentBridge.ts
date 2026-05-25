import type { Edge, Node } from "@xyflow/react";
import {
  flowNodeSizeStyle,
  parseFlowNodeDimension,
  readFlowNodeSize,
} from "./etlFlowNodeSizing";
import {
  kindToRfType,
  rfTypeToKind,
  TRANSFORM_CANVAS_SCHEMA_VERSION,
  type TransformCanvasDocument,
  type TransformCanvasEdge,
  type TransformCanvasHandleOrientation,
  type TransformCanvasNode,
  type TransformCanvasNodeData,
  normalizeTransformCanvasHandleOrientation,
  normalizeTransformCanvasEdgePathStyle,
  type TransformCanvasEdgePathStyle,
  isTransformCanvasNodeEnabled,
} from "../../types/transformCanvas";

export type FlowEdgeData = { kind?: "data" | "sequence" | "parallel_group" };

export const transformFlowEdgeVisualDefaults = { animated: false as const };

function sortCanvasNodesForReactFlow(nodes: TransformCanvasNode[]): TransformCanvasNode[] {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const memo = new Map<string, number>();
  function depth(id: string): number {
    if (memo.has(id)) return memo.get(id)!;
    const n = byId.get(id);
    const p = n?.parent_id != null && String(n.parent_id).trim() ? String(n.parent_id).trim() : "";
    if (!n || !p || !byId.has(p)) {
      memo.set(id, 0);
      return 0;
    }
    const d = 1 + depth(p);
    memo.set(id, d);
    return d;
  }
  return [...nodes].sort((a, b) => depth(a.id) - depth(b.id) || a.id.localeCompare(b.id));
}

function defaultNodePosition(index: number): { x: number; y: number } {
  const col = index % 4;
  const row = Math.floor(index / 4);
  return { x: 48 + col * 220, y: 48 + row * 120 };
}

export function canvasToFlowNodes(nodes: TransformCanvasNode[]): Node[] {
  const ordered = sortCanvasNodesForReactFlow(nodes);
  return ordered.map((n, index) => {
    const nodeEnabled = isTransformCanvasNodeEnabled(n);
    const pos =
      n.position != null &&
      Number.isFinite(n.position.x) &&
      Number.isFinite(n.position.y)
        ? n.position
        : defaultNodePosition(index);
    const base: Node = {
      id: n.id,
      type: kindToRfType(n.kind),
      position: pos,
      data: {
        ...n.data,
        label: n.data.label,
        kind: n.kind,
        canvas_node_enabled: nodeEnabled,
      } as Record<string, unknown>,
    };
    const pid = n.parent_id != null && String(n.parent_id).trim() ? String(n.parent_id).trim() : "";
    if (pid) {
      base.parentId = pid;
      base.extent = "parent";
    }
    if (n.kind === "start" || n.kind === "end") {
      base.deletable = false;
    }
    const size = readFlowNodeSize(
      { width: n.width, height: n.height, style: undefined },
      n.kind
    );
    base.width = size.width;
    base.height = size.height;
    base.style = { ...(base.style as Record<string, unknown>), ...flowNodeSizeStyle(size.width, size.height) };
    return base;
  });
}

export function canvasToFlowEdges(
  edges: TransformCanvasEdge[],
  edgePathStyle?: TransformCanvasEdgePathStyle
): Edge[] {
  const rfEdgeType = normalizeTransformCanvasEdgePathStyle(edgePathStyle);
  return edges.map((e) => ({
    ...transformFlowEdgeVisualDefaults,
    type: rfEdgeType,
    id: e.id,
    source: e.source,
    target: e.target,
    sourceHandle: e.source_handle ?? "out",
    targetHandle: e.target_handle ?? "in",
    data: { kind: e.kind ?? "data" } satisfies FlowEdgeData,
  }));
}

export function flowToCanvasDocument(
  nodes: Node[],
  edges: Edge[],
  opts?: {
    handleOrientation?: TransformCanvasHandleOrientation;
    edgePathStyle?: TransformCanvasEdgePathStyle;
  }
): TransformCanvasDocument {
  const handle_orientation = normalizeTransformCanvasHandleOrientation(opts?.handleOrientation);
  const edge_path_style = normalizeTransformCanvasEdgePathStyle(opts?.edgePathStyle);
  const cn: TransformCanvasNode[] = nodes.map((n) => {
    const kind = rfTypeToKind(n.type);
    const rawData = (n.data as TransformCanvasNodeData & { canvas_node_enabled?: boolean }) ?? {};
    const nodeEnabled =
      rawData.canvas_node_enabled !== undefined ? rawData.canvas_node_enabled !== false : true;
    const { canvas_node_enabled: _cea, ...dataRest } = rawData as TransformCanvasNodeData & {
      canvas_node_enabled?: boolean;
    };
    const entry: TransformCanvasNode = {
      id: n.id,
      kind,
      position: { x: n.position.x, y: n.position.y },
      data: {
        label: dataRest.label,
        notes: dataRest.notes,
        config: dataRest.config,
        node_color: dataRest.node_color,
        node_bg_color: dataRest.node_bg_color,
      },
    };
    if (!nodeEnabled) entry.enabled = false;
    if (n.parentId && String(n.parentId).trim()) {
      entry.parent_id = String(n.parentId).trim();
    }
    const width =
      parseFlowNodeDimension(n.width) ??
      parseFlowNodeDimension((n.style as Record<string, unknown> | undefined)?.width);
    const height =
      parseFlowNodeDimension(n.height) ??
      parseFlowNodeDimension((n.style as Record<string, unknown> | undefined)?.height);
    if (width != null) entry.width = width;
    if (height != null) entry.height = height;
    return entry;
  });
  const ce: TransformCanvasEdge[] = edges.map((e) => {
    const fd = (e.data ?? {}) as FlowEdgeData;
    return {
      id: e.id,
      source: e.source,
      target: e.target,
      source_handle: e.sourceHandle ?? null,
      target_handle: e.targetHandle ?? null,
      kind: fd.kind === "sequence" || fd.kind === "parallel_group" ? fd.kind : "data",
    };
  });
  return {
    schemaVersion: TRANSFORM_CANVAS_SCHEMA_VERSION,
    handle_orientation,
    edge_path_style,
    nodes: cn,
    edges: ce,
  };
}

export function applyTransformFlowNodeDisplayClasses(nodes: Node[]): Node[] {
  return nodes.map((n) => ({
    ...n,
    className: [n.className, `etl-flow-node--${rfTypeToKind(n.type)}`].filter(Boolean).join(" "),
  }));
}

export type TransformFlowRunDisplayState = {
  runFailed?: boolean;
  runWarning?: boolean;
  executing?: boolean;
  completed?: boolean;
  dimmed?: boolean;
};

const RUN_CLASSES = [
  "transform-flow-node--executing",
  "transform-flow-node--run-completed",
  "transform-flow-node--run-failed",
  "transform-flow-node--run-warning",
  "transform-flow-node--dimmed",
] as const;

function stripRunClasses(className: string | undefined): string {
  if (!className) return "";
  return className
    .split(/\s+/)
    .filter((c) => c && !RUN_CLASSES.includes(c as (typeof RUN_CLASSES)[number]))
    .join(" ");
}

/** Apply local-run progress outline classes on the React Flow canvas. */
export function applyTransformFlowRunDisplayClasses(
  node: Node,
  state: TransformFlowRunDisplayState
): Node {
  const failed = state.runFailed === true;
  const warned = state.runWarning === true;
  const executing = state.executing === true;
  const completed = state.completed === true;
  let className = stripRunClasses(node.className);
  if (failed) {
    className = className ? `${className} transform-flow-node--run-failed` : "transform-flow-node--run-failed";
  } else if (warned) {
    className = className ? `${className} transform-flow-node--run-warning` : "transform-flow-node--run-warning";
  } else if (executing) {
    className = className ? `${className} transform-flow-node--executing` : "transform-flow-node--executing";
  } else if (completed) {
    className = className ? `${className} transform-flow-node--run-completed` : "transform-flow-node--run-completed";
  }
  if (state.dimmed === true) {
    className = className ? `${className} transform-flow-node--dimmed` : "transform-flow-node--dimmed";
  }
  return { ...node, className: className || undefined };
}
