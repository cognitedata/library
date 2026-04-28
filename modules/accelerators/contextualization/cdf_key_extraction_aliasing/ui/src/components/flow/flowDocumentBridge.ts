import type { Edge, Node } from "@xyflow/react";
import {
  kindToRfType,
  normalizeWorkflowCanvasHandleOrientation,
  rfTypeToKind,
  type CanvasEdgeKind,
  type WorkflowCanvasDocument,
  type WorkflowCanvasEdge,
  type WorkflowCanvasHandleOrientation,
  type WorkflowCanvasNode,
  type WorkflowCanvasNodeData,
  WORKFLOW_CANVAS_SCHEMA_VERSION,
} from "../../types/workflowCanvas";

export type FlowEdgeData = { kind?: CanvasEdgeKind };

/** React Flow–only edge flags (not persisted in canvas YAML). */
export const keaFlowEdgeVisualDefaults = { animated: true as const };

const DEFAULT_SUBFLOW_W = 380;
const DEFAULT_SUBFLOW_H = 260;

function sortCanvasNodesForReactFlow(nodes: WorkflowCanvasNode[]): WorkflowCanvasNode[] {
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

/**
 * React Flow expects **parents before children** in the ``nodes`` array. Call after any edit
 * that changes ``parentId`` while the graph is live in ``useNodesState``.
 */
export function orderFlowNodesForReactFlow(nodes: Node[]): Node[] {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const memo = new Map<string, number>();
  function depth(id: string): number {
    if (memo.has(id)) return memo.get(id)!;
    const n = byId.get(id);
    const p = n?.parentId != null && String(n.parentId).trim() ? String(n.parentId).trim() : "";
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

export function canvasToFlowNodes(nodes: WorkflowCanvasNode[]): Node[] {
  const ordered = sortCanvasNodesForReactFlow(nodes);
  return ordered.map((n) => {
    const base: Node = {
      id: n.id,
      type: kindToRfType(n.kind),
      position: n.position,
      data: { ...n.data, label: n.data.label } as Record<string, unknown>,
    };
    const pid = n.parent_id != null && String(n.parent_id).trim() ? String(n.parent_id).trim() : "";
    if (pid) {
      base.parentId = pid;
      base.extent = "parent";
      base.expandParent = true;
    }
    if (n.kind === "subflow") {
      const w = n.size?.width && n.size.width > 0 ? n.size.width : DEFAULT_SUBFLOW_W;
      const h = n.size?.height && n.size.height > 0 ? n.size.height : DEFAULT_SUBFLOW_H;
      base.style = { ...((base.style as Record<string, unknown>) ?? {}), width: w, height: h };
    }
    return base;
  });
}

export function canvasToFlowEdges(edges: WorkflowCanvasEdge[]): Edge[] {
  return edges.map((e) => ({
    ...keaFlowEdgeVisualDefaults,
    id: e.id,
    source: e.source,
    target: e.target,
    sourceHandle: e.source_handle ?? undefined,
    targetHandle: e.target_handle ?? undefined,
    data: { kind: e.kind ?? "data" } satisfies FlowEdgeData,
  }));
}

function parseCssDim(v: unknown): number | undefined {
  if (typeof v === "number" && Number.isFinite(v) && v > 0) return v;
  if (typeof v === "string" && v.trim().endsWith("px")) {
    const n = parseFloat(v);
    return Number.isFinite(n) && n > 0 ? n : undefined;
  }
  return undefined;
}

function readPersistedSubflowSize(n: Node): { width: number; height: number } | undefined {
  const ext = n as Node & {
    width?: number;
    height?: number;
    measured?: { width?: number; height?: number };
  };
  const style = (n.style ?? {}) as Record<string, unknown>;
  const w =
    parseCssDim(style.width) ?? (typeof ext.width === "number" ? ext.width : ext.measured?.width);
  const h =
    parseCssDim(style.height) ?? (typeof ext.height === "number" ? ext.height : ext.measured?.height);
  if (w != null && h != null && w > 0 && h > 0) {
    return { width: Math.round(w), height: Math.round(h) };
  }
  return undefined;
}

export function flowToCanvasDocument(
  nodes: Node[],
  edges: Edge[],
  opts?: { handleOrientation?: WorkflowCanvasHandleOrientation }
): WorkflowCanvasDocument {
  const handle_orientation = normalizeWorkflowCanvasHandleOrientation(opts?.handleOrientation);
  const orderedNodes = orderFlowNodesForReactFlow(nodes);
  const cn: WorkflowCanvasNode[] = orderedNodes.map((n) => {
    const kind = rfTypeToKind(n.type);
    const entry: WorkflowCanvasNode = {
      id: n.id,
      kind,
      position: { x: n.position.x, y: n.position.y },
      data: (n.data as WorkflowCanvasNodeData) ?? {},
    };
    if (n.parentId && String(n.parentId).trim()) {
      entry.parent_id = String(n.parentId).trim();
    }
    if (kind === "subflow") {
      const sz = readPersistedSubflowSize(n);
      if (sz) entry.size = sz;
    }
    return entry;
  });
  const ce: WorkflowCanvasEdge[] = edges.map((e) => {
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
    schemaVersion: WORKFLOW_CANVAS_SCHEMA_VERSION,
    nodes: cn,
    edges: ce,
    handle_orientation,
  };
}

export function newNodeId(): string {
  return `n_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}
