import type { Edge, Node } from "@xyflow/react";
import {
  kindToRfType,
  rfTypeToKind,
  type CanvasEdgeKind,
  type WorkflowCanvasDocument,
  type WorkflowCanvasEdge,
  type WorkflowCanvasNode,
  type WorkflowCanvasNodeData,
  WORKFLOW_CANVAS_SCHEMA_VERSION,
} from "../../types/workflowCanvas";

export type FlowEdgeData = { kind?: CanvasEdgeKind };

export function canvasToFlowNodes(nodes: WorkflowCanvasNode[]): Node[] {
  return nodes.map((n) => ({
    id: n.id,
    type: kindToRfType(n.kind),
    position: n.position,
    data: { ...n.data, label: n.data.label } as Record<string, unknown>,
  }));
}

export function canvasToFlowEdges(edges: WorkflowCanvasEdge[]): Edge[] {
  return edges.map((e) => ({
    id: e.id,
    source: e.source,
    target: e.target,
    sourceHandle: e.source_handle ?? undefined,
    targetHandle: e.target_handle ?? undefined,
    data: { kind: e.kind ?? "data" } satisfies FlowEdgeData,
  }));
}

export function flowToCanvasDocument(nodes: Node[], edges: Edge[]): WorkflowCanvasDocument {
  const cn: WorkflowCanvasNode[] = nodes.map((n) => ({
    id: n.id,
    kind: rfTypeToKind(n.type),
    position: { x: n.position.x, y: n.position.y },
    data: (n.data as WorkflowCanvasNodeData) ?? {},
  }));
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
  };
}

export function newNodeId(): string {
  return `n_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}
