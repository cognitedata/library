import type { Edge, Node } from "@xyflow/react";
import {
  isWorkflowCanvasNodeEnabled,
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
export const discoveryFlowEdgeVisualDefaults = { animated: false as const };

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
    const nodeEnabled = isWorkflowCanvasNodeEnabled(n);
    const cascadeDisabled = n.cascade_disabled === true;
    const base: Node = {
      id: n.id,
      type: kindToRfType(n.kind),
      position: n.position,
      data: {
        ...n.data,
        label: n.data.label,
        canvas_node_enabled: nodeEnabled,
        canvas_node_cascade_disabled: cascadeDisabled,
      } as Record<string, unknown>,
    };
    const pid = n.parent_id != null && String(n.parent_id).trim() ? String(n.parent_id).trim() : "";
    if (pid) {
      base.parentId = pid;
      base.extent = "parent";
      base.expandParent = true;
    }
    if (n.kind === "start" || n.kind === "end") {
      base.deletable = false;
    }
    return base;
  });
}

export function canvasToFlowEdges(edges: WorkflowCanvasEdge[]): Edge[] {
  return edges.map((e) => ({
    ...discoveryFlowEdgeVisualDefaults,
    id: e.id,
    source: e.source,
    target: e.target,
    sourceHandle: e.source_handle ?? undefined,
    targetHandle: e.target_handle ?? undefined,
    data: { kind: e.kind ?? "data" } satisfies FlowEdgeData,
  }));
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
    const rawData = (n.data as WorkflowCanvasNodeData) ?? {};
    const { canvas_node_enabled: _cea, canvas_node_cascade_disabled: _ccd, ...dataRest } =
      rawData as WorkflowCanvasNodeData & {
        canvas_node_enabled?: boolean;
        canvas_node_cascade_disabled?: boolean;
      };
    const nodeEnabled =
      rawData.canvas_node_enabled !== undefined
        ? rawData.canvas_node_enabled !== false
        : true;
    const cascadeDisabled =
      rawData.canvas_node_cascade_disabled !== undefined
        ? rawData.canvas_node_cascade_disabled === true
        : false;
    const entry: WorkflowCanvasNode = {
      id: n.id,
      kind,
      position: { x: n.position.x, y: n.position.y },
      data: dataRest,
    };
    if (!nodeEnabled) entry.enabled = false;
    if (cascadeDisabled) entry.cascade_disabled = true;
    if (n.parentId && String(n.parentId).trim()) {
      entry.parent_id = String(n.parentId).trim();
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

export type DiscoveryFlowNodeDisplayState = {
  runFailed?: boolean;
  /** Task finished with errors (local skipTask / CDF COMPLETED_WITH_ERRORS). */
  runWarning?: boolean;
  executing?: boolean;
  completed?: boolean;
  /** Search filter: non-matching nodes are faded on the canvas. */
  dimmed?: boolean;
};

/** Apply local-run progress outline classes for React Flow preview / main canvas. */
export function applyDiscoveryFlowNodeDisplayClasses(
  node: Node,
  state: DiscoveryFlowNodeDisplayState
): Node {
  const failed = state.runFailed === true;
  const warned = state.runWarning === true;
  const executing = state.executing === true;
  const completed = state.completed === true;
  const dimmed = state.dimmed === true;
  let className = node.className;
  if (dimmed) {
    className = className ? `${className} discovery-flow-node--dimmed` : "discovery-flow-node--dimmed";
  }
  if (failed) {
    className = className ? `${className} discovery-flow-node--run-failed` : "discovery-flow-node--run-failed";
  } else if (warned) {
    className = className
      ? `${className} discovery-flow-node--run-warning`
      : "discovery-flow-node--run-warning";
  } else if (executing) {
    className = className ? `${className} discovery-flow-node--executing` : "discovery-flow-node--executing";
  } else if (completed) {
    className = className ? `${className} discovery-flow-node--run-completed` : "discovery-flow-node--run-completed";
  }
  return { ...node, className: className || undefined };
}
