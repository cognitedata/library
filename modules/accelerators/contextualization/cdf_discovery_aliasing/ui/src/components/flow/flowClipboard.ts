import type { Edge, Node } from "@xyflow/react";
import type { WorkflowCanvasDocument, WorkflowCanvasNode } from "../../types/workflowCanvas";
import {
  canvasToFlowEdges,
  canvasToFlowNodes,
  flowToCanvasDocument,
  newNodeId,
  orderFlowNodesForReactFlow,
} from "./flowDocumentBridge";

export const FLOW_CLIPBOARD_VERSION = 1;
export const FLOW_CLIPBOARD_PREFIX = "__DISCOVERY_FLOW_V1__\n";

const NON_COPYABLE_RF_TYPES = new Set(["discoveryStart", "discoveryEnd"]);

const DEFAULT_PASTE_OFFSET = { x: 48, y: 48 };

export type FlowClipboardPayload = {
  version: typeof FLOW_CLIPBOARD_VERSION;
  nodes: WorkflowCanvasNode[];
  edges: WorkflowCanvasDocument["edges"];
};

export function isFlowClipboardRfType(type: string | undefined): boolean {
  return type != null && !NON_COPYABLE_RF_TYPES.has(type);
}

export function buildFlowClipboardPayload(
  allNodes: Node[],
  allEdges: Edge[],
  selected: Node[]
): FlowClipboardPayload | null {
  const copyable = selected.filter((n) => isFlowClipboardRfType(n.type));
  if (copyable.length === 0) return null;

  const selectedIds = new Set(copyable.map((n) => n.id));
  const subsetNodes = allNodes.filter((n) => selectedIds.has(n.id));
  const subsetEdges = allEdges.filter((e) => selectedIds.has(e.source) && selectedIds.has(e.target));
  const doc = flowToCanvasDocument(subsetNodes, subsetEdges);
  return {
    version: FLOW_CLIPBOARD_VERSION,
    nodes: structuredClone(doc.nodes),
    edges: structuredClone(doc.edges),
  };
}

function remapCanvasDocumentIds(doc: WorkflowCanvasDocument): WorkflowCanvasDocument {
  const idMap = new Map<string, string>();
  for (const n of doc.nodes) {
    idMap.set(n.id, newNodeId());
  }
  const nodes = doc.nodes.map((n) => {
    const nextId = idMap.get(n.id)!;
    const parent =
      n.parent_id != null && String(n.parent_id).trim() && idMap.has(String(n.parent_id).trim())
        ? idMap.get(String(n.parent_id).trim())!
        : n.parent_id;
    return {
      ...structuredClone(n),
      id: nextId,
      ...(parent != null && String(parent).trim() ? { parent_id: parent } : {}),
    };
  });
  const edges = doc.edges.map((e) => ({
    ...structuredClone(e),
    id: `e_${idMap.get(e.source)!}_${idMap.get(e.target)!}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    source: idMap.get(e.source)!,
    target: idMap.get(e.target)!,
  }));
  return { ...doc, nodes, edges };
}

function remapSubgraphInnerCanvas(node: WorkflowCanvasNode): WorkflowCanvasNode {
  const ic = node.data?.inner_canvas;
  if (node.kind !== "subgraph" || !ic?.nodes?.length) return node;
  return {
    ...node,
    data: {
      ...node.data,
      inner_canvas: remapCanvasDocumentIds(ic),
    },
  };
}

export function pasteFlowClipboard(
  allNodes: Node[],
  allEdges: Edge[],
  payload: FlowClipboardPayload,
  opts?: { offset?: { x: number; y: number }; existingNodeIds?: Set<string> }
): { nodes: Node[]; edges: Edge[]; newNodeIds: string[] } | null {
  if (payload.version !== FLOW_CLIPBOARD_VERSION || payload.nodes.length === 0) return null;

  const offset = opts?.offset ?? DEFAULT_PASTE_OFFSET;
  const taken = opts?.existingNodeIds ?? new Set(allNodes.map((n) => n.id));
  const idMap = new Map<string, string>();

  for (const n of payload.nodes) {
    let next = newNodeId();
    while (taken.has(next)) next = newNodeId();
    taken.add(next);
    idMap.set(n.id, next);
  }

  const canvasNodes: WorkflowCanvasNode[] = payload.nodes.map((n) => {
    let entry = structuredClone(n);
    entry.id = idMap.get(n.id)!;
    entry.position = {
      x: n.position.x + offset.x,
      y: n.position.y + offset.y,
    };
    if (n.parent_id != null && String(n.parent_id).trim()) {
      const pid = String(n.parent_id).trim();
      entry.parent_id = idMap.has(pid) ? idMap.get(pid)! : pid;
    } else {
      delete entry.parent_id;
    }
    if (entry.kind === "subgraph") {
      entry = remapSubgraphInnerCanvas(entry);
    }
    return entry;
  });

  const canvasEdges = payload.edges.map((e) => ({
    ...structuredClone(e),
    id: `e_${idMap.get(e.source)!}_${idMap.get(e.target)!}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    source: idMap.get(e.source)!,
    target: idMap.get(e.target)!,
  }));

  const newRfNodes = canvasToFlowNodes(canvasNodes);
  const newRfEdges = canvasToFlowEdges(canvasEdges);
  const newNodeIds = newRfNodes.map((n) => n.id);

  return {
    nodes: orderFlowNodesForReactFlow([...allNodes, ...newRfNodes]),
    edges: [...allEdges, ...newRfEdges],
    newNodeIds,
  };
}

export function serializeFlowClipboardPayload(payload: FlowClipboardPayload): string {
  return `${FLOW_CLIPBOARD_PREFIX}${JSON.stringify(payload)}`;
}

export function parseFlowClipboardText(text: string): FlowClipboardPayload | null {
  const trimmed = text.trim();
  if (!trimmed.startsWith(FLOW_CLIPBOARD_PREFIX)) return null;
  const prefix = FLOW_CLIPBOARD_PREFIX;
  try {
    const raw = JSON.parse(trimmed.slice(prefix.length)) as FlowClipboardPayload;
    if (raw?.version !== FLOW_CLIPBOARD_VERSION || !Array.isArray(raw.nodes) || !Array.isArray(raw.edges)) {
      return null;
    }
    return raw;
  } catch {
    return null;
  }
}

export function isFlowKeyboardShortcutBlockedTarget(t: EventTarget | null): boolean {
  if (!(t instanceof HTMLElement)) return false;
  if (t.closest("[data-discovery-flow-undo-ignore='true']")) return true;
  if (t.isContentEditable) return true;
  const tag = t.tagName;
  if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return true;
  return Boolean(t.closest("input, textarea, select, [contenteditable='true']"));
}
