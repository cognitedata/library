import type { Edge, Node } from "@xyflow/react";
import type { TransformCanvasDocument, TransformCanvasNode } from "../../types/transformCanvas";
import { canvasToFlowEdges, canvasToFlowNodes, flowToCanvasDocument } from "./flowDocumentBridge";
import { nextEtlNodeId } from "./flowNodeRegistry";

export const TRANSFORM_FLOW_CLIPBOARD_VERSION = 1;
export const TRANSFORM_FLOW_CLIPBOARD_PREFIX = "__DISCOVERY_TRANSFORM_V1__\n";

const NON_COPYABLE_RF_TYPES = new Set(["etlStart", "etlEnd"]);

const DEFAULT_PASTE_OFFSET = { x: 48, y: 48 };

export type TransformFlowClipboardPayload = {
  version: typeof TRANSFORM_FLOW_CLIPBOARD_VERSION;
  nodes: TransformCanvasNode[];
  edges: TransformCanvasDocument["edges"];
};

export function isTransformFlowClipboardRfType(type: string | undefined): boolean {
  return type != null && !NON_COPYABLE_RF_TYPES.has(type);
}

export function buildTransformFlowClipboardPayload(
  allNodes: Node[],
  allEdges: Edge[],
  selected: Node[]
): TransformFlowClipboardPayload | null {
  const copyable = selected.filter((n) => isTransformFlowClipboardRfType(n.type));
  if (copyable.length === 0) return null;

  const selectedIds = new Set(copyable.map((n) => n.id));
  const subsetNodes = allNodes.filter((n) => selectedIds.has(n.id));
  const subsetEdges = allEdges.filter((e) => selectedIds.has(e.source) && selectedIds.has(e.target));
  const doc = flowToCanvasDocument(subsetNodes, subsetEdges);
  return {
    version: TRANSFORM_FLOW_CLIPBOARD_VERSION,
    nodes: structuredClone(doc.nodes),
    edges: structuredClone(doc.edges),
  };
}

export function pasteTransformFlowClipboard(
  allNodes: Node[],
  allEdges: Edge[],
  payload: TransformFlowClipboardPayload,
  opts?: { offset?: { x: number; y: number }; existingNodeIds?: Set<string> }
): { nodes: Node[]; edges: Edge[]; newNodeIds: string[] } | null {
  if (payload.version !== TRANSFORM_FLOW_CLIPBOARD_VERSION || payload.nodes.length === 0) return null;

  const offset = opts?.offset ?? DEFAULT_PASTE_OFFSET;
  const taken = opts?.existingNodeIds ?? new Set(allNodes.map((n) => n.id));
  const idMap = new Map<string, string>();

  for (const n of payload.nodes) {
    let next = nextEtlNodeId(n.kind, taken);
    while (taken.has(next)) next = nextEtlNodeId(n.kind, taken);
    taken.add(next);
    idMap.set(n.id, next);
  }

  const canvasNodes: TransformCanvasNode[] = payload.nodes.map((n) => {
    const entry = structuredClone(n);
    entry.id = idMap.get(n.id)!;
    entry.position = {
      x: n.position.x + offset.x,
      y: n.position.y + offset.y,
    };
    if (n.parent_id != null && String(n.parent_id).trim()) {
      const pid = String(n.parent_id).trim();
      if (idMap.has(pid)) {
        entry.parent_id = idMap.get(pid)!;
      } else {
        delete entry.parent_id;
      }
    } else {
      delete entry.parent_id;
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
    nodes: [...allNodes, ...newRfNodes],
    edges: [...allEdges, ...newRfEdges],
    newNodeIds,
  };
}

export function serializeTransformFlowClipboardPayload(payload: TransformFlowClipboardPayload): string {
  return `${TRANSFORM_FLOW_CLIPBOARD_PREFIX}${JSON.stringify(payload)}`;
}

export function parseTransformFlowClipboardText(text: string): TransformFlowClipboardPayload | null {
  const trimmed = text.trim();
  if (!trimmed.startsWith(TRANSFORM_FLOW_CLIPBOARD_PREFIX)) return null;
  try {
    const raw = JSON.parse(trimmed.slice(TRANSFORM_FLOW_CLIPBOARD_PREFIX.length)) as TransformFlowClipboardPayload;
    if (raw?.version !== TRANSFORM_FLOW_CLIPBOARD_VERSION || !Array.isArray(raw.nodes) || !Array.isArray(raw.edges)) {
      return null;
    }
    return raw;
  } catch {
    return null;
  }
}

export function isTransformFlowKeyboardShortcutBlockedTarget(t: EventTarget | null): boolean {
  if (!(t instanceof HTMLElement)) return false;
  if (t.closest("[data-transform-flow-undo-ignore='true']")) return true;
  if (t.isContentEditable) return true;
  const tag = t.tagName;
  if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return true;
  return Boolean(t.closest("input, textarea, select, [contenteditable='true']"));
}
