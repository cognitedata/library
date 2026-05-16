import { useCallback, useEffect, useRef, type MutableRefObject, type RefObject } from "react";
import type { Edge, Node } from "@xyflow/react";
import { dedupeEdgesByHandles } from "./flowEdgeHelpers";
import {
  buildFlowClipboardPayload,
  isFlowKeyboardShortcutBlockedTarget,
  parseFlowClipboardText,
  pasteFlowClipboard,
  serializeFlowClipboardPayload,
  type FlowClipboardPayload,
} from "./flowClipboard";

export type UseFlowClipboardOptions = {
  nodes: Node[];
  edges: Edge[];
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>;
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>;
  rfSelectionRef: MutableRefObject<Node[]>;
  flowRootRef: RefObject<HTMLElement | null>;
  readOnly?: boolean;
};

export type UseFlowClipboardResult = {
  copySelection: () => boolean;
  pasteClipboard: () => Promise<boolean>;
  canPaste: boolean;
};

function eventInsideFlowRoot(e: KeyboardEvent, root: HTMLElement | null): boolean {
  if (!root) return false;
  const path = e.composedPath();
  return path.some((n) => n instanceof Node && root.contains(n));
}

/**
 * In-memory buffer plus optional system clipboard (``__KEA_FLOW_V1__`` JSON prefix).
 * Cmd/Ctrl+C / Cmd/Ctrl+V when focus is inside ``flowRootRef``.
 */
export function useFlowClipboard({
  nodes,
  edges,
  setNodes,
  setEdges,
  rfSelectionRef,
  flowRootRef,
  readOnly = false,
}: UseFlowClipboardOptions): UseFlowClipboardResult {
  const bufferRef = useRef<FlowClipboardPayload | null>(null);
  const pasteGenerationRef = useRef(0);

  const copySelection = useCallback((): boolean => {
    if (readOnly) return false;
    const payload = buildFlowClipboardPayload(nodes, edges, rfSelectionRef.current);
    if (!payload) return false;
    bufferRef.current = payload;
    pasteGenerationRef.current = 0;
    const text = serializeFlowClipboardPayload(payload);
    void navigator.clipboard?.writeText(text).catch(() => {});
    return true;
  }, [nodes, edges, rfSelectionRef, readOnly]);

  const applyPaste = useCallback(
    (payload: FlowClipboardPayload): boolean => {
      pasteGenerationRef.current += 1;
      const gen = pasteGenerationRef.current;
      const offset = { x: 48 * gen, y: 48 * gen };
      const existingNodeIds = new Set(nodes.map((n) => n.id));
      const result = pasteFlowClipboard(nodes, edges, payload, { offset, existingNodeIds });
      if (!result) return false;
      setNodes(result.nodes);
      setEdges(dedupeEdgesByHandles(result.edges));
      return true;
    },
    [nodes, edges, setNodes, setEdges]
  );

  const pasteClipboard = useCallback(async (): Promise<boolean> => {
    if (readOnly) return false;
    if (bufferRef.current) {
      return applyPaste(bufferRef.current);
    }
    try {
      const text = await navigator.clipboard?.readText();
      if (text) {
        const parsed = parseFlowClipboardText(text);
        if (parsed) {
          bufferRef.current = parsed;
          return applyPaste(parsed);
        }
      }
    } catch {
      /* clipboard permission denied */
    }
    return false;
  }, [readOnly, applyPaste]);

  const copyRef = useRef(copySelection);
  const pasteRef = useRef(pasteClipboard);
  copyRef.current = copySelection;
  pasteRef.current = pasteClipboard;

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (!(e.metaKey || e.ctrlKey)) return;
      if (isFlowKeyboardShortcutBlockedTarget(e.target)) return;
      if (!eventInsideFlowRoot(e, flowRootRef.current)) return;

      const key = e.key.toLowerCase();
      if (key === "c" && !e.shiftKey) {
        if (copyRef.current()) e.preventDefault();
        return;
      }
      if (key === "v" && !e.shiftKey) {
        e.preventDefault();
        void pasteRef.current();
      }
    };
    window.addEventListener("keydown", onKeyDown, true);
    return () => window.removeEventListener("keydown", onKeyDown, true);
  }, [flowRootRef]);

  return {
    copySelection,
    pasteClipboard,
    canPaste: Boolean(bufferRef.current) || !readOnly,
  };
}
