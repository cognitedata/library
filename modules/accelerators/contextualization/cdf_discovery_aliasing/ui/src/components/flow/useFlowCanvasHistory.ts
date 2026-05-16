import { useCallback, useEffect, useRef, type MutableRefObject, type RefObject } from "react";
import type { Edge, Node } from "@xyflow/react";
import type { WorkflowCanvasHandleOrientation } from "../../types/workflowCanvas";

export type FlowCanvasSnapshot = {
  nodes: Node[];
  edges: Edge[];
  /** When omitted, undo/redo only restores nodes and edges. */
  handleOrientation?: WorkflowCanvasHandleOrientation;
};

const DEFAULT_MAX = 50;
const DEFAULT_DEBOUNCE_MS = 450;

function cloneSnap(s: FlowCanvasSnapshot): FlowCanvasSnapshot {
  return {
    nodes: structuredClone(s.nodes),
    edges: structuredClone(s.edges),
    ...(s.handleOrientation != null ? { handleOrientation: s.handleOrientation } : {}),
  };
}

function serializeSnap(s: FlowCanvasSnapshot): string {
  return JSON.stringify({
    ho: s.handleOrientation ?? null,
    edges: s.edges.map((e) => ({
      id: e.id,
      s: e.source,
      t: e.target,
      sh: e.sourceHandle ?? null,
      th: e.targetHandle ?? null,
      k: (e.data as { kind?: string } | undefined)?.kind ?? null,
    })),
    nodes: s.nodes.map((n) => ({
      id: n.id,
      t: n.type,
      x: n.position.x,
      y: n.position.y,
      p: n.parentId ?? null,
      d: n.data,
    })),
  });
}

function isEditableFieldTarget(t: EventTarget | null): boolean {
  if (!(t instanceof HTMLElement)) return false;
  if (t.closest("[data-kea-flow-undo-ignore='true']")) return true;
  if (t.isContentEditable) return true;
  const tag = t.tagName;
  if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return true;
  return Boolean(t.closest("input, textarea, select, [contenteditable='true']"));
}

export type UseFlowCanvasHistoryOptions = {
  nodes: Node[];
  edges: Edge[];
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>;
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>;
  handleOrientation?: WorkflowCanvasHandleOrientation;
  setHandleOrientation?: (o: WorkflowCanvasHandleOrientation) => void;
  /** Skip debounced history commits (e.g. while applying undo/redo). */
  suspendRef: MutableRefObject<boolean>;
  /** Only Cmd/Ctrl+Z when the event path intersects this subtree (main canvas wrap). */
  flowRootRef: RefObject<HTMLElement | null>;
  maxDepth?: number;
  debounceMs?: number;
};

export type UseFlowCanvasHistoryResult = {
  /** Replace stacks and baseline snapshot (reload, drill hydrate). */
  reset: (snap: FlowCanvasSnapshot) => void;
};

/**
 * Debounced undo/redo for a React Flow canvas: Cmd/Ctrl+Z and Cmd/Ctrl+Shift+Z when focus or
 * event target lies inside ``flowRootRef``. Coalesces rapid changes (e.g. node drag) into one step.
 */
export function useFlowCanvasHistory({
  nodes,
  edges,
  setNodes,
  setEdges,
  handleOrientation,
  setHandleOrientation,
  suspendRef,
  flowRootRef,
  maxDepth = DEFAULT_MAX,
  debounceMs = DEFAULT_DEBOUNCE_MS,
}: UseFlowCanvasHistoryOptions): UseFlowCanvasHistoryResult {
  const past = useRef<FlowCanvasSnapshot[]>([]);
  const redo = useRef<FlowCanvasSnapshot[]>([]);
  const lastStackedRef = useRef<FlowCanvasSnapshot | null>(null);
  const debounceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const reset = useCallback((snap: FlowCanvasSnapshot) => {
    suspendRef.current = true;
    past.current = [];
    redo.current = [];
    lastStackedRef.current = cloneSnap(snap);
    queueMicrotask(() => {
      suspendRef.current = false;
    });
  }, [suspendRef]);

  useEffect(() => {
    if (suspendRef.current) return;
    if (debounceTimerRef.current) clearTimeout(debounceTimerRef.current);
    debounceTimerRef.current = setTimeout(() => {
      debounceTimerRef.current = null;
      if (suspendRef.current) return;
      const cur: FlowCanvasSnapshot = {
        nodes: structuredClone(nodes),
        edges: structuredClone(edges),
        ...(handleOrientation != null ? { handleOrientation } : {}),
      };
      const prev = lastStackedRef.current;
      if (!prev) {
        lastStackedRef.current = cur;
        return;
      }
      if (serializeSnap(cur) === serializeSnap(prev)) return;
      past.current.push(prev);
      if (past.current.length > maxDepth) past.current.shift();
      redo.current = [];
      lastStackedRef.current = cur;
    }, debounceMs);
    return () => {
      if (debounceTimerRef.current) clearTimeout(debounceTimerRef.current);
    };
  }, [nodes, edges, handleOrientation, debounceMs, maxDepth, suspendRef]);

  const undo = useCallback(() => {
    if (past.current.length === 0) return false;
    suspendRef.current = true;
    const currentSnap: FlowCanvasSnapshot = {
      nodes: structuredClone(nodes),
      edges: structuredClone(edges),
      ...(handleOrientation != null ? { handleOrientation } : {}),
    };
    redo.current.push(currentSnap);
    if (redo.current.length > maxDepth) redo.current.shift();
    const target = past.current.pop()!;
    setNodes(target.nodes);
    setEdges(target.edges);
    if (target.handleOrientation != null && setHandleOrientation) {
      setHandleOrientation(target.handleOrientation);
    }
    lastStackedRef.current = cloneSnap(target);
    queueMicrotask(() => {
      suspendRef.current = false;
    });
    return true;
  }, [nodes, edges, handleOrientation, setNodes, setEdges, setHandleOrientation, maxDepth, suspendRef]);

  const redoFn = useCallback(() => {
    if (redo.current.length === 0) return false;
    suspendRef.current = true;
    const currentSnap: FlowCanvasSnapshot = {
      nodes: structuredClone(nodes),
      edges: structuredClone(edges),
      ...(handleOrientation != null ? { handleOrientation } : {}),
    };
    past.current.push(currentSnap);
    if (past.current.length > maxDepth) past.current.shift();
    const target = redo.current.pop()!;
    setNodes(target.nodes);
    setEdges(target.edges);
    if (target.handleOrientation != null && setHandleOrientation) {
      setHandleOrientation(target.handleOrientation);
    }
    lastStackedRef.current = cloneSnap(target);
    queueMicrotask(() => {
      suspendRef.current = false;
    });
    return true;
  }, [nodes, edges, handleOrientation, setNodes, setEdges, setHandleOrientation, maxDepth, suspendRef]);

  const undoRef = useRef(undo);
  const redoRef = useRef(redoFn);
  undoRef.current = undo;
  redoRef.current = redoFn;

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (!(e.metaKey || e.ctrlKey)) return;
      if (e.key.toLowerCase() !== "z") return;
      if (isEditableFieldTarget(e.target)) return;
      const root = flowRootRef.current;
      if (!root) return;
      const path = e.composedPath();
      if (!path.some((n) => n instanceof Node && root.contains(n))) return;
      if (e.shiftKey) {
        if (redoRef.current()) e.preventDefault();
      } else if (undoRef.current()) {
        e.preventDefault();
      }
    };
    window.addEventListener("keydown", onKeyDown, true);
    return () => window.removeEventListener("keydown", onKeyDown, true);
  }, [flowRootRef]);

  return { reset };
}
