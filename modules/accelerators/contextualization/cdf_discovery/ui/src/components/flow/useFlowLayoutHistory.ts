import { useCallback, useRef, useState } from "react";
import type { Node } from "@xyflow/react";
import type { TransformCanvasViewport } from "../../types/transformCanvasViewport";
import {
  cloneFlowLayoutHistorySnapshot,
  pushFlowLayoutHistory,
  type FlowLayoutHistorySnapshot,
} from "../transform/transformFlowHistory";

export type UseFlowLayoutHistoryArgs = {
  nodes: Node[];
  viewportRef: React.MutableRefObject<TransformCanvasViewport | null>;
};

export type UseFlowLayoutHistoryResult = {
  canUndo: boolean;
  canRedo: boolean;
  recordBeforeChange: () => void;
  undo: () => FlowLayoutHistorySnapshot | null;
  redo: () => FlowLayoutHistorySnapshot | null;
  reset: () => void;
  isApplying: () => boolean;
};

/** Undo/redo for canvas layout (node positions and viewport) on viewer panes. */
export function useFlowLayoutHistory({
  nodes,
  viewportRef,
}: UseFlowLayoutHistoryArgs): UseFlowLayoutHistoryResult {
  const pastRef = useRef<FlowLayoutHistorySnapshot[]>([]);
  const futureRef = useRef<FlowLayoutHistorySnapshot[]>([]);
  const applyingRef = useRef(false);
  const [canUndo, setCanUndo] = useState(false);
  const [canRedo, setCanRedo] = useState(false);

  const syncFlags = useCallback(() => {
    setCanUndo(pastRef.current.length > 0);
    setCanRedo(futureRef.current.length > 0);
  }, []);

  const currentSnapshot = useCallback((): FlowLayoutHistorySnapshot => {
    return {
      nodes,
      viewport: viewportRef.current ? { ...viewportRef.current } : null,
    };
  }, [nodes, viewportRef]);

  const recordBeforeChange = useCallback(() => {
    if (applyingRef.current) return;
    pastRef.current = pushFlowLayoutHistory(pastRef.current, currentSnapshot());
    futureRef.current = [];
    syncFlags();
  }, [currentSnapshot, syncFlags]);

  const undo = useCallback((): FlowLayoutHistorySnapshot | null => {
    if (pastRef.current.length === 0) return null;
    applyingRef.current = true;
    try {
      const prev = pastRef.current[pastRef.current.length - 1]!;
      pastRef.current = pastRef.current.slice(0, -1);
      futureRef.current = pushFlowLayoutHistory(futureRef.current, currentSnapshot());
      syncFlags();
      return cloneFlowLayoutHistorySnapshot(prev);
    } finally {
      applyingRef.current = false;
    }
  }, [currentSnapshot, syncFlags]);

  const redo = useCallback((): FlowLayoutHistorySnapshot | null => {
    if (futureRef.current.length === 0) return null;
    applyingRef.current = true;
    try {
      const next = futureRef.current[futureRef.current.length - 1]!;
      futureRef.current = futureRef.current.slice(0, -1);
      pastRef.current = pushFlowLayoutHistory(pastRef.current, currentSnapshot());
      syncFlags();
      return cloneFlowLayoutHistorySnapshot(next);
    } finally {
      applyingRef.current = false;
    }
  }, [currentSnapshot, syncFlags]);

  const reset = useCallback(() => {
    pastRef.current = [];
    futureRef.current = [];
    syncFlags();
  }, [syncFlags]);

  const isApplying = useCallback(() => applyingRef.current, []);

  return {
    canUndo,
    canRedo,
    recordBeforeChange,
    undo,
    redo,
    reset,
    isApplying,
  };
}
