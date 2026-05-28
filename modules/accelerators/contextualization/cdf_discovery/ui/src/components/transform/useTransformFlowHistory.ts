import { useCallback, useRef, useState } from "react";
import type { Edge, Node } from "@xyflow/react";
import type {
  TransformCanvasEdgePathStyle,
  TransformCanvasHandleOrientation,
  TransformCanvasLayoutMethod,
} from "../../types/transformCanvas";
import type { TransformCanvasViewport } from "../../types/transformCanvasViewport";
import {
  cloneTransformFlowHistorySnapshot,
  pushTransformFlowHistory,
  type TransformFlowHistorySnapshot,
} from "./transformFlowHistory";

export type UseTransformFlowHistoryArgs = {
  nodes: Node[];
  edges: Edge[];
  handleOrientation: TransformCanvasHandleOrientation;
  layoutMethod: TransformCanvasLayoutMethod;
  edgePathStyle: TransformCanvasEdgePathStyle;
  viewportRef: React.MutableRefObject<TransformCanvasViewport | null>;
};

export type UseTransformFlowHistoryResult = {
  canUndo: boolean;
  canRedo: boolean;
  recordBeforeChange: () => void;
  undo: () => TransformFlowHistorySnapshot | null;
  redo: () => TransformFlowHistorySnapshot | null;
  reset: () => void;
  isApplying: () => boolean;
};

export function useTransformFlowHistory({
  nodes,
  edges,
  handleOrientation,
  layoutMethod,
  edgePathStyle,
  viewportRef,
}: UseTransformFlowHistoryArgs): UseTransformFlowHistoryResult {
  const pastRef = useRef<TransformFlowHistorySnapshot[]>([]);
  const futureRef = useRef<TransformFlowHistorySnapshot[]>([]);
  const applyingRef = useRef(false);
  const [canUndo, setCanUndo] = useState(false);
  const [canRedo, setCanRedo] = useState(false);

  const syncFlags = useCallback(() => {
    setCanUndo(pastRef.current.length > 0);
    setCanRedo(futureRef.current.length > 0);
  }, []);

  const currentSnapshot = useCallback((): TransformFlowHistorySnapshot => {
    return {
      nodes,
      edges,
      handleOrientation,
      layoutMethod,
      edgePathStyle,
      viewport: viewportRef.current ? { ...viewportRef.current } : null,
    };
  }, [nodes, edges, handleOrientation, layoutMethod, edgePathStyle, viewportRef]);

  const recordBeforeChange = useCallback(() => {
    if (applyingRef.current) return;
    pastRef.current = pushTransformFlowHistory(pastRef.current, currentSnapshot());
    futureRef.current = [];
    syncFlags();
  }, [currentSnapshot, syncFlags]);

  const undo = useCallback((): TransformFlowHistorySnapshot | null => {
    if (pastRef.current.length === 0) return null;
    applyingRef.current = true;
    try {
      const prev = pastRef.current[pastRef.current.length - 1]!;
      pastRef.current = pastRef.current.slice(0, -1);
      futureRef.current = pushTransformFlowHistory(futureRef.current, currentSnapshot());
      syncFlags();
      return cloneTransformFlowHistorySnapshot(prev);
    } finally {
      applyingRef.current = false;
    }
  }, [currentSnapshot, syncFlags]);

  const redo = useCallback((): TransformFlowHistorySnapshot | null => {
    if (futureRef.current.length === 0) return null;
    applyingRef.current = true;
    try {
      const next = futureRef.current[futureRef.current.length - 1]!;
      futureRef.current = futureRef.current.slice(0, -1);
      pastRef.current = pushTransformFlowHistory(pastRef.current, currentSnapshot());
      syncFlags();
      return cloneTransformFlowHistorySnapshot(next);
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
