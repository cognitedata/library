import { useEffect, useRef, type RefObject } from "react";
import { isTransformFlowKeyboardShortcutBlockedTarget } from "../transform/transformFlowClipboard";

export type UseFlowCanvasKeyboardOptions = {
  flowRootRef: RefObject<HTMLElement | null>;
  readOnly?: boolean;
  onUndo: () => void;
  onRedo: () => void;
  canUndo: boolean;
  canRedo: boolean;
};

function eventInsideFlowRoot(e: KeyboardEvent, root: HTMLElement | null): boolean {
  if (!root) return false;
  const path = e.composedPath();
  return path.some((n) => n instanceof Node && root.contains(n));
}

/** Undo/redo shortcuts when focus is inside the flow canvas. */
export function useFlowCanvasKeyboard({
  flowRootRef,
  readOnly = false,
  onUndo,
  onRedo,
  canUndo,
  canRedo,
}: UseFlowCanvasKeyboardOptions): void {
  const undoRef = useRef(onUndo);
  const redoRef = useRef(onRedo);
  undoRef.current = onUndo;
  redoRef.current = onRedo;

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (isTransformFlowKeyboardShortcutBlockedTarget(e.target)) return;
      if (!eventInsideFlowRoot(e, flowRootRef.current)) return;

      const mod = e.metaKey || e.ctrlKey;
      if (!mod) return;
      const key = e.key.toLowerCase();

      if (key === "z" && !e.shiftKey) {
        if (!readOnly && canUndo) {
          e.preventDefault();
          undoRef.current();
        }
        return;
      }
      if ((key === "z" && e.shiftKey) || key === "y") {
        if (!readOnly && canRedo) {
          e.preventDefault();
          redoRef.current();
        }
      }
    };
    window.addEventListener("keydown", onKeyDown, true);
    return () => window.removeEventListener("keydown", onKeyDown, true);
  }, [flowRootRef, readOnly, canUndo, canRedo]);
}
