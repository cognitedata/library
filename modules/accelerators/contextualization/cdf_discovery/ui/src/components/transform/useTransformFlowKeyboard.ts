import { useEffect, useRef, type MutableRefObject, type RefObject } from "react";
import type { Node } from "@xyflow/react";
import { isTransformFlowKeyboardShortcutBlockedTarget } from "./transformFlowClipboard";

export type UseTransformFlowKeyboardOptions = {
  flowRootRef: RefObject<HTMLElement | null>;
  readOnly?: boolean;
  rfSelectionRef: MutableRefObject<Node[]>;
  onCopy: () => boolean;
  onPaste: () => Promise<boolean>;
  onCut: () => boolean;
  onDuplicate: () => boolean;
  onSelectAll: () => void;
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

/** Canvas keyboard shortcuts when focus is inside the flow root. */
export function useTransformFlowKeyboard({
  flowRootRef,
  readOnly = false,
  onCopy,
  onPaste,
  onCut,
  onDuplicate,
  onSelectAll,
  onUndo,
  onRedo,
  canUndo,
  canRedo,
}: UseTransformFlowKeyboardOptions): void {
  const copyRef = useRef(onCopy);
  const pasteRef = useRef(onPaste);
  const cutRef = useRef(onCut);
  const duplicateRef = useRef(onDuplicate);
  const selectAllRef = useRef(onSelectAll);
  const undoRef = useRef(onUndo);
  const redoRef = useRef(onRedo);
  copyRef.current = onCopy;
  pasteRef.current = onPaste;
  cutRef.current = onCut;
  duplicateRef.current = onDuplicate;
  selectAllRef.current = onSelectAll;
  undoRef.current = onUndo;
  redoRef.current = onRedo;

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (isTransformFlowKeyboardShortcutBlockedTarget(e.target)) return;
      if (!eventInsideFlowRoot(e, flowRootRef.current)) return;

      const mod = e.metaKey || e.ctrlKey;
      const key = e.key.toLowerCase();

      if (mod && key === "z" && !e.shiftKey) {
        if (!readOnly && canUndo) {
          e.preventDefault();
          undoRef.current();
        }
        return;
      }
      if (mod && ((key === "z" && e.shiftKey) || key === "y")) {
        if (!readOnly && canRedo) {
          e.preventDefault();
          redoRef.current();
        }
        return;
      }
      if (mod && key === "a") {
        e.preventDefault();
        selectAllRef.current();
        return;
      }
      if (readOnly) return;
      if (!mod) return;

      if (key === "c" && !e.shiftKey) {
        if (copyRef.current()) e.preventDefault();
        return;
      }
      if (key === "x" && !e.shiftKey) {
        if (cutRef.current()) e.preventDefault();
        return;
      }
      if (key === "v" && !e.shiftKey) {
        e.preventDefault();
        void pasteRef.current();
        return;
      }
      if (key === "d" && !e.shiftKey) {
        if (duplicateRef.current()) e.preventDefault();
      }
    };
    window.addEventListener("keydown", onKeyDown, true);
    return () => window.removeEventListener("keydown", onKeyDown, true);
  }, [flowRootRef, readOnly, canUndo, canRedo]);
}
