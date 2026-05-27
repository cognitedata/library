import { useCallback, useEffect, useRef } from "react";

/** Debounce node data patches so the flow canvas is not updated on every keystroke. */
export function useDebouncedNodePatch(
  nodeId: string,
  onPatchNode: (nodeId: string, data: Record<string, unknown>) => void,
  delayMs = 400
): {
  schedulePatch: (data: Record<string, unknown>) => void;
  flushNow: () => void;
} {
  const onPatchNodeRef = useRef(onPatchNode);
  onPatchNodeRef.current = onPatchNode;
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pendingRef = useRef<{ nodeId: string; data: Record<string, unknown> } | null>(null);

  const flush = useCallback(() => {
    if (timerRef.current) {
      window.clearTimeout(timerRef.current);
      timerRef.current = null;
    }
    const pending = pendingRef.current;
    if (!pending) return;
    pendingRef.current = null;
    onPatchNodeRef.current(pending.nodeId, pending.data);
  }, []);

  const schedulePatch = useCallback(
    (data: Record<string, unknown>) => {
      pendingRef.current = { nodeId, data };
      if (timerRef.current) window.clearTimeout(timerRef.current);
      timerRef.current = window.setTimeout(flush, delayMs);
    },
    [nodeId, delayMs, flush]
  );

  useEffect(() => {
    return () => {
      if (timerRef.current) window.clearTimeout(timerRef.current);
      if (pendingRef.current) {
        onPatchNodeRef.current(pendingRef.current.nodeId, pendingRef.current.data);
        pendingRef.current = null;
      }
    };
  }, [nodeId]);

  return { schedulePatch, flushNow: flush };
}
