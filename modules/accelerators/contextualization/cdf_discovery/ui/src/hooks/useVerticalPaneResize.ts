import { useCallback, useEffect, useRef, useState } from "react";

function clamp(n: number, lo: number, hi: number): number {
  return Math.min(hi, Math.max(lo, n));
}

export type VerticalPaneResizeOptions = {
  initialHeight?: number;
  minHeight?: number;
  maxHeight?: number;
  storageKey?: string;
};

export function useVerticalPaneResize({
  initialHeight = 160,
  minHeight = 80,
  maxHeight,
  storageKey,
}: VerticalPaneResizeOptions = {}) {
  const resolveMax = useCallback(
    () => maxHeight ?? Math.round(window.innerHeight * 0.5),
    [maxHeight]
  );

  const [height, setHeight] = useState(initialHeight);
  const hydrated = useRef(false);

  useEffect(() => {
    if (!storageKey) {
      hydrated.current = true;
      return;
    }
    try {
      const raw = localStorage.getItem(storageKey);
      if (raw) {
        const n = parseInt(raw, 10);
        if (Number.isFinite(n)) setHeight(clamp(n, minHeight, resolveMax()));
      }
    } catch {
      /* ignore */
    }
    hydrated.current = true;
  }, [storageKey, minHeight, resolveMax]);

  useEffect(() => {
    if (!storageKey || !hydrated.current) return;
    try {
      localStorage.setItem(storageKey, String(height));
    } catch {
      /* ignore */
    }
  }, [height, storageKey]);

  const onResizeStart = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      const startY = e.clientY;
      const startH = height;
      const onMove = (ev: MouseEvent) => {
        setHeight(clamp(startH + (ev.clientY - startY), minHeight, resolveMax()));
      };
      const onUp = () => {
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
      };
      document.body.style.cursor = "row-resize";
      document.body.style.userSelect = "none";
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    },
    [height, minHeight, resolveMax]
  );

  return { height, onResizeStart, setHeight };
}
