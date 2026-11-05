import { type RefObject, useEffect } from "react";
import type { GraphCanvasRef } from "reagraph";

export function useCanvasResize(
  containerRef: RefObject<HTMLDivElement | null>,
  graphRef: RefObject<GraphCanvasRef | null>
) {
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const triggerResize = () => {
      if (graphRef.current) {
        requestAnimationFrame(() => {
          window.dispatchEvent(new Event("resize"));
        });
      }
    };

    const resizeObserver = new ResizeObserver(triggerResize);
    resizeObserver.observe(container);

    const timeoutId = setTimeout(triggerResize, 100);

    return () => {
      resizeObserver.disconnect();
      clearTimeout(timeoutId);
    };
  }, [containerRef, graphRef]);
}
