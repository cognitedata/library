import { useCallback, useRef, useState } from "react";
import { LAYOUT } from "@/lib/diagramData";

interface DiagramPaneProps {
  children: React.ReactNode;
  baseWidth?: number;
  baseHeight?: number;
  className?: string;
  style?: React.CSSProperties;
}

const MIN_ZOOM = 0.4;
const MAX_ZOOM = 3;
const ZOOM_STEP = 0.2;

/** Total vertical space to subtract from main pane height so diagram fits at 100%: zoom toolbar row + caption added to inner content height. */
export const DIAGRAM_PANE_TOOLBAR_HEIGHT = 40 + 28;

export function DiagramPane({
  children,
  baseWidth = LAYOUT.TARGET_LAYOUT_WIDTH,
  baseHeight = LAYOUT.TARGET_LAYOUT_HEIGHT,
  className = "",
  style,
}: DiagramPaneProps) {
  const [zoom, setZoom] = useState(1);
  const containerRef = useRef<HTMLDivElement>(null);

  const clampZoom = useCallback((z: number) => Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, z)), []);

  const handleWheel = useCallback(
    (e: React.WheelEvent) => {
      if (!e.ctrlKey && !e.metaKey) return;
      e.preventDefault();
      setZoom((z) => clampZoom(z * (e.deltaY > 0 ? 1 / 1.15 : 1.15)));
    },
    [clampZoom]
  );

  const zoomIn = () => setZoom((z) => clampZoom(z + ZOOM_STEP));
  const zoomOut = () => setZoom((z) => clampZoom(z - ZOOM_STEP));
  const zoomReset = () => setZoom(1);

  const w = Math.round(baseWidth * zoom);
  const captionHeight = 28;
  const h = Math.round(baseHeight * zoom) + captionHeight;

  return (
    <div className={`flex flex-col flex-1 min-h-0 ${className}`} style={style}>
      <div className="flex items-center gap-2 flex-shrink-0 mb-1.5">
        <span className="text-xs text-slate-500 tabular-nums">{Math.round(zoom * 100)}%</span>
        <button
          type="button"
          onClick={zoomOut}
          className="rounded border border-slate-500 bg-slate-700/50 text-slate-200 px-2 py-0.5 text-sm hover:bg-slate-600 disabled:opacity-40"
          disabled={zoom <= MIN_ZOOM}
          aria-label="Zoom out"
        >
          −
        </button>
        <button
          type="button"
          onClick={zoomIn}
          className="rounded border border-slate-500 bg-slate-700/50 text-slate-200 px-2 py-0.5 text-sm hover:bg-slate-600 disabled:opacity-40"
          disabled={zoom >= MAX_ZOOM}
          aria-label="Zoom in"
        >
          +
        </button>
        <button
          type="button"
          onClick={zoomReset}
          className="rounded border border-slate-500 bg-slate-700/50 text-slate-200 px-2 py-0.5 text-xs hover:bg-slate-600"
          aria-label="Reset zoom"
        >
          Reset
        </button>
      </div>
      <div
        ref={containerRef}
        className="flex-1 min-h-0 overflow-auto rounded border border-slate-600/50 bg-slate-900/30"
        onWheel={handleWheel}
        style={{ touchAction: "none" }}
      >
        <div style={{ width: w, height: h, minWidth: w, minHeight: h }} className="flex flex-col">
          {children}
        </div>
      </div>
    </div>
  );
}
