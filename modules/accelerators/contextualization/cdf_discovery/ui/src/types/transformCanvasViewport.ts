/** Persisted React Flow viewport (pan + zoom) on the canvas document. */
export type TransformCanvasViewport = {
  x: number;
  y: number;
  zoom: number;
};

export function normalizeTransformCanvasViewport(raw: unknown): TransformCanvasViewport | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  const o = raw as Record<string, unknown>;
  const x = o.x;
  const y = o.y;
  const zoom = o.zoom;
  if (
    typeof x !== "number" ||
    typeof y !== "number" ||
    typeof zoom !== "number" ||
    !Number.isFinite(x) ||
    !Number.isFinite(y) ||
    !Number.isFinite(zoom) ||
    zoom <= 0
  ) {
    return undefined;
  }
  return { x, y, zoom };
}
