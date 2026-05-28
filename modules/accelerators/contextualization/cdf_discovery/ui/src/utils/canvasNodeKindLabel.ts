import type { MessageKey } from "../i18n/types";
import type { TransformCanvasNodeKind } from "../types/transformCanvas";

export type CanvasNodeTranslate = (
  key: MessageKey,
  vars?: Record<string, string | number>
) => string;

export function canvasNodeKindMessageKey(kind: TransformCanvasNodeKind): MessageKey {
  if (kind === "start") return "transform.canvas.defaultStartLabel";
  if (kind === "end") return "transform.canvas.defaultEndLabel";
  return `transform.palette.${kind}` as MessageKey;
}

export function canvasNodeKindLabel(kind: TransformCanvasNodeKind, t: CanvasNodeTranslate): string {
  return t(canvasNodeKindMessageKey(kind));
}

/** Stable palette / picker order by localized stage label (group order unchanged). */
export function sortPaletteStages(
  stages: readonly TransformCanvasNodeKind[],
  t: CanvasNodeTranslate
): TransformCanvasNodeKind[] {
  return [...stages].sort((a, b) =>
    canvasNodeKindLabel(a, t).localeCompare(canvasNodeKindLabel(b, t), undefined, {
      sensitivity: "base",
    })
  );
}

export function canvasNodeDisplayLabel(
  data: { label?: string; notes?: string; config?: unknown } | undefined,
  kind: TransformCanvasNodeKind,
  t: CanvasNodeTranslate
): string {
  const label = data?.label != null ? String(data.label).trim() : "";
  if (label) return label;
  const notes = data?.notes != null ? String(data.notes).trim() : "";
  if (notes) return notes;
  return canvasNodeKindLabel(kind, t);
}
