import type { DragEvent } from "react";
import type { TransformCanvasNodeKind } from "../../types/transformCanvas";
import type { TreeNode } from "../../types/discoveryNodes";
import type { DataTreeEntityDragPayload } from "../../utils/dataTreeEntityDrop";

export type PaletteDragPayload = {
  kind: "etl_stage";
  stage: TransformCanvasNodeKind;
};

export type TransformFlowDropPayload = PaletteDragPayload | DataTreeEntityDragPayload;

export const TRANSFORM_PALETTE_DRAG_MIME = "application/x-transform-flow-palette";
export const TRANSFORM_DATA_TREE_DRAG_MIME = "application/x-transform-data-tree-entity";

export function setTransformPaletteDragData(e: DragEvent, payload: PaletteDragPayload) {
  e.dataTransfer.setData(TRANSFORM_PALETTE_DRAG_MIME, JSON.stringify(payload));
  e.dataTransfer.effectAllowed = "copy";
}

export function setDataTreeEntityDragData(e: DragEvent, node: TreeNode) {
  const payload: DataTreeEntityDragPayload = { kind: "data_tree_entity", node };
  e.dataTransfer.setData(TRANSFORM_DATA_TREE_DRAG_MIME, JSON.stringify(payload));
  e.dataTransfer.effectAllowed = "copy";
}

export function getTransformFlowDropPayload(e: DragEvent): TransformFlowDropPayload | null {
  const paletteRaw = e.dataTransfer.getData(TRANSFORM_PALETTE_DRAG_MIME);
  if (paletteRaw) {
    try {
      return JSON.parse(paletteRaw) as PaletteDragPayload;
    } catch {
      return null;
    }
  }
  const treeRaw = e.dataTransfer.getData(TRANSFORM_DATA_TREE_DRAG_MIME);
  if (treeRaw) {
    try {
      return JSON.parse(treeRaw) as DataTreeEntityDragPayload;
    } catch {
      return null;
    }
  }
  return null;
}

/** @deprecated Use getTransformFlowDropPayload */
export function getTransformPaletteDropPayload(e: DragEvent): PaletteDragPayload | null {
  const payload = getTransformFlowDropPayload(e);
  if (payload?.kind === "etl_stage") return payload;
  return null;
}
