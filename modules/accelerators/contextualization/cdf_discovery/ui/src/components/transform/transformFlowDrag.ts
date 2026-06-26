import type { DragEvent } from "react";
import type { TransformCanvasNodeKind } from "../../types/transformCanvas";
import type { TreeNode } from "../../types/discoveryNodes";
import type { DataTreeEntityDragPayload } from "../../utils/dataTreeEntityDrop";
import type { CdfResourceDragPayload } from "../../utils/cdfResourceDrop";
import { cdfResourceDragPayloadFromNode } from "../../utils/cdfResourceDrop";

export type PaletteDragPayload = {
  kind: "etl_stage";
  stage: TransformCanvasNodeKind;
  /** Set when the user picks a handler from the drop menu (transform / build_index). */
  handlerId?: string;
};

export type TransformFlowDropPayload =
  | PaletteDragPayload
  | DataTreeEntityDragPayload
  | CdfResourceDragPayload;

export const TRANSFORM_PALETTE_DRAG_MIME = "application/x-transform-flow-palette";
export const TRANSFORM_DATA_TREE_DRAG_MIME = "application/x-transform-data-tree-entity";
export const TRANSFORM_CDF_RESOURCE_DRAG_MIME = "application/x-transform-cdf-resource";

export function setTransformPaletteDragData(e: DragEvent, payload: PaletteDragPayload) {
  e.dataTransfer.setData(TRANSFORM_PALETTE_DRAG_MIME, JSON.stringify(payload));
  e.dataTransfer.effectAllowed = "copy";
}

export function setDataTreeEntityDragData(e: DragEvent, node: TreeNode) {
  const payload: DataTreeEntityDragPayload = { kind: "data_tree_entity", node };
  e.dataTransfer.setData(TRANSFORM_DATA_TREE_DRAG_MIME, JSON.stringify(payload));
  e.dataTransfer.effectAllowed = "copy";
}

export function setCdfResourceDragData(e: DragEvent, node: TreeNode) {
  const payload = cdfResourceDragPayloadFromNode(node);
  if (!payload) return;
  const encoded = JSON.stringify(payload);
  e.dataTransfer.setData(TRANSFORM_CDF_RESOURCE_DRAG_MIME, encoded);
  e.dataTransfer.setData("text/plain", encoded);
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
  const cdfRaw = e.dataTransfer.getData(TRANSFORM_CDF_RESOURCE_DRAG_MIME);
  if (cdfRaw) {
    try {
      const parsed = JSON.parse(cdfRaw) as CdfResourceDragPayload;
      if (
        parsed?.kind === "cdf_function" &&
        typeof parsed.functionExternalId === "string" &&
        parsed.functionExternalId.trim()
      ) {
        return parsed;
      }
      if (
        parsed?.kind === "cdf_transformation" &&
        typeof parsed.transformationExternalId === "string" &&
        parsed.transformationExternalId.trim()
      ) {
        return parsed;
      }
      if (
        parsed?.kind === "cdf_workflow" &&
        typeof parsed.workflowExternalId === "string" &&
        parsed.workflowExternalId.trim()
      ) {
        return parsed;
      }
      return null;
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

/** Whether drag event likely carries a Transform Flow payload (safe for dragover). */
export function hasTransformFlowDragPayload(e: DragEvent): boolean {
  const types = Array.from(e.dataTransfer.types ?? []);
  return (
    types.includes(TRANSFORM_PALETTE_DRAG_MIME) ||
    types.includes(TRANSFORM_DATA_TREE_DRAG_MIME) ||
    types.includes(TRANSFORM_CDF_RESOURCE_DRAG_MIME)
  );
}

