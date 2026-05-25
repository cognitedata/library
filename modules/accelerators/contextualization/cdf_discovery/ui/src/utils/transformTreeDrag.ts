import type { DragEvent } from "react";
import type { TreeNode } from "../types/discoveryNodes";
import { TRANSFORM_ROOT, TRANSFORM_TEMPLATES } from "./treeNodeIds";
import {
  isTransformPipelineTreeNode,
  isTransformTemplateTreeNode,
  pipelineIdFromNode,
  pipelineLabelFromMeta,
  templateIdFromNode,
  templateLabelFromMeta,
} from "./transformTabs";

export const TRANSFORM_TREE_DRAG_MIME = "application/x-transform-tree-item";

export type TransformTreeDragPayload =
  | { kind: "etl_pipeline"; pipelineId: string; label: string }
  | { kind: "etl_template"; templateId: string; label: string };

let activeTransformTreeDrag: TransformTreeDragPayload | null = null;

export function beginTransformTreeDrag(payload: TransformTreeDragPayload): void {
  activeTransformTreeDrag = payload;
}

export function endTransformTreeDrag(): void {
  activeTransformTreeDrag = null;
}

export function peekTransformTreeDragPayload(): TransformTreeDragPayload | null {
  return activeTransformTreeDrag;
}

export function canDragTransformTreeItem(node: TreeNode): boolean {
  return node.kind === "etl_pipeline" || node.kind === "etl_template";
}

export function transformTreeDragPayloadFromNode(node: TreeNode): TransformTreeDragPayload | null {
  if (node.kind === "etl_pipeline") {
    const pipelineId = pipelineIdFromNode(node);
    if (!pipelineId) return null;
    return {
      kind: "etl_pipeline",
      pipelineId,
      label: pipelineLabelFromMeta(node.meta),
    };
  }
  if (node.kind === "etl_template") {
    const templateId = templateIdFromNode(node);
    if (!templateId) return null;
    return {
      kind: "etl_template",
      templateId,
      label: templateLabelFromMeta(node.meta),
    };
  }
  return null;
}

export function setTransformTreeDragData(e: DragEvent, node: TreeNode) {
  const payload = transformTreeDragPayloadFromNode(node);
  if (!payload) return;
  beginTransformTreeDrag(payload);
  const encoded = JSON.stringify(payload);
  e.dataTransfer.setData(TRANSFORM_TREE_DRAG_MIME, encoded);
  e.dataTransfer.setData("text/plain", encoded);
  e.dataTransfer.effectAllowed = "copy";
}

function parseTransformTreeDragPayload(raw: string): TransformTreeDragPayload | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as TransformTreeDragPayload;
    if (parsed?.kind === "etl_pipeline" && parsed.pipelineId) return parsed;
    if (parsed?.kind === "etl_template" && parsed.templateId) return parsed;
    return null;
  } catch {
    return null;
  }
}

/** Read drag payload during dragover/drop (session ref; dataTransfer is often empty until drop). */
export function getTransformTreeDragPayload(e: DragEvent): TransformTreeDragPayload | null {
  const fromSession = peekTransformTreeDragPayload();
  if (fromSession) return fromSession;
  const raw =
    e.dataTransfer.getData(TRANSFORM_TREE_DRAG_MIME) || e.dataTransfer.getData("text/plain");
  return parseTransformTreeDragPayload(raw);
}

/** Rows that accept cross-kind drops (pipeline → save as template, template → new pipeline). */
export function resolveTransformTreeDropTarget(
  node: Pick<TreeNode, "id" | "kind">
): "pipelines" | "templates" | null {
  if (node.id === TRANSFORM_TEMPLATES) return "templates";
  if (isTransformTemplateTreeNode(node)) return "templates";
  if (node.id === TRANSFORM_ROOT) return "pipelines";
  if (isTransformPipelineTreeNode(node)) return "pipelines";
  return null;
}

export function transformTreeDropAccepts(
  target: "pipelines" | "templates",
  payload: TransformTreeDragPayload
): boolean {
  if (target === "templates" && payload.kind === "etl_pipeline") return true;
  if (target === "pipelines" && payload.kind === "etl_template") return true;
  return false;
}
