import type { TransformCanvasNodeKind } from "../types/transformCanvas";
import type { TreeNode } from "../types/discoveryNodes";

export type CdfFunctionDragPayload = {
  kind: "cdf_function";
  functionExternalId: string;
  label: string;
};

export type CdfTransformationDragPayload = {
  kind: "cdf_transformation";
  transformationExternalId: string;
  label: string;
};

export type CdfWorkflowDragPayload = {
  kind: "cdf_workflow";
  workflowExternalId: string;
  workflowVersion: string;
  label: string;
};

export type CdfResourceDragPayload =
  | CdfFunctionDragPayload
  | CdfTransformationDragPayload
  | CdfWorkflowDragPayload;

export type CdfResourceDropStage = {
  stage: TransformCanvasNodeKind;
  label: string;
  config: Record<string, unknown>;
};

function metaString(meta: Record<string, unknown> | undefined, key: string): string {
  const raw = meta?.[key];
  return raw != null ? String(raw).trim() : "";
}

export function cdfResourceDragPayloadFromNode(node: TreeNode): CdfResourceDragPayload | null {
  const meta = node.meta;
  const label = node.label.trim() || "Resource";

  if (node.kind === "function") {
    const functionExternalId =
      metaString(meta, "external_id") || metaString(meta, "id");
    if (!functionExternalId) return null;
    return { kind: "cdf_function", functionExternalId, label };
  }

  if (node.kind === "transformation") {
    const transformationExternalId =
      metaString(meta, "external_id") ||
      (meta?.id != null && String(meta.id).trim() ? String(meta.id).trim() : "");
    if (!transformationExternalId) return null;
    return { kind: "cdf_transformation", transformationExternalId, label };
  }

  if (node.kind === "workflow") {
    const workflowExternalId = metaString(meta, "external_id");
    if (!workflowExternalId) return null;
    const workflowVersion = metaString(meta, "version") || "1";
    return { kind: "cdf_workflow", workflowExternalId, workflowVersion, label };
  }

  return null;
}

export function canDragCdfResourceToTransformCanvas(node: TreeNode): boolean {
  return cdfResourceDragPayloadFromNode(node) != null;
}

export function cdfResourceDropStage(payload: CdfResourceDragPayload): CdfResourceDropStage {
  switch (payload.kind) {
    case "cdf_function":
      return {
        stage: "function_ref",
        label: payload.label,
        config: {
          description: payload.label,
          function_external_id: payload.functionExternalId,
        },
      };
    case "cdf_transformation":
      return {
        stage: "transformation_ref",
        label: payload.label,
        config: {
          description: payload.label,
          transformation_external_id: payload.transformationExternalId,
        },
      };
    case "cdf_workflow":
      return {
        stage: "subworkflow",
        label: payload.label,
        config: {
          description: payload.label,
          workflow_external_id: payload.workflowExternalId,
          workflow_version: payload.workflowVersion,
        },
      };
  }
}
