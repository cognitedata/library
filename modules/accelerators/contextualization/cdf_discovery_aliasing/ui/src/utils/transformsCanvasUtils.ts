import type { JsonObject } from "../types/scopeConfig";
import type { WorkflowCanvasDocument, WorkflowCanvasNode, WorkflowCanvasNodeData } from "../types/workflowCanvas";
import { newNodeId } from "../components/flow/flowDocumentBridge";
import {
  TRANSFORM_HANDLER_IDS,
  type DiscoveryTransformHandlerId,
} from "../components/flow/handlerRegistry";
import {
  defaultTransformNodeConfig,
  isDiscoveryTransformHandlerId,
  readTransformHandlerId as readCfgHandlerId,
  sanitizeTransformNodeConfig,
} from "./transformHandlerTemplates";

export { TRANSFORM_HANDLER_IDS };
export type TransformHandlerId = DiscoveryTransformHandlerId;

export function listTransformNodes(canvas: WorkflowCanvasDocument): WorkflowCanvasNode[] {
  return canvas.nodes.filter((n) => n.kind === "transform");
}

/** Palette / per-handler nodes: handler is fixed at creation (`preset_from_palette` or `handler_id`). */
export function isHandlerTypedTransformNode(node: WorkflowCanvasNode): boolean {
  if (node.kind !== "transform") return false;
  if (node.data?.preset_from_palette === true) return true;
  const handler = readTransformHandlerId(node);
  return Boolean(handler && isDiscoveryTransformHandlerId(handler));
}

export function readTransformHandlerId(node: WorkflowCanvasNode): string {
  const fromData = String(node.data?.handler_id ?? "").trim();
  if (fromData) return fromData;
  const cfg = readTransformConfig(node);
  return readCfgHandlerId(cfg as Record<string, unknown>);
}

export function readTransformConfig(node: WorkflowCanvasNode): JsonObject {
  const data = node.data ?? {};
  const cfg = data.config;
  if (cfg && typeof cfg === "object" && !Array.isArray(cfg)) {
    return sanitizeTransformNodeConfig(cfg as Record<string, unknown>) as JsonObject;
  }
  return {};
}

export function patchTransformNode(
  canvas: WorkflowCanvasDocument,
  nodeId: string,
  nextConfig: JsonObject,
  handlerId?: string
): WorkflowCanvasDocument {
  return {
    ...canvas,
    nodes: canvas.nodes.map((n) => {
      if (n.id !== nodeId) return n;
      const handler = handlerId ?? readCfgHandlerId(nextConfig as Record<string, unknown>);
      const data: WorkflowCanvasNodeData = {
        ...(n.data ?? {}),
        config: sanitizeTransformNodeConfig(nextConfig as Record<string, unknown>) as JsonObject,
        handler_family: "discovery",
      };
      if (handler) data.handler_id = handler;
      return { ...n, data };
    }),
  };
}

export function addTransformNode(
  canvas: WorkflowCanvasDocument,
  handler: TransformHandlerId = "regex_substitution"
): { canvas: WorkflowCanvasDocument; nodeId: string } {
  const id = newNodeId();
  const existing = listTransformNodes(canvas);
  const last = existing.length > 0 ? existing[existing.length - 1]! : null;
  const previousOutputField = last
    ? String(readTransformConfig(last).output_field ?? "").trim() || null
    : null;
  const config = defaultTransformNodeConfig(handler, { previousOutputField });
  const node: WorkflowCanvasNode = {
    id,
    kind: "transform",
    position: { x: 0, y: 0 },
    data: {
      label: handler,
      handler_family: "discovery",
      handler_id: handler,
      preset_from_palette: true,
      config,
    },
  };
  return { canvas: { ...canvas, nodes: [...canvas.nodes, node] }, nodeId: id };
}

export function removeTransformNode(canvas: WorkflowCanvasDocument, nodeId: string): WorkflowCanvasDocument {
  return {
    ...canvas,
    nodes: canvas.nodes.filter((n) => n.id !== nodeId),
    edges: canvas.edges.filter((e) => e.source !== nodeId && e.target !== nodeId),
  };
}

export function transformNodeListLabel(node: WorkflowCanvasNode): string {
  const cfg = readTransformConfig(node);
  const handler = readTransformHandlerId(node);
  if (handler) return handler;
  const desc = String(cfg.description ?? "").trim();
  if (desc) return desc;
  const label = String(node.data?.label ?? "").trim();
  if (label) return label;
  return node.id;
}

export function isTransformHandlerId(h: string): h is TransformHandlerId {
  return isDiscoveryTransformHandlerId(h);
}
