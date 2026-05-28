import type { Node } from "@xyflow/react";
import { buildIndexHandlerDisplayName, isBuildIndexHandlerId } from "../components/transform/etlBuildIndexHandlerRegistry";
import { transformHandlerDisplayName } from "../components/transform/etlHandlerRegistry";
import { rfTypeToKind, type TransformCanvasNodeKind } from "../types/transformCanvas";
import {
  canvasNodeDisplayLabel,
  canvasNodeKindLabel,
  type CanvasNodeTranslate,
} from "./canvasNodeKindLabel";
import { isEtlTransformHandlerId } from "./etlTransformHandlerTemplates";

export function etlStageTypeDisplayLabel(
  kind: TransformCanvasNodeKind,
  t: CanvasNodeTranslate,
  handlerId?: string
): string {
  if (kind === "transform" && handlerId && isEtlTransformHandlerId(handlerId)) {
    return transformHandlerDisplayName(handlerId, t);
  }
  if (kind === "build_index" && handlerId && isBuildIndexHandlerId(handlerId)) {
    return buildIndexHandlerDisplayName(handlerId, t);
  }
  return canvasNodeKindLabel(kind, t);
}

export function predecessorFlowNodeDisplayLabel(node: Node, t: CanvasNodeTranslate): string {
  const data = (node.data ?? {}) as { label?: string; notes?: string; config?: unknown; kind?: string };
  const kind = (data.kind as TransformCanvasNodeKind | undefined) ?? rfTypeToKind(node.type);
  return canvasNodeDisplayLabel(data, kind, t);
}

/** Label for a node wired from a single data predecessor (palette on edge, connect-end). */
export function composeWiredEtlNodeLabel(
  kind: TransformCanvasNodeKind,
  predecessorNode: Node,
  t: CanvasNodeTranslate,
  handlerId?: string
): string {
  const typeLabel = etlStageTypeDisplayLabel(kind, t, handlerId).trim();
  const predLabel = predecessorFlowNodeDisplayLabel(predecessorNode, t).trim();
  if (!typeLabel) return predLabel;
  if (!predLabel) return typeLabel;
  return `${typeLabel} · ${predLabel}`;
}

function readHandlerIdFromNodeData(data: Record<string, unknown>): string | undefined {
  const cfg = data.config;
  if (!cfg || typeof cfg !== "object" || Array.isArray(cfg)) return undefined;
  const hid = String((cfg as Record<string, unknown>).handler_id ?? "").trim();
  return hid || undefined;
}

/** Set `data.label` (and config description when present) from type + predecessor; skips if label already set. */
export function applyWiredCreationLabel(
  node: Node,
  predecessorNode: Node | undefined | null,
  t: CanvasNodeTranslate
): Node {
  if (!predecessorNode) return node;
  const data = (node.data ?? {}) as Record<string, unknown>;
  if (String(data.label ?? "").trim()) return node;

  const kind = (data.kind as TransformCanvasNodeKind | undefined) ?? rfTypeToKind(node.type);
  if (kind === "start" || kind === "end") return node;

  const label = composeWiredEtlNodeLabel(kind, predecessorNode, t, readHandlerIdFromNodeData(data));
  if (!label) return node;

  const config =
    data.config && typeof data.config === "object" && !Array.isArray(data.config)
      ? { ...(data.config as Record<string, unknown>) }
      : undefined;
  if (config) {
    config.description = label;
  }

  return {
    ...node,
    data: {
      ...data,
      label,
      ...(config ? { config } : {}),
    },
  };
}
