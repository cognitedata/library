import type { Edge, Node } from "@xyflow/react";
import type { JsonObject } from "../../types/jsonConfig";
import type { TransformCanvasHandleOrientation } from "../../types/transformCanvas";
import { rfTypeToKind } from "../../types/transformCanvas";
import {
  isMultiStepTransformConfig,
  materializeTransformSteps,
  parseTransformNodeConfig,
  serializeTransformNodeConfig,
} from "../../utils/etlTransformNodeConfigModel";
import { wireEdgesForExplodedNodes } from "./explodeFlowNodeEdges";
import { nextEtlNodeId } from "./flowNodeRegistry";
import { withEtlNodeDimensions } from "./etlFlowNodeSizing";

const NODE_SPACING_LR = 220;
const NODE_SPACING_TB = 120;

function readTransformNodeConfig(node: Node): JsonObject | null {
  const data = (node.data ?? {}) as Record<string, unknown>;
  const cfg = data.config;
  if (!cfg || typeof cfg !== "object" || Array.isArray(cfg)) return null;
  return cfg as JsonObject;
}

export function isExplodableMultiStepTransformFlowNode(node: Node): boolean {
  if (rfTypeToKind(node.type) !== "transform") return false;
  const cfg = readTransformNodeConfig(node);
  if (!cfg || !isMultiStepTransformConfig(cfg)) return false;
  return materializeTransformSteps(cfg).length >= 2;
}

function stepNodeConfig(step: JsonObject): JsonObject {
  return serializeTransformNodeConfig({
    description: String(step.description ?? "").trim(),
    executionMode: "ordered",
    steps: [{ ...step }],
    fieldPolicies: undefined,
    multiStep: false,
    extras: {},
  });
}

function stepLabel(step: JsonObject, index: number, parentLabel: string): string {
  const desc = String(step.description ?? "").trim();
  if (desc) return desc;
  const parent = parentLabel.trim();
  if (parent) return `${parent} (${index + 1})`;
  return `transform (${index + 1})`;
}

function layoutPositions(
  source: Node,
  count: number,
  orientation: TransformCanvasHandleOrientation
): { x: number; y: number }[] {
  const out: { x: number; y: number }[] = [];
  for (let i = 0; i < count; i++) {
    if (orientation === "tb") {
      out.push({
        x: source.position.x,
        y: source.position.y + i * NODE_SPACING_TB,
      });
    } else {
      out.push({
        x: source.position.x + i * NODE_SPACING_LR,
        y: source.position.y,
      });
    }
  }
  return out;
}

export type ExplodeMultiStepTransformResult = {
  nodes: Node[];
  edges: Edge[];
  newNodeIds: string[];
};

/**
 * Replace one multi-step transform node with one canvas node per step, preserving external wiring.
 */
export function explodeMultiStepTransformFlowNode(
  nodes: Node[],
  edges: Edge[],
  sourceNodeId: string,
  opts?: { handleOrientation?: TransformCanvasHandleOrientation }
): ExplodeMultiStepTransformResult | null {
  const source = nodes.find((n) => n.id === sourceNodeId);
  if (!source || !isExplodableMultiStepTransformFlowNode(source)) return null;

  const cfg = readTransformNodeConfig(source)!;
  const parsed = parseTransformNodeConfig(cfg);
  const steps = parsed.steps;
  if (steps.length < 2) return null;

  const sourceData = (source.data ?? {}) as Record<string, unknown>;
  const parentLabel = String(sourceData.label ?? "").trim();
  const existingIds = new Set(nodes.map((n) => n.id));
  const positions = layoutPositions(source, steps.length, opts?.handleOrientation ?? "lr");

  const newNodes: Node[] = [];
  const newNodeIds: string[] = [];

  for (let i = 0; i < steps.length; i++) {
    const id = nextEtlNodeId("transform", existingIds);
    existingIds.add(id);
    newNodeIds.push(id);

    const step = steps[i]!;
    const nodeData: Record<string, unknown> = {
      kind: "transform",
      label: stepLabel(step, i, parentLabel),
      config: stepNodeConfig(step),
    };
    if (sourceData.notes != null) nodeData.notes = sourceData.notes;
    if (sourceData.node_color != null) nodeData.node_color = sourceData.node_color;
    if (sourceData.node_bg_color != null) nodeData.node_bg_color = sourceData.node_bg_color;
    if (sourceData.canvas_node_enabled === false) nodeData.canvas_node_enabled = false;

    const rfNode = withEtlNodeDimensions(
      {
        id,
        type: "etlTransform",
        position: positions[i]!,
        data: nodeData,
        parentId: source.parentId,
      },
      "transform"
    );
    newNodes.push(rfNode);
  }

  const withoutSource = nodes.filter((n) => n.id !== sourceNodeId);
  const allNodes = [...withoutSource, ...newNodes];
  const getNode = (id: string) => allNodes.find((n) => n.id === id);
  const wiring = parsed.executionMode === "parallel" ? "parallel" : "ordered";
  const nextEdges = wireEdgesForExplodedNodes(edges, sourceNodeId, newNodeIds, wiring, getNode);

  return { nodes: allNodes, edges: nextEdges, newNodeIds };
}
