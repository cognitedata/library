import type { Connection, Edge, Node } from "@xyflow/react";
import type { TransformCanvasNodeKind } from "../../types/transformCanvas";
import { rfTypeToKind } from "../../types/transformCanvas";
import type { TreeNode } from "../../types/discoveryNodes";
import {
  entityDropStages,
  seedConfigForEntityDrop,
} from "../../utils/dataTreeEntityDrop";
import { defaultJsonMappingNodeConfig } from "../../utils/jsonMappingNodeConfigModel";
import { workflowOutputRef } from "../../utils/canvasPredecessorTasks";
import { isBuildIndexHandlerId } from "./etlBuildIndexHandlerRegistry";
import {
  defaultBuildIndexNodeConfig,
} from "../../utils/buildIndexHandlerTemplates";
import {
  defaultTransformNodeConfig,
  isEtlTransformHandlerId,
} from "../../utils/etlTransformHandlerTemplates";
import { isValidHandlerForStage } from "./handlerDropMenuOptions";
import type { FlowEdgeData } from "./flowDocumentBridge";
import { cdfResourceDropStage } from "../../utils/cdfResourceDrop";
import { getTransformFlowDropPayload } from "./transformFlowDrag";
import {
  appendEtlConnectionEdge,
  dedupeEdgesByHandles,
  persistenceOutboundEdgesToEnd,
} from "./transformFlowEdgeHelpers";
import { nextEtlNodeId, rfTypeForKind } from "./flowNodeRegistry";
import { readFlowNodeSize, withEtlNodeDimensions } from "./etlFlowNodeSizing";

/** Horizontal gap between query and save nodes when dropping a wired pair. */
const ENTITY_DROP_PAIR_NODE_GAP = 48;

type GetNode = (id: string) => Node | undefined;

const HIT_THRESHOLD_BASE = 22;
const STRUCTURAL_RF_TYPES = new Set(["etlStart", "etlEnd"]);

function nodeCenter(n: Node): { x: number; y: number } {
  const w = n.measured?.width ?? n.width ?? 160;
  const h = n.measured?.height ?? n.height ?? 48;
  return { x: n.position.x + w / 2, y: n.position.y + h / 2 };
}

function distancePointToSegment(
  p: { x: number; y: number },
  a: { x: number; y: number },
  b: { x: number; y: number }
): number {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const lenSq = dx * dx + dy * dy;
  if (lenSq < 1e-6) {
    return Math.hypot(p.x - a.x, p.y - a.y);
  }
  const t = Math.max(0, Math.min(1, ((p.x - a.x) * dx + (p.y - a.y) * dy) / lenSq));
  const px = a.x + t * dx;
  const py = a.y + t * dy;
  return Math.hypot(p.x - px, p.y - py);
}

/** Primary data edges that may accept an inserted palette node. */
export function isSplittableDataEdge(edge: Edge): boolean {
  const kind = (edge.data as FlowEdgeData | undefined)?.kind ?? "data";
  return kind === "data";
}

export function findEdgeAtFlowPoint(
  flowPoint: { x: number; y: number },
  edges: Edge[],
  getNode: GetNode,
  zoom = 1
): Edge | null {
  const threshold = HIT_THRESHOLD_BASE / Math.max(zoom, 0.25);
  let best: { edge: Edge; dist: number } | null = null;
  for (const edge of edges) {
    if (!isSplittableDataEdge(edge)) continue;
    const src = getNode(edge.source);
    const tgt = getNode(edge.target);
    if (!src || !tgt) continue;
    const dist = distancePointToSegment(flowPoint, nodeCenter(src), nodeCenter(tgt));
    if (dist > threshold) continue;
    if (!best || dist < best.dist) best = { edge, dist };
  }
  return best?.edge ?? null;
}

function getNodeWithSyntheticTypes(getNode: GetNode, synthetic: Map<string, string>): GetNode {
  return (id: string) => {
    const rf = synthetic.get(id);
    if (rf) {
      return { id, type: rf, position: { x: 0, y: 0 }, data: {} } as Node;
    }
    return getNode(id);
  };
}

export function canInsertNodesOnEdge(
  edge: Edge,
  segments: Array<{ nodeId: string; rfType: string }>,
  getNode: GetNode
): boolean {
  if (!isSplittableDataEdge(edge) || segments.length === 0) return false;

  for (const seg of segments) {
    if (STRUCTURAL_RF_TYPES.has(seg.rfType)) return false;
  }

  const synthetic = new Map(segments.map((s) => [s.nodeId, s.rfType]));
  const resolve = getNodeWithSyntheticTypes(getNode, synthetic);

  const src = resolve(edge.source);
  const tgt = resolve(edge.target);
  if (src?.type === "etlEnd" || tgt?.type === "etlStart") return false;

  let prevSource = edge.source;
  for (const seg of segments) {
    if (!resolve(prevSource) || !resolve(seg.nodeId)) return false;
    prevSource = seg.nodeId;
  }
  return Boolean(resolve(prevSource) && resolve(edge.target));
}

export function replaceEdgeWithInsertedChain(
  getNode: GetNode,
  edges: Edge[],
  edge: Edge,
  chainNodeIds: string[]
): Edge[] {
  if (chainNodeIds.length === 0) return edges;
  const without = edges.filter((e) => e.id !== edge.id);
  const ids = [edge.source, ...chainNodeIds, edge.target];
  let merged = without;
  for (let i = 0; i < ids.length - 1; i++) {
    const conn: Connection = {
      source: ids[i]!,
      sourceHandle: i === 0 ? edge.sourceHandle ?? "out" : "out",
      target: ids[i + 1]!,
      targetHandle: i === ids.length - 2 ? edge.targetHandle ?? "in" : "in",
    };
    merged = appendEtlConnectionEdge(getNode, merged, conn);
  }
  return dedupeEdgesByHandles(merged);
}

type MaterializedDrop = {
  node: Node;
  rfType: string;
};

function materializeDropNode(
  payload: NonNullable<ReturnType<typeof getTransformFlowDropPayload>>,
  flowPosition: { x: number; y: number },
  existingIds: Set<string>
): MaterializedDrop | null {
  let stage: TransformCanvasNodeKind;
  let label: string;

  if (payload.kind === "etl_stage") {
    stage = payload.stage;
    label = "";
    const handlerId = String(payload.handlerId ?? "").trim();
    const id = nextEtlNodeId(stage, existingIds);
    const rfType = rfTypeForKind(stage);
    let config: Record<string, unknown> | undefined;
    if (handlerId && isValidHandlerForStage(stage, handlerId)) {
      if (stage === "transform" && isEtlTransformHandlerId(handlerId)) {
        config = defaultTransformNodeConfig(handlerId);
      } else if (stage === "build_index" && isBuildIndexHandlerId(handlerId)) {
        config = defaultBuildIndexNodeConfig(handlerId);
      }
    } else if (stage === "build_index") {
      config = defaultBuildIndexNodeConfig();
    } else if (stage !== "start" && stage !== "end") {
      config = { description: label };
    }
    const node: Node = withEtlNodeDimensions(
      {
        id,
        type: rfType,
        position: flowPosition,
        data: {
          kind: stage,
          label,
          ...(config ? { config } : {}),
          ...(handlerId ? { palette_handler_locked: true } : {}),
        },
      },
      stage
    );
    return { node, rfType };
  } else if (
    payload.kind === "cdf_function" ||
    payload.kind === "cdf_transformation" ||
    payload.kind === "cdf_workflow"
  ) {
    const drop = cdfResourceDropStage(payload);
    stage = drop.stage;
    label = drop.label;
    const id = nextEtlNodeId(stage, existingIds);
    const rfType = rfTypeForKind(stage);
    const node: Node = withEtlNodeDimensions(
      {
        id,
        type: rfType,
        position: flowPosition,
        data: {
          kind: stage,
          label,
          config: drop.config,
        },
      },
      stage
    );
    return { node, rfType };
  }
  return null;
}

export type MaterializedEtlStage = {
  node: Node;
  rfType: string;
};

/** Create a palette stage node at a flow position (connect-end menu, etc.). */
export function materializeEtlStageAtPosition(
  payload: NonNullable<ReturnType<typeof getTransformFlowDropPayload>>,
  flowPosition: { x: number; y: number },
  existingIds: Set<string>
): MaterializedEtlStage | null {
  return materializeDropNode(payload, flowPosition, existingIds);
}

export type ApplyTransformCanvasDropInput = {
  event: React.DragEvent;
  screenToFlowPosition: (pos: { x: number; y: number }) => { x: number; y: number };
  getNode: GetNode;
  getEdges: () => Edge[];
  zoom?: number;
  nodes: Node[];
};

export type ApplyTransformCanvasDropResult = {
  nodes: Node[];
  edges: Edge[];
  selectNodeId: string;
};

function seedJsonMappingNodeIfOnEdge(node: Node, edge: Edge | null): Node {
  const data = (node.data ?? {}) as Record<string, unknown>;
  const kind = (data.kind as TransformCanvasNodeKind | undefined) ?? rfTypeToKind(node.type);
  if (kind !== "json_mapping" || !edge) return node;
  const sourceId = edge.source?.trim();
  if (!sourceId) return node;
  const base = defaultJsonMappingNodeConfig();
  const ref = workflowOutputRef(sourceId);
  return {
    ...node,
    data: {
      ...data,
      config: {
        ...base,
        input: { data: ref },
        expression: "input.data",
      },
    },
  };
}

function insertMaterializedNode(
  node: Node,
  rfType: string,
  flowPosition: { x: number; y: number },
  input: Pick<ApplyTransformCanvasDropInput, "getNode" | "getEdges" | "zoom" | "nodes">
): ApplyTransformCanvasDropResult {
  const { getNode, getEdges, zoom = 1, nodes } = input;
  const candidateEdge = findEdgeAtFlowPoint(flowPosition, getEdges(), getNode, zoom);
  const hitEdge =
    candidateEdge &&
    canInsertNodesOnEdge(candidateEdge, [{ nodeId: node.id, rfType }], getNode)
      ? candidateEdge
      : null;

  const seededNode = seedJsonMappingNodeIfOnEdge(node, hitEdge);
  const allNodesForEnd = [...nodes, seededNode];
  const toEnd = persistenceOutboundEdgesToEnd(rfType, seededNode.id, allNodesForEnd);

  if (hitEdge) {
    const nextEdges = dedupeEdgesByHandles([
      ...replaceEdgeWithInsertedChain(getNode, getEdges(), hitEdge, [seededNode.id]),
      ...toEnd,
    ]);
    return {
      nodes: allNodesForEnd,
      edges: nextEdges,
      selectNodeId: seededNode.id,
    };
  }

  return {
    nodes: allNodesForEnd,
    edges: dedupeEdgesByHandles([...getEdges(), ...toEnd]),
    selectNodeId: seededNode.id,
  };
}

export type ApplyEntityCanvasDropInput = {
  node: TreeNode;
  stage: TransformCanvasNodeKind;
  flowPosition: { x: number; y: number };
  getNode: GetNode;
  getEdges: () => Edge[];
  zoom?: number;
  nodes: Node[];
};

function materializeEntityDropNode(
  treeNode: TreeNode,
  stage: TransformCanvasNodeKind,
  position: { x: number; y: number },
  existingIds: Set<string>
): MaterializedDrop {
  const label = treeNode.label.trim();
  const config = seedConfigForEntityDrop(treeNode, stage);
  const id = nextEtlNodeId(stage, existingIds);
  existingIds.add(id);
  const rfType = rfTypeForKind(stage);
  const node: Node = withEtlNodeDimensions(
    {
      id,
      type: rfType,
      position,
      data: {
        kind: stage,
        label,
        config,
      },
    },
    stage
  );
  return { node, rfType };
}

/** Materialize a query or save node from a Data tree entity drop. */
export function applyEntityCanvasDrop(
  input: ApplyEntityCanvasDropInput
): ApplyTransformCanvasDropResult | null {
  const { node: treeNode, stage, flowPosition, nodes } = input;
  const stages = entityDropStages(treeNode);
  if (!stages || (stage !== stages.query && stage !== stages.save)) return null;

  const existingIds = new Set(nodes.map((n) => n.id));
  const { node, rfType } = materializeEntityDropNode(treeNode, stage, flowPosition, existingIds);
  return insertMaterializedNode(node, rfType, flowPosition, input);
}

/** Materialize a query → save pair from a Data tree entity drop, wired with a data edge. */
export function applyEntityCanvasDropPair(
  input: Omit<ApplyEntityCanvasDropInput, "stage">
): ApplyTransformCanvasDropResult | null {
  const { node: treeNode, flowPosition, getNode, getEdges, zoom = 1, nodes } = input;
  const stages = entityDropStages(treeNode);
  if (!stages) return null;

  const existingIds = new Set(nodes.map((n) => n.id));
  const queryMat = materializeEntityDropNode(treeNode, stages.query, flowPosition, existingIds);
  const querySize = readFlowNodeSize(queryMat.node, stages.query);
  const savePosition = {
    x: flowPosition.x + querySize.width + ENTITY_DROP_PAIR_NODE_GAP,
    y: flowPosition.y,
  };
  const saveMat = materializeEntityDropNode(treeNode, stages.save, savePosition, existingIds);

  const candidateEdge = findEdgeAtFlowPoint(flowPosition, getEdges(), getNode, zoom);
  const chainIds = [queryMat.node.id, saveMat.node.id];
  const hitEdge =
    candidateEdge &&
    canInsertNodesOnEdge(
      candidateEdge,
      [
        { nodeId: queryMat.node.id, rfType: queryMat.rfType },
        { nodeId: saveMat.node.id, rfType: saveMat.rfType },
      ],
      getNode
    )
      ? candidateEdge
      : null;

  const allNodes = [...nodes, queryMat.node, saveMat.node];
  let nextEdges = getEdges();
  if (hitEdge) {
    nextEdges = replaceEdgeWithInsertedChain(getNode, nextEdges, hitEdge, chainIds);
  } else {
    nextEdges = appendEtlConnectionEdge(getNode, nextEdges, {
      source: queryMat.node.id,
      sourceHandle: "out",
      target: saveMat.node.id,
      targetHandle: "in",
    });
  }
  const toEnd = persistenceOutboundEdgesToEnd(saveMat.rfType, saveMat.node.id, allNodes);
  return {
    nodes: allNodes,
    edges: dedupeEdgesByHandles([...nextEdges, ...toEnd]),
    selectNodeId: queryMat.node.id,
  };
}

/** Apply palette drop at a flow position; returns updated graph when handled. */
export function applyTransformCanvasDropAtPosition(
  flowPosition: { x: number; y: number },
  payload: NonNullable<ReturnType<typeof getTransformFlowDropPayload>>,
  input: Omit<ApplyTransformCanvasDropInput, "event" | "screenToFlowPosition">
): ApplyTransformCanvasDropResult | null {
  const { nodes } = input;
  const existingIds = new Set(nodes.map((n) => n.id));
  const materialized = materializeDropNode(payload, flowPosition, existingIds);
  if (!materialized) return null;

  const { node, rfType } = materialized;
  return insertMaterializedNode(node, rfType, flowPosition, input);
}

/** Apply palette drop; returns updated graph when handled. Data tree drops use applyEntityCanvasDrop after prompting. */
export function applyTransformCanvasDrop(
  input: ApplyTransformCanvasDropInput
): ApplyTransformCanvasDropResult | null {
  const { event, screenToFlowPosition } = input;
  const payload = getTransformFlowDropPayload(event);
  if (!payload || payload.kind !== "etl_stage") return null;

  const flowPosition = screenToFlowPosition({ x: event.clientX, y: event.clientY });
  return applyTransformCanvasDropAtPosition(flowPosition, payload, input);
}
