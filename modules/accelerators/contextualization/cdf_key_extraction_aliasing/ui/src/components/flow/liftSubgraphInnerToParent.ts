import type { Edge, Node } from "@xyflow/react";
import {
  isSubflowGraphHubRfType,
  parsePortIdFromSubflowSourceHandle,
  parsePortIdFromSubflowTargetHandle,
  type WorkflowCanvasDocument,
  type WorkflowCanvasHandleOrientation,
  type WorkflowCanvasNodeData,
} from "../../types/workflowCanvas";
import { absoluteNodePosition } from "./flowParentGeometry";
import {
  canvasToFlowEdges,
  canvasToFlowNodes,
  keaFlowEdgeVisualDefaults,
  newNodeId,
  orderFlowNodesForReactFlow,
  type FlowEdgeData,
} from "./flowDocumentBridge";
import { assignFlowNodeSubflowParent } from "./subflowDropAssociation";
import { dedupeEdgesByHandles } from "./flowEdgeHelpers";

/**
 * Padding between subgraph frame origin and inner node coordinates — must match
 * ``collapseSelectionToSubgraph`` so lifted nodes land where they were before collapse.
 */
export const SUBGRAPH_LIFT_ORIGIN_PAD = 20;

export function resolveHubIds(innerRf: readonly Node[], data: WorkflowCanvasNodeData): { hubInId: string; hubOutId: string } {
  let hubIn = String(data.subflow_hub_input_id ?? "").trim();
  let hubOut = String(data.subflow_hub_output_id ?? "").trim();
  if (!hubIn) hubIn = innerRf.find((n) => n.type === "keaSubflowGraphIn")?.id ?? "";
  if (!hubOut) hubOut = innerRf.find((n) => n.type === "keaSubflowGraphOut")?.id ?? "";
  return { hubInId: hubIn, hubOutId: hubOut };
}

export function subgraphHasLiftableInnerContent(nodes: Node[], subgraphId: string): boolean {
  const S = nodes.find((n) => n.id === subgraphId && n.type === "keaSubgraph");
  if (!S) return false;
  const data = (S.data ?? {}) as WorkflowCanvasNodeData;
  const innerDoc = data.inner_canvas;
  if (!innerDoc?.nodes?.length) return false;
  const innerRf = canvasToFlowNodes(innerDoc.nodes);
  const { hubInId, hubOutId } = resolveHubIds(innerRf, data);
  return innerRf.some(
    (n) => n.id !== hubInId && n.id !== hubOutId && !isSubflowGraphHubRfType(n.type)
  );
}

/**
 * Remove a ``keaSubgraph`` node and place its inner workflow (except graph in/out hubs) on the
 * parent canvas, rewiring outer edges through the former boundary ports. Returns ``null`` if the
 * subgraph is missing, empty, or inner node ids collide with outer ids.
 */
export function liftSubgraphInnerToParentWorkflow(
  nodes: Node[],
  edges: Edge[],
  subgraphId: string,
  _handleOrientation: WorkflowCanvasHandleOrientation
): { nodes: Node[]; edges: Edge[] } | null {
  const S = nodes.find((n) => n.id === subgraphId && n.type === "keaSubgraph");
  if (!S) return null;
  const data = (S.data ?? {}) as WorkflowCanvasNodeData;
  const innerDoc = data.inner_canvas as WorkflowCanvasDocument | undefined;
  if (!innerDoc?.nodes?.length) return null;

  const innerRfNodes = canvasToFlowNodes(innerDoc.nodes);
  const innerRfEdges = canvasToFlowEdges(innerDoc.edges ?? []);
  const { hubInId, hubOutId } = resolveHubIds(innerRfNodes, data);
  if (!hubInId || !hubOutId) return null;

  const outerIds = new Set(nodes.map((n) => n.id));
  outerIds.delete(subgraphId);

  const contentNodes = innerRfNodes.filter(
    (n) => n.id !== hubInId && n.id !== hubOutId && !isSubflowGraphHubRfType(n.type)
  );
  if (contentNodes.length < 1) return null;

  for (const n of contentNodes) {
    if (outerIds.has(n.id)) return null;
  }

  const subAbs = absoluteNodePosition(nodes, subgraphId);
  const parentId =
    S.parentId != null && String(S.parentId).trim() ? String(S.parentId).trim() : "";
  const parentNode = parentId ? nodes.find((n) => n.id === parentId) : undefined;

  const stripRfParent = (n: Node): Node => {
    const { parentId: _p, extent: _e, expandParent: _x, ...rest } = n as Node & {
      parentId?: string;
      extent?: "parent" | null;
      expandParent?: boolean;
    };
    const baseAbs = {
      x: subAbs.x + n.position.x + SUBGRAPH_LIFT_ORIGIN_PAD,
      y: subAbs.y + n.position.y + SUBGRAPH_LIFT_ORIGIN_PAD,
    };
    return { ...(rest as Node), position: baseAbs, selected: false };
  };

  let nextNodes = nodes.filter((n) => n.id !== subgraphId);
  for (const cn of contentNodes) {
    const ln = stripRfParent(cn);
    nextNodes = [...nextNodes, ln];
    nextNodes =
      parentId && parentNode?.type === "keaSubflow"
        ? assignFlowNodeSubflowParent(nextNodes, ln.id, parentId)
        : assignFlowNodeSubflowParent(nextNodes, ln.id, null);
  }

  const contentIdSet = new Set(contentNodes.map((n) => n.id));
  const nextEdgeList: Edge[] = [];

  for (const e of edges) {
    if (e.target === subgraphId) {
      const portIn = parsePortIdFromSubflowTargetHandle(e.targetHandle) ?? "in";
      for (const ie of innerRfEdges) {
        if (ie.source !== hubInId) continue;
        const p = parsePortIdFromSubflowSourceHandle(ie.sourceHandle);
        if (p !== portIn) continue;
        nextEdgeList.push({
          ...e,
          ...keaFlowEdgeVisualDefaults,
          id: newNodeId(),
          target: ie.target,
          targetHandle: ie.targetHandle ?? undefined,
          data: (e.data ?? { kind: "data" }) as FlowEdgeData,
        });
      }
      continue;
    }
    if (e.source === subgraphId) {
      const portOut = parsePortIdFromSubflowSourceHandle(e.sourceHandle) ?? "out";
      for (const ie of innerRfEdges) {
        if (ie.target !== hubOutId) continue;
        const p = parsePortIdFromSubflowTargetHandle(ie.targetHandle);
        if (p !== portOut) continue;
        nextEdgeList.push({
          ...e,
          ...keaFlowEdgeVisualDefaults,
          id: newNodeId(),
          source: ie.source,
          sourceHandle: ie.sourceHandle ?? undefined,
          data: (e.data ?? { kind: "data" }) as FlowEdgeData,
        });
      }
      continue;
    }
    if (e.source === subgraphId || e.target === subgraphId) continue;
    nextEdgeList.push(e);
  }

  for (const ie of innerRfEdges) {
    if (ie.source === hubInId || ie.target === hubOutId) continue;
    if (!contentIdSet.has(ie.source) || !contentIdSet.has(ie.target)) continue;
    nextEdgeList.push({ ...ie, ...keaFlowEdgeVisualDefaults });
  }

  const alive = new Set(nextNodes.map((n) => n.id));
  const filtered = nextEdgeList.filter((e) => alive.has(e.source) && alive.has(e.target));

  return {
    nodes: orderFlowNodesForReactFlow(nextNodes),
    edges: dedupeEdgesByHandles(filtered),
  };
}
