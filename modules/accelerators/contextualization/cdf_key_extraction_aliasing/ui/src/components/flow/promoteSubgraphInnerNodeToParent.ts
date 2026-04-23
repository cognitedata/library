import type { Edge, Node } from "@xyflow/react";
import {
  isSubflowGraphHubRfType,
  parsePortIdFromSubflowSourceHandle,
  parsePortIdFromSubflowTargetHandle,
  subflowSourceHandleForPort,
  subflowTargetHandleForPort,
  type WorkflowCanvasDocument,
  type WorkflowCanvasHandleOrientation,
  type WorkflowCanvasNodeData,
} from "../../types/workflowCanvas";
import { absoluteNodePosition, collectSubtreeNodeIds } from "./flowParentGeometry";
import {
  canvasToFlowEdges,
  canvasToFlowNodes,
  flowToCanvasDocument,
  keaFlowEdgeVisualDefaults,
  newNodeId,
  orderFlowNodesForReactFlow,
  type FlowEdgeData,
} from "./flowDocumentBridge";
import { assignFlowNodeSubflowParent } from "./subflowDropAssociation";
import { dedupeEdgesByHandles } from "./flowEdgeHelpers";
import { SUBGRAPH_LIFT_ORIGIN_PAD, resolveHubIds } from "./liftSubgraphInnerToParent";

export { SUBGRAPH_LIFT_ORIGIN_PAD };

function portFeedingPromotedNode(innerEdges: Edge[], hubInId: string, promotedNodeId: string): string {
  const direct = innerEdges.find((e) => e.source === hubInId && e.target === promotedNodeId);
  if (direct) return parsePortIdFromSubflowSourceHandle(direct.sourceHandle) ?? "in";
  return "in";
}

/** True if ``rootId`` (and its nested children) may be promoted off the inner canvas to the parent graph. */
export function canPromoteInnerSubtreeToOwningGraph(
  innerRf: readonly Node[],
  hubInId: string,
  hubOutId: string,
  rootId: string
): boolean {
  const root = innerRf.find((n) => n.id === rootId);
  if (!root) return false;
  if (root.type === "keaStart" || root.type === "keaEnd") return false;
  if (isSubflowGraphHubRfType(root.type)) return false;
  const promoted = collectSubtreeNodeIds([...innerRf], rootId);
  for (const id of promoted) {
    if (id === hubInId || id === hubOutId) return false;
    const t = innerRf.find((x) => x.id === id)?.type;
    if (t && isSubflowGraphHubRfType(t)) return false;
  }
  return promoted.size >= 1;
}

/**
 * Move an inner subgraph subtree (``rootInnerNodeId`` and its ``parentId`` descendants) to the
 * parent canvas, update ``inner_canvas`` on the subgraph node, and rewire outer↔subgraph edges
 * through boundary ports (same rules as full subgraph lift).
 */
export function promoteSubgraphInnerSubtreeToParentWorkflow(
  nodes: Node[],
  edges: Edge[],
  subgraphId: string,
  rootInnerNodeId: string,
  handleOrientation: WorkflowCanvasHandleOrientation
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

  if (!canPromoteInnerSubtreeToOwningGraph(innerRfNodes, hubInId, hubOutId, rootInnerNodeId)) {
    return null;
  }

  const promotedSet = collectSubtreeNodeIds([...innerRfNodes], rootInnerNodeId);
  const outerIds = new Set(nodes.map((n) => n.id));
  outerIds.delete(subgraphId);
  for (const id of promotedSet) {
    if (outerIds.has(id)) return null;
  }

  const remainingInnerRf = innerRfNodes.filter((n) => !promotedSet.has(n.id));
  const remainingInnerEdges = innerRfEdges.filter(
    (e) => !promotedSet.has(e.source) && !promotedSet.has(e.target)
  );

  const innerHo = normalizeInnerHandleOrientation(innerDoc, handleOrientation);
  const innerCrossEdges: Edge[] = [];

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

  const promotedRfList = innerRfNodes.filter((n) => promotedSet.has(n.id));

  const nextEdgeList: Edge[] = [];
  for (const e of edges) {
    if (e.target === subgraphId) {
      const portIn = parsePortIdFromSubflowTargetHandle(e.targetHandle) ?? "in";
      for (const ie of innerRfEdges) {
        if (ie.source !== hubInId) continue;
        if (!promotedSet.has(ie.target)) continue;
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
        if (!promotedSet.has(ie.source)) continue;
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

  // Cross edges between promoted nodes (now on parent graph) and nodes still inside the subgraph:
  // reconnect through the subgraph frame (in__ / out__ ports) like normal parent↔subgraph wiring.
  for (const ie of innerRfEdges) {
    if (ie.source === hubInId || ie.source === hubOutId || ie.target === hubInId || ie.target === hubOutId) {
      continue;
    }
    const srcPromoted = promotedSet.has(ie.source);
    const tgtPromoted = promotedSet.has(ie.target);
    if (srcPromoted === tgtPromoted) continue;

    if (srcPromoted && !tgtPromoted) {
      const port = portFeedingPromotedNode(innerRfEdges, hubInId, ie.source);
      nextEdgeList.push({
        ...keaFlowEdgeVisualDefaults,
        id: newNodeId(),
        source: ie.source,
        target: subgraphId,
        sourceHandle: ie.sourceHandle ?? undefined,
        targetHandle: subflowTargetHandleForPort(port),
        data: (ie.data ?? { kind: "data" }) as FlowEdgeData,
      });
      innerCrossEdges.push({
        ...keaFlowEdgeVisualDefaults,
        id: newNodeId(),
        source: hubInId,
        target: ie.target,
        sourceHandle: subflowSourceHandleForPort(port),
        targetHandle: ie.targetHandle ?? undefined,
        data: (ie.data ?? { kind: "data" }) as FlowEdgeData,
      });
      continue;
    }

    // inner -> promoted (ie.target is on the parent graph after promotion)
    const outs = innerRfEdges.filter((e) => e.source === ie.target && e.target === hubOutId);
    for (const outE of outs) {
      const pOut = parsePortIdFromSubflowTargetHandle(outE.targetHandle) ?? "out";
      innerCrossEdges.push({
        ...keaFlowEdgeVisualDefaults,
        id: newNodeId(),
        source: ie.source,
        target: hubOutId,
        sourceHandle: ie.sourceHandle ?? undefined,
        targetHandle: outE.targetHandle ?? undefined,
        data: (outE.data ?? ie.data ?? { kind: "data" }) as FlowEdgeData,
      });
      nextEdgeList.push({
        ...keaFlowEdgeVisualDefaults,
        id: newNodeId(),
        source: subgraphId,
        target: ie.target,
        sourceHandle: subflowSourceHandleForPort(pOut),
        targetHandle: ie.targetHandle ?? undefined,
        data: (ie.data ?? { kind: "data" }) as FlowEdgeData,
      });
    }
  }

  for (const ie of innerRfEdges) {
    if (ie.source === hubInId || ie.target === hubOutId) continue;
    if (!promotedSet.has(ie.source) || !promotedSet.has(ie.target)) continue;
    nextEdgeList.push({ ...ie, ...keaFlowEdgeVisualDefaults });
  }

  const innerEdgesMerged = dedupeEdgesByHandles([...remainingInnerEdges, ...innerCrossEdges]);
  const innerCanvasNext: WorkflowCanvasDocument = flowToCanvasDocument(remainingInnerRf, innerEdgesMerged, {
    handleOrientation: innerHo,
  });

  let nextNodes = nodes.map((n) => {
    if (n.id !== subgraphId || n.type !== "keaSubgraph") return n;
    const d = (n.data ?? {}) as WorkflowCanvasNodeData;
    return {
      ...n,
      data: {
        ...d,
        inner_canvas: innerCanvasNext,
      } as Record<string, unknown>,
    };
  });

  for (const cn of promotedRfList) {
    const ln = stripRfParent(cn);
    nextNodes = [...nextNodes, ln];
    nextNodes =
      parentId && parentNode?.type === "keaSubflow"
        ? assignFlowNodeSubflowParent(nextNodes, ln.id, parentId)
        : assignFlowNodeSubflowParent(nextNodes, ln.id, null);
  }

  const alive = new Set(nextNodes.map((n) => n.id));
  const filtered = nextEdgeList.filter((e) => alive.has(e.source) && alive.has(e.target));

  return {
    nodes: orderFlowNodesForReactFlow(nextNodes),
    edges: dedupeEdgesByHandles(filtered),
  };
}

function normalizeInnerHandleOrientation(
  innerDoc: WorkflowCanvasDocument,
  fallback: WorkflowCanvasHandleOrientation
): WorkflowCanvasHandleOrientation {
  const raw = innerDoc.handle_orientation;
  return raw === "tb" || raw === "lr" ? raw : fallback;
}
