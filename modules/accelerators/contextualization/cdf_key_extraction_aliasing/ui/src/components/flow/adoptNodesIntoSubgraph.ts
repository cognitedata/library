import type { Edge, Node } from "@xyflow/react";
import {
  emptyWorkflowCanvasDocument,
  subflowSourceHandleForPort,
  subflowTargetHandleForPort,
  type SubflowPortsConfig,
  type WorkflowCanvasHandleOrientation,
  type WorkflowCanvasNodeData,
} from "../../types/workflowCanvas";
import { absoluteNodePosition } from "./flowParentGeometry";
import {
  canvasToFlowEdges,
  canvasToFlowNodes,
  flowToCanvasDocument,
  keaFlowEdgeVisualDefaults,
  orderFlowNodesForReactFlow,
  type FlowEdgeData,
} from "./flowDocumentBridge";
import {
  buildMergedInputPortsForMemberCrossings,
  buildMergedOutputPortsForMemberCrossings,
  memberInboundPortKey,
  memberOutboundPortKey,
} from "./subgraphExternalInputPorts";
import { absoluteNodeRect, type FlowRect } from "./flowNodeGeometry";
import { assignFlowNodeSubflowParent } from "./subflowDropAssociation";
import { isWrapGroupableNodeType } from "./subflowMembership";
import {
  ensureSubgraphInnerBoundaryCanvasDocument,
  repositionSubgraphBoundaryHubsInDoc,
} from "./subgraphInnerBoundaryHubs";

function parentDepth(nodes: Node[], id: string): number {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  let d = 0;
  let cur: string | undefined = id;
  const guard = new Set<string>();
  while (cur && !guard.has(cur)) {
    guard.add(cur);
    const p: string | undefined = byId.get(cur)?.parentId;
    if (!p || !String(p).trim()) break;
    d++;
    cur = p;
  }
  return d;
}

function rectCenter(r: FlowRect): { x: number; y: number } {
  return { x: r.x + r.w / 2, y: r.y + r.h / 2 };
}

function pointInRect(p: { x: number; y: number }, r: FlowRect): boolean {
  return p.x >= r.x && p.x <= r.x + r.w && p.y >= r.y && p.y <= r.y + r.h;
}

function rectArea(r: FlowRect): number {
  return Math.max(0, r.w) * Math.max(0, r.h);
}

function toInnerRfNode(nodes: Node[], n: Node, subOrigin: { x: number; y: number }): Node {
  const abs = absoluteNodePosition(nodes, n.id);
  const { parentId: _p, extent: _e, expandParent: _x, ...rest } = n as Node & {
    parentId?: string;
    extent?: "parent" | null;
    expandParent?: boolean;
  };
  return {
    ...(rest as Node),
    position: { x: abs.x - subOrigin.x, y: abs.y - subOrigin.y },
    selected: false,
  };
}

/**
 * Move ``memberIds`` from the outer canvas into ``subgraphId``'s ``inner_canvas``, adding
 * inner boundary hub edges so outer↔inner connectivity matches the previous crossing edges.
 */
export function adoptNodesIntoSubgraph(
  nodes: Node[],
  edges: Edge[],
  memberIds: Set<string>,
  subgraphId: string,
  handleOrientation: WorkflowCanvasHandleOrientation
): { nodes: Node[]; edges: Edge[] } | null {
  const G = nodes.find((n) => n.id === subgraphId && n.type === "keaSubgraph");
  if (!G || memberIds.has(subgraphId)) return null;

  const members = [...memberIds]
    .map((id) => nodes.find((n) => n.id === id))
    .filter((n): n is Node => Boolean(n));
  if (members.length < 1) return null;
  if (members.some((m) => !isWrapGroupableNodeType(m.type))) return null;

  let next: Node[] = [...nodes];
  const sortedDetach = [...members].sort((a, b) => parentDepth(next, b.id) - parentDepth(next, a.id));
  for (const m of sortedDetach) {
    const cur = next.find((x) => x.id === m.id);
    const pid = cur?.parentId != null && String(cur.parentId).trim() ? String(cur.parentId).trim() : "";
    if (pid && !memberIds.has(pid)) {
      next = assignFlowNodeSubflowParent(next, m.id, null);
    }
  }

  const subOrigin = absoluteNodePosition(next, subgraphId);
  const adoptedRf: Node[] = members.map((m) => {
    const cur = next.find((x) => x.id === m.id);
    if (!cur) return m;
    return toInnerRfNode(next, cur, subOrigin);
  });

  const gData = (G.data ?? {}) as WorkflowCanvasNodeData;
  const rawInner =
    gData.inner_canvas?.nodes && Array.isArray(gData.inner_canvas.nodes) && gData.inner_canvas.nodes.length > 0
      ? gData.inner_canvas
      : emptyWorkflowCanvasDocument();
  const baseFrame: SubflowPortsConfig =
    gData.subflow_ports?.inputs?.length || gData.subflow_ports?.outputs?.length
      ? (gData.subflow_ports as SubflowPortsConfig)
      : { inputs: [{ id: "in", label: "in" }], outputs: [{ id: "out", label: "out" }] };
  const { frame: frameAfterInputs, keyToPortId: inputKeyToPortId } =
    buildMergedInputPortsForMemberCrossings(next, edges, memberIds, baseFrame, subgraphId);
  const { frame, outputKeyToPortId } = buildMergedOutputPortsForMemberCrossings(
    next,
    edges,
    memberIds,
    frameAfterInputs,
    subgraphId
  );
  const hubInHint = String(gData.subflow_hub_input_id ?? "").trim();
  const hubOutHint = String(gData.subflow_hub_output_id ?? "").trim();

  const ensured = ensureSubgraphInnerBoundaryCanvasDocument(rawInner, frame, hubInHint, hubOutHint);
  const hubInId = ensured.hubInId;
  const hubOutId = ensured.hubOutId;

  const innerRfFromDoc = canvasToFlowNodes(ensured.doc.nodes);
  const innerEfFromDoc = canvasToFlowEdges(ensured.doc.edges);
  const innerEdges: Edge[] = [...innerEfFromDoc];

  for (const e of edges) {
    const srcIn = memberIds.has(e.source);
    const tgtIn = memberIds.has(e.target);
    if (srcIn && tgtIn) {
      innerEdges.push({ ...e, ...keaFlowEdgeVisualDefaults, selected: false });
      continue;
    }
    if (!srcIn && tgtIn) {
      if (e.source === subgraphId) continue;
      const inPort = inputKeyToPortId.get(memberInboundPortKey(e)) ?? "in";
      innerEdges.push({
        ...e,
        id: `e_${hubInId}_to_${e.target}_${e.id}`,
        source: hubInId,
        sourceHandle: subflowSourceHandleForPort(inPort),
        target: e.target,
        targetHandle: e.targetHandle ?? undefined,
        data: { kind: "data" } satisfies FlowEdgeData,
        ...keaFlowEdgeVisualDefaults,
        selected: false,
      });
    } else if (srcIn && !tgtIn) {
      if (e.target === subgraphId) continue;
      const outPort = outputKeyToPortId.get(memberOutboundPortKey(e)) ?? "out";
      innerEdges.push({
        ...e,
        id: `e_${e.source}_to_${hubOutId}_${e.id}`,
        source: e.source,
        sourceHandle: e.sourceHandle ?? undefined,
        target: hubOutId,
        targetHandle: subflowTargetHandleForPort(outPort),
        data: { kind: "data" } satisfies FlowEdgeData,
        ...keaFlowEdgeVisualDefaults,
        selected: false,
      });
    }
  }

  const innerRfAll = orderFlowNodesForReactFlow([...innerRfFromDoc, ...adoptedRf]);
  let innerDoc = flowToCanvasDocument(innerRfAll, innerEdges, { handleOrientation });
  innerDoc = repositionSubgraphBoundaryHubsInDoc(innerDoc, hubInId, hubOutId);

  const nextSubgraphData: WorkflowCanvasNodeData = {
    ...gData,
    subflow_ports: frame,
    inner_canvas: innerDoc,
    ...(ensured.mutatedBoundaryMeta
      ? { subflow_hub_input_id: hubInId, subflow_hub_output_id: hubOutId }
      : {}),
  };

  const withoutMembers = next.filter((x) => !memberIds.has(x.id));
  const outerNodes = withoutMembers.map((n) =>
    n.id === subgraphId
      ? { ...n, data: { ...((n.data ?? {}) as WorkflowCanvasNodeData), ...nextSubgraphData } as Record<string, unknown> }
      : n
  );
  const outerIds = new Set(outerNodes.map((n) => n.id));

  const nextEdges: Edge[] = [];
  for (const e of edges) {
    const srcIn = memberIds.has(e.source);
    const tgtIn = memberIds.has(e.target);
    if (srcIn && tgtIn) continue;
    if (!srcIn && !tgtIn) {
      if (outerIds.has(e.source) && outerIds.has(e.target)) nextEdges.push(e);
      continue;
    }
    if (srcIn && !tgtIn) {
      if (!outerIds.has(e.target)) continue;
      if (e.target === subgraphId) continue;
      const outPort = outputKeyToPortId.get(memberOutboundPortKey(e)) ?? "out";
      nextEdges.push({
        ...e,
        id: `e_${subgraphId}_out_${e.target}_${e.id}`,
        source: subgraphId,
        sourceHandle: subflowSourceHandleForPort(outPort),
      });
      continue;
    }
    if (!srcIn && tgtIn) {
      if (!outerIds.has(e.source)) continue;
      if (e.source === subgraphId) continue;
      const inPort = inputKeyToPortId.get(memberInboundPortKey(e)) ?? "in";
      nextEdges.push({
        ...e,
        id: `e_${e.source}_in_${subgraphId}_${e.id}`,
        target: subgraphId,
        targetHandle: subflowTargetHandleForPort(inPort),
      });
    }
  }

  return { nodes: orderFlowNodesForReactFlow(outerNodes), edges: nextEdges };
}

/**
 * After a drag, if all selected groupable nodes lie inside a ``keaSubgraph`` card (smallest
 * containing subgraph wins), move them into that subgraph's inner canvas and preserve crossing edges.
 */
export function resolveAdoptIntoSubgraphAfterDrag(
  nodes: Node[],
  edges: Edge[],
  primaryId: string,
  handleOrientation: WorkflowCanvasHandleOrientation
): { nodes: Node[]; edges: Edge[] } | null {
  const primary = nodes.find((n) => n.id === primaryId);
  if (!primary || !isWrapGroupableNodeType(primary.type)) return null;

  const center = rectCenter(absoluteNodeRect(nodes, primary));
  const subgraphCandidates = nodes
    .filter((n) => n.type === "keaSubgraph" && n.id !== primaryId)
    .map((n) => ({ n, rect: absoluteNodeRect(nodes, n) }))
    .filter(({ rect }) => pointInRect(center, rect))
    .sort((a, b) => rectArea(a.rect) - rectArea(b.rect));

  const G = subgraphCandidates[0]?.n;
  if (!G) return null;

  const gRect = absoluteNodeRect(nodes, G);
  const candidates = nodes.filter(
    (n) =>
      n.selected &&
      isWrapGroupableNodeType(n.type) &&
      n.id !== G.id &&
      pointInRect(rectCenter(absoluteNodeRect(nodes, n)), gRect)
  );
  if (!candidates.some((c) => c.id === primaryId)) return null;
  if (candidates.length < 1) return null;

  return adoptNodesIntoSubgraph(nodes, edges, new Set(candidates.map((c) => c.id)), G.id, handleOrientation);
}
