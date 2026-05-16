import type { Edge, Node } from "@xyflow/react";
import type { WorkflowCanvasHandleOrientation, WorkflowCanvasNodeData } from "../../types/workflowCanvas";
import {
  subflowSourceHandleForPort,
  subflowTargetHandleForPort,
} from "../../types/workflowCanvas";
import {
  flowToCanvasDocument,
  keaFlowEdgeVisualDefaults,
  newNodeId,
  orderFlowNodesForReactFlow,
  type FlowEdgeData,
} from "./flowDocumentBridge";
import { absoluteNodeRect, nodeFlowSize } from "./flowNodeGeometry";
import {
  DEFAULT_SUBGRAPH_FRAME_PORTS,
  HUB_GAP,
  HUB_LANE_W,
  subgraphFramePortsForInnerHubs,
  SUBGRAPH_INNER_HEADER,
} from "./subgraphInnerBoundaryHubs";
import {
  buildMergedInputPortsForMemberCrossings,
  buildMergedOutputPortsForMemberCrossings,
  memberInboundPortKey,
  memberOutboundPortKey,
} from "./subgraphExternalInputPorts";
import { assignFlowNodeSubflowParent } from "./subflowDropAssociation";
import { isWrapGroupableNodeType } from "./subflowMembership";

const PAD = 20;

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

/**
 * Replace the current multi-selection with one ``keaSubgraph`` node whose ``inner_canvas`` holds
 * the selected nodes (flattened to root coordinates) and internal edges. Cross-boundary edges
 * are rewired to default ``in`` / ``out`` ports.
 */
export function collapseSelectionToSubgraph(
  nodes: Node[],
  edges: Edge[],
  selected: Node[],
  handleOrientation: WorkflowCanvasHandleOrientation
): { nodes: Node[]; edges: Edge[] } | null {
  const groupable = selected.filter((n) => isWrapGroupableNodeType(n.type));
  if (groupable.length < 1) return null;

  const selectedIds = new Set(groupable.map((n) => n.id));
  let next: Node[] = [...nodes];

  const sortedDetach = [...groupable].sort((a, b) => parentDepth(nodes, b.id) - parentDepth(nodes, a.id));
  for (const n of sortedDetach) {
    const cur = next.find((x) => x.id === n.id);
    const pid = cur?.parentId != null && String(cur.parentId).trim() ? String(cur.parentId).trim() : "";
    if (pid && !selectedIds.has(pid)) {
      next = assignFlowNodeSubflowParent(next, n.id, null);
    }
  }

  const fresh = (id: string) => next.find((x) => x.id === id);
  const members = groupable.map((g) => fresh(g.id)).filter((x): x is Node => Boolean(x));
  if (members.length < 1) return null;

  let minX = Infinity;
  let minY = Infinity;
  for (const m of members) {
    const r = absoluteNodeRect(next, m);
    minX = Math.min(minX, r.x);
    minY = Math.min(minY, r.y);
  }
  if (!Number.isFinite(minX) || !Number.isFinite(minY)) return null;

  const subId = newNodeId();
  const subAbsX = minX - PAD;
  const subAbsY = minY - PAD;

  const innerRfNodes: Node[] = members.map((m) => {
    const r = absoluteNodeRect(next, m);
    const { parentId: _p, extent: _e, expandParent: _x, ...rest } = m as Node & {
      parentId?: string;
      extent?: "parent" | null;
      expandParent?: boolean;
    };
    return {
      ...(rest as Node),
      position: { x: r.x - minX, y: r.y - minY },
    };
  });

  const innerEdges: Edge[] = edges
    .filter((e) => selectedIds.has(e.source) && selectedIds.has(e.target))
    .map((e) => ({ ...e, ...keaFlowEdgeVisualDefaults }));

  const { frame: afterIn, keyToPortId: inputKeyToPortId } = buildMergedInputPortsForMemberCrossings(
    nodes,
    edges,
    selectedIds,
    DEFAULT_SUBGRAPH_FRAME_PORTS
  );
  const { frame: collapseFrame, outputKeyToPortId } = buildMergedOutputPortsForMemberCrossings(
    nodes,
    edges,
    selectedIds,
    afterIn
  );

  const hubInId = `${subId}_hub_in`;
  const hubOutId = `${subId}_hub_out`;
  const { hubInData, hubOutData } = subgraphFramePortsForInnerHubs(collapseFrame);

  let maxMemberX = 0;
  let minMemberY = Infinity;
  for (const m of innerRfNodes) {
    const sz = nodeFlowSize(m);
    maxMemberX = Math.max(maxMemberX, m.position.x + sz.w);
    minMemberY = Math.min(minMemberY, m.position.y);
  }
  if (!Number.isFinite(minMemberY)) minMemberY = 0;

  const hubInNode: Node = {
    id: hubInId,
    type: "keaSubflowGraphIn",
    position: { x: 0 - HUB_LANE_W - HUB_GAP, y: minMemberY + SUBGRAPH_INNER_HEADER },
    data: { ...hubInData } as Record<string, unknown>,
  };
  const hubOutNode: Node = {
    id: hubOutId,
    type: "keaSubflowGraphOut",
    position: { x: maxMemberX + HUB_GAP, y: minMemberY + SUBGRAPH_INNER_HEADER },
    data: { ...hubOutData } as Record<string, unknown>,
  };

  const innerBridgeEdges: Edge[] = [];
  for (const e of edges) {
    const srcIn = selectedIds.has(e.source);
    const tgtIn = selectedIds.has(e.target);
    if (srcIn && tgtIn) continue;
    if (!srcIn && tgtIn) {
      const inPort = inputKeyToPortId.get(memberInboundPortKey(e)) ?? "in";
      innerBridgeEdges.push({
        ...e,
        id: `e_${hubInId}_to_${e.target}_${e.id}`,
        source: hubInId,
        sourceHandle: subflowSourceHandleForPort(inPort),
        target: e.target,
        targetHandle: e.targetHandle ?? undefined,
        data: { kind: "data" } satisfies FlowEdgeData,
        ...keaFlowEdgeVisualDefaults,
      });
    } else if (srcIn && !tgtIn) {
      const outPort = outputKeyToPortId.get(memberOutboundPortKey(e)) ?? "out";
      innerBridgeEdges.push({
        ...e,
        id: `e_${e.source}_to_${hubOutId}_${e.id}`,
        source: e.source,
        sourceHandle: e.sourceHandle ?? undefined,
        target: hubOutId,
        targetHandle: subflowTargetHandleForPort(outPort),
        data: { kind: "data" } satisfies FlowEdgeData,
        ...keaFlowEdgeVisualDefaults,
      });
    }
  }

  const innerRfAll = orderFlowNodesForReactFlow([hubInNode, hubOutNode, ...innerRfNodes]);
  const innerDoc = flowToCanvasDocument(innerRfAll, [...innerEdges, ...innerBridgeEdges], { handleOrientation });

  const subgraphData: WorkflowCanvasNodeData = {
    label: "Subgraph",
    subflow_ports: collapseFrame,
    subflow_hub_input_id: hubInId,
    subflow_hub_output_id: hubOutId,
    inner_canvas: innerDoc,
  };

  const subgraphNode: Node = {
    id: subId,
    type: "keaSubgraph",
    position: { x: subAbsX, y: subAbsY },
    data: { ...subgraphData } as Record<string, unknown>,
  };

  const without = next.filter((x) => !selectedIds.has(x.id));
  const outerNodes = [...without, subgraphNode];
  const outerIds = new Set(outerNodes.map((n) => n.id));

  const nextEdges: Edge[] = [];
  for (const e of edges) {
    const srcIn = selectedIds.has(e.source);
    const tgtIn = selectedIds.has(e.target);
    if (srcIn && tgtIn) continue;
    if (!srcIn && !tgtIn) {
      if (outerIds.has(e.source) && outerIds.has(e.target)) nextEdges.push(e);
      continue;
    }
    if (srcIn && !tgtIn) {
      if (!outerIds.has(e.target)) continue;
      const outPort = outputKeyToPortId.get(memberOutboundPortKey(e)) ?? "out";
      nextEdges.push({
        ...e,
        id: `e_${subId}_out_${e.target}_${e.id}`,
        source: subId,
        sourceHandle: subflowSourceHandleForPort(outPort),
      });
      continue;
    }
    if (!srcIn && tgtIn) {
      if (!outerIds.has(e.source)) continue;
      const inPort = inputKeyToPortId.get(memberInboundPortKey(e)) ?? "in";
      nextEdges.push({
        ...e,
        id: `e_${e.source}_in_${subId}_${e.id}`,
        target: subId,
        targetHandle: subflowTargetHandleForPort(inPort),
      });
    }
  }

  return { nodes: orderFlowNodesForReactFlow(outerNodes), edges: nextEdges };
}
