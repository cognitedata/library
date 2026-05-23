import dagre from "@dagrejs/dagre";
import type { Edge, Node } from "@xyflow/react";
import type { DataModelGraph } from "../types/discoveryNodes";

/** Matches ``.disc-dm-flow-node`` rendered size for Dagre. */
export const DM_VIEW_NODE_WIDTH = 220;
export const DM_VIEW_NODE_HEIGHT = 88;

const GRID_GAP_X = 48;
const GRID_GAP_Y = 40;

function viewIdFromRef(ref: { space: string; external_id: string; version: string }): string {
  return `${ref.space}|${ref.external_id}|${ref.version}`;
}

function gridPositions(count: number): { x: number; y: number }[] {
  const cols = Math.max(1, Math.ceil(Math.sqrt(count)));
  return Array.from({ length: count }, (_, i) => ({
    x: (i % cols) * (DM_VIEW_NODE_WIDTH + GRID_GAP_X),
    y: Math.floor(i / cols) * (DM_VIEW_NODE_HEIGHT + GRID_GAP_Y),
  }));
}

/** Hierarchical left-to-right layout when the model has relation edges; grid otherwise. */
export function layoutDmViewNodes(nodes: Node[], edges: Edge[]): Node[] {
  if (nodes.length === 0) return nodes;

  const nodeIds = new Set(nodes.map((n) => n.id));
  const layoutEdges = edges.filter((e) => nodeIds.has(e.source) && nodeIds.has(e.target));
  if (layoutEdges.length === 0) {
    const positions = gridPositions(nodes.length);
    return nodes.map((node, i) => ({ ...node, position: positions[i] }));
  }

  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({
    rankdir: "LR",
    align: "UL",
    nodesep: 48,
    ranksep: 72,
    marginx: 32,
    marginy: 32,
  });

  for (const node of nodes) {
    g.setNode(node.id, { width: DM_VIEW_NODE_WIDTH, height: DM_VIEW_NODE_HEIGHT });
  }
  for (const edge of layoutEdges) {
    g.setEdge(edge.source, edge.target);
  }

  dagre.layout(g);

  return nodes.map((node) => {
    const pos = g.node(node.id);
    return {
      ...node,
      position: {
        x: pos.x - DM_VIEW_NODE_WIDTH / 2,
        y: pos.y - DM_VIEW_NODE_HEIGHT / 2,
      },
    };
  });
}

export function graphToFlow(graph: DataModelGraph): { nodes: Node[]; edges: Edge[] } {
  const views = graph.views;
  const nodeIds = new Set(views.map((v) => v.id));

  const baseNodes: Node[] = views.map((v) => ({
    id: v.id,
    type: "dmView",
    position: { x: 0, y: 0 },
    data: { view: v },
  }));

  const edges: Edge[] = graph.edges
    .filter((e) => {
      const src = viewIdFromRef(e.from);
      const tgt = viewIdFromRef(e.to);
      return nodeIds.has(src) && nodeIds.has(tgt);
    })
    .map((e) => ({
      id: e.id,
      source: viewIdFromRef(e.from),
      target: viewIdFromRef(e.to),
      type: "smoothstep",
      label: e.label,
      animated: false,
      style: { stroke: "var(--disc-primary)" },
      labelStyle: { fill: "var(--disc-text-muted)", fontSize: 10 },
    }));

  const nodes = layoutDmViewNodes(baseNodes, edges);
  return { nodes, edges };
}
