import dagre from "@dagrejs/dagre";
import type { Edge, Node } from "@xyflow/react";
import type { WorkflowGraph } from "../types/explorerNodes";

export const WF_TASK_NODE_WIDTH = 240;
export const WF_TASK_NODE_HEIGHT = 96;

const GRID_GAP_X = 48;
const GRID_GAP_Y = 40;

function gridPositions(count: number): { x: number; y: number }[] {
  const cols = Math.max(1, Math.ceil(Math.sqrt(count)));
  return Array.from({ length: count }, (_, i) => ({
    x: (i % cols) * (WF_TASK_NODE_WIDTH + GRID_GAP_X),
    y: Math.floor(i / cols) * (WF_TASK_NODE_HEIGHT + GRID_GAP_Y),
  }));
}

export function layoutWfTaskNodes(nodes: Node[], edges: Edge[]): Node[] {
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
    g.setNode(node.id, { width: WF_TASK_NODE_WIDTH, height: WF_TASK_NODE_HEIGHT });
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
        x: pos.x - WF_TASK_NODE_WIDTH / 2,
        y: pos.y - WF_TASK_NODE_HEIGHT / 2,
      },
    };
  });
}

export function workflowGraphToFlow(graph: WorkflowGraph): { nodes: Node[]; edges: Edge[] } {
  const tasks = graph.tasks;
  const taskIds = new Set(tasks.map((t) => t.id));

  const baseNodes: Node[] = tasks.map((t) => ({
    id: t.id,
    type: "wfTask",
    position: { x: 0, y: 0 },
    data: { task: t },
  }));

  const edges: Edge[] = graph.edges
    .filter((e) => taskIds.has(e.from) && taskIds.has(e.to))
    .map((e) => ({
      id: e.id,
      source: e.from,
      target: e.to,
      type: "smoothstep",
      animated: false,
      style: { stroke: "var(--exp-primary)" },
    }));

  const nodes = layoutWfTaskNodes(baseNodes, edges);
  return { nodes, edges };
}
