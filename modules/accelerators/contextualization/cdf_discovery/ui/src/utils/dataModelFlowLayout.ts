import dagre from "@dagrejs/dagre";
import type { Edge, Node } from "@xyflow/react";
import type { DataModelGraph } from "../types/discoveryNodes";
import {
  normalizeTransformCanvasEdgePathStyle,
  normalizeTransformCanvasHandleOrientation,
  type TransformCanvasEdgePathStyle,
  type TransformCanvasHandleOrientation,
} from "../types/transformCanvas";

/** Data model canvas auto-layout algorithms. */
export type DmFlowLayoutMethod = "force" | "dagre" | "grid";

export function normalizeDmFlowLayoutMethod(raw: unknown): DmFlowLayoutMethod {
  if (raw === "force") return "force";
  if (raw === "grid") return "grid";
  return "force";
}

export const DM_FLOW_LAYOUT_METHODS: DmFlowLayoutMethod[] = ["force", "dagre", "grid"];

export type DmFlowLayoutOptions = {
  handleOrientation?: TransformCanvasHandleOrientation;
  edgePathStyle?: TransformCanvasEdgePathStyle;
  layoutMethod?: DmFlowLayoutMethod;
};

/** Matches ``.disc-dm-flow-node`` rendered size for Dagre. */
export const DM_VIEW_NODE_WIDTH = 220;
export const DM_VIEW_NODE_HEIGHT = 88;

const GRID_GAP_X = 48;
const GRID_GAP_Y = 40;
const FORCE_ITERATIONS = 220;
const FORCE_EDGE_SPRING = 0.016;
const FORCE_REPULSION = 120000;
const FORCE_DAMPING = 0.78;
const FORCE_JITTER = 0.075;
const FORCE_MIN_DISTANCE = 18;
const FORCE_MAX_STEP = 24;
const FORCE_CLUSTER_RADIUS = 360;
const FORCE_COMPONENT_GAP = 160;

function deterministicJitter(seed: number): number {
  const x = Math.sin(seed * 12.9898) * 43758.5453;
  return x - Math.floor(x);
}

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

/** Square grid — useful when the model has few relation edges or views should be browsed in a catalog layout. */
export function layoutDmViewNodesGrid(nodes: Node[]): Node[] {
  if (nodes.length === 0) return nodes;
  const positions = gridPositions(nodes.length);
  return nodes.map((node, i) => ({ ...node, position: positions[i]! }));
}

/** Hierarchical Dagre layout along direct relation edges (view-to-view links in the data model). */
export function layoutDmViewNodesDagre(
  nodes: Node[],
  edges: Edge[],
  orientation: TransformCanvasHandleOrientation = "lr"
): Node[] {
  if (nodes.length === 0) return nodes;

  const nodeIds = new Set(nodes.map((n) => n.id));
  const layoutEdges = edges.filter((e) => nodeIds.has(e.source) && nodeIds.has(e.target));
  if (layoutEdges.length === 0) {
    return layoutDmViewNodesGrid(nodes);
  }

  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({
    rankdir: orientation === "tb" ? "TB" : "LR",
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

function connectedComponents(nodes: Node[], edges: Edge[]): string[][] {
  const ids = nodes.map((n) => n.id);
  const adjacency = new Map<string, Set<string>>();
  for (const id of ids) adjacency.set(id, new Set());
  for (const edge of edges) {
    if (!adjacency.has(edge.source) || !adjacency.has(edge.target)) continue;
    adjacency.get(edge.source)!.add(edge.target);
    adjacency.get(edge.target)!.add(edge.source);
  }
  const seen = new Set<string>();
  const components: string[][] = [];
  for (const id of ids) {
    if (seen.has(id)) continue;
    const stack = [id];
    const part: string[] = [];
    seen.add(id);
    while (stack.length > 0) {
      const curr = stack.pop()!;
      part.push(curr);
      for (const next of adjacency.get(curr) ?? []) {
        if (seen.has(next)) continue;
        seen.add(next);
        stack.push(next);
      }
    }
    components.push(part);
  }
  return components.sort((a, b) => b.length - a.length);
}

/** ForceAtlas-inspired layout suitable for knowledge-graph style relationships. */
export function layoutDmViewNodesForce(nodes: Node[], edges: Edge[]): Node[] {
  if (nodes.length === 0) return nodes;
  if (nodes.length === 1) return [{ ...nodes[0]!, position: { x: 0, y: 0 } }];

  const nodeMap = new Map(nodes.map((node) => [node.id, node] as const));
  const validEdges = edges.filter((edge) => nodeMap.has(edge.source) && nodeMap.has(edge.target));
  if (validEdges.length === 0) return layoutDmViewNodesGrid(nodes);

  const degrees = new Map<string, number>();
  for (const node of nodes) degrees.set(node.id, 0);
  for (const edge of validEdges) {
    degrees.set(edge.source, (degrees.get(edge.source) ?? 0) + 1);
    degrees.set(edge.target, (degrees.get(edge.target) ?? 0) + 1);
  }

  const components = connectedComponents(nodes, validEdges);
  const packed = new Map<string, { x: number; y: number }>();
  let componentOffsetX = 0;

  for (const componentIds of components) {
    const compNodes = componentIds.map((id) => nodeMap.get(id)!);
    const indexById = new Map(componentIds.map((id, idx) => [id, idx] as const));
    const links = validEdges
      .filter((edge) => indexById.has(edge.source) && indexById.has(edge.target))
      .map((edge) => ({ s: indexById.get(edge.source)!, t: indexById.get(edge.target)! }));

    const pos = compNodes.map((_, idx) => {
      const angle = (idx / compNodes.length) * Math.PI * 2;
      const radius = FORCE_CLUSTER_RADIUS * Math.sqrt(compNodes.length / Math.max(1, nodes.length));
      return { x: Math.cos(angle) * radius, y: Math.sin(angle) * radius };
    });
    const vel = compNodes.map(() => ({ x: 0, y: 0 }));

    for (let iter = 0; iter < FORCE_ITERATIONS; iter++) {
      const forces = compNodes.map(() => ({ x: 0, y: 0 }));
      for (let i = 0; i < compNodes.length; i++) {
        for (let j = i + 1; j < compNodes.length; j++) {
          const dx = pos[j]!.x - pos[i]!.x;
          const dy = pos[j]!.y - pos[i]!.y;
          const distSq = Math.max(FORCE_MIN_DISTANCE ** 2, dx * dx + dy * dy);
          const dist = Math.sqrt(distSq);
          const weightI = 1 + (degrees.get(compNodes[i]!.id) ?? 0);
          const weightJ = 1 + (degrees.get(compNodes[j]!.id) ?? 0);
          const repel = (FORCE_REPULSION * weightI * weightJ) / distSq;
          const fx = (dx / dist) * repel;
          const fy = (dy / dist) * repel;
          forces[i]!.x -= fx;
          forces[i]!.y -= fy;
          forces[j]!.x += fx;
          forces[j]!.y += fy;
        }
      }

      for (const link of links) {
        const a = link.s;
        const b = link.t;
        const dx = pos[b]!.x - pos[a]!.x;
        const dy = pos[b]!.y - pos[a]!.y;
        const dist = Math.max(FORCE_MIN_DISTANCE, Math.sqrt(dx * dx + dy * dy));
        const desired = DM_VIEW_NODE_WIDTH * 1.15;
        const pull = (dist - desired) * FORCE_EDGE_SPRING;
        const fx = (dx / dist) * pull;
        const fy = (dy / dist) * pull;
        forces[a]!.x += fx;
        forces[a]!.y += fy;
        forces[b]!.x -= fx;
        forces[b]!.y -= fy;
      }

      const cool = 1 - iter / FORCE_ITERATIONS;
      const jitter = FORCE_JITTER * cool;
      for (let i = 0; i < compNodes.length; i++) {
        vel[i]!.x = (vel[i]!.x + forces[i]!.x * 0.1) * FORCE_DAMPING;
        vel[i]!.y = (vel[i]!.y + forces[i]!.y * 0.1) * FORCE_DAMPING;
        vel[i]!.x += (deterministicJitter((iter + 1) * (i + 7)) - 0.5) * jitter;
        vel[i]!.y += (deterministicJitter((iter + 3) * (i + 11)) - 0.5) * jitter;
        const speed = Math.hypot(vel[i]!.x, vel[i]!.y);
        if (speed > FORCE_MAX_STEP) {
          const scale = FORCE_MAX_STEP / speed;
          vel[i]!.x *= scale;
          vel[i]!.y *= scale;
        }
        pos[i]!.x += vel[i]!.x;
        pos[i]!.y += vel[i]!.y;
      }
    }

    const minX = Math.min(...pos.map((p) => p.x));
    const maxX = Math.max(...pos.map((p) => p.x));
    const minY = Math.min(...pos.map((p) => p.y));
    const maxY = Math.max(...pos.map((p) => p.y));
    const centerX = (minX + maxX) / 2;
    const centerY = (minY + maxY) / 2;
    const width = maxX - minX + DM_VIEW_NODE_WIDTH;

    for (let i = 0; i < componentIds.length; i++) {
      packed.set(componentIds[i]!, {
        x: pos[i]!.x - centerX + componentOffsetX,
        y: pos[i]!.y - centerY,
      });
    }
    componentOffsetX += width + FORCE_COMPONENT_GAP;
  }

  return nodes.map((node) => {
    const p = packed.get(node.id) ?? { x: 0, y: 0 };
    return {
      ...node,
      position: {
        x: p.x - DM_VIEW_NODE_WIDTH / 2,
        y: p.y - DM_VIEW_NODE_HEIGHT / 2,
      },
    };
  });
}

export function layoutDmViewNodesByMethod(
  nodes: Node[],
  edges: Edge[],
  orientation: TransformCanvasHandleOrientation = "lr",
  method: DmFlowLayoutMethod = "force"
): Node[] {
  const normalized = normalizeDmFlowLayoutMethod(method);
  if (normalized === "grid") {
    return layoutDmViewNodesGrid(nodes);
  }
  if (normalized === "dagre") {
    return layoutDmViewNodesDagre(nodes, edges, orientation);
  }
  return layoutDmViewNodesForce(nodes, edges);
}

export function graphToFlow(
  graph: DataModelGraph,
  opts?: DmFlowLayoutOptions
): { nodes: Node[]; edges: Edge[] } {
  const orientation = normalizeTransformCanvasHandleOrientation(opts?.handleOrientation);
  const layoutMethod = normalizeDmFlowLayoutMethod(opts?.layoutMethod);
  const edgeType = normalizeTransformCanvasEdgePathStyle(opts?.edgePathStyle);
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
      type: edgeType,
      label: e.label,
      animated: false,
      style: { stroke: "var(--disc-primary)" },
      labelStyle: { fill: "var(--disc-text-muted)", fontSize: 10 },
    }));

  const nodes = layoutDmViewNodesByMethod(baseNodes, edges, orientation, layoutMethod);
  return { nodes, edges };
}
