import type { Edge, Node } from "@xyflow/react";

const GAP_X = 56;
const GAP_Y = 36;
const COMP_GAP_X = 120;
/** Extra margin around measured boxes to avoid sub-pixel overlap. */
const BOX_PAD = 6;

type Rect = { w: number; h: number };

function estimateNodeRect(n: Node): Rect {
  const ext = n as Node & {
    width?: number;
    height?: number;
    measured?: { width?: number; height?: number };
  };
  const mw = ext.measured?.width ?? ext.width;
  const mh = ext.measured?.height ?? ext.height;
  if (typeof mw === "number" && typeof mh === "number" && mw > 0 && mh > 0) {
    return { w: Math.ceil(mw) + BOX_PAD, h: Math.ceil(mh) + BOX_PAD };
  }

  const t = n.type ?? "";
  if (t === "keaStart" || t === "keaEnd") {
    return { w: 148, h: 68 };
  }
  if (
    t === "keaMatchValidationRuleSourceView" ||
    t === "keaMatchValidationRuleExtraction" ||
    t === "keaMatchValidationRuleAliasing"
  ) {
    return { w: 192, h: 128 };
  }
  if (t === "keaSourceView") {
    return { w: 192, h: 118 };
  }
  if (t === "keaExtraction" || t === "keaAliasing") {
    return { w: 192, h: 112 };
  }
  if (t === "keaValidation") {
    return { w: 192, h: 108 };
  }
  if (t === "keaAliasPersistence" || t === "keaReferenceIndex") {
    return { w: 192, h: 120 };
  }
  return { w: 192, h: 108 };
}

function nodeById(nodes: Node[]): Map<string, Node> {
  return new Map(nodes.map((n) => [n.id, n]));
}

function weakComponents(nodeIds: string[], edges: Edge[]): string[][] {
  const adj = new Map<string, Set<string>>();
  for (const id of nodeIds) adj.set(id, new Set());
  for (const e of edges) {
    if (!adj.has(e.source) || !adj.has(e.target)) continue;
    adj.get(e.source)!.add(e.target);
    adj.get(e.target)!.add(e.source);
  }
  const seen = new Set<string>();
  const out: string[][] = [];
  for (const id of nodeIds) {
    if (seen.has(id)) continue;
    const comp: string[] = [];
    const stack = [id];
    seen.add(id);
    while (stack.length) {
      const cur = stack.pop()!;
      comp.push(cur);
      for (const nb of adj.get(cur) ?? []) {
        if (!seen.has(nb)) {
          seen.add(nb);
          stack.push(nb);
        }
      }
    }
    out.push(comp);
  }
  return out;
}

/** Longest-path layering from predecessors; Start pinned to column 0 when present. */
function layersForComponent(
  compIds: Set<string>,
  nodes: Node[],
  edges: Edge[]
): Map<string, number> {
  const preds = new Map<string, string[]>();
  for (const id of compIds) preds.set(id, []);
  for (const e of edges) {
    if (!compIds.has(e.source) || !compIds.has(e.target)) continue;
    preds.get(e.target)!.push(e.source);
  }

  const startId = nodes.find((n) => compIds.has(n.id) && n.type === "keaStart")?.id;

  const layer = new Map<string, number>();
  for (const id of compIds) {
    const ps = preds.get(id)!;
    if (id === startId) layer.set(id, 0);
    else if (ps.length === 0) layer.set(id, 0);
    else layer.set(id, 1);
  }

  const maxIter = compIds.size + 3;
  for (let i = 0; i < maxIter; i++) {
    let changed = false;
    for (const id of compIds) {
      if (id === startId) {
        if (layer.get(id) !== 0) {
          layer.set(id, 0);
          changed = true;
        }
        continue;
      }
      const ps = preds.get(id)!;
      const L = ps.length === 0 ? 0 : Math.max(...ps.map((p) => layer.get(p) ?? 0)) + 1;
      if (layer.get(id) !== L) {
        layer.set(id, L);
        changed = true;
      }
    }
    if (!changed) break;
  }

  if (startId) layer.set(startId, 0);
  return layer;
}

function positionsForComponent(
  compIds: string[],
  nodes: Node[],
  edges: Edge[],
  xOffset: number
): Map<string, { x: number; y: number }> {
  const idSet = new Set(compIds);
  const layer = layersForComponent(idSet, nodes, edges);
  const maxL = Math.max(0, ...compIds.map((id) => layer.get(id) ?? 0));
  const byId = nodeById(nodes);

  const byLayer = new Map<number, string[]>();
  for (let L = 0; L <= maxL; L++) byLayer.set(L, []);
  for (const id of compIds) {
    const L = layer.get(id) ?? 0;
    byLayer.get(L)!.push(id);
  }
  for (const ids of byLayer.values()) ids.sort();

  const pos = new Map<string, { x: number; y: number }>();

  const layerX: number[] = [];
  let xCursor = xOffset;
  for (let L = 0; L <= maxL; L++) {
    layerX[L] = xCursor;
    const row = byLayer.get(L)!;
    const maxW = row.length === 0 ? 0 : Math.max(...row.map((id) => estimateNodeRect(byId.get(id)!).w));
    xCursor += maxW + GAP_X;
  }

  for (let L = 0; L <= maxL; L++) {
    const row = byLayer.get(L)!;
    if (row.length === 0) continue;

    const rects = row.map((id) => estimateNodeRect(byId.get(id)!));
    const totalH = rects.reduce((s, r) => s + r.h, 0) + (row.length - 1) * GAP_Y;
    let y = -totalH / 2;

    row.forEach((id, i) => {
      const r = rects[i];
      pos.set(id, {
        x: layerX[L],
        y: y,
      });
      y += r.h + GAP_Y;
    });
  }

  return pos;
}

/**
 * Left-to-right layered layout (pipeline-style). Multiple disconnected subgraphs are placed side by side.
 * Uses per-node size estimates (or measured dimensions when available) so stacked nodes do not overlap.
 */
export function layoutFlowNodes(nodes: Node[], edges: Edge[]): Node[] {
  if (nodes.length === 0) return nodes;

  const idList = nodes.map((n) => n.id);
  const comps = weakComponents(idList, edges);
  const byId = nodeById(nodes);

  const pos = new Map<string, { x: number; y: number }>();
  let xOffset = 0;

  for (const comp of comps) {
    const subEdges = edges.filter((e) => comp.includes(e.source) && comp.includes(e.target));
    const compPos = positionsForComponent(comp, nodes, subEdges, xOffset);
    let maxRight = xOffset;
    for (const [id, p] of compPos) {
      pos.set(id, p);
      const r = estimateNodeRect(byId.get(id)!);
      maxRight = Math.max(maxRight, p.x + r.w);
    }
    xOffset = maxRight + COMP_GAP_X;
  }

  return nodes.map((n) => {
    const p = pos.get(n.id);
    if (!p) return n;
    return { ...n, position: { x: p.x, y: p.y } };
  });
}
