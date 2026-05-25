import type { Edge, Node } from "@xyflow/react";
import type { TransformCanvasHandleOrientation } from "../../types/transformCanvas";
import type { FlowEdgeData } from "./flowDocumentBridge";

const GAP_X = 56;
const GAP_Y = 36;
const COMP_GAP_X = 120;
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
  if (n.type === "etlStart" || n.type === "etlEnd") {
    return { w: 148, h: 68 };
  }
  return { w: 192, h: 96 };
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

function layersForComponent(compIds: Set<string>, nodes: Node[], edges: Edge[]): Map<string, number> {
  const preds = new Map<string, string[]>();
  for (const id of compIds) preds.set(id, []);
  for (const e of edges) {
    if (!compIds.has(e.source) || !compIds.has(e.target)) continue;
    preds.get(e.target)!.push(e.source);
  }

  const startId = nodes.find((n) => compIds.has(n.id) && n.type === "etlStart")?.id;

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

  const endId = nodes.find((n) => compIds.has(n.id) && n.type === "etlEnd")?.id;
  if (endId) {
    const m = Math.max(0, ...[...compIds].filter((id) => id !== endId).map((id) => layer.get(id) ?? 0));
    layer.set(endId, Math.max(layer.get(endId) ?? 0, m + 1));
  }

  return layer;
}

function positionsForComponent(
  compIds: string[],
  nodes: Node[],
  edges: Edge[],
  primaryOffset: number,
  orientation: TransformCanvasHandleOrientation
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
  for (const ids of byLayer.values()) ids.sort((a, b) => a.localeCompare(b));

  const pos = new Map<string, { x: number; y: number }>();
  const layerPrimary: number[] = [];
  let cursor = primaryOffset;

  if (orientation === "lr") {
    for (let L = 0; L <= maxL; L++) {
      layerPrimary[L] = cursor;
      const row = byLayer.get(L)!;
      const maxW = row.length === 0 ? 0 : Math.max(...row.map((id) => estimateNodeRect(byId.get(id)!).w));
      cursor += maxW + GAP_X;
    }

    for (let L = 0; L <= maxL; L++) {
      const row = byLayer.get(L)!;
      if (row.length === 0) continue;
      const rects = row.map((id) => estimateNodeRect(byId.get(id)!));
      const totalH = rects.reduce((s, r) => s + r.h, 0) + (row.length - 1) * GAP_Y;
      let y = -totalH / 2;
      row.forEach((id, i) => {
        const r = rects[i]!;
        pos.set(id, { x: layerPrimary[L]!, y });
        y += r.h + GAP_Y;
      });
    }
  } else {
    for (let L = 0; L <= maxL; L++) {
      layerPrimary[L] = cursor;
      const row = byLayer.get(L)!;
      const maxH = row.length === 0 ? 0 : Math.max(...row.map((id) => estimateNodeRect(byId.get(id)!).h));
      cursor += maxH + GAP_X;
    }

    for (let L = 0; L <= maxL; L++) {
      const row = byLayer.get(L)!;
      if (row.length === 0) continue;
      const rects = row.map((id) => estimateNodeRect(byId.get(id)!));
      const totalW = rects.reduce((s, r) => s + r.w, 0) + (row.length - 1) * GAP_Y;
      let x = -totalW / 2;
      row.forEach((id, i) => {
        const r = rects[i]!;
        pos.set(id, { x, y: layerPrimary[L]! });
        x += r.w + GAP_Y;
      });
    }
  }

  return pos;
}

function layoutDataEdges(edges: Edge[]): Edge[] {
  return edges.filter((e) => {
    const kind = ((e.data ?? {}) as FlowEdgeData).kind ?? "data";
    return kind === "data";
  });
}

/** Layered layout for transform pipeline nodes (data edges only). */
export function layoutTransformFlowNodes(
  nodes: Node[],
  edges: Edge[],
  orientation: TransformCanvasHandleOrientation = "lr"
): Node[] {
  if (nodes.length === 0) return nodes;

  const next = [...nodes];
  const roots = next.filter((n) => !n.parentId);
  if (roots.length === 0) return nodes;

  const rootIdSet = new Set(roots.map((r) => r.id));
  const dataEdges = layoutDataEdges(edges).filter((e) => rootIdSet.has(e.source) && rootIdSet.has(e.target));
  const comps = weakComponents(roots.map((n) => n.id), dataEdges);
  const byId = nodeById(next);

  const pos = new Map<string, { x: number; y: number }>();
  let primaryOffset = 0;

  for (const comp of comps) {
    const subEdges = dataEdges.filter((e) => comp.includes(e.source) && comp.includes(e.target));
    const compPos = positionsForComponent(comp, next, subEdges, primaryOffset, orientation);
    let maxExtent = primaryOffset;
    for (const [id, p] of compPos) {
      pos.set(id, p);
      const r = estimateNodeRect(byId.get(id)!);
      if (orientation === "lr") {
        maxExtent = Math.max(maxExtent, p.x + r.w);
      } else {
        maxExtent = Math.max(maxExtent, p.y + r.h);
      }
    }
    primaryOffset = maxExtent + COMP_GAP_X;
  }

  return next.map((n) => {
    if (n.parentId) return n;
    const p = pos.get(n.id);
    if (!p) return n;
    return { ...n, position: { x: p.x, y: p.y } };
  });
}
