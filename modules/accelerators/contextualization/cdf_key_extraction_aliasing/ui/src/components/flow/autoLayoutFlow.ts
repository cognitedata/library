import type { Edge, Node } from "@xyflow/react";
import type { WorkflowCanvasHandleOrientation } from "../../types/workflowCanvas";

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
    return { w: 192, h: 124 };
  }
  if (t === "keaValidation") {
    return { w: 192, h: 108 };
  }
  if (t === "keaAliasPersistence" || t === "keaReferenceIndex") {
    return { w: 192, h: 120 };
  }
  if (t === "keaSubflowGraphIn" || t === "keaSubflowGraphOut") {
    return { w: 132, h: 72 };
  }
  if (t === "keaSubgraph") {
    return { w: 192 + BOX_PAD, h: 112 + BOX_PAD };
  }
  if (t === "keaSubflow") {
    const style = (n.style ?? {}) as Record<string, unknown>;
    const sw = style.width;
    const sh = style.height;
    const w = typeof sw === "number" ? sw : typeof sw === "string" ? parseFloat(sw) : NaN;
    const h = typeof sh === "number" ? sh : typeof sh === "string" ? parseFloat(sh) : NaN;
    if (Number.isFinite(w) && Number.isFinite(h) && w > 0 && h > 0) {
      return { w: Math.ceil(w) + BOX_PAD, h: Math.ceil(h) + BOX_PAD };
    }
    return { w: 380 + BOX_PAD, h: 260 + BOX_PAD };
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

  const endId = nodes.find((n) => compIds.has(n.id) && n.type === "keaEnd")?.id;
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
  orientation: WorkflowCanvasHandleOrientation
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
        pos.set(id, {
          x: layerPrimary[L]!,
          y,
        });
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
        pos.set(id, {
          x,
          y: layerPrimary[L]!,
        });
        x += r.w + GAP_Y;
      });
    }
  }

  return pos;
}

const SUB_INTERIOR_PAD = 20;
const SUB_INTERIOR_HEADER = 40;
const SUB_MIN_W = 200;
const SUB_MIN_H = 140;
const INTERIOR_COMP_GAP = 48;

/** How many subflow-only ancestor levels sit above this subflow (inner = larger). */
function subflowNestingDepth(nodes: Node[], subflowId: string): number {
  let d = 0;
  const start = nodes.find((n) => n.id === subflowId);
  if (!start) return 0;
  const seen = new Set<string>();
  let walk: Node = start;
  while (walk.parentId && !seen.has(walk.parentId)) {
    seen.add(walk.parentId);
    const p = nodes.find((n) => n.id === walk.parentId);
    if (!p || p.type !== "keaSubflow") break;
    d++;
    walk = p;
  }
  return d;
}

/** Layer children of ``subflowId`` using the same pipeline rules as the root canvas; resize the subflow frame to fit. */
const HUB_LANE_W = 136;
const HUB_GAP = 24;

function layoutSubflowInterior(
  nodes: Node[],
  edges: Edge[],
  subflowId: string,
  orientation: WorkflowCanvasHandleOrientation
): Node[] {
  const children = nodes.filter((n) => n.parentId === subflowId);
  if (children.length === 0) return nodes;

  const hubTypes = new Set(["keaSubflowGraphIn", "keaSubflowGraphOut"]);
  const layoutChildren = children.filter((c) => !hubTypes.has(c.type ?? ""));
  const hubIn = children.find((c) => c.type === "keaSubflowGraphIn");
  const hubOut = children.find((c) => c.type === "keaSubflowGraphOut");

  const byId = nodeById(nodes);

  if (layoutChildren.length === 0) {
    const subW = Math.max(SUB_MIN_W, SUB_INTERIOR_PAD * 2 + HUB_LANE_W * 2 + HUB_GAP);
    const subH = Math.max(SUB_MIN_H, SUB_INTERIOR_HEADER + SUB_INTERIOR_PAD * 2 + 72);
    return nodes.map((n) => {
      if (n.id === subflowId) {
        const prev = (n.style ?? {}) as Record<string, unknown>;
        return { ...n, style: { ...prev, width: subW, height: subH } };
      }
      if (hubIn && n.id === hubIn.id) {
        return { ...n, position: { x: SUB_INTERIOR_PAD, y: SUB_INTERIOR_HEADER + SUB_INTERIOR_PAD } };
      }
      if (hubOut && n.id === hubOut.id) {
        return {
          ...n,
          position: { x: subW - SUB_INTERIOR_PAD - HUB_LANE_W, y: SUB_INTERIOR_HEADER + SUB_INTERIOR_PAD },
        };
      }
      return n;
    });
  }

  const childIds = layoutChildren.map((c) => c.id);
  const subEdges = edges.filter((e) => childIds.includes(e.source) && childIds.includes(e.target));
  const comps = weakComponents(childIds, subEdges);

  const interiorPos = new Map<string, { x: number; y: number }>();
  let primaryCursor = 0;

  for (const comp of comps) {
    const compInternalEdges = subEdges.filter((e) => comp.includes(e.source) && comp.includes(e.target));
    const compPos = positionsForComponent(comp, nodes, compInternalEdges, primaryCursor, orientation);
    let maxExt = primaryCursor;
    for (const [id, p] of compPos) {
      interiorPos.set(id, p);
      const r = estimateNodeRect(byId.get(id)!);
      if (orientation === "lr") {
        maxExt = Math.max(maxExt, p.x + r.w);
      } else {
        maxExt = Math.max(maxExt, p.y + r.h);
      }
    }
    primaryCursor = maxExt + INTERIOR_COMP_GAP;
  }

  let minX = Infinity;
  let minY = Infinity;
  for (const [, p] of interiorPos) {
    minX = Math.min(minX, p.x);
    minY = Math.min(minY, p.y);
  }
  if (!Number.isFinite(minX) || !Number.isFinite(minY)) return nodes;

  const laneOffset = hubIn ? SUB_INTERIOR_PAD + HUB_LANE_W + HUB_GAP : SUB_INTERIOR_PAD;
  const shiftX = laneOffset - minX;
  const shiftY = SUB_INTERIOR_HEADER + SUB_INTERIOR_PAD - minY;
  const shifted = new Map<string, { x: number; y: number }>();
  for (const [id, p] of interiorPos) {
    shifted.set(id, { x: p.x + shiftX, y: p.y + shiftY });
  }

  let bboxMaxX = 0;
  let bboxMaxY = 0;
  for (const [id, p] of shifted) {
    const node = byId.get(id);
    if (!node) continue;
    const r = estimateNodeRect(node);
    bboxMaxX = Math.max(bboxMaxX, p.x + r.w);
    bboxMaxY = Math.max(bboxMaxY, p.y + r.h);
  }

  const rightReserve = hubOut ? HUB_GAP + HUB_LANE_W + SUB_INTERIOR_PAD : SUB_INTERIOR_PAD;
  const subW = Math.max(SUB_MIN_W, bboxMaxX + rightReserve);
  const subH = Math.max(SUB_MIN_H, bboxMaxY + SUB_INTERIOR_PAD);

  return nodes.map((n) => {
    if (n.id === subflowId) {
      const prev = (n.style ?? {}) as Record<string, unknown>;
      return { ...n, style: { ...prev, width: subW, height: subH } };
    }
    if (hubIn && n.id === hubIn.id) {
      return { ...n, position: { x: SUB_INTERIOR_PAD, y: SUB_INTERIOR_HEADER + SUB_INTERIOR_PAD } };
    }
    if (hubOut && n.id === hubOut.id) {
      return {
        ...n,
        position: { x: subW - SUB_INTERIOR_PAD - HUB_LANE_W, y: SUB_INTERIOR_HEADER + SUB_INTERIOR_PAD },
      };
    }
    const np = shifted.get(n.id);
    if (np) {
      return { ...n, position: np };
    }
    return n;
  });
}

/**
 * Layered layout: ``lr`` = pipeline to the east; ``tb`` = pipeline downward.
 * Multiple disconnected subgraphs are spaced along the layout primary axis.
 * Nested subflows are laid out inside-out (deepest first), then root nodes (including subflow frames).
 */
export function layoutFlowNodes(
  nodes: Node[],
  edges: Edge[],
  orientation: WorkflowCanvasHandleOrientation = "lr"
): Node[] {
  if (nodes.length === 0) return nodes;

  const subflowIds = nodes.filter((n) => n.type === "keaSubflow").map((n) => n.id);
  let next = [...nodes];
  subflowIds.sort((a, b) => subflowNestingDepth(next, b) - subflowNestingDepth(next, a));
  for (const sfId of subflowIds) {
    next = layoutSubflowInterior(next, edges, sfId, orientation);
  }

  const roots = next.filter((n) => !n.parentId);
  if (roots.length === 0) return nodes;
  const rootIdSet = new Set(roots.map((r) => r.id));
  const idList = roots.map((n) => n.id);
  const comps = weakComponents(
    idList,
    edges.filter((e) => rootIdSet.has(e.source) && rootIdSet.has(e.target))
  );
  const byId = nodeById(next);

  const pos = new Map<string, { x: number; y: number }>();
  let primaryOffset = 0;

  for (const comp of comps) {
    const subEdges = edges.filter((e) => comp.includes(e.source) && comp.includes(e.target));
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
