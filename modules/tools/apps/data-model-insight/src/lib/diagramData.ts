/**
 * Build graph data for SVG diagram: nodes and edges from inheritance + relations.
 * Keeps diagram structured and vertical even when direct_relations is empty.
 */

import type { DocModel, CategoriesMap, CategoryLabel } from "@/types/dataModel";

export interface DiagramNode {
  id: string;
  label: string;
  depth: number; // 0 = root, 1 = one level down, etc.
  /** Width in px from label length (tight padding), used for layout and drawing */
  width: number;
}

/** Box width from label character count: tight horizontal padding, no unnecessary cut-offs */
const LABEL_PAD_X = 6;
const CHAR_WIDTH_APPROX = 5.2;
const MIN_BOX_WIDTH = 44;
const MAX_BOX_WIDTH = 220;

export function labelToBoxWidth(label: string): number {
  return Math.min(MAX_BOX_WIDTH, Math.max(MIN_BOX_WIDTH, LABEL_PAD_X * 2 + label.length * CHAR_WIDTH_APPROX));
}

export interface DiagramEdge {
  from: string;
  to: string;
  kind: "inheritance" | "relation";
  label?: string;
}

export const COGNITE_CORE_NODE_ID = "__CogniteCore__";

/** CDF core types in order of significance; diagrams show extensions (or core if no extensions) with relations. */
export const CDF_CORE_TYPES = [
  "CogniteAsset",
  "CogniteActivity",
  "CogniteFile",
  "CogniteEquipment",
  "CogniteTimeSeries",
] as const;

const MAX_OVERVIEW_NODES = 45;
const MAX_OVERVIEW_NODES_WITH_CORE = 40; // leave room for synthetic root
const BOX_WIDTH = 88;
const BOX_HEIGHT = 20;
const LANE_GAP = 12;
const ROW_GAP = 20;

/** Organic layout: force-directed with stable bounds and scaling */
const ORGANIC_ROW_GAP = 24;
const ORGANIC_LANE_GAP = 16;
const FORCE_ITERATIONS = 120;
const IDEAL_LINK_LENGTH = 110;
const REPULSION = 4000;
const ATTRACTION = 0.04;
const DAMPING = 0.82;
const MIN_DIST = 28; // cap repulsion at small d to avoid explosion
const CENTER_PULL = 0.018; // pull center nodes toward diagram center
const TARGET_LAYOUT_WIDTH = 2800;
const TARGET_LAYOUT_HEIGHT = 700;

/**
 * Returns true if any view in the model implements a type not in the model (e.g. Cognite Core).
 */
function modelExtendsExternalTypes(doc: DocModel): boolean {
  const viewIds = new Set(Object.keys(doc.views));
  for (const view of Object.values(doc.views)) {
    const impl = view?.implements?.split(",").map((p) => p.trim()).filter(Boolean) ?? [];
    for (const p of impl) {
      if (!viewIds.has(p)) return true;
    }
  }
  return false;
}

/**
 * Get root view ids: views that implement at least one type not in the model.
 */
function getRootViewIds(doc: DocModel): string[] {
  const viewIds = new Set(Object.keys(doc.views));
  const roots: string[] = [];
  for (const id of viewIds) {
    const view = doc.views[id];
    const impl = view?.implements?.split(",").map((p) => p.trim()).filter(Boolean) ?? [];
    const hasExternalParent = impl.some((p) => !viewIds.has(p));
    if (hasExternalParent) roots.push(id);
  }
  return roots;
}

/**
 * Get nodes and edges for overview. Uses inheritance (implements) so we always have structure.
 * Limits nodes so the diagram stays readable.
 */
export function buildOverviewGraph(doc: DocModel): { nodes: DiagramNode[]; edges: DiagramEdge[] } {
  const viewIds = Object.keys(doc.views);
  const depthByNode = new Map<string, number>();

  function getDepth(id: string): number {
    if (depthByNode.has(id)) return depthByNode.get(id)!;
    const view = doc.views[id];
    const impl = view?.implements?.split(",").map((p) => p.trim()).filter(Boolean) ?? [];
    if (impl.length === 0) {
      depthByNode.set(id, 0);
      return 0;
    }
    let maxParentDepth = -1;
    for (const p of impl) {
      if (viewIds.includes(p)) maxParentDepth = Math.max(maxParentDepth, getDepth(p));
    }
    const d = maxParentDepth + 1;
    depthByNode.set(id, d);
    return d;
  }

  for (const id of viewIds) getDepth(id);

  const edges: DiagramEdge[] = [];
  const seenInheritance = new Set<string>();
  for (const id of viewIds) {
    const view = doc.views[id];
    const impl = view?.implements?.split(",").map((p) => p.trim()).filter(Boolean) ?? [];
    for (const p of impl) {
      if (!viewIds.includes(p)) continue;
      const key = `${id}--|>${p}`;
      if (seenInheritance.has(key)) continue;
      seenInheritance.add(key);
      edges.push({ from: id, to: p, kind: "inheritance" });
    }
  }
  for (const rel of doc.direct_relations) {
    if (!viewIds.includes(rel.source) || !viewIds.includes(rel.target)) continue;
    edges.push({
      from: rel.source,
      to: rel.target,
      kind: "relation",
      label: rel.display_name || rel.property,
    });
  }

  const allIds = new Set<string>();
  for (const e of edges) {
    allIds.add(e.from);
    allIds.add(e.to);
  }
  for (const id of viewIds) allIds.add(id);

  let included = Array.from(allIds);
  if (included.length > MAX_OVERVIEW_NODES) {
    const withEdges = new Set<string>();
    for (const e of edges) {
      withEdges.add(e.from);
      withEdges.add(e.to);
    }
    included = included.filter((id) => withEdges.has(id));
    if (included.length > MAX_OVERVIEW_NODES) {
      included = included.slice(0, MAX_OVERVIEW_NODES);
    }
  }
  const includedSet = new Set(included);
  const nodes: DiagramNode[] = included
    .map((id) => {
      const label = doc.views[id]?.display_name || id;
      return { id, label, depth: getDepth(id), width: labelToBoxWidth(label) };
    })
    .sort((a, b) => a.depth - b.depth);
  const filteredEdges = edges.filter((e) => includedSet.has(e.from) && includedSet.has(e.to));

  return { nodes, edges: filteredEdges };
}

const MAX_TOPIC_DIAGRAM_NODES = 999;

/**
 * Build graph for a subset of view IDs (topic/category diagram). Same structure as overview:
 * inheritance + relations, optional Cognite Core if any selected view extends external. Uses same
 * organic layout as Overview.
 */
export function buildGraphForViewIds(
  doc: DocModel,
  viewIds: string[]
): { nodes: DiagramNode[]; edges: DiagramEdge[] } {
  const set = new Set(viewIds);
  if (set.size === 0) return { nodes: [], edges: [] };

  const allViewIds = Object.keys(doc.views);
  const rootsInSet = viewIds.filter((id) => {
    const view = doc.views[id];
    const impl = view?.implements?.split(",").map((p) => p.trim()).filter(Boolean) ?? [];
    return impl.some((p) => !allViewIds.includes(p));
  });
  const useCore = rootsInSet.length > 0;

  const depthByNode = new Map<string, number>();
  if (useCore) depthByNode.set(COGNITE_CORE_NODE_ID, 0);
  function getDepth(id: string): number {
    if (depthByNode.has(id)) return depthByNode.get(id)!;
    const view = doc.views[id];
    const impl = view?.implements?.split(",").map((p) => p.trim()).filter(Boolean) ?? [];
    if (impl.length === 0) {
      depthByNode.set(id, useCore ? 1 : 0);
      return depthByNode.get(id)!;
    }
    let maxParentDepth = useCore ? 0 : -1;
    for (const p of impl) {
      if (p === COGNITE_CORE_NODE_ID || set.has(p)) {
        const parentDepth = p === COGNITE_CORE_NODE_ID ? 0 : getDepth(p);
        maxParentDepth = Math.max(maxParentDepth, parentDepth);
      }
    }
    const d = maxParentDepth + 1;
    depthByNode.set(id, d);
    return d;
  }
  for (const id of viewIds) getDepth(id);

  const edges: DiagramEdge[] = [];
  const seenInheritance = new Set<string>();
  for (const id of viewIds) {
    const view = doc.views[id];
    const impl = view?.implements?.split(",").map((p) => p.trim()).filter(Boolean) ?? [];
    for (const p of impl) {
      if (!set.has(p) && p !== COGNITE_CORE_NODE_ID) continue;
      const key = `${id}--|>${p}`;
      if (seenInheritance.has(key)) continue;
      seenInheritance.add(key);
      edges.push({ from: id, to: p, kind: "inheritance" });
    }
  }
  if (useCore) {
    for (const id of rootsInSet) {
      edges.push({ from: id, to: COGNITE_CORE_NODE_ID, kind: "inheritance" });
    }
  }
  for (const rel of doc.direct_relations) {
    if (!set.has(rel.source) || !set.has(rel.target)) continue;
    edges.push({
      from: rel.source,
      to: rel.target,
      kind: "relation",
      label: rel.display_name || rel.property,
    });
  }

  let included = useCore ? [COGNITE_CORE_NODE_ID, ...viewIds] : viewIds;
  if (included.length > MAX_TOPIC_DIAGRAM_NODES) {
    const withEdges = new Set<string>();
    for (const e of edges) {
      withEdges.add(e.from);
      withEdges.add(e.to);
    }
    included = included.filter((id) => withEdges.has(id));
    if (included.length > MAX_TOPIC_DIAGRAM_NODES) {
      included = included.slice(0, MAX_TOPIC_DIAGRAM_NODES);
    }
  }
  const includedSet = new Set(included);
  const nodes: DiagramNode[] = (
    useCore
      ? [{ id: COGNITE_CORE_NODE_ID, label: "Cognite Core", depth: 0, width: labelToBoxWidth("Cognite Core") }]
      : []
  ).concat(
    included
      .filter((id) => id !== COGNITE_CORE_NODE_ID)
      .map((id) => {
        const label = doc.views[id]?.display_name || id;
        return { id, label, depth: getDepth(id), width: labelToBoxWidth(label) };
      })
      .sort((a, b) => a.depth - b.depth)
  );
  const filteredEdges = edges.filter((e) => includedSet.has(e.from) && includedSet.has(e.to));
  return { nodes, edges: filteredEdges };
}

/**
 * Overview diagram that shows main parts of the model extending from Cognite Core when the model
 * extends external types (e.g. CogniteCore); otherwise returns a normal model overview.
 */
export function buildOverviewGraphWithOptionalCore(doc: DocModel): { nodes: DiagramNode[]; edges: DiagramEdge[] } {
  if (!modelExtendsExternalTypes(doc)) {
    return buildOverviewGraph(doc);
  }

  const viewIds = Object.keys(doc.views);
  const roots = getRootViewIds(doc);
  if (roots.length === 0) return buildOverviewGraph(doc);

  const depthByNode = new Map<string, number>();
  function getDepth(id: string): number {
    if (depthByNode.has(id)) return depthByNode.get(id)!;
    const view = doc.views[id];
    const impl = view?.implements?.split(",").map((p) => p.trim()).filter(Boolean) ?? [];
    if (impl.length === 0) {
      depthByNode.set(id, 1);
      return 1;
    }
    let maxParentDepth = 0;
    for (const p of impl) {
      if (viewIds.includes(p)) maxParentDepth = Math.max(maxParentDepth, getDepth(p));
    }
    const d = maxParentDepth + 1;
    depthByNode.set(id, d);
    return d;
  }
  for (const id of viewIds) getDepth(id);

  const edges: DiagramEdge[] = [];
  const seenInheritance = new Set<string>();
  for (const id of viewIds) {
    const view = doc.views[id];
    const impl = view?.implements?.split(",").map((p) => p.trim()).filter(Boolean) ?? [];
    for (const p of impl) {
      if (!viewIds.includes(p)) continue;
      const key = `${id}--|>${p}`;
      if (seenInheritance.has(key)) continue;
      seenInheritance.add(key);
      edges.push({ from: id, to: p, kind: "inheritance" });
    }
  }
  for (const id of roots) {
    edges.push({ from: id, to: COGNITE_CORE_NODE_ID, kind: "inheritance" });
  }
  for (const rel of doc.direct_relations) {
    if (!viewIds.includes(rel.source) || !viewIds.includes(rel.target)) continue;
    edges.push({
      from: rel.source,
      to: rel.target,
      kind: "relation",
      label: rel.display_name || rel.property,
    });
  }

  const allIds = new Set<string>([COGNITE_CORE_NODE_ID]);
  for (const e of edges) {
    allIds.add(e.from);
    allIds.add(e.to);
  }
  for (const id of viewIds) allIds.add(id);

  let included = Array.from(allIds);
  if (included.length > MAX_OVERVIEW_NODES_WITH_CORE) {
    const withEdges = new Set<string>();
    for (const e of edges) {
      withEdges.add(e.from);
      withEdges.add(e.to);
    }
    included = included.filter((id) => withEdges.has(id));
    if (included.length > MAX_OVERVIEW_NODES_WITH_CORE) {
      included = included.slice(0, MAX_OVERVIEW_NODES_WITH_CORE);
    }
  }
  const includedSet = new Set(included);
  const nodes: DiagramNode[] = [
    { id: COGNITE_CORE_NODE_ID, label: "Cognite Core", depth: 0, width: labelToBoxWidth("Cognite Core") },
    ...included
      .filter((id) => id !== COGNITE_CORE_NODE_ID)
      .map((id) => {
        const label = doc.views[id]?.display_name || id;
        return { id, label, depth: getDepth(id), width: labelToBoxWidth(label) };
      })
      .sort((a, b) => a.depth - b.depth),
  ];
  const filteredEdges = edges.filter((e) => includedSet.has(e.from) && includedSet.has(e.to));

  return { nodes, edges: filteredEdges };
}

/** Legacy grid layout (kept for reference). Overview uses computeOrganicLayout. */
export function computeLayout(
  nodes: DiagramNode[],
  _edges: DiagramEdge[]
): Map<string, { x: number; y: number }> {
  const pos = new Map<string, { x: number; y: number }>();
  const byDepth = new Map<number, string[]>();
  for (const n of nodes) {
    if (!byDepth.has(n.depth)) byDepth.set(n.depth, []);
    byDepth.get(n.depth)!.push(n.id);
  }
  const depths = Array.from(byDepth.keys()).sort((a, b) => a - b);
  let y = 20;
  for (const depth of depths) {
    const ids = byDepth.get(depth)!;
    let x = LANE_GAP;
    for (const id of ids) {
      pos.set(id, { x, y });
      x += BOX_WIDTH + LANE_GAP;
    }
    y += BOX_HEIGHT + ROW_GAP;
  }
  return pos;
}

/**
 * Organic layout: force-directed with two-phase updates, repulsion cap, containment,
 * and final scale-to-fit. Optionally pull centerNodeIds toward the diagram center.
 * targetWidth/targetHeight: available estate (e.g. container size when data model was chosen); uses LAYOUT defaults if omitted.
 */
export function computeOrganicLayout(
  nodes: DiagramNode[],
  edges: DiagramEdge[],
  centerNodeIds?: Set<string>,
  targetWidth?: number,
  targetHeight?: number
): Map<string, { x: number; y: number }> {
  if (nodes.length === 0) return new Map();

  const pos = new Map<string, { x: number; y: number }>();
  const vel = new Map<string, { vx: number; vy: number }>();

  const byDepth = new Map<number, DiagramNode[]>();
  for (const n of nodes) {
    if (!byDepth.has(n.depth)) byDepth.set(n.depth, []);
    byDepth.get(n.depth)!.push(n);
  }
  const depths = Array.from(byDepth.keys()).sort((a, b) => a - b);

  function hash(s: string): number {
    let h = 0;
    for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0;
    return h;
  }

  const nodeWidth = new Map<string, number>();
  for (const node of nodes) nodeWidth.set(node.id, node.width);
  const maxW = Math.max(...nodes.map((n) => n.width), BOX_WIDTH);

  // Wide containment so diagram uses horizontal space; more width than height
  const n = nodes.length;
  const areaW = 1000 + n * 36;
  const areaH = 350 + n * 10;
  let y = 20;
  for (const depth of depths) {
    const rowNodes = byDepth.get(depth)!;
    const rowWidth = rowNodes.reduce((s, node) => s + node.width + ORGANIC_LANE_GAP, 0) - ORGANIC_LANE_GAP;
    let x = (areaW - rowWidth) / 2 + ORGANIC_LANE_GAP;
    for (let i = 0; i < rowNodes.length; i++) {
      const node = rowNodes[i];
      const jitterX = (hash(node.id + "x") % 17) - 8;
      const jitterY = (hash(node.id + "y") % 11) - 5;
      pos.set(node.id, { x: x + jitterX, y: y + jitterY });
      vel.set(node.id, { vx: 0, vy: 0 });
      x += node.width + ORGANIC_LANE_GAP;
    }
    y += BOX_HEIGHT + ORGANIC_ROW_GAP;
  }

  const idList = nodes.map((n) => n.id);

  // Two-phase update: read all positions, then write all updates (avoids order-dependent collapse)
  for (let iter = 0; iter < FORCE_ITERATIONS; iter++) {
    const nextPos = new Map<string, { x: number; y: number }>();
    const nextVel = new Map<string, { vx: number; vy: number }>();

    for (const id of idList) {
      const p = pos.get(id)!;
      let fx = 0,
        fy = 0;

      for (const other of idList) {
        if (other === id) continue;
        const q = pos.get(other)!;
        const dx = p.x - q.x;
        const dy = p.y - q.y;
        const d = Math.sqrt(dx * dx + dy * dy) || 0.01;
        const dSafe = Math.max(d, MIN_DIST);
        const rep = REPULSION / (dSafe * dSafe);
        fx += (dx / d) * rep;
        fy += (dy / d) * rep;
      }

      for (const e of edges) {
        if (e.from !== id && e.to !== id) continue;
        const other = e.from === id ? e.to : e.from;
        const q = pos.get(other)!;
        const dx = p.x - q.x;
        const dy = p.y - q.y;
        const d = Math.sqrt(dx * dx + dy * dy) || 1;
        const stretch = d - IDEAL_LINK_LENGTH;
        const f = ATTRACTION * stretch;
        fx -= (dx / d) * f;
        fy -= (dy / d) * f;
      }

      if (centerNodeIds?.has(id)) {
        const centerX = areaW / 2;
        const centerY = areaH / 2;
        fx += (centerX - p.x) * CENTER_PULL;
        fy += (centerY - p.y) * CENTER_PULL;
      }

      const v = vel.get(id)!;
      const vx = (v.vx + fx) * DAMPING;
      const vy = (v.vy + fy) * DAMPING;
      nextVel.set(id, { vx, vy });
      let nx = p.x + vx;
      let ny = p.y + vy;
      const w = nodeWidth.get(id) ?? maxW;
      nx = Math.max(0, Math.min(areaW - w, nx));
      ny = Math.max(0, Math.min(areaH - BOX_HEIGHT, ny));
      nextPos.set(id, { x: nx, y: ny });
    }

    for (const id of idList) {
      pos.set(id, nextPos.get(id)!);
      vel.set(id, nextVel.get(id)!);
    }
  }

  let minX = Infinity,
    minY = Infinity,
    maxX = -Infinity,
    maxY = -Infinity;
  for (const id of idList) {
    const p = pos.get(id)!;
    const w = nodeWidth.get(id) ?? maxW;
    minX = Math.min(minX, p.x);
    minY = Math.min(minY, p.y);
    maxX = Math.max(maxX, p.x + w);
    maxY = Math.max(maxY, p.y + BOX_HEIGHT);
  }
  const contentW = maxX - minX || 1;
  const contentH = maxY - minY || 1;
  const pad = 20;
  const tw = targetWidth ?? TARGET_LAYOUT_WIDTH;
  const th = targetHeight ?? TARGET_LAYOUT_HEIGHT;
  const scaleX = (tw - 2 * pad) / contentW;
  const scaleY = (th - 2 * pad) / contentH;
  const result = new Map<string, { x: number; y: number }>();
  for (const id of idList) {
    const p = pos.get(id)!;
    result.set(id, {
      x: pad + (p.x - minX) * scaleX,
      y: pad + (p.y - minY) * scaleY,
    });
  }
  return result;
}

export const LAYOUT = {
  BOX_WIDTH,
  BOX_HEIGHT,
  LANE_GAP,
  ROW_GAP,
  ORGANIC_ROW_GAP,
  ORGANIC_LANE_GAP,
  TARGET_LAYOUT_WIDTH,
  TARGET_LAYOUT_HEIGHT,
};

export interface OrganicDiagramSpec {
  title: string;
  description: string;
  viewIds: string[];
  /** View IDs to pull toward diagram center (core type and its extenders). */
  centerViewIds?: string[];
}

/** All view IDs that (directly or indirectly) implement coreType. Includes coreType if it exists in the model. */
function getTransitiveExtenders(doc: DocModel, coreType: string): Set<string> {
  const viewIds = Object.keys(doc.views);
  const hub = new Set<string>();
  if (doc.views[coreType]) hub.add(coreType);
  let changed = true;
  while (changed) {
    changed = false;
    for (const id of viewIds) {
      if (hub.has(id)) continue;
      const impl = doc.views[id]?.implements?.split(",").map((p) => p.trim()).filter(Boolean) ?? [];
      if (impl.some((p) => hub.has(p))) {
        hub.add(id);
        changed = true;
      }
    }
  }
  return hub;
}

/** Returns viewIds and centerViewIds for a diagram focused on one CDF core type.
 * ViewIds = extenders + any view with a relation (in/out) to them. Center = same set so extenders and relation-connected views are equally dominant in center. */
function getViewIdsForCdfCoreDiagram(
  doc: DocModel,
  coreType: string
): { viewIds: string[]; centerViewIds: string[] } | null {
  const hub = getTransitiveExtenders(doc, coreType);
  if (hub.size === 0) return null;

  const related = new Set(hub);
  for (const r of doc.direct_relations) {
    if (hub.has(r.source)) related.add(r.target);
    if (hub.has(r.target)) related.add(r.source);
  }
  const outViewIds = [...related].filter((id) => doc.views[id]);
  const centerViewIds = outViewIds;
  return { viewIds: outViewIds, centerViewIds };
}

/** Combined significant view IDs and center view IDs for Overview (all 5 CDF core types). */
export function getSignificantOverviewViewIds(
  doc: DocModel
): { viewIds: string[]; centerViewIds: string[] } {
  const allViewIds = new Set<string>();
  const allCenterViewIds = new Set<string>();
  for (const coreType of CDF_CORE_TYPES) {
    const result = getViewIdsForCdfCoreDiagram(doc, coreType);
    if (result) {
      for (const id of result.viewIds) allViewIds.add(id);
      for (const id of result.centerViewIds) allCenterViewIds.add(id);
    }
  }
  return {
    viewIds: [...allViewIds],
    centerViewIds: [...allCenterViewIds],
  };
}

/** CDF-core-focused diagram specs: one per core type. All extension levels (transitive) + any view that relates to them; core/extensions centered. */
export function getOrganicDiagramSpecs(
  _doc: DocModel,
  _categories: CategoriesMap,
  _categoryLabels: Record<string, CategoryLabel>
): OrganicDiagramSpec[] {
  const doc = _doc;
  const specs: OrganicDiagramSpec[] = [];
  const labels: Record<string, string> = {
    CogniteAsset: "Asset",
    CogniteActivity: "Activity",
    CogniteFile: "File",
    CogniteEquipment: "Equipment",
    CogniteTimeSeries: "Time series",
  };

  for (const coreType of CDF_CORE_TYPES) {
    const result = getViewIdsForCdfCoreDiagram(doc, coreType);
    if (!result || result.viewIds.length === 0) continue;
    const viewIds = result.viewIds;
    const centerViewIds = result.centerViewIds;
    specs.push({
      title: `${labels[coreType] ?? coreType} (${coreType})`,
      description: `Types extending ${coreType} and their relations`,
      viewIds,
      centerViewIds: centerViewIds.length > 0 ? centerViewIds : undefined,
    });
  }

  return specs;
}
