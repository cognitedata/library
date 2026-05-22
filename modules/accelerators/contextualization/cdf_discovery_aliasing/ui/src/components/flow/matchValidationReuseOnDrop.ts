import type { Edge, Node } from "@xyflow/react";
import type { FlowEdgeData } from "./flowDocumentBridge";
import type { PaletteDragPayload } from "./FlowPalette";
import { resolveConfidenceMatchRuleNames } from "../../utils/confidenceMatchRuleNames";

const MAX_REUSE_DIST_SQ = 520 * 520;

function distSq(a: { x: number; y: number }, b: { x: number; y: number }): number {
  const dx = a.x - b.x;
  const dy = a.y - b.y;
  return dx * dx + dy * dy;
}

function isDataEdge(e: Edge): boolean {
  const k = (e.data as FlowEdgeData | undefined)?.kind;
  return k !== "sequence" && k !== "parallel_group";
}

function isChainEdge(e: Edge): boolean {
  const k = (e.data as FlowEdgeData | undefined)?.kind;
  return k === "sequence" || k === "parallel_group";
}

function ruleLabel(n: Node): string {
  const c = (n.data as Record<string, unknown> | undefined)?.validation_rule_name;
  return c != null && String(c).trim() ? String(c).trim() : "";
}

function refBool(ref: unknown, key: string): boolean {
  if (!ref || typeof ref !== "object" || Array.isArray(ref)) return false;
  return Boolean((ref as Record<string, unknown>)[key]);
}

function refNum(ref: unknown, key: string): number | undefined {
  if (!ref || typeof ref !== "object" || Array.isArray(ref)) return undefined;
  const v = (ref as Record<string, unknown>)[key];
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v.trim() && !Number.isNaN(Number(v))) return Number(v);
  return undefined;
}

function listsEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

function nearestNode(pos: { x: number; y: number }, nodes: Node[], types: Set<string>): Node | null {
  let best: Node | null = null;
  let bestD = MAX_REUSE_DIST_SQ;
  for (const n of nodes) {
    if (!n.type || !types.has(n.type)) continue;
    const d = distSq(pos, n.position);
    if (d <= bestD) {
      bestD = d;
      best = n;
    }
  }
  return best;
}

function walkGlobalMatchChain(
  headId: string,
  rfType: "discoveryMatchValidationRuleExtraction" | "discoveryMatchValidationRuleAliasing",
  globalRefKey: "extraction_global_validation" | "aliasing_global_validation",
  edges: Edge[],
  byId: Map<string, Node>
): string[] {
  const names: string[] = [];
  let cur: string | null = headId;
  const visited = new Set<string>();
  while (cur && !visited.has(cur)) {
    visited.add(cur);
    const node = byId.get(cur);
    if (!node || node.type !== rfType) break;
    const tr = (node.data as Record<string, unknown> | undefined)?.ref;
    if (!refBool(tr, globalRefKey)) break;
    const nm = ruleLabel(node);
    if (nm) names.push(nm);
    const outs = edges
      .filter((e) => e.source === cur)
      .filter(
        (e) =>
          isChainEdge(e) &&
          byId.get(e.target)?.type === rfType &&
          refBool((byId.get(e.target)?.data as Record<string, unknown> | undefined)?.ref, globalRefKey)
      );
    if (outs.length === 0) break;
    outs.sort((a, b) => a.target.localeCompare(b.target));
    cur = outs[0]!.target;
  }
  return names;
}

function svNodeApplies(node: Node, idx: number): boolean {
  if (node.type !== "discoveryMatchValidationRuleSourceView") return false;
  const r = (node.data as Record<string, unknown> | undefined)?.ref;
  if (refNum(r, "source_view_index") === idx) return true;
  if (refBool(r, "shared_source_view_validation_chain")) {
    const indices = (r as Record<string, unknown> | undefined)?.source_view_indices;
    if (Array.isArray(indices)) return (indices as number[]).includes(idx);
  }
  return false;
}

function walkSourceViewChain(headId: string, idx: number, edges: Edge[], byId: Map<string, Node>): string[] {
  const names: string[] = [];
  let cur: string | null = headId;
  const visited = new Set<string>();
  while (cur && !visited.has(cur)) {
    visited.add(cur);
    const node = byId.get(cur);
    if (!node || !svNodeApplies(node, idx)) break;
    const nm = ruleLabel(node);
    if (nm) names.push(nm);
    const outs = edges
      .filter((e) => e.source === cur)
      .filter(
        (e) =>
          isChainEdge(e) &&
          byId.get(e.target)?.type === "discoveryMatchValidationRuleSourceView" &&
          svNodeApplies(byId.get(e.target)!, idx)
      );
    if (outs.length === 0) break;
    outs.sort((a, b) => a.target.localeCompare(b.target));
    cur = outs[0]!.target;
  }
  return names;
}

function globalKeValidationNames(scope: Record<string, unknown>): string[] {
  const ke = scope.key_extraction as Record<string, unknown> | undefined;
  const data = ke?.config as Record<string, unknown> | undefined;
  const d = data?.data as Record<string, unknown> | undefined;
  return resolveConfidenceMatchRuleNames(d?.validation, scope);
}

function globalAliasingValidationNames(scope: Record<string, unknown>): string[] {
  const al = scope.aliasing as Record<string, unknown> | undefined;
  const data = al?.config as Record<string, unknown> | undefined;
  const d = data?.data as Record<string, unknown> | undefined;
  return resolveConfidenceMatchRuleNames(d?.validation, scope);
}

function getSourceViewValidationNames(scope: Record<string, unknown>, index: number): string[] {
  const svs = scope.source_views;
  if (!Array.isArray(svs) || index < 0 || index >= svs.length) return [];
  const row = svs[index];
  if (!row || typeof row !== "object" || Array.isArray(row)) return [];
  return resolveConfidenceMatchRuleNames((row as Record<string, unknown>).validation, scope);
}

function hasDataEdgeBetween(edges: Edge[], fromId: string, toId: string): boolean {
  return edges.some((e) => e.source === fromId && e.target === toId && isDataEdge(e));
}

export type ValidationRuleLayoutReuseResult =
  | { action: "reuse"; headId: string; connectFromId?: string }
  | { action: "create" };

export function validationRuleLayoutReuseOnDrop(
  payload: PaletteDragPayload,
  position: { x: number; y: number },
  nodes: Node[],
  edges: Edge[],
  scopeDoc: Record<string, unknown>
): ValidationRuleLayoutReuseResult {
  if (payload.kind === "match_definition") return { action: "create" };
  if (payload.kind !== "structural") return { action: "create" };
  const nk = payload.nodeKind;
  if (
    nk !== "match_validation_extraction" &&
    nk !== "match_validation_aliasing" &&
    nk !== "match_validation_source_view"
  ) {
    return { action: "create" };
  }

  const byId = new Map(nodes.map((n) => [n.id, n]));

  if (nk === "match_validation_extraction") {
    const anchor = nearestNode(position, nodes, new Set(["discoveryTransform", "discoveryViewQuery", "discoveryJoin"]));
    if (!anchor) return { action: "create" };
    const expected = globalKeValidationNames(scopeDoc);
    if (expected.length === 0) return { action: "create" };
    for (const e of edges) {
      if (e.source !== anchor.id || !isDataEdge(e)) continue;
      const t = byId.get(e.target);
      if (t?.type !== "discoveryMatchValidationRuleExtraction") continue;
      const tr = (t.data as Record<string, unknown> | undefined)?.ref;
      if (!refBool(tr, "extraction_global_validation")) continue;
      const chain = walkGlobalMatchChain(t.id, "discoveryMatchValidationRuleExtraction", "extraction_global_validation", edges, byId);
      if (!listsEqual(chain, expected)) continue;
      return {
        action: "reuse",
        headId: t.id,
        connectFromId: hasDataEdgeBetween(edges, anchor.id, t.id) ? undefined : anchor.id,
      };
    }
    for (const n of nodes) {
      if (n.type !== "discoveryMatchValidationRuleExtraction") continue;
      const tr = (n.data as Record<string, unknown> | undefined)?.ref;
      if (!refBool(tr, "extraction_global_validation")) continue;
      const chain = walkGlobalMatchChain(n.id, "discoveryMatchValidationRuleExtraction", "extraction_global_validation", edges, byId);
      if (!listsEqual(chain, expected)) continue;
      return {
        action: "reuse",
        headId: n.id,
        connectFromId: hasDataEdgeBetween(edges, anchor.id, n.id) ? undefined : anchor.id,
      };
    }
    return { action: "create" };
  }

  if (nk === "match_validation_aliasing") {
    const anchor = nearestNode(position, nodes, new Set(["discoveryTransform", "discoveryAliasPersistence"]));
    if (!anchor) return { action: "create" };
    const expected = globalAliasingValidationNames(scopeDoc);
    if (expected.length === 0) return { action: "create" };
    for (const e of edges) {
      if (e.source !== anchor.id || !isDataEdge(e)) continue;
      const t = byId.get(e.target);
      if (t?.type !== "discoveryMatchValidationRuleAliasing") continue;
      const tr = (t.data as Record<string, unknown> | undefined)?.ref;
      if (!refBool(tr, "aliasing_global_validation")) continue;
      const chain = walkGlobalMatchChain(t.id, "discoveryMatchValidationRuleAliasing", "aliasing_global_validation", edges, byId);
      if (!listsEqual(chain, expected)) continue;
      return {
        action: "reuse",
        headId: t.id,
        connectFromId: hasDataEdgeBetween(edges, anchor.id, t.id) ? undefined : anchor.id,
      };
    }
    for (const n of nodes) {
      if (n.type !== "discoveryMatchValidationRuleAliasing") continue;
      const tr = (n.data as Record<string, unknown> | undefined)?.ref;
      if (!refBool(tr, "aliasing_global_validation")) continue;
      const chain = walkGlobalMatchChain(n.id, "discoveryMatchValidationRuleAliasing", "aliasing_global_validation", edges, byId);
      if (!listsEqual(chain, expected)) continue;
      return {
        action: "reuse",
        headId: n.id,
        connectFromId: hasDataEdgeBetween(edges, anchor.id, n.id) ? undefined : anchor.id,
      };
    }
    return { action: "create" };
  }

  const svAnchor = nearestNode(position, nodes, new Set(["discoverySourceView"]));
  if (!svAnchor) return { action: "create" };
  const ref = (svAnchor.data as Record<string, unknown> | undefined)?.ref;
  const svIx = refNum(ref, "source_view_index");
  if (svIx === undefined) return { action: "create" };
  const idx = Math.floor(svIx);
  const expected = getSourceViewValidationNames(scopeDoc, idx);
  if (expected.length === 0) return { action: "create" };

  for (const e of edges) {
    if (e.source !== svAnchor.id || !isDataEdge(e)) continue;
    const t = byId.get(e.target);
    if (!t || !svNodeApplies(t, idx)) continue;
    const chain = walkSourceViewChain(t.id, idx, edges, byId);
    if (!listsEqual(chain, expected)) continue;
    return {
      action: "reuse",
      headId: t.id,
      connectFromId: hasDataEdgeBetween(edges, svAnchor.id, t.id) ? undefined : svAnchor.id,
    };
  }
  for (const n of nodes) {
    if (!svNodeApplies(n, idx)) continue;
    const chain = walkSourceViewChain(n.id, idx, edges, byId);
    if (!listsEqual(chain, expected)) continue;
    return {
      action: "reuse",
      headId: n.id,
      connectFromId: hasDataEdgeBetween(edges, svAnchor.id, n.id) ? undefined : svAnchor.id,
    };
  }
  return { action: "create" };
}
