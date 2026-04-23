import type { Edge, Node } from "@xyflow/react";
import type { FlowEdgeData } from "./flowDocumentBridge";
import type { PaletteDragPayload } from "./FlowPalette";
import { resolveConfidenceMatchRuleNames } from "../../utils/confidenceMatchRuleNames";
import { getAliasingTransformRuleRows } from "./aliasingScopeData";

/** Squared distance threshold in flow coordinates (same units as node positions). */
const MAX_REUSE_DIST_SQ = 520 * 520;

function distSq(
  a: { x: number; y: number },
  b: { x: number; y: number }
): number {
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

function refStr(ref: unknown, key: string): string | undefined {
  if (!ref || typeof ref !== "object" || Array.isArray(ref)) return undefined;
  const v = (ref as Record<string, unknown>)[key];
  return v != null && String(v).trim() ? String(v).trim() : undefined;
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

function nearestNode(
  pos: { x: number; y: number },
  nodes: Node[],
  types: Set<string>
): Node | null {
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

function extractionHeadAcceptsRule(target: Node, ruleName: string): boolean {
  if (target.type !== "keaMatchValidationRuleExtraction") return false;
  const tr = (target.data as Record<string, unknown> | undefined)?.ref;
  if (refBool(tr, "extraction_global_validation")) return false;
  if (refStr(tr, "extraction_rule_name") === ruleName) return true;
  if (refBool(tr, "shared_extraction_validation_chain")) {
    const names = (tr as Record<string, unknown> | undefined)?.extraction_rule_names;
    if (Array.isArray(names)) return names.map(String).includes(ruleName);
  }
  return false;
}

function walkExtractionPerRuleChain(
  headId: string,
  edges: Edge[],
  byId: Map<string, Node>
): string[] {
  const names: string[] = [];
  let cur: string | null = headId;
  const visited = new Set<string>();
  while (cur && !visited.has(cur)) {
    visited.add(cur);
    const node = byId.get(cur);
    if (!node || node.type !== "keaMatchValidationRuleExtraction") break;
    const tr = (node.data as Record<string, unknown> | undefined)?.ref;
    if (refBool(tr, "extraction_global_validation")) break;
    const nm = ruleLabel(node);
    if (nm) names.push(nm);
    const outs = (edges.filter((e) => e.source === cur) as Edge[]).filter(
      (e) =>
        isChainEdge(e) &&
        byId.get(e.target)?.type === "keaMatchValidationRuleExtraction" &&
        !refBool(
          (byId.get(e.target)?.data as Record<string, unknown> | undefined)?.ref,
          "extraction_global_validation"
        )
    );
    if (outs.length === 0) break;
    outs.sort((a, b) => a.target.localeCompare(b.target));
    cur = outs[0]!.target;
  }
  return names;
}

function walkExtractionGlobalChain(headId: string, edges: Edge[], byId: Map<string, Node>): string[] {
  const names: string[] = [];
  let cur: string | null = headId;
  const visited = new Set<string>();
  while (cur && !visited.has(cur)) {
    visited.add(cur);
    const node = byId.get(cur);
    if (!node || node.type !== "keaMatchValidationRuleExtraction") break;
    const tr = (node.data as Record<string, unknown> | undefined)?.ref;
    if (!refBool(tr, "extraction_global_validation")) break;
    const nm = ruleLabel(node);
    if (nm) names.push(nm);
    const outs = edges
      .filter((e) => e.source === cur)
      .filter(
        (e) =>
          isChainEdge(e) &&
          byId.get(e.target)?.type === "keaMatchValidationRuleExtraction" &&
          refBool(
            (byId.get(e.target)?.data as Record<string, unknown> | undefined)?.ref,
            "extraction_global_validation"
          )
      );
    if (outs.length === 0) break;
    outs.sort((a, b) => a.target.localeCompare(b.target));
    cur = outs[0]!.target;
  }
  return names;
}

function walkAliasingPerRuleChain(headId: string, edges: Edge[], byId: Map<string, Node>): string[] {
  const names: string[] = [];
  let cur: string | null = headId;
  const visited = new Set<string>();
  while (cur && !visited.has(cur)) {
    visited.add(cur);
    const node = byId.get(cur);
    if (!node || node.type !== "keaMatchValidationRuleAliasing") break;
    const tr = (node.data as Record<string, unknown> | undefined)?.ref;
    if (refBool(tr, "aliasing_global_validation")) break;
    const nm = ruleLabel(node);
    if (nm) names.push(nm);
    const outs = edges
      .filter((e) => e.source === cur)
      .filter(
        (e) =>
          isChainEdge(e) &&
          byId.get(e.target)?.type === "keaMatchValidationRuleAliasing" &&
          !refBool(
            (byId.get(e.target)?.data as Record<string, unknown> | undefined)?.ref,
            "aliasing_global_validation"
          )
      );
    if (outs.length === 0) break;
    outs.sort((a, b) => a.target.localeCompare(b.target));
    cur = outs[0]!.target;
  }
  return names;
}

function walkAliasingGlobalChain(headId: string, edges: Edge[], byId: Map<string, Node>): string[] {
  const names: string[] = [];
  let cur: string | null = headId;
  const visited = new Set<string>();
  while (cur && !visited.has(cur)) {
    visited.add(cur);
    const node = byId.get(cur);
    if (!node || node.type !== "keaMatchValidationRuleAliasing") break;
    const tr = (node.data as Record<string, unknown> | undefined)?.ref;
    if (!refBool(tr, "aliasing_global_validation")) break;
    const nm = ruleLabel(node);
    if (nm) names.push(nm);
    const outs = edges
      .filter((e) => e.source === cur)
      .filter(
        (e) =>
          isChainEdge(e) &&
          byId.get(e.target)?.type === "keaMatchValidationRuleAliasing" &&
          refBool(
            (byId.get(e.target)?.data as Record<string, unknown> | undefined)?.ref,
            "aliasing_global_validation"
          )
      );
    if (outs.length === 0) break;
    outs.sort((a, b) => a.target.localeCompare(b.target));
    cur = outs[0]!.target;
  }
  return names;
}

function svNodeApplies(node: Node, idx: number): boolean {
  if (node.type !== "keaMatchValidationRuleSourceView") return false;
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
          byId.get(e.target)?.type === "keaMatchValidationRuleSourceView" &&
          svNodeApplies(byId.get(e.target)!, idx)
      );
    if (outs.length === 0) break;
    outs.sort((a, b) => a.target.localeCompare(b.target));
    cur = outs[0]!.target;
  }
  return names;
}

function aliasingHeadAcceptsRule(target: Node, ruleName: string): boolean {
  if (target.type !== "keaMatchValidationRuleAliasing") return false;
  const tr = (target.data as Record<string, unknown> | undefined)?.ref;
  if (refBool(tr, "aliasing_global_validation")) return false;
  if (refStr(tr, "aliasing_rule_name") === ruleName) return true;
  if (refBool(tr, "shared_aliasing_validation_chain")) {
    const names = (tr as Record<string, unknown> | undefined)?.aliasing_rule_names;
    if (Array.isArray(names)) return names.map(String).includes(ruleName);
  }
  return false;
}

function svHeadAccepts(target: Node, idx: number): boolean {
  if (target.type !== "keaMatchValidationRuleSourceView") return false;
  return svNodeApplies(target, idx);
}

function getExtractionRuleValidationNames(
  scope: Record<string, unknown>,
  ruleName: string
): string[] {
  const ke = scope.key_extraction as Record<string, unknown> | undefined;
  const data = ke?.config as Record<string, unknown> | undefined;
  const d = data?.data as Record<string, unknown> | undefined;
  const rules = d?.extraction_rules;
  if (!Array.isArray(rules)) return [];
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    if (String(row.name ?? "").trim() === ruleName) {
      return resolveConfidenceMatchRuleNames(row.validation, scope);
    }
  }
  return [];
}

function globalKeValidationNames(scope: Record<string, unknown>): string[] {
  const ke = scope.key_extraction as Record<string, unknown> | undefined;
  const data = ke?.config as Record<string, unknown> | undefined;
  const d = data?.data as Record<string, unknown> | undefined;
  return resolveConfidenceMatchRuleNames(d?.validation, scope);
}

function getAliasingRuleValidationNames(scope: Record<string, unknown>, ruleName: string): string[] {
  const al = scope.aliasing as Record<string, unknown> | undefined;
  const data = al?.config as Record<string, unknown> | undefined;
  const d = data?.data as Record<string, unknown> | undefined;
  const rules = d ? getAliasingTransformRuleRows(d) : [];
  if (!Array.isArray(rules)) return [];
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    if (String(row.name ?? "").trim() === ruleName) {
      return resolveConfidenceMatchRuleNames(row.validation, scope);
    }
  }
  return [];
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

/**
 * When dropping a structural validation-rule layout node from the palette, reuse an existing
 * chain head if scope defines the same ordered rule list and the canvas already has that chain.
 */
export function validationRuleLayoutReuseOnDrop(
  payload: PaletteDragPayload,
  position: { x: number; y: number },
  nodes: Node[],
  edges: Edge[],
  scopeDoc: Record<string, unknown>
): ValidationRuleLayoutReuseResult {
  if (payload.kind === "match_definition") {
    return { action: "create" };
  }
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
    const anchor = nearestNode(position, nodes, new Set(["keaExtraction"]));
    if (!anchor) return { action: "create" };
    const ref = (anchor.data as Record<string, unknown> | undefined)?.ref;
    const ruleName = refStr(ref, "extraction_rule_name");

    if (ruleName) {
      const expected = getExtractionRuleValidationNames(scopeDoc, ruleName);
      if (expected.length === 0) {
        const g = globalKeValidationNames(scopeDoc);
        if (g.length === 0) return { action: "create" };
        for (const e of edges) {
          if (e.source !== anchor.id || !isDataEdge(e)) continue;
          const t = byId.get(e.target);
          if (t?.type !== "keaMatchValidationRuleExtraction") continue;
          const tr = (t.data as Record<string, unknown> | undefined)?.ref;
          if (!refBool(tr, "extraction_global_validation")) continue;
          const chain = walkExtractionGlobalChain(t.id, edges, byId);
          if (!listsEqual(chain, g)) continue;
          const connectFrom = hasDataEdgeBetween(edges, anchor.id, t.id) ? undefined : anchor.id;
          return { action: "reuse", headId: t.id, connectFromId: connectFrom };
        }
        for (const n of nodes) {
          if (n.type !== "keaMatchValidationRuleExtraction") continue;
          const tr = (n.data as Record<string, unknown> | undefined)?.ref;
          if (!refBool(tr, "extraction_global_validation")) continue;
          const chain = walkExtractionGlobalChain(n.id, edges, byId);
          if (!listsEqual(chain, g)) continue;
          const connectFrom = hasDataEdgeBetween(edges, anchor.id, n.id) ? undefined : anchor.id;
          return { action: "reuse", headId: n.id, connectFromId: connectFrom };
        }
        return { action: "create" };
      }

      for (const e of edges) {
        if (e.source !== anchor.id || !isDataEdge(e)) continue;
        const t = byId.get(e.target);
        if (!t || !extractionHeadAcceptsRule(t, ruleName)) continue;
        const chain = walkExtractionPerRuleChain(t.id, edges, byId);
        if (!listsEqual(chain, expected)) continue;
        const connectFrom = hasDataEdgeBetween(edges, anchor.id, t.id) ? undefined : anchor.id;
        return { action: "reuse", headId: t.id, connectFromId: connectFrom };
      }
      for (const n of nodes) {
        if (!extractionHeadAcceptsRule(n, ruleName)) continue;
        const chain = walkExtractionPerRuleChain(n.id, edges, byId);
        if (!listsEqual(chain, expected)) continue;
        const connectFrom = hasDataEdgeBetween(edges, anchor.id, n.id) ? undefined : anchor.id;
        return { action: "reuse", headId: n.id, connectFromId: connectFrom };
      }
      return { action: "create" };
    }

    const g = globalKeValidationNames(scopeDoc);
    if (g.length === 0) return { action: "create" };
    for (const e of edges) {
      if (e.source !== anchor.id || !isDataEdge(e)) continue;
      const t = byId.get(e.target);
      if (t?.type !== "keaMatchValidationRuleExtraction") continue;
      const tr = (t.data as Record<string, unknown> | undefined)?.ref;
      if (!refBool(tr, "extraction_global_validation")) continue;
      const chain = walkExtractionGlobalChain(t.id, edges, byId);
      if (!listsEqual(chain, g)) continue;
      const connectFrom = hasDataEdgeBetween(edges, anchor.id, t.id) ? undefined : anchor.id;
      return { action: "reuse", headId: t.id, connectFromId: connectFrom };
    }
    for (const n of nodes) {
      if (n.type !== "keaMatchValidationRuleExtraction") continue;
      const tr = (n.data as Record<string, unknown> | undefined)?.ref;
      if (!refBool(tr, "extraction_global_validation")) continue;
      const chain = walkExtractionGlobalChain(n.id, edges, byId);
      if (!listsEqual(chain, g)) continue;
      const connectFrom = hasDataEdgeBetween(edges, anchor.id, n.id) ? undefined : anchor.id;
      return { action: "reuse", headId: n.id, connectFromId: connectFrom };
    }
    return { action: "create" };
  }

  if (nk === "match_validation_aliasing") {
    const anchor = nearestNode(position, nodes, new Set(["keaAliasing"]));
    if (!anchor) return { action: "create" };
    const ref = (anchor.data as Record<string, unknown> | undefined)?.ref;
    const ruleName = refStr(ref, "aliasing_rule_name");
    if (ruleName) {
      const expected = getAliasingRuleValidationNames(scopeDoc, ruleName);
      if (expected.length === 0) {
        const g = globalAliasingValidationNames(scopeDoc);
        if (g.length === 0) return { action: "create" };
        for (const e of edges) {
          if (e.source !== anchor.id || !isDataEdge(e)) continue;
          const t = byId.get(e.target);
          if (t?.type !== "keaMatchValidationRuleAliasing") continue;
          const tr = (t.data as Record<string, unknown> | undefined)?.ref;
          if (!refBool(tr, "aliasing_global_validation")) continue;
          const chain = walkAliasingGlobalChain(t.id, edges, byId);
          if (!listsEqual(chain, g)) continue;
          const connectFrom = hasDataEdgeBetween(edges, anchor.id, t.id) ? undefined : anchor.id;
          return { action: "reuse", headId: t.id, connectFromId: connectFrom };
        }
        for (const n of nodes) {
          if (n.type !== "keaMatchValidationRuleAliasing") continue;
          const tr = (n.data as Record<string, unknown> | undefined)?.ref;
          if (!refBool(tr, "aliasing_global_validation")) continue;
          const chain = walkAliasingGlobalChain(n.id, edges, byId);
          if (!listsEqual(chain, g)) continue;
          const connectFrom = hasDataEdgeBetween(edges, anchor.id, n.id) ? undefined : anchor.id;
          return { action: "reuse", headId: n.id, connectFromId: connectFrom };
        }
        return { action: "create" };
      }
      for (const e of edges) {
        if (e.source !== anchor.id || !isDataEdge(e)) continue;
        const t = byId.get(e.target);
        if (!t || !aliasingHeadAcceptsRule(t, ruleName)) continue;
        const chain = walkAliasingPerRuleChain(t.id, edges, byId);
        if (!listsEqual(chain, expected)) continue;
        const connectFrom = hasDataEdgeBetween(edges, anchor.id, t.id) ? undefined : anchor.id;
        return { action: "reuse", headId: t.id, connectFromId: connectFrom };
      }
      for (const n of nodes) {
        if (!aliasingHeadAcceptsRule(n, ruleName)) continue;
        const chain = walkAliasingPerRuleChain(n.id, edges, byId);
        if (!listsEqual(chain, expected)) continue;
        const connectFrom = hasDataEdgeBetween(edges, anchor.id, n.id) ? undefined : anchor.id;
        return { action: "reuse", headId: n.id, connectFromId: connectFrom };
      }
      return { action: "create" };
    }
    const g = globalAliasingValidationNames(scopeDoc);
    if (g.length === 0) return { action: "create" };
    for (const e of edges) {
      if (e.source !== anchor.id || !isDataEdge(e)) continue;
      const t = byId.get(e.target);
      if (t?.type !== "keaMatchValidationRuleAliasing") continue;
      const tr = (t.data as Record<string, unknown> | undefined)?.ref;
      if (!refBool(tr, "aliasing_global_validation")) continue;
      const chain = walkAliasingGlobalChain(t.id, edges, byId);
      if (!listsEqual(chain, g)) continue;
      const connectFrom = hasDataEdgeBetween(edges, anchor.id, t.id) ? undefined : anchor.id;
      return { action: "reuse", headId: t.id, connectFromId: connectFrom };
    }
    for (const n of nodes) {
      if (n.type !== "keaMatchValidationRuleAliasing") continue;
      const tr = (n.data as Record<string, unknown> | undefined)?.ref;
      if (!refBool(tr, "aliasing_global_validation")) continue;
      const chain = walkAliasingGlobalChain(n.id, edges, byId);
      if (!listsEqual(chain, g)) continue;
      const connectFrom = hasDataEdgeBetween(edges, anchor.id, n.id) ? undefined : anchor.id;
      return { action: "reuse", headId: n.id, connectFromId: connectFrom };
    }
    return { action: "create" };
  }

  const svAnchor = nearestNode(position, nodes, new Set(["keaSourceView"]));
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
    if (!t || !svHeadAccepts(t, idx)) continue;
    const chain = walkSourceViewChain(t.id, idx, edges, byId);
    if (!listsEqual(chain, expected)) continue;
    const connectFrom = hasDataEdgeBetween(edges, svAnchor.id, t.id) ? undefined : svAnchor.id;
    return { action: "reuse", headId: t.id, connectFromId: connectFrom };
  }
  for (const n of nodes) {
    if (!svHeadAccepts(n, idx)) continue;
    const chain = walkSourceViewChain(n.id, idx, edges, byId);
    if (!listsEqual(chain, expected)) continue;
    const connectFrom = hasDataEdgeBetween(edges, svAnchor.id, n.id) ? undefined : svAnchor.id;
    return { action: "reuse", headId: n.id, connectFromId: connectFrom };
  }
  return { action: "create" };
}
