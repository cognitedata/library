import type { JsonObject } from "../../types/scopeConfig";
import type {
  WorkflowCanvasDocument,
  WorkflowCanvasEdge,
  WorkflowCanvasNode,
} from "../../types/workflowCanvas";
import { patchExtractionRuleAliasingPipeline } from "./workflowScopePatch";
/**
 * Merge flow canvas structure into the workflow scope document (YAML model).
 *
 * Wires `confidence_match_rules` from:
 * - data edges: parent rule / source view → first match-rule node (extraction may also wire to a global
 *   `match_validation_aliasing` head alongside extraction match chains)
 * - sequence edges: parent→child chain as **nested** ordered hierarchies (not flat sibling lists)
 * - parallel_group edges: `{ hierarchy: { mode: concurrent, children } }` with recursive subtrees
 *
 * Also reorders `aliasing.config.data.aliasing_rules` using the **same composition edges** as validation
 * (`sequence` / `parallel_group` between `kind: "aliasing"` nodes; not plain `data` edges).
 *
 * Writes `key_extraction.config.data.extraction_rules[].aliasing_pipeline` from **data** edges
 * (extraction → first `kind: "aliasing"` node(s)) plus composition edges among aliasing nodes — the
 * tag-aliasing engine routes transforms per extraction rule using this field (not global `aliasing_rules` order).
 */
export function syncWorkflowScopeFromCanvas(
  canvas: WorkflowCanvasDocument,
  scopeDoc: Record<string, unknown>
): Record<string, unknown> {
  const wired = applyCanvasMatchWiring(canvas, scopeDoc);
  const withPipelines = applyExtractionAliasingPipelinesFromCanvas(canvas, wired);
  return applyAliasingRulesOrderFromCanvas(canvas, withPipelines);
}

function isDataEdge(e: WorkflowCanvasEdge): boolean {
  return e.kind !== "sequence" && e.kind !== "parallel_group";
}

function ruleNameFromMatchNode(n: WorkflowCanvasNode): string | null {
  const c = n.data.confidence_match_rule_name;
  return c != null && String(c).trim() ? String(c).trim() : null;
}

function nodesById(canvas: WorkflowCanvasDocument): Map<string, WorkflowCanvasNode> {
  return new Map(canvas.nodes.map((n) => [n.id, n]));
}

function buildOutgoing(canvas: WorkflowCanvasDocument): Map<string, WorkflowCanvasEdge[]> {
  const m = new Map<string, WorkflowCanvasEdge[]>();
  for (const e of canvas.edges) {
    const list = m.get(e.source) ?? [];
    list.push(e);
    m.set(e.source, list);
  }
  return m;
}

function hierarchyOrdered(children: unknown[]): JsonObject {
  return { hierarchy: { mode: "ordered", children } };
}

function hierarchyConcurrent(children: unknown[]): JsonObject {
  return { hierarchy: { mode: "concurrent", children } };
}

/**
 * Right-nested shorthand so the last rule is a "child" of the previous:
 * `[a,b]` → `{ a: [b] }`, `[a,b,c]` → `{ a: [ { b: [c] } ] }` (same evaluation order as a flat list).
 */
function linearChainToShorthand(names: string[]): unknown {
  if (names.length === 0) return null;
  if (names.length === 1) return names[0]!;
  const head = names[0]!;
  const rest = names.slice(1);
  const nested = linearChainToShorthand(rest);
  const tailArr: unknown[] =
    nested !== null && typeof nested === "object" && !Array.isArray(nested) ? [nested] : [nested as string];
  return { [head]: tailArr };
}

/** Collapse a linear list of rule-id strings into one nested tree; pass through mixed / single values. */
function shapeMatchStepsLinearOne(raw: unknown[]): unknown | null {
  if (raw.length === 0) return null;
  if (raw.every((x) => typeof x === "string")) {
    const names = raw as string[];
    if (names.length === 1) return names[0]!;
    return linearChainToShorthand(names);
  }
  if (raw.length === 1) return raw[0]!;
  return hierarchyOrdered(raw);
}

function stepsFromShapedLinear(raw: unknown[]): unknown[] {
  const one = shapeMatchStepsLinearOne(raw);
  return one === null ? [] : [one];
}

function sortHeadIdsByRuleName(heads: string[], byId: Map<string, WorkflowCanvasNode>): void {
  heads.sort((a, b) => {
    const na = ruleNameFromMatchNode(byId.get(a)!) ?? a;
    const nb = ruleNameFromMatchNode(byId.get(b)!) ?? b;
    return na.localeCompare(nb);
  });
}

function mergeValidation(
  prev: unknown,
  ruleIds: string[],
  orderMode: "priority" | "sequential"
): JsonObject {
  const base =
    prev !== null && typeof prev === "object" && !Array.isArray(prev)
      ? { ...(prev as Record<string, unknown>) }
      : {};
  if (orderMode === "priority" && ruleIds.length > 0) {
    base.confidence_match_rules = [hierarchyConcurrent(ruleIds)];
  } else if (ruleIds.length > 1) {
    base.confidence_match_rules = [hierarchyOrdered(ruleIds)];
  } else {
    base.confidence_match_rules = ruleIds;
  }
  return base as JsonObject;
}

/** Writes a hierarchical `confidence_match_rules` list (strings + `hierarchy` groups). */
function mergeValidationNested(prev: unknown, steps: unknown[]): JsonObject {
  const base =
    prev !== null && typeof prev === "object" && !Array.isArray(prev)
      ? { ...(prev as Record<string, unknown>) }
      : {};
  base.confidence_match_rules = steps;
  return base as JsonObject;
}

type ChainTargetAccept = (n: WorkflowCanvasNode | undefined) => boolean;

/** Match-rule chains use `sequence` or `parallel_group` edges (not plain `data`). */
function edgeCompositionKind(e: WorkflowCanvasEdge): "sequence" | "parallel_group" | null {
  if (e.kind === "parallel_group") return "parallel_group";
  if (e.kind === "sequence") return "sequence";
  return null;
}

function partitionChainOut(
  nodeId: string,
  outgoing: Map<string, WorkflowCanvasEdge[]>,
  byId: Map<string, WorkflowCanvasNode>,
  acceptTarget: ChainTargetAccept
): { seq: WorkflowCanvasEdge[]; par: WorkflowCanvasEdge[] } {
  const outs = (outgoing.get(nodeId) ?? []).filter((e) => {
    const comp = edgeCompositionKind(e);
    return comp != null && acceptTarget(byId.get(e.target));
  });
  const seq = outs.filter((e) => edgeCompositionKind(e) === "sequence");
  const par = outs.filter((e) => edgeCompositionKind(e) === "parallel_group");
  return { seq, par };
}

function sortEdgesByTarget(edges: WorkflowCanvasEdge[]): WorkflowCanvasEdge[] {
  return [...edges].sort((a, b) => a.target.localeCompare(b.target));
}

/**
 * Serialize match-rule subgraph as hierarchical steps. `sequence` = next top-level step;
 * `parallel_group` = concurrent `hierarchy` (subtrees use ordered `hierarchy` when linear).
 */
function buildMatchSubtree(
  nodeId: string,
  byId: Map<string, WorkflowCanvasNode>,
  outgoing: Map<string, WorkflowCanvasEdge[]>,
  acceptTarget: ChainTargetAccept,
  visited: Set<string>
): unknown {
  const parts: unknown[] = [];
  let cur: string | null = nodeId;
  while (cur) {
    if (visited.has(cur)) break;
    visited.add(cur);
    const n = byId.get(cur);
    if (!n || !acceptTarget(n)) break;
    const nm = ruleNameFromMatchNode(n);
    const { seq, par } = partitionChainOut(cur, outgoing, byId, acceptTarget);
    if (par.length > 0) {
      if (nm) parts.push(nm);
      parts.push(
        hierarchyConcurrent(
          sortEdgesByTarget(par).map((e) =>
            buildMatchSubtree(e.target, byId, outgoing, acceptTarget, visited)
          )
        )
      );
      if (seq.length === 1) {
        cur = seq[0]!.target;
        continue;
      }
      break;
    }
    if (nm) parts.push(nm);
    if (seq.length === 1) {
      cur = seq[0]!.target;
      continue;
    }
    break;
  }
  if (parts.length === 0) return null;
  if (parts.length === 1) return parts[0];
  if (parts.every((p) => typeof p === "string")) {
    return linearChainToShorthand(parts as string[]);
  }
  return hierarchyOrdered(parts);
}

function buildMatchTopLevelSteps(
  headId: string,
  byId: Map<string, WorkflowCanvasNode>,
  outgoing: Map<string, WorkflowCanvasEdge[]>,
  acceptTarget: ChainTargetAccept
): unknown[] {
  const steps: unknown[] = [];
  const visited = new Set<string>();
  let cur: string | null = headId;
  while (cur) {
    if (visited.has(cur)) break;
    visited.add(cur);
    const n = byId.get(cur);
    if (!n || !acceptTarget(n)) break;
    const nm = ruleNameFromMatchNode(n);
    const { seq, par } = partitionChainOut(cur, outgoing, byId, acceptTarget);
    if (par.length > 0) {
      if (nm) steps.push(nm);
      steps.push(
        hierarchyConcurrent(
          sortEdgesByTarget(par).map((e) =>
            buildMatchSubtree(e.target, byId, outgoing, acceptTarget, visited)
          )
        )
      );
      if (seq.length === 1) {
        cur = seq[0]!.target;
        continue;
      }
      break;
    }
    if (nm) steps.push(nm);
    if (seq.length === 1) {
      cur = seq[0]!.target;
      continue;
    }
    break;
  }
  return steps;
}

/** Rule label for `kind: "aliasing"` canvas nodes (`ref.aliasing_rule_name`). */
function aliasingRuleNameFromNode(n: WorkflowCanvasNode | undefined): string | null {
  if (!n || n.kind !== "aliasing") return null;
  const ref = n.data.ref;
  if (!ref || typeof ref !== "object" || Array.isArray(ref)) return null;
  const v = (ref as Record<string, unknown>).aliasing_rule_name;
  return v != null && String(v).trim() ? String(v).trim() : null;
}

const acceptAliasingCompositionNode: ChainTargetAccept = (tn) =>
  Boolean(tn && tn.kind === "aliasing" && aliasingRuleNameFromNode(tn));

/** Same structure as `buildMatchSubtree` / `buildMatchTopLevelSteps` but for `kind: "aliasing"` transform nodes. */
function buildAliasingTransformSubtree(
  nodeId: string,
  byId: Map<string, WorkflowCanvasNode>,
  outgoing: Map<string, WorkflowCanvasEdge[]>,
  visited: Set<string>
): unknown {
  const parts: unknown[] = [];
  let cur: string | null = nodeId;
  while (cur) {
    if (visited.has(cur)) break;
    visited.add(cur);
    const n = byId.get(cur);
    if (!n || !acceptAliasingCompositionNode(n)) break;
    const nm = aliasingRuleNameFromNode(n);
    const { seq, par } = partitionChainOut(cur, outgoing, byId, acceptAliasingCompositionNode);
    if (par.length > 0) {
      if (nm) parts.push(nm);
      parts.push(
        hierarchyConcurrent(
          sortEdgesByTarget(par).map((e) =>
            buildAliasingTransformSubtree(e.target, byId, outgoing, visited)
          )
        )
      );
      if (seq.length === 1) {
        cur = seq[0]!.target;
        continue;
      }
      break;
    }
    if (nm) parts.push(nm);
    if (seq.length === 1) {
      cur = seq[0]!.target;
      continue;
    }
    break;
  }
  if (parts.length === 0) return null;
  if (parts.length === 1) return parts[0];
  if (parts.every((p) => typeof p === "string")) {
    return linearChainToShorthand(parts as string[]);
  }
  return hierarchyOrdered(parts);
}

function buildAliasingTransformTopLevelSteps(
  headId: string,
  byId: Map<string, WorkflowCanvasNode>,
  outgoing: Map<string, WorkflowCanvasEdge[]>
): unknown[] {
  const steps: unknown[] = [];
  const visited = new Set<string>();
  let cur: string | null = headId;
  while (cur) {
    if (visited.has(cur)) break;
    visited.add(cur);
    const n = byId.get(cur);
    if (!n || !acceptAliasingCompositionNode(n)) break;
    const nm = aliasingRuleNameFromNode(n);
    const { seq, par } = partitionChainOut(cur, outgoing, byId, acceptAliasingCompositionNode);
    if (par.length > 0) {
      if (nm) steps.push(nm);
      steps.push(
        hierarchyConcurrent(
          sortEdgesByTarget(par).map((e) =>
            buildAliasingTransformSubtree(e.target, byId, outgoing, visited)
          )
        )
      );
      if (seq.length === 1) {
        cur = seq[0]!.target;
        continue;
      }
      break;
    }
    if (nm) steps.push(nm);
    if (seq.length === 1) {
      cur = seq[0]!.target;
      continue;
    }
    break;
  }
  return steps;
}

/**
 * Persist per-extraction `aliasing_pipeline` from extraction → aliasing **data** edges and
 * aliasing composition edges (matches engine `extraction_aliasing_pipelines`).
 */
function applyExtractionAliasingPipelinesFromCanvas(
  canvas: WorkflowCanvasDocument,
  scopeDoc: Record<string, unknown>
): Record<string, unknown> {
  const byId = nodesById(canvas);
  const outgoing = buildOutgoing(canvas);
  let doc: Record<string, unknown> = { ...scopeDoc };

  for (const n of canvas.nodes) {
    if (n.kind !== "extraction") continue;
    const ruleName = refStr(n.data.ref, "extraction_rule_name");
    if (!ruleName) continue;

    const heads: string[] = [];
    for (const e of outgoing.get(n.id) ?? []) {
      if (!isDataEdge(e)) continue;
      const t = byId.get(e.target);
      if (!t || !acceptAliasingCompositionNode(t)) continue;
      heads.push(e.target);
    }
    sortAliasingHeadIdsByRuleName(heads, byId);

    if (heads.length === 0) {
      doc = patchExtractionRuleAliasingPipeline(doc, ruleName, []);
      continue;
    }

    const topParts: unknown[] = [];
    for (const hid of heads) {
      const raw = buildAliasingTransformTopLevelSteps(hid, byId, outgoing);
      const one = shapeMatchStepsLinearOne(raw);
      if (one !== null) topParts.push(one);
    }

    let pipeline: unknown[];
    if (topParts.length === 0) {
      pipeline = [];
    } else if (topParts.length === 1) {
      pipeline = [topParts[0]!];
    } else {
      pipeline = [hierarchyConcurrent(topParts)];
    }

    doc = patchExtractionRuleAliasingPipeline(doc, ruleName, pipeline);
  }

  return doc;
}

function sortAliasingHeadIdsByRuleName(heads: string[], byId: Map<string, WorkflowCanvasNode>): void {
  heads.sort((a, b) => {
    const na = aliasingRuleNameFromNode(byId.get(a)) ?? a;
    const nb = aliasingRuleNameFromNode(byId.get(b)) ?? b;
    return na.localeCompare(nb);
  });
}

/** Mirrors `buildMatchSubtree` — flatten to rule names for `aliasing_rules[]` (validation uses nested trees). */
function flattenAliasingCompositionSubtree(
  nodeId: string,
  byId: Map<string, WorkflowCanvasNode>,
  outgoing: Map<string, WorkflowCanvasEdge[]>,
  visited: Set<string>
): string[] {
  const out: string[] = [];
  let cur: string | null = nodeId;
  while (cur) {
    if (visited.has(cur)) break;
    visited.add(cur);
    const n = byId.get(cur);
    if (!n || !acceptAliasingCompositionNode(n)) break;
    const nm = aliasingRuleNameFromNode(n);
    const { seq, par } = partitionChainOut(cur, outgoing, byId, acceptAliasingCompositionNode);
    if (par.length > 0) {
      if (nm) out.push(nm);
      for (const e of sortEdgesByTarget(par)) {
        out.push(...flattenAliasingCompositionSubtree(e.target, byId, outgoing, visited));
      }
      if (seq.length === 1) {
        cur = seq[0]!.target;
        continue;
      }
      break;
    }
    if (nm) out.push(nm);
    if (seq.length === 1) {
      cur = seq[0]!.target;
      continue;
    }
    break;
  }
  return out;
}

/** Mirrors `buildMatchTopLevelSteps` for aliasing transform nodes (sequence / parallel_group only). */
function flattenAliasingCompositionTopLevel(
  headId: string,
  byId: Map<string, WorkflowCanvasNode>,
  outgoing: Map<string, WorkflowCanvasEdge[]>,
  visited: Set<string>
): string[] {
  const out: string[] = [];
  let cur: string | null = headId;
  while (cur) {
    if (visited.has(cur)) break;
    visited.add(cur);
    const n = byId.get(cur);
    if (!n || !acceptAliasingCompositionNode(n)) break;
    const nm = aliasingRuleNameFromNode(n);
    const { seq, par } = partitionChainOut(cur, outgoing, byId, acceptAliasingCompositionNode);
    if (par.length > 0) {
      if (nm) out.push(nm);
      for (const e of sortEdgesByTarget(par)) {
        out.push(...flattenAliasingCompositionSubtree(e.target, byId, outgoing, visited));
      }
      if (seq.length === 1) {
        cur = seq[0]!.target;
        continue;
      }
      break;
    }
    if (nm) out.push(nm);
    if (seq.length === 1) {
      cur = seq[0]!.target;
      continue;
    }
    break;
  }
  return out;
}

/**
 * Ordered `aliasing_rule_name` values from composition edges among `kind: "aliasing"` nodes
 * (same `sequence` / `parallel_group` semantics as match-validation rule chains).
 */
function buildAliasingRulesOrderFromCanvas(canvas: WorkflowCanvasDocument): string[] | null {
  const byId = nodesById(canvas);
  const outgoing = buildOutgoing(canvas);

  const alIds = new Set<string>();
  for (const n of canvas.nodes) {
    if (acceptAliasingCompositionNode(n)) alIds.add(n.id);
  }
  if (alIds.size === 0) return null;

  let hasCompositionEdge = false;
  const incoming = new Map<string, number>();
  for (const id of alIds) incoming.set(id, 0);

  const seenEdge = new Set<string>();
  for (const e of canvas.edges) {
    if (!alIds.has(e.source) || !alIds.has(e.target)) continue;
    if (edgeCompositionKind(e) == null) continue;
    const k = `${e.source}\0${e.target}`;
    if (seenEdge.has(k)) continue;
    seenEdge.add(k);
    hasCompositionEdge = true;
    incoming.set(e.target, (incoming.get(e.target) ?? 0) + 1);
  }
  if (!hasCompositionEdge) return null;

  const heads: string[] = [];
  for (const id of alIds) {
    if ((incoming.get(id) ?? 0) === 0) heads.push(id);
  }
  sortAliasingHeadIdsByRuleName(heads, byId);

  const visited = new Set<string>();
  const orderedNames: string[] = [];
  for (const hid of heads) {
    orderedNames.push(...flattenAliasingCompositionTopLevel(hid, byId, outgoing, visited));
  }

  if (visited.size !== alIds.size) return null;

  return orderedNames;
}

function patchAliasingRulesArrayOrder(
  doc: Record<string, unknown>,
  orderedNames: string[]
): Record<string, unknown> {
  const al = doc.aliasing as Record<string, unknown> | undefined;
  if (!al || typeof al !== "object" || Array.isArray(al)) return doc;
  const config = al.config as Record<string, unknown> | undefined;
  if (!config || typeof config !== "object" || Array.isArray(config)) return doc;
  const data = config.data as Record<string, unknown> | undefined;
  if (!data || typeof data !== "object" || Array.isArray(data)) return doc;
  const rules = data.aliasing_rules;
  if (!Array.isArray(rules)) return doc;

  const byName = new Map<string, unknown>();
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    const nm = String(row.name ?? "").trim();
    if (nm && !byName.has(nm)) byName.set(nm, r);
  }

  const seen = new Set<string>();
  const nextRules: unknown[] = [];
  for (const n of orderedNames) {
    const nm = String(n ?? "").trim();
    if (!nm || seen.has(nm)) continue;
    const row = byName.get(nm);
    if (!row) continue;
    seen.add(nm);
    nextRules.push(row);
  }
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) {
      nextRules.push(r);
      continue;
    }
    const nm = String((r as Record<string, unknown>).name ?? "").trim();
    if (nm && seen.has(nm)) continue;
    nextRules.push(r);
  }

  return {
    ...doc,
    aliasing: {
      ...al,
      config: {
        ...config,
        data: {
          ...data,
          aliasing_rules: nextRules,
        },
      },
    },
  };
}

function applyAliasingRulesOrderFromCanvas(
  canvas: WorkflowCanvasDocument,
  scopeDoc: Record<string, unknown>
): Record<string, unknown> {
  const order = buildAliasingRulesOrderFromCanvas(canvas);
  if (!order || order.length === 0) return scopeDoc;
  return patchAliasingRulesArrayOrder(scopeDoc, order);
}

/**
 * Match nodes dropped from the palette often have empty `ref`; the extraction rule is then
 * implied by a data edge from an extraction node whose `ref.extraction_rule_name` matches.
 */
function buildMatchNodeExtractionRulesFromDataEdges(
  canvas: WorkflowCanvasDocument,
  byId: Map<string, WorkflowCanvasNode>
): Map<string, Set<string>> {
  const m = new Map<string, Set<string>>();
  for (const e of canvas.edges) {
    if (!isDataEdge(e)) continue;
    const src = byId.get(e.source);
    const tgt = byId.get(e.target);
    if (!src || src.kind !== "extraction") continue;
    if (!tgt || tgt.kind !== "match_validation_extraction") continue;
    if (refBool(tgt.data.ref, "extraction_global_validation")) continue;
    const rn = refStr(src.data.ref, "extraction_rule_name");
    if (!rn) continue;
    let set = m.get(tgt.id);
    if (!set) {
      set = new Set<string>();
      m.set(tgt.id, set);
    }
    set.add(rn);
  }
  return m;
}

/** Copy seeded string tags along composition edges between two match-validation nodes of the same kind. */
function propagateStringSetsAlongMatchKindChains(
  canvas: WorkflowCanvasDocument,
  byId: Map<string, WorkflowCanvasNode>,
  seed: Map<string, Set<string>>,
  kind: "match_validation_extraction" | "match_validation_aliasing",
  skipNode: (n: WorkflowCanvasNode) => boolean
): Map<string, Set<string>> {
  const M = new Map<string, Set<string>>();
  for (const [id, s] of seed) M.set(id, new Set(s));

  let changed = true;
  while (changed) {
    changed = false;
    for (const e of canvas.edges) {
      if (edgeCompositionKind(e) == null) continue;
      const srcNode = byId.get(e.source);
      const tgtNode = byId.get(e.target);
      if (!srcNode || !tgtNode) continue;
      if (srcNode.kind !== kind || skipNode(srcNode)) continue;
      if (tgtNode.kind !== kind || skipNode(tgtNode)) continue;

      const from = M.get(e.source);
      if (!from || from.size === 0) continue;
      let to = M.get(e.target);
      if (!to) {
        to = new Set<string>();
        M.set(e.target, to);
      }
      const before = to.size;
      for (const r of from) to.add(r);
      if (to.size > before) changed = true;
    }
  }
  return M;
}

function buildMatchNodeAliasingRulesFromDataEdges(
  canvas: WorkflowCanvasDocument,
  byId: Map<string, WorkflowCanvasNode>
): Map<string, Set<string>> {
  const m = new Map<string, Set<string>>();
  for (const e of canvas.edges) {
    if (!isDataEdge(e)) continue;
    const src = byId.get(e.source);
    const tgt = byId.get(e.target);
    if (!src || src.kind !== "aliasing") continue;
    if (!tgt || tgt.kind !== "match_validation_aliasing") continue;
    if (refBool(tgt.data.ref, "aliasing_global_validation")) continue;
    const rn = refStr(src.data.ref, "aliasing_rule_name");
    if (!rn) continue;
    let set = m.get(tgt.id);
    if (!set) {
      set = new Set<string>();
      m.set(tgt.id, set);
    }
    set.add(rn);
  }
  return m;
}

function buildMatchNodeSourceViewIndicesFromDataEdges(
  canvas: WorkflowCanvasDocument,
  byId: Map<string, WorkflowCanvasNode>
): Map<string, Set<number>> {
  const m = new Map<string, Set<number>>();
  for (const e of canvas.edges) {
    if (!isDataEdge(e)) continue;
    const src = byId.get(e.source);
    const tgt = byId.get(e.target);
    if (!src || src.kind !== "source_view") continue;
    if (!tgt || tgt.kind !== "match_validation_source_view") continue;
    const idx = refNum(src.data.ref, "source_view_index");
    if (idx === undefined || !Number.isFinite(idx) || idx < 0) continue;
    const i = Math.floor(idx);
    let set = m.get(tgt.id);
    if (!set) {
      set = new Set<number>();
      m.set(tgt.id, set);
    }
    set.add(i);
  }
  return m;
}

function propagateSourceViewIndexInferenceAlongChains(
  canvas: WorkflowCanvasDocument,
  byId: Map<string, WorkflowCanvasNode>,
  seed: Map<string, Set<number>>
): Map<string, Set<number>> {
  const M = new Map<string, Set<number>>();
  for (const [id, s] of seed) M.set(id, new Set(s));

  let changed = true;
  while (changed) {
    changed = false;
    for (const e of canvas.edges) {
      if (edgeCompositionKind(e) == null) continue;
      const srcNode = byId.get(e.source);
      const tgtNode = byId.get(e.target);
      if (!srcNode || !tgtNode) continue;
      if (srcNode.kind !== "match_validation_source_view") continue;
      if (tgtNode.kind !== "match_validation_source_view") continue;

      const from = M.get(e.source);
      if (!from || from.size === 0) continue;
      let to = M.get(e.target);
      if (!to) {
        to = new Set<number>();
        M.set(e.target, to);
      }
      const before = to.size;
      for (const r of from) to.add(r);
      if (to.size > before) changed = true;
    }
  }
  return M;
}

function extractionPerRuleHeadAcceptsTarget(
  target: WorkflowCanvasNode,
  ruleName: string,
  inferredExtractionRules: Map<string, Set<string>>
): boolean {
  if (target.kind !== "match_validation_extraction") return false;
  if (refBool(target.data.ref, "extraction_global_validation")) return false;
  const tr = target.data.ref;
  if (refStr(tr, "extraction_rule_name") === ruleName) return true;
  if (refBool(tr, "shared_extraction_validation_chain")) {
    const names = (tr as Record<string, unknown> | undefined)?.extraction_rule_names;
    if (Array.isArray(names)) return names.map(String).includes(ruleName);
  }
  return Boolean(inferredExtractionRules.get(target.id)?.has(ruleName));
}

/** Data edge from an extraction node may target a per-rule extraction match head or a global aliasing match node. */
function extractionDataEdgeHeadAcceptsTarget(
  target: WorkflowCanvasNode,
  ruleName: string,
  inferredExtractionRules: Map<string, Set<string>>
): boolean {
  if (target.kind === "match_validation_aliasing" && refBool(target.data.ref, "aliasing_global_validation")) {
    return true;
  }
  return extractionPerRuleHeadAcceptsTarget(target, ruleName, inferredExtractionRules);
}

function aliasingPerRuleHeadAcceptsTarget(
  target: WorkflowCanvasNode,
  ruleName: string,
  inferredAliasingRules: Map<string, Set<string>>
): boolean {
  if (target.kind !== "match_validation_aliasing") return false;
  if (refBool(target.data.ref, "aliasing_global_validation")) return false;
  const tr = target.data.ref;
  if (refStr(tr, "aliasing_rule_name") === ruleName) return true;
  if (refBool(tr, "shared_aliasing_validation_chain")) {
    const names = (tr as Record<string, unknown> | undefined)?.aliasing_rule_names;
    if (Array.isArray(names)) return names.map(String).includes(ruleName);
  }
  return Boolean(inferredAliasingRules.get(target.id)?.has(ruleName));
}

function sourceViewNodeAppliesToIndex(node: WorkflowCanvasNode, idx: number): boolean {
  if (node.kind !== "match_validation_source_view") return false;
  const r = node.data.ref;
  if (refNum(r, "source_view_index") === idx) return true;
  if (refBool(r, "shared_source_view_validation_chain")) {
    const indices = (r as Record<string, unknown> | undefined)?.source_view_indices;
    if (Array.isArray(indices)) return (indices as number[]).includes(idx);
  }
  return false;
}

function sourceViewHeadAcceptsTarget(
  target: WorkflowCanvasNode,
  idx: number,
  inferredSourceViewIndices: Map<string, Set<number>>
): boolean {
  if (sourceViewNodeAppliesToIndex(target, idx)) return true;
  return Boolean(inferredSourceViewIndices.get(target.id)?.has(idx));
}

function patchExtractionRuleValidation(
  doc: Record<string, unknown>,
  ruleName: string,
  validation: JsonObject
): Record<string, unknown> {
  const ke = doc.key_extraction as Record<string, unknown> | undefined;
  if (!ke || typeof ke !== "object" || Array.isArray(ke)) return doc;
  const config = ke.config as Record<string, unknown> | undefined;
  if (!config || typeof config !== "object" || Array.isArray(config)) return doc;
  const data = config.data as Record<string, unknown> | undefined;
  if (!data || typeof data !== "object" || Array.isArray(data)) return doc;
  const rules = data.extraction_rules;
  if (!Array.isArray(rules)) return doc;
  let found = false;
  const nextRules = rules.map((r) => {
    if (!r || typeof r !== "object" || Array.isArray(r)) return r;
    const row = r as Record<string, unknown>;
    if (String(row.name ?? "").trim() !== ruleName) return r;
    found = true;
    return { ...row, validation };
  });
  if (!found) return doc;
  return {
    ...doc,
    key_extraction: {
      ...ke,
      config: {
        ...config,
        data: {
          ...data,
          extraction_rules: nextRules,
        },
      },
    },
  };
}

function patchAliasingRuleValidation(
  doc: Record<string, unknown>,
  ruleName: string,
  validation: JsonObject
): Record<string, unknown> {
  const al = doc.aliasing as Record<string, unknown> | undefined;
  if (!al || typeof al !== "object" || Array.isArray(al)) return doc;
  const config = al.config as Record<string, unknown> | undefined;
  if (!config || typeof config !== "object" || Array.isArray(config)) return doc;
  const data = config.data as Record<string, unknown> | undefined;
  if (!data || typeof data !== "object" || Array.isArray(data)) return doc;
  const rules = data.aliasing_rules;
  if (!Array.isArray(rules)) return doc;
  let found = false;
  const nextRules = rules.map((r) => {
    if (!r || typeof r !== "object" || Array.isArray(r)) return r;
    const row = r as Record<string, unknown>;
    if (String(row.name ?? "").trim() !== ruleName) return r;
    found = true;
    return { ...row, validation };
  });
  if (!found) return doc;
  return {
    ...doc,
    aliasing: {
      ...al,
      config: {
        ...config,
        data: {
          ...data,
          aliasing_rules: nextRules,
        },
      },
    },
  };
}

function patchKeyExtractionDataValidation(doc: Record<string, unknown>, validation: JsonObject): Record<string, unknown> {
  const ke = doc.key_extraction as Record<string, unknown> | undefined;
  if (!ke || typeof ke !== "object" || Array.isArray(ke)) return doc;
  const config = ke.config as Record<string, unknown> | undefined;
  if (!config || typeof config !== "object" || Array.isArray(config)) return doc;
  const data = config.data as Record<string, unknown> | undefined;
  if (!data || typeof data !== "object" || Array.isArray(data)) return doc;
  return {
    ...doc,
    key_extraction: {
      ...ke,
      config: {
        ...config,
        data: {
          ...data,
          validation,
        },
      },
    },
  };
}

function patchAliasingDataValidation(doc: Record<string, unknown>, validation: JsonObject): Record<string, unknown> {
  const al = doc.aliasing as Record<string, unknown> | undefined;
  if (!al || typeof al !== "object" || Array.isArray(al)) return doc;
  const config = al.config as Record<string, unknown> | undefined;
  if (!config || typeof config !== "object" || Array.isArray(config)) return doc;
  const data = config.data as Record<string, unknown> | undefined;
  if (!data || typeof data !== "object" || Array.isArray(data)) return doc;
  return {
    ...doc,
    aliasing: {
      ...al,
      config: {
        ...config,
        data: {
          ...data,
          validation,
        },
      },
    },
  };
}

function patchSourceViewValidation(
  doc: Record<string, unknown>,
  index: number,
  validation: JsonObject
): Record<string, unknown> {
  const svs = doc.source_views;
  if (!Array.isArray(svs) || index < 0 || index >= svs.length) return doc;
  const nextSvs = svs.map((v, i) => {
    if (i !== index) return v;
    if (!v || typeof v !== "object" || Array.isArray(v)) return { validation };
    return { ...(v as Record<string, unknown>), validation };
  });
  return { ...doc, source_views: nextSvs };
}

function refStr(ref: unknown, key: string): string | undefined {
  if (!ref || typeof ref !== "object" || Array.isArray(ref)) return undefined;
  const v = (ref as Record<string, unknown>)[key];
  return v != null && String(v).trim() ? String(v).trim() : undefined;
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

function applyCanvasMatchWiring(
  canvas: WorkflowCanvasDocument,
  scopeDoc: Record<string, unknown>
): Record<string, unknown> {
  const byId = nodesById(canvas);
  const outgoing = buildOutgoing(canvas);
  const inferredExtractionRules = propagateStringSetsAlongMatchKindChains(
    canvas,
    byId,
    buildMatchNodeExtractionRulesFromDataEdges(canvas, byId),
    "match_validation_extraction",
    (n) => refBool(n.data.ref, "extraction_global_validation")
  );
  const inferredAliasingRules = propagateStringSetsAlongMatchKindChains(
    canvas,
    byId,
    buildMatchNodeAliasingRulesFromDataEdges(canvas, byId),
    "match_validation_aliasing",
    (n) => refBool(n.data.ref, "aliasing_global_validation")
  );
  const inferredSourceViewIndices = propagateSourceViewIndexInferenceAlongChains(
    canvas,
    byId,
    buildMatchNodeSourceViewIndicesFromDataEdges(canvas, byId)
  );

  let doc: Record<string, unknown> = { ...scopeDoc };

  // —— Per extraction rule ——
  for (const n of canvas.nodes) {
    if (n.kind !== "extraction") continue;
    const ruleName = refStr(n.data.ref, "extraction_rule_name");
    if (!ruleName) continue;

    const hasLegacyNode = canvas.nodes.some(
      (cn) =>
        cn.kind === "match_validation_extraction" &&
        !refBool(cn.data.ref, "extraction_global_validation") &&
        refStr(cn.data.ref, "extraction_rule_name") === ruleName
    );
    const hasSharedNode = canvas.nodes.some((cn) => {
      if (cn.kind !== "match_validation_extraction" || refBool(cn.data.ref, "extraction_global_validation"))
        return false;
      if (!refBool(cn.data.ref, "shared_extraction_validation_chain")) return false;
      const names = (cn.data.ref as Record<string, unknown> | undefined)?.extraction_rule_names;
      return Array.isArray(names) && names.map(String).includes(ruleName);
    });
    const hasDataEdgeToMatch = (outgoing.get(n.id) ?? []).some((e) => {
      if (!isDataEdge(e)) return false;
      const t = byId.get(e.target);
      return Boolean(t && extractionDataEdgeHeadAcceptsTarget(t, ruleName, inferredExtractionRules));
    });
    if (!hasLegacyNode && !hasSharedNode && !hasDataEdgeToMatch) continue;

    const heads: string[] = [];
    for (const e of outgoing.get(n.id) ?? []) {
      if (!isDataEdge(e)) continue;
      const t = byId.get(e.target);
      if (!t || !extractionDataEdgeHeadAcceptsTarget(t, ruleName, inferredExtractionRules)) continue;
      heads.push(t.id);
    }
    sortHeadIdsByRuleName(heads, byId);

    const prevRow = findExtractionRule(doc, ruleName);
    if (heads.length === 0) {
      doc = patchExtractionRuleValidation(doc, ruleName, mergeValidation(prevRow?.validation, [], "sequential"));
    } else {
      const acceptExtraction: ChainTargetAccept = (tn) =>
        Boolean(
          tn &&
            tn.kind === "match_validation_extraction" &&
            !refBool(tn.data.ref, "extraction_global_validation") &&
            extractionPerRuleHeadAcceptsTarget(tn, ruleName, inferredExtractionRules)
        );
      const topSteps: unknown[] = [];
      for (const hid of heads) {
        const hn = byId.get(hid)!;
        if (hn.kind === "match_validation_aliasing" && refBool(hn.data.ref, "aliasing_global_validation")) {
          const nm = ruleNameFromMatchNode(hn);
          if (nm) topSteps.push(nm);
          continue;
        }
        const raw = buildMatchTopLevelSteps(hid, byId, outgoing, acceptExtraction);
        const one = shapeMatchStepsLinearOne(raw);
        if (one !== null) topSteps.push(one);
      }
      doc = patchExtractionRuleValidation(doc, ruleName, mergeValidationNested(prevRow?.validation, topSteps));
    }
  }

  // —— Global key_extraction.config.data.validation ——
  {
    const globalIds: string[] = [];
    for (const n of canvas.nodes) {
      if (n.kind !== "match_validation_extraction") continue;
      if (!refBool(n.data.ref, "extraction_global_validation")) continue;
      globalIds.push(n.id);
    }
    if (globalIds.length > 0) {
      const G = new Set(globalIds);
      let head: string | null = null;
      for (const e of canvas.edges) {
        if (!isDataEdge(e)) continue;
        const src = byId.get(e.source);
        const tgt = byId.get(e.target);
        if (!src || src.kind !== "extraction") continue;
        if (!tgt || !G.has(tgt.id)) continue;
        head = tgt.id;
        break;
      }
      if (head === null && G.size > 0) {
        const sorted = [...G].sort();
        head = sorted[0] ?? null;
      }
      const acceptGlobalExt: ChainTargetAccept = (tn) =>
        Boolean(
          tn &&
            tn.kind === "match_validation_extraction" &&
            refBool(tn.data.ref, "extraction_global_validation") &&
            G.has(tn.id)
        );
      const steps = stepsFromShapedLinear(
        head !== null ? buildMatchTopLevelSteps(head, byId, outgoing, acceptGlobalExt) : []
      );
      const ke = doc.key_extraction as Record<string, unknown> | undefined;
      const data = ke?.config as Record<string, unknown> | undefined;
      const d = data?.data as Record<string, unknown> | undefined;
      doc = patchKeyExtractionDataValidation(doc, mergeValidationNested(d?.validation, steps));
    }
  }

  // —— Per source view ——
  for (const n of canvas.nodes) {
    if (n.kind !== "source_view") continue;
    const ref = n.data.ref;
    const svIndex = refNum(ref, "source_view_index");
    if (svIndex === undefined || svIndex < 0 || !Number.isFinite(svIndex)) continue;
    const idx = Math.floor(svIndex);

    const hasSvLegacy = canvas.nodes.some(
      (cn) =>
        cn.kind === "match_validation_source_view" && refNum(cn.data.ref, "source_view_index") === idx
    );
    const hasSvShared = canvas.nodes.some((cn) => {
      if (cn.kind !== "match_validation_source_view") return false;
      if (!refBool(cn.data.ref, "shared_source_view_validation_chain")) return false;
      const indices = (cn.data.ref as Record<string, unknown> | undefined)?.source_view_indices;
      return Array.isArray(indices) && (indices as number[]).includes(idx);
    });
    const hasSvData = (outgoing.get(n.id) ?? []).some((e) => {
      if (!isDataEdge(e)) return false;
      const t = byId.get(e.target);
      return Boolean(t && sourceViewHeadAcceptsTarget(t, idx, inferredSourceViewIndices));
    });
    if (!hasSvLegacy && !hasSvShared && !hasSvData) continue;

    const heads: string[] = [];
    for (const e of outgoing.get(n.id) ?? []) {
      if (!isDataEdge(e)) continue;
      const t = byId.get(e.target);
      if (!t || !sourceViewHeadAcceptsTarget(t, idx, inferredSourceViewIndices)) continue;
      heads.push(t.id);
    }
    sortHeadIdsByRuleName(heads, byId);

    const svs = doc.source_views;
    const prevSv =
      Array.isArray(svs) && idx < svs.length && svs[idx] && typeof svs[idx] === "object"
        ? (svs[idx] as Record<string, unknown>)
        : undefined;

    if (heads.length === 0) {
      doc = patchSourceViewValidation(doc, idx, mergeValidation(prevSv?.validation, [], "sequential"));
    } else {
      const acceptSv: ChainTargetAccept = (tn) =>
        Boolean(
          tn &&
            tn.kind === "match_validation_source_view" &&
            sourceViewHeadAcceptsTarget(tn, idx, inferredSourceViewIndices)
        );
      const topSteps: unknown[] = [];
      for (const hid of heads) {
        const raw = buildMatchTopLevelSteps(hid, byId, outgoing, acceptSv);
        const one = shapeMatchStepsLinearOne(raw);
        if (one !== null) topSteps.push(one);
      }
      doc = patchSourceViewValidation(doc, idx, mergeValidationNested(prevSv?.validation, topSteps));
    }
  }

  // —— Per aliasing rule ——
  for (const n of canvas.nodes) {
    if (n.kind !== "aliasing") continue;
    const ruleName = refStr(n.data.ref, "aliasing_rule_name");
    if (!ruleName) continue;

    const hasAlLegacy = canvas.nodes.some(
      (cn) =>
        cn.kind === "match_validation_aliasing" &&
        !refBool(cn.data.ref, "aliasing_global_validation") &&
        refStr(cn.data.ref, "aliasing_rule_name") === ruleName
    );
    const hasAlShared = canvas.nodes.some((cn) => {
      if (cn.kind !== "match_validation_aliasing" || refBool(cn.data.ref, "aliasing_global_validation"))
        return false;
      if (!refBool(cn.data.ref, "shared_aliasing_validation_chain")) return false;
      const names = (cn.data.ref as Record<string, unknown> | undefined)?.aliasing_rule_names;
      return Array.isArray(names) && names.map(String).includes(ruleName);
    });
    const hasAlData = (outgoing.get(n.id) ?? []).some((e) => {
      if (!isDataEdge(e)) return false;
      const t = byId.get(e.target);
      return Boolean(t && aliasingPerRuleHeadAcceptsTarget(t, ruleName, inferredAliasingRules));
    });
    if (!hasAlLegacy && !hasAlShared && !hasAlData) continue;

    const heads: string[] = [];
    for (const e of outgoing.get(n.id) ?? []) {
      if (!isDataEdge(e)) continue;
      const t = byId.get(e.target);
      if (!t || !aliasingPerRuleHeadAcceptsTarget(t, ruleName, inferredAliasingRules)) continue;
      heads.push(t.id);
    }
    sortHeadIdsByRuleName(heads, byId);

    const prevRow = findAliasingRule(doc, ruleName);
    if (heads.length === 0) {
      doc = patchAliasingRuleValidation(doc, ruleName, mergeValidation(prevRow?.validation, [], "sequential"));
    } else {
      const acceptAl: ChainTargetAccept = (tn) =>
        Boolean(tn && aliasingPerRuleHeadAcceptsTarget(tn, ruleName, inferredAliasingRules));
      const topSteps: unknown[] = [];
      for (const hid of heads) {
        const raw = buildMatchTopLevelSteps(hid, byId, outgoing, acceptAl);
        const one = shapeMatchStepsLinearOne(raw);
        if (one !== null) topSteps.push(one);
      }
      doc = patchAliasingRuleValidation(doc, ruleName, mergeValidationNested(prevRow?.validation, topSteps));
    }
  }

  // —— Global aliasing.config.data.validation ——
  {
    const globalIds: string[] = [];
    for (const n of canvas.nodes) {
      if (n.kind !== "match_validation_aliasing") continue;
      if (!refBool(n.data.ref, "aliasing_global_validation")) continue;
      globalIds.push(n.id);
    }
    if (globalIds.length > 0) {
      const G = new Set(globalIds);
      let head: string | null = null;
      for (const e of canvas.edges) {
        if (!isDataEdge(e)) continue;
        const src = byId.get(e.source);
        const tgt = byId.get(e.target);
        if (!src || src.kind !== "aliasing") continue;
        if (!tgt || !G.has(tgt.id)) continue;
        head = tgt.id;
        break;
      }
      if (head === null && G.size > 0) {
        const sorted = [...G].sort();
        head = sorted[0] ?? null;
      }
      const acceptGlobalAl: ChainTargetAccept = (tn) =>
        Boolean(
          tn &&
            tn.kind === "match_validation_aliasing" &&
            refBool(tn.data.ref, "aliasing_global_validation") &&
            G.has(tn.id)
        );
      const steps = stepsFromShapedLinear(
        head !== null ? buildMatchTopLevelSteps(head, byId, outgoing, acceptGlobalAl) : []
      );
      const al = doc.aliasing as Record<string, unknown> | undefined;
      const data = al?.config as Record<string, unknown> | undefined;
      const d = data?.data as Record<string, unknown> | undefined;
      doc = patchAliasingDataValidation(doc, mergeValidationNested(d?.validation, steps));
    }
  }

  return doc;
}

function findExtractionRule(
  doc: Record<string, unknown>,
  ruleName: string
): Record<string, unknown> | undefined {
  const ke = doc.key_extraction as Record<string, unknown> | undefined;
  const data = ke?.config as Record<string, unknown> | undefined;
  const d = data?.data as Record<string, unknown> | undefined;
  const rules = d?.extraction_rules;
  if (!Array.isArray(rules)) return undefined;
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    if (String(row.name ?? "").trim() === ruleName) return row;
  }
  return undefined;
}

function findAliasingRule(
  doc: Record<string, unknown>,
  ruleName: string
): Record<string, unknown> | undefined {
  const al = doc.aliasing as Record<string, unknown> | undefined;
  const data = al?.config as Record<string, unknown> | undefined;
  const d = data?.data as Record<string, unknown> | undefined;
  const rules = d?.aliasing_rules;
  if (!Array.isArray(rules)) return undefined;
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    if (String(row.name ?? "").trim() === ruleName) return row;
  }
  return undefined;
}
