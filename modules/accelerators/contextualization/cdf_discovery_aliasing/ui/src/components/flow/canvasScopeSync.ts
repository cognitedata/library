import type { JsonObject } from "../../types/scopeConfig";
import type {
  WorkflowCanvasDocument,
  WorkflowCanvasEdge,
  WorkflowCanvasNode,
} from "../../types/workflowCanvas";
import { expandCanvasForScopeSync } from "./subgraphBoundaryVirtualization";
import { stripScopeRuleListsFromScopeDoc } from "../../utils/stripScopeRuleLists";

/**
 * Merge flow canvas match-validation wiring into the scope document and strip legacy rule lists.
 */
export function syncWorkflowScopeFromCanvas(
  canvas: WorkflowCanvasDocument,
  scopeDoc: Record<string, unknown>
): Record<string, unknown> {
  const expanded = expandCanvasForScopeSync(canvas);
  const wired = applyCanvasMatchWiring(expanded, scopeDoc);
  return stripScopeRuleListsFromScopeDoc(wired);
}

function isDataEdge(e: WorkflowCanvasEdge): boolean {
  return e.kind !== "sequence" && e.kind !== "parallel_group";
}

function ruleNameFromMatchNode(n: WorkflowCanvasNode): string | null {
  const c = n.data.validation_rule_name;
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

function validationRulesFromCanvasPreserveMeta(prev: unknown, steps: unknown[]): JsonObject {
  const o =
    prev !== null && typeof prev === "object" && !Array.isArray(prev)
      ? { ...(prev as Record<string, unknown>) }
      : {};
  return { ...o, validation_rules: steps } as JsonObject;
}

type ChainTargetAccept = (n: WorkflowCanvasNode | undefined) => boolean;

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
  const visited = new Set<string>();
  const raw = buildMatchSubtree(headId, byId, outgoing, acceptTarget, visited);
  if (raw === null) return [];
  return [raw];
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

function refBool(ref: unknown, key: string): boolean {
  if (!ref || typeof ref !== "object" || Array.isArray(ref)) return false;
  return Boolean((ref as Record<string, unknown>)[key]);
}

function applyCanvasMatchWiring(
  canvas: WorkflowCanvasDocument,
  scopeDoc: Record<string, unknown>
): Record<string, unknown> {
  const byId = nodesById(canvas);
  const outgoing = buildOutgoing(canvas);
  let doc: Record<string, unknown> = { ...scopeDoc };

  const globalExtIds: string[] = [];
  for (const n of canvas.nodes) {
    if (n.kind !== "match_validation_extraction") continue;
    if (!refBool(n.data.ref, "extraction_global_validation")) continue;
    globalExtIds.push(n.id);
  }
  if (globalExtIds.length > 0) {
    const G = new Set(globalExtIds);
    let head: string | null = null;
    for (const e of canvas.edges) {
      if (!isDataEdge(e)) continue;
      const src = byId.get(e.source);
      const tgt = byId.get(e.target);
      if (!src || !tgt || !G.has(tgt.id)) continue;
      head = tgt.id;
      break;
    }
    if (head === null && G.size > 0) head = [...G].sort()[0] ?? null;
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
    const data = (ke?.config as Record<string, unknown> | undefined)?.data as Record<string, unknown> | undefined;
    doc = patchKeyExtractionDataValidation(doc, validationRulesFromCanvasPreserveMeta(data?.validation, steps));
  }

  const globalAlIds: string[] = [];
  for (const n of canvas.nodes) {
    if (n.kind !== "match_validation_aliasing") continue;
    if (!refBool(n.data.ref, "aliasing_global_validation")) continue;
    globalAlIds.push(n.id);
  }
  if (globalAlIds.length > 0) {
    const G = new Set(globalAlIds);
    let head: string | null = null;
    for (const e of canvas.edges) {
      if (!isDataEdge(e)) continue;
      const src = byId.get(e.source);
      const tgt = byId.get(e.target);
      if (!src || !tgt || !G.has(tgt.id)) continue;
      head = tgt.id;
      break;
    }
    if (head === null && G.size > 0) head = [...G].sort()[0] ?? null;
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
    const data = (al?.config as Record<string, unknown> | undefined)?.data as Record<string, unknown> | undefined;
    doc = patchAliasingDataValidation(doc, validationRulesFromCanvasPreserveMeta(data?.validation, steps));
  }

  return doc;
}
