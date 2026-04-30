import {
  emptyWorkflowCanvasDocument,
  normalizeWorkflowCanvasDocumentEdgeHandles,
  type WorkflowCanvasDocument,
  type WorkflowCanvasEdge,
  type WorkflowCanvasNode,
} from "../../types/workflowCanvas";

import { buildAliasingPathwayTagByRuleName, getAliasingTransformRuleRows } from "./aliasingScopeData";
import { parseSourceViewToExtractionPairs } from "./workflowScopeAssociations";
import {
  aliasingRuleDefinitionsLookup,
  buildAliasingMatchValidationSubgraph,
  buildExtractionMatchValidationSubgraph,
  collectAliasingRuleNamesReferencedByExtractionPipelines,
  edgeAliasingToMatchFirst,
  edgeExtractionToMatchFirst,
  inferAliasingHeadNameFromConfig,
  orderedAliasingRuleNamesForSeed,
  planAliasingPipelineComposition,
  type AssociationPair,
  validationRulesLinearNamesForSeed,
  validationStepChainsEqual,
} from "./seedScopeConfigHelpers";

/** Default primary data path (``Kea*`` node ``out`` / ``in``; normalized for match nodes). */
const SEED_DATA_OUT_IN = { source_handle: "out", target_handle: "in" } as const;
/** Chained sequence steps between transform nodes. */
const SEED_SEQ_OUT_IN = { source_handle: "out", target_handle: "in" } as const;
const SEED_PAR_OUT_IN = { source_handle: "out", target_handle: "in" } as const;

function isRecord(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

/** Slug for stable node ids (letters, digits, underscore). */
function canvasIdSlug(s: string): string {
  const t = s.trim().toLowerCase();
  if (!t) return "x";
  const out = t.replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "");
  return out || "x";
}

type ExtractionRuleSeed = {
  name: string;
  handler: string;
  enabled: boolean;
  priority: number;
  row: Record<string, unknown>;
};

type AliasingRuleSeed = {
  name: string;
  handler: string;
  enabled: boolean;
  priority: number;
  row: Record<string, unknown>;
};

function parseExtractionRules(scopeDoc: Record<string, unknown>): ExtractionRuleSeed[] {
  const ke = scopeDoc.key_extraction as Record<string, unknown> | undefined;
  const config = ke?.config as Record<string, unknown> | undefined;
  const data = config?.data as Record<string, unknown> | undefined;
  const rules = data?.extraction_rules;
  if (!Array.isArray(rules)) return [];
  const out: ExtractionRuleSeed[] = [];
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    const name = row.name != null ? String(row.name).trim() : "";
    if (!name) continue;
    const enabled = row.enabled !== false;
    const handler = row.handler != null ? String(row.handler) : "regex_handler";
    const priority = typeof row.priority === "number" && Number.isFinite(row.priority) ? row.priority : 100;
    out.push({ name, handler, enabled, priority, row });
  }
  out.sort((a, b) => {
    if (a.priority !== b.priority) return a.priority - b.priority;
    return a.name.localeCompare(b.name);
  });
  return out;
}

function parseAliasingRules(
  alData: Record<string, unknown> | undefined
): { seeds: AliasingRuleSeed[]; rawRows: unknown[] } {
  const rules = alData ? getAliasingTransformRuleRows(alData) : [];
  const rawRows = Array.isArray(rules) ? [...rules] : [];
  if (rawRows.length === 0) {
    return { seeds: [], rawRows };
  }
  const out: AliasingRuleSeed[] = [];
  for (const r of rawRows) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    const name = row.name != null ? String(row.name).trim() : "";
    if (!name) continue;
    const enabled = row.enabled !== false;
    const handler = row.handler != null ? String(row.handler) : "character_substitution";
    const priority = typeof row.priority === "number" && Number.isFinite(row.priority) ? row.priority : 100;
    out.push({ name, handler, enabled, priority, row });
  }
  const seenNames = new Set<string>();
  const deduped: typeof out = [];
  for (const s of out) {
    if (seenNames.has(s.name)) {
      continue;
    }
    seenNames.add(s.name);
    deduped.push(s);
  }
  return { seeds: deduped, rawRows };
}

/**
 * When the canvas file is empty, seed Start → source views → extraction (from ``associations``) →
 * aliasing using each rule’s ``aliasing_pipeline`` when present; otherwise a best-effort head from
 * ``scope_filters`` / source view ``entity_type``; otherwise all to the first transform in
 * pathway/YAML order. Composition edges mirror sync (``sequence`` / ``parallel_group``). ``End`` is
 * wired from al nodes with no outgoing composition edge. Non-empty ``validation`` / ``validation_rules`` seed
 * ``match_validation_*`` nodes so ``syncWorkflowScopeFromCanvas`` can round-trip.
 */
export function seedCanvasFromScope(scopeDoc: Record<string, unknown>): WorkflowCanvasDocument {
  const base = emptyWorkflowCanvasDocument();
  const nodes: WorkflowCanvasNode[] = [];
  const edges: WorkflowCanvasEdge[] = [];

  const COL_START = 0;
  const COL_SV = 200;
  const COL_EXT = 400;
  const COL_AL = 640;
  const COL_MVE = 900;
  const COL_MVA = 1100;
  const COL_END = 1380;
  const GAP_Y = 76;

  const ID_START = "flow_start";
  const ID_END = "flow_end";

  type SvRow = { index: number; viewLabel: string };
  const svRows: SvRow[] = [];
  const svs = scopeDoc.source_views;
  if (Array.isArray(svs)) {
    for (let i = 0; i < svs.length; i++) {
      const v = svs[i];
      if (!v || typeof v !== "object" || Array.isArray(v)) continue;
      const row = v as Record<string, unknown>;
      const ext = row.view_external_id != null ? String(row.view_external_id) : `view_${i}`;
      svRows.push({ index: i, viewLabel: ext });
    }
  }

  const alScope = scopeDoc.aliasing as Record<string, unknown> | undefined;
  const alConfig = alScope?.config as Record<string, unknown> | undefined;
  const alData = alConfig?.data as Record<string, unknown> | undefined;

  const { seeds: aliasingRulesAll, rawRows: alRawForInfer } = parseAliasingRules(alData);
  let mergedAliasingRules = aliasingRulesAll.filter((r) => r.enabled);
  const extractionRulesAll = parseExtractionRules(scopeDoc);
  const extractionRules = extractionRulesAll.filter((r) => r.enabled);

  const alRowsForInfer: Record<string, unknown>[] = [];
  for (const r of alRawForInfer) {
    if (!isRecord(r)) continue;
    if (r.enabled === false) continue;
    alRowsForInfer.push(r);
  }

  const defsByName = aliasingRuleDefinitionsLookup(scopeDoc);
  const pipelineRefNames = collectAliasingRuleNamesReferencedByExtractionPipelines(scopeDoc);
  const seededAlNames = new Set(mergedAliasingRules.map((r) => r.name));
  const extras: AliasingRuleSeed[] = [];
  for (const nm of pipelineRefNames) {
    if (seededAlNames.has(nm)) continue;
    const defRow = defsByName.get(nm);
    if (!defRow) continue;
    if (defRow.enabled === false) continue;
    const handlerRaw = defRow.handler ?? defRow.type;
    const handler =
      handlerRaw != null && String(handlerRaw).trim() ? String(handlerRaw).trim() : "regex_substitution";
    const priority =
      typeof defRow.priority === "number" && Number.isFinite(defRow.priority) ? defRow.priority : 100;
    const row = { ...defRow } as Record<string, unknown>;
    extras.push({ name: nm, handler, enabled: true, priority, row });
    seededAlNames.add(nm);
    if (!alRowsForInfer.some((x) => String((x as Record<string, unknown>).name ?? "").trim() === nm)) {
      alRowsForInfer.push(row);
    }
  }
  if (extras.length > 0) {
    mergedAliasingRules = [...mergedAliasingRules, ...extras];
  }

  const byAlName = new Map(mergedAliasingRules.map((r) => [r.name, r]));
  const orderedNames = orderedAliasingRuleNamesForSeed(scopeDoc);
  let aliasingRules: AliasingRuleSeed[] = orderedNames
    .map((nm) => byAlName.get(nm))
    .filter((r): r is AliasingRuleSeed => Boolean(r));
  const orderSeen = new Set(aliasingRules.map((r) => r.name));
  for (const r of mergedAliasingRules) {
    if (!orderSeen.has(r.name)) {
      orderSeen.add(r.name);
      aliasingRules.push(r);
    }
  }

  if (svRows.length === 0 && extractionRules.length === 0 && aliasingRules.length === 0) {
    return base;
  }

  const maxRows = Math.max(svRows.length, extractionRules.length, aliasingRules.length, 1);
  const midY = 48 + ((maxRows - 1) * GAP_Y) / 2;

  nodes.push({
    id: ID_START,
    kind: "start",
    position: { x: COL_START, y: midY },
    data: { label: "Start" },
  });
  nodes.push({
    id: ID_END,
    kind: "end",
    position: { x: COL_END, y: midY },
    data: { label: "End" },
  });

  svRows.forEach((r, idx) => {
    const svId = `sv_${r.index}`;
    const svsRow = Array.isArray(svs) ? svs[r.index] : undefined;
    const row =
      svsRow && typeof svsRow === "object" && !Array.isArray(svsRow)
        ? (svsRow as Record<string, unknown>)
        : {};
    nodes.push({
      id: svId,
      kind: "source_view",
      position: { x: COL_SV, y: 48 + idx * GAP_Y },
      data: {
        label: r.viewLabel,
        ref: {
          source_view_index: r.index,
          view_space: row.view_space != null ? String(row.view_space) : undefined,
          view_external_id: row.view_external_id != null ? String(row.view_external_id) : undefined,
          view_version: row.view_version != null ? String(row.view_version) : undefined,
        },
      },
    });
  });

  const extIds: string[] = [];
  const extIdByName = new Map<string, string>();
  extractionRules.forEach((rule, j) => {
    const id = `ext_${canvasIdSlug(rule.name)}`;
    extIds.push(id);
    extIdByName.set(rule.name, id);
    nodes.push({
      id,
      kind: "extraction",
      position: { x: COL_EXT, y: 48 + j * GAP_Y },
      data: {
        label: rule.name,
        handler_id: rule.handler,
        handler_family: "extraction",
        ref: { extraction_rule_name: rule.name },
      },
    });
  });

  for (const r of svRows) {
    edges.push({
      id: `e_${ID_START}_sv_${r.index}`,
      source: ID_START,
      target: `sv_${r.index}`,
      kind: "data",
      ...SEED_DATA_OUT_IN,
    });
  }

  const assocPairs: AssociationPair[] = parseSourceViewToExtractionPairs(scopeDoc);
  for (const r of svRows) {
    for (const rule of extractionRules) {
      const bound = assocPairs.some(
        (p) => p.source_view_index === r.index && p.extraction_rule_name === rule.name
      );
      if (!bound) continue;
      const extId = extIdByName.get(rule.name) ?? `ext_${canvasIdSlug(rule.name)}`;
      edges.push({
        id: `e_sv_${r.index}_${extId}`,
        source: `sv_${r.index}`,
        target: extId,
        kind: "data",
        ...SEED_DATA_OUT_IN,
      });
    }
  }

  const alIds: string[] = [];
  const alIdByName = new Map<string, string>();
  aliasingRules.forEach((rule, k) => {
    const id = `al_${canvasIdSlug(rule.name)}`;
    alIds.push(id);
    alIdByName.set(rule.name, id);
    nodes.push({
      id,
      kind: "aliasing",
      position: { x: COL_AL, y: 48 + k * GAP_Y },
      data: {
        label: rule.name,
        handler_id: rule.handler,
        handler_family: "aliasing",
        ref: { aliasing_rule_name: rule.name },
        pipeline_rank: k,
      },
    });
  });

  const knownAlNames = new Set(aliasingRules.map((a) => a.name));
  const alIdSet = new Set(alIds);
  const firstAlName = aliasingRules[0]?.name;
  const firstAlId = firstAlName != null ? alIdByName.get(firstAlName) : undefined;

  const seededEdgeSig = new Set<string>();
  const addSeedEdge = (source: string, target: string, kind: WorkflowCanvasEdge["kind"]): void => {
    const sig = `${source}\0${target}\0${kind}`;
    if (seededEdgeSig.has(sig)) return;
    seededEdgeSig.add(sig);
    const handleProps =
      kind === "sequence" ? SEED_SEQ_OUT_IN : kind === "parallel_group" ? SEED_PAR_OUT_IN : SEED_DATA_OUT_IN;
    edges.push({
      id: `e_${source}_${target}_${edges.length}`,
      source,
      target,
      kind,
      ...handleProps,
    });
  };

  const hasCompositionOut = new Set<string>();

  if (alIds.length > 0 && extractionRules.length > 0) {
    for (let j = 0; j < extractionRules.length; j++) {
      const er = extractionRules[j]!;
      const row = er.row;
      const extId = extIdByName.get(er.name) ?? `ext_${canvasIdSlug(er.name)}`;

      const resolveAlId = (nm: string): string | null => {
        const id = alIdByName.get(nm);
        return id && alIdSet.has(id) ? id : null;
      };
      let plan = planAliasingPipelineComposition(
        (row as Record<string, unknown>).aliasing_pipeline,
        knownAlNames,
        resolveAlId
      );
      if (plan.dataTargets.length === 0 && plan.compositionEdges.length === 0) {
        const inferred = inferAliasingHeadNameFromConfig(
          row,
          knownAlNames,
          assocPairs,
          svs,
          alRowsForInfer
        );
        if (inferred) {
          plan = planAliasingPipelineComposition([inferred], knownAlNames, resolveAlId);
        }
      }
      if (plan.dataTargets.length === 0 && firstAlId) {
        plan = planAliasingPipelineComposition([firstAlName!], knownAlNames, resolveAlId);
      }
      for (const tid of plan.dataTargets) {
        addSeedEdge(extId, tid, "data");
      }
      for (const ce of plan.compositionEdges) {
        hasCompositionOut.add(ce.source);
        addSeedEdge(ce.source, ce.target, ce.kind);
      }
    }
    // Pathway spine: when `aliasing_pipeline` / inference only set a head, later `pathways` rows can
    // be left with no `data` or composition in-edges. Walk pathway flatten order and add `sequence`
    // links so the full transform stack is reachable — but **not** across parallel-branch borders
    // (same as lifted inner canvas: concurrent branches are not chained by flatten order).
    if (alIds.length > 1) {
      const pathwayTagByName = buildAliasingPathwayTagByRuleName(alData);
      const alIncoming = (alId: string): boolean =>
        edges.some(
          (e) =>
            e.target === alId &&
            (e.kind === "data" || e.kind === "sequence" || e.kind === "parallel_group")
        );
      for (let i = 1; i < alIds.length; i++) {
        const cur = alIds[i]!;
        const prev = alIds[i - 1]!;
        const prevRule = aliasingRules[i - 1]!;
        const curRule = aliasingRules[i]!;
        const tPrev = pathwayTagByName.get(prevRule.name);
        const tCur = pathwayTagByName.get(curRule.name);
        if (
          tPrev &&
          tCur &&
          tPrev.stepIndex === tCur.stepIndex &&
          tPrev.parallelBranch != null &&
          tCur.parallelBranch != null &&
          tPrev.parallelBranch !== tCur.parallelBranch
        ) {
          continue;
        }
        if (alIncoming(cur)) {
          continue;
        }
        hasCompositionOut.add(prev);
        addSeedEdge(prev, cur, "sequence");
      }
    }
  } else if (alIds.length > 0 && extractionRules.length === 0) {
    for (let i = 0; i < alIds.length - 1; i++) {
      const a = alIds[i]!;
      const b = alIds[i + 1]!;
      hasCompositionOut.add(a);
      addSeedEdge(a, b, "sequence");
    }
  }

  for (const al of alIds) {
    if (!hasCompositionOut.has(al)) {
      addSeedEdge(al, ID_END, "data");
    }
  }
  if (alIds.length === 0) {
    for (const extId of extIds) {
      addSeedEdge(extId, ID_END, "data");
    }
  }

  if (svRows.length === 0 && extractionRules.length > 0) {
    edges.push({
      id: `e_${ID_START}_${extIds[0]}`,
      source: ID_START,
      target: extIds[0]!,
      kind: "data",
      ...SEED_DATA_OUT_IN,
    });
  }

  // —— Match validation (MVP: linear ``validation_rules``) ——
  let matchEdgeCount = 0;
  const pushMatch = (g: { nodes: WorkflowCanvasNode[]; edges: WorkflowCanvasEdge[]; headId: string | null }) => {
    for (const n of g.nodes) {
      nodes.push(n);
    }
    for (const e of g.edges) {
      edges.push(e);
    }
  };

  const perExtractionValidationChains: string[][] = [];
  for (let j = 0; j < extractionRules.length; j++) {
    const er = extractionRules[j]!;
    const v = (er.row as Record<string, unknown>).validation;
    if (!isRecord(v)) {
      continue;
    }
    const stepNames = validationRulesLinearNamesForSeed(v.validation_rules);
    if (stepNames.length === 0) {
      continue;
    }
    perExtractionValidationChains.push(stepNames);
    const extId = extIdByName.get(er.name) ?? `ext_${canvasIdSlug(er.name)}`;
    const y = 48 + j * GAP_Y;
    const g = buildExtractionMatchValidationSubgraph(
      stepNames,
      er.name,
      false,
      { y, xStart: COL_MVE, xStep: 200, idPrefix: `e${j}` }
    );
    if (g.headId) {
      matchEdgeCount += 1;
      edges.push(edgeExtractionToMatchFirst(extId, g.headId, matchEdgeCount));
    }
    pushMatch(g);
  }

  const ke = scopeDoc.key_extraction as Record<string, unknown> | undefined;
  const keConfig = ke?.config as Record<string, unknown> | undefined;
  const keData = keConfig?.data as Record<string, unknown> | undefined;
  const gExtNames = keData
    ? validationRulesLinearNamesForSeed(
        (keData.validation as Record<string, unknown> | undefined)?.validation_rules
      )
    : [];
  if (
    gExtNames.length > 0 &&
    !perExtractionValidationChains.some((sl) => validationStepChainsEqual(sl, gExtNames))
  ) {
    const y = 12;
    const g = buildExtractionMatchValidationSubgraph(gExtNames, null, true, {
      y,
      xStart: COL_MVE,
      xStep: 200,
      idPrefix: "gex",
    });
    if (g.headId) {
      matchEdgeCount += 1;
      const fromExt = extIds[0];
      if (fromExt) {
        edges.push(edgeExtractionToMatchFirst(fromExt, g.headId, matchEdgeCount));
      }
    }
    pushMatch(g);
  }

  const perAliasingValidationChains: string[][] = [];
  for (let k = 0; k < aliasingRules.length; k++) {
    const ar = aliasingRules[k]!;
    const v = ar.row.validation;
    if (!isRecord(v)) {
      continue;
    }
    const stepNames = validationRulesLinearNamesForSeed(v.validation_rules);
    if (stepNames.length === 0) {
      continue;
    }
    perAliasingValidationChains.push(stepNames);
    const alId = alIdByName.get(ar.name) ?? `al_${canvasIdSlug(ar.name)}`;
    const y = 48 + k * GAP_Y;
    const g = buildAliasingMatchValidationSubgraph(
      stepNames,
      ar.name,
      false,
      { y, xStart: COL_MVA, xStep: 200, idPrefix: `a${k}` }
    );
    if (g.headId) {
      matchEdgeCount += 1;
      edges.push(edgeAliasingToMatchFirst(alId, g.headId, matchEdgeCount));
    }
    pushMatch(g);
  }

  const gAlNames = alData
    ? validationRulesLinearNamesForSeed(
        (alData.validation as Record<string, unknown> | undefined)?.validation_rules
      )
    : [];
  if (
    gAlNames.length > 0 &&
    !perAliasingValidationChains.some((sl) => validationStepChainsEqual(sl, gAlNames))
  ) {
    const y = 12;
    const g = buildAliasingMatchValidationSubgraph(gAlNames, null, true, {
      y,
      xStart: COL_MVA,
      xStep: 200,
      idPrefix: "gal",
    });
    if (g.headId) {
      matchEdgeCount += 1;
      const fromAl = alIds[0];
      if (fromAl) {
        edges.push(edgeAliasingToMatchFirst(fromAl, g.headId, matchEdgeCount));
      }
    }
    pushMatch(g);
  }

  const doc: WorkflowCanvasDocument = { ...base, nodes, edges };
  normalizeWorkflowCanvasDocumentEdgeHandles(doc);
  return doc;
}

/**
 * When the canvas file is missing or has no nodes, populate from the scope document.
 */
export function canvasDocWithScopeSeedIfEmpty(
  parsed: WorkflowCanvasDocument,
  scopeDoc: Record<string, unknown>
): WorkflowCanvasDocument {
  if (parsed.nodes.length > 0) return parsed;
  const seeded = seedCanvasFromScope(scopeDoc);
  return seeded.nodes.length > 0 ? seeded : parsed;
}

const SCOPE_ROOT_KEYS_FOR_FLOW_SEED = ["aliasing_rule_definitions", "aliasing_rule_sequences"] as const;

function isNonEmptyRecord(v: unknown): boolean {
  return v !== null && typeof v === "object" && !Array.isArray(v) && Object.keys(v as object).length > 0;
}

function isNonEmptyArray(v: unknown): boolean {
  return Array.isArray(v) && v.length > 0;
}

/**
 * WorkflowTrigger ``input.configuration`` is often a trimmed copy of the site scope. If
 * ``aliasing_rule_definitions`` / ``aliasing_rule_sequences`` are missing or empty there but present
 * on the loaded workflow scope or template doc, overlay them so ``seedCanvasFromScope`` / sync
 * resolve the same rule names as the main scope editor (e.g. Strip Delimiter only in definitions).
 */
export function mergeScopeRootsForTriggerFlowSeed(
  triggerConfiguration: Record<string, unknown>,
  ...fallbacks: Array<Record<string, unknown> | undefined>
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...triggerConfiguration };
  for (const key of SCOPE_ROOT_KEYS_FOR_FLOW_SEED) {
    const cur = out[key];
    const has =
      key === "aliasing_rule_sequences"
        ? isNonEmptyRecord(cur) || isNonEmptyArray(cur)
        : isNonEmptyRecord(cur);
    if (has) continue;
    for (const fb of fallbacks) {
      if (!fb) continue;
      const cand = fb[key];
      if (key === "aliasing_rule_sequences") {
        if (isNonEmptyRecord(cand) || isNonEmptyArray(cand)) {
          out[key] = cand;
          break;
        }
      } else if (isNonEmptyRecord(cand)) {
        out[key] = cand;
        break;
      }
    }
  }
  return out;
}
