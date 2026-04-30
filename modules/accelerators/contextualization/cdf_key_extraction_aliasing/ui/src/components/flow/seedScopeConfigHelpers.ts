/**
 * Helpers to rebuild flow canvas structure from v1 scope YAML (used by ``seedCanvasFromScope``).
 */

import { getAliasingTransformRuleRows } from "./aliasingScopeData";
import type { WorkflowCanvasEdge, WorkflowCanvasNode } from "../../types/workflowCanvas";

/** Composition edges among ``kind: "aliasing"`` nodes (matches ``canvasScopeSync`` / sync). */
export type SeedAliasingCompositionEdge = {
  source: string;
  target: string;
  kind: "sequence" | "parallel_group";
};

function isRecord(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

/**
 * Keys / structural tokens in ``aliasing_pipeline`` YAML that are not transform rule names
 * (aligned with ``canvasScopeSync`` ``_PIPELINE_NOISE``).
 */
const PIPELINE_NAME_NOISE = new Set(
  [
    "sequential",
    "parallel",
    "concurrent",
    "ordered",
    "hierarchy",
    "mode",
    "children",
    "branches",
    "rules",
    "config",
    "validation",
    "scope_filters",
    "conditions",
    "description",
    "enabled",
    "priority",
    "preserve_original",
    "name",
    "handler",
    "type",
    "match",
    "expression",
    "expressions",
    "extraction",
    "aliasing",
  ].map((s) => s.toLowerCase())
);

function collectPipelineRuleNameTokens(x: unknown, ordered: string[], seen: Set<string>): void {
  if (x === null || x === undefined) return;
  if (typeof x === "string") {
    const t = x.trim();
    if (t && t.length < 512 && !PIPELINE_NAME_NOISE.has(t.toLowerCase()) && !seen.has(t)) {
      seen.add(t);
      ordered.push(t);
    }
    return;
  }
  if (Array.isArray(x)) {
    for (const v of x) collectPipelineRuleNameTokens(v, ordered, seen);
    return;
  }
  if (isRecord(x)) {
    for (const [k, v] of Object.entries(x)) {
      const kt = k.trim();
      if (kt && kt.length < 512 && !PIPELINE_NAME_NOISE.has(kt.toLowerCase()) && !seen.has(kt)) {
        seen.add(kt);
        ordered.push(kt);
      }
      collectPipelineRuleNameTokens(v, ordered, seen);
    }
  }
}

/**
 * Rule names referenced under ``key_extraction.config.data.extraction_rules[].aliasing_pipeline``,
 * in encounter order (first occurrence wins). Matches Python ``resolve_aliasing_pipeline_refs`` string refs.
 */
export function collectAliasingRuleNamesReferencedByExtractionPipelines(scopeDoc: Record<string, unknown>): string[] {
  const ordered: string[] = [];
  const seen = new Set<string>();
  const ke = scopeDoc.key_extraction;
  if (!isRecord(ke)) return ordered;
  const config = ke.config;
  if (!isRecord(config)) return ordered;
  const data = config.data;
  if (!isRecord(data)) return ordered;
  const rules = data.extraction_rules;
  if (!Array.isArray(rules)) return ordered;
  for (const r of rules) {
    if (!isRecord(r)) continue;
    const ap = (r as Record<string, unknown>).aliasing_pipeline;
    collectPipelineRuleNameTokens(ap, ordered, seen);
  }
  return ordered;
}

/**
 * Enabled aliasing transform rule names in the same order ``seedCanvasFromScope`` uses
 * (pathway / flat rows, definition-only extras from pipelines, then pipeline-first merge).
 * Used by auto-layout when persisted nodes omit ``pipeline_rank``.
 */
export function orderedAliasingRuleNamesForSeed(scopeDoc: Record<string, unknown>): string[] {
  const alScope = scopeDoc.aliasing as Record<string, unknown> | undefined;
  const alConfig = alScope?.config as Record<string, unknown> | undefined;
  const alData = alConfig?.data as Record<string, unknown> | undefined;
  const rawRows = alData ? [...getAliasingTransformRuleRows(alData)] : [];
  const names: string[] = [];
  const seenNames = new Set<string>();
  for (const r of rawRows) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    const name = row.name != null ? String(row.name).trim() : "";
    if (!name || row.enabled === false) continue;
    if (seenNames.has(name)) continue;
    seenNames.add(name);
    names.push(name);
  }
  const defsByName = aliasingRuleDefinitionsLookup(scopeDoc);
  const pipelineRefNames = collectAliasingRuleNamesReferencedByExtractionPipelines(scopeDoc);
  const seeded = new Set(names);
  for (const nm of pipelineRefNames) {
    if (seeded.has(nm)) continue;
    const defRow = defsByName.get(nm);
    if (!defRow) continue;
    if (defRow.enabled === false) continue;
    names.push(nm);
    seeded.add(nm);
  }
  if (pipelineRefNames.length === 0 || names.length === 0) {
    return names;
  }
  const asSet = new Set(names);
  const ordered: string[] = [];
  const seen2 = new Set<string>();
  for (const nm of pipelineRefNames) {
    if (!asSet.has(nm) || seen2.has(nm)) continue;
    seen2.add(nm);
    ordered.push(nm);
  }
  for (const nm of names) {
    if (!seen2.has(nm)) {
      seen2.add(nm);
      ordered.push(nm);
    }
  }
  return ordered;
}

/**
 * ``aliasing_rule_definitions`` as name → row (dict or list form), mirroring Python ``_definitions_as_lookup``.
 */
export function aliasingRuleDefinitionsLookup(scopeDoc: Record<string, unknown>): Map<string, Record<string, unknown>> {
  const out = new Map<string, Record<string, unknown>>();
  const raw = scopeDoc.aliasing_rule_definitions;
  if (raw == null) return out;
  if (Array.isArray(raw)) {
    for (const v of raw) {
      if (!isRecord(v)) continue;
      const nm = String(v.name ?? "").trim();
      if (nm) out.set(nm, { ...v });
    }
    return out;
  }
  if (!isRecord(raw)) return out;
  for (const [k, v] of Object.entries(raw)) {
    if (!isRecord(v)) continue;
    const row: Record<string, unknown> = { ...v };
    const nm = String(row.name ?? k).trim();
    if (!nm) continue;
    if (row.name == null || !String(row.name).trim()) {
      row.name = nm;
    }
    out.set(nm, row);
  }
  return out;
}

type PipelineSeg = {
  /** Al node ids that receive ``data`` from the extraction when this segment is a top-level entry. */
  dataEntryAlIds: string[];
  /** Al node ids at the end of this segment (for chaining ordered siblings). */
  terminalAlIds: string[];
  compositionEdges: SeedAliasingCompositionEdge[];
};

function pushComp(
  edges: SeedAliasingCompositionEdge[],
  sig: Set<string>,
  source: string,
  target: string,
  kind: "sequence" | "parallel_group"
): void {
  const k = `${source}\0${target}\0${kind}`;
  if (sig.has(k)) {
    return;
  }
  sig.add(k);
  edges.push({ source, target, kind });
}

function hierarchyMode(x: Record<string, unknown>): string | null {
  const h = x.hierarchy;
  if (!isRecord(h)) {
    return null;
  }
  return typeof h.mode === "string" ? h.mode.trim().toLowerCase() : null;
}

function hierarchyChildren(x: Record<string, unknown>): unknown[] {
  const h = x.hierarchy;
  if (!isRecord(h) || !Array.isArray(h.children)) {
    return [];
  }
  return h.children;
}

/**
 * Build extraction→aliasing ``data`` targets and ``sequence`` / ``parallel_group`` edges from
 * ``aliasing_pipeline`` YAML (inverse of ``applyExtractionAliasingPipelinesFromCanvas`` for supported shapes).
 */
export function planAliasingPipelineComposition(
  pipeline: unknown,
  knownAlNames: Set<string>,
  resolveAlId: (ruleName: string) => string | null
): { dataTargets: string[]; compositionEdges: SeedAliasingCompositionEdge[] } {
  const compSig = new Set<string>();
  const allEdges: SeedAliasingCompositionEdge[] = [];

  if (Array.isArray(pipeline) && pipeline.length === 0) {
    return { dataTargets: [], compositionEdges: [] };
  }

  const filterKnownNames = (names: string[]): string[] => {
    const out: string[] = [];
    for (const n of names) {
      const t = n.trim();
      if (t && knownAlNames.has(t)) {
        out.push(t);
      }
    }
    return out;
  };

  const idsForNames = (names: string[]): string[] => {
    const ids: string[] = [];
    for (const nm of names) {
      const id = resolveAlId(nm);
      if (id) {
        ids.push(id);
      }
    }
    return ids;
  };

  const linearFromNames = (names: string[]): PipelineSeg => {
    const filtered = filterKnownNames(names);
    const ids = idsForNames(filtered);
    if (ids.length === 0) {
      return { dataEntryAlIds: [], terminalAlIds: [], compositionEdges: [] };
    }
    const local: SeedAliasingCompositionEdge[] = [];
    for (let i = 0; i < ids.length - 1; i++) {
      pushComp(local, compSig, ids[i]!, ids[i + 1]!, "sequence");
    }
    return {
      dataEntryAlIds: [ids[0]!],
      terminalAlIds: [ids[ids.length - 1]!],
      compositionEdges: local,
    };
  };

  const emitPart = (x: unknown): PipelineSeg => {
    if (x === null || x === undefined) {
      return { dataEntryAlIds: [], terminalAlIds: [], compositionEdges: [] };
    }
    if (typeof x === "string") {
      return linearFromNames([x]);
    }
    if (Array.isArray(x)) {
      if (x.length === 0) {
        return { dataEntryAlIds: [], terminalAlIds: [], compositionEdges: [] };
      }
      if (x.every((v) => typeof v === "string")) {
        return linearFromNames(x as string[]);
      }
      return emitOrderedList(x);
    }
    if (!isRecord(x)) {
      return { dataEntryAlIds: [], terminalAlIds: [], compositionEdges: [] };
    }
    const hm = hierarchyMode(x);
    if (hm === "concurrent") {
      return emitConcurrentList(hierarchyChildren(x));
    }
    if (hm === "ordered") {
      return emitOrderedList(hierarchyChildren(x));
    }
    if (typeof x.name === "string" && x.name.trim() && knownAlNames.has(String(x.name).trim())) {
      const rowName = String(x.name).trim();
      const ch = hierarchyChildren(x);
      if (ch.length > 0) {
        return emitOrderedList(ch);
      }
      return linearFromNames([rowName]);
    }
    if (Object.keys(x).length === 1 && !isRecord(x.hierarchy)) {
      const names = validationRulesStepsToLinearNames(x);
      return linearFromNames(names);
    }
    return { dataEntryAlIds: [], terminalAlIds: [], compositionEdges: [] };
  };

  function emitConcurrentList(branches: unknown[]): PipelineSeg {
    const dataEntryAlIds: string[] = [];
    const terminalAlIds: string[] = [];
    const local: SeedAliasingCompositionEdge[] = [];
    for (const br of branches) {
      const seg = emitPart(br);
      local.push(...seg.compositionEdges);
      dataEntryAlIds.push(...seg.dataEntryAlIds);
      terminalAlIds.push(...seg.terminalAlIds);
    }
    return { dataEntryAlIds, terminalAlIds, compositionEdges: local };
  }

  function emitOrderedList(children: unknown[]): PipelineSeg {
    if (children.length === 0) {
      return { dataEntryAlIds: [], terminalAlIds: [], compositionEdges: [] };
    }
    const acc = emitPart(children[0]);
    let edges = [...acc.compositionEdges];
    let prevTerminals = acc.terminalAlIds;
    let firstEntries = acc.dataEntryAlIds;
    for (let i = 1; i < children.length; i++) {
      const next = emitPart(children[i]);
      edges = [...edges, ...next.compositionEdges];
      for (const ex of prevTerminals) {
        for (const en of next.dataEntryAlIds) {
          pushComp(edges, compSig, ex, en, "sequence");
        }
      }
      prevTerminals = next.terminalAlIds;
    }
    return {
      dataEntryAlIds: firstEntries,
      terminalAlIds: prevTerminals,
      compositionEdges: edges,
    };
  }

  const topItems: unknown[] = Array.isArray(pipeline) ? pipeline : pipeline == null ? [] : [pipeline];
  if (topItems.length === 0) {
    return { dataTargets: [], compositionEdges: [] };
  }
  if (topItems.length === 1) {
    const seg = emitPart(topItems[0]);
    for (const e of seg.compositionEdges) {
      pushComp(allEdges, compSig, e.source, e.target, e.kind);
    }
    return { dataTargets: [...seg.dataEntryAlIds], compositionEdges: allEdges };
  }
  if (topItems.every((v) => typeof v === "string")) {
    const seg = linearFromNames(topItems as string[]);
    return { dataTargets: seg.dataEntryAlIds, compositionEdges: seg.compositionEdges };
  }
  const conc = emitConcurrentList(topItems);
  for (const e of conc.compositionEdges) {
    pushComp(allEdges, compSig, e.source, e.target, e.kind);
  }
  return { dataTargets: conc.dataEntryAlIds, compositionEdges: allEdges };
}

/** Walk ``aliasing_pipeline`` and collect rule **names** that appear in *known* (in encounter order, deduped). */
export function flattenAliasingPipelineToKnownRuleNames(
  pipeline: unknown,
  known: Set<string>
): string[] {
  const out: string[] = [];
  const seen = new Set<string>();

  const take = (s: string) => {
    const t = s.trim();
    if (t && known.has(t) && !seen.has(t)) {
      seen.add(t);
      out.push(t);
    }
  };

  const walk = (x: unknown): void => {
    if (x === null || x === undefined) return;
    if (typeof x === "string") {
      take(x);
      return;
    }
    if (Array.isArray(x)) {
      for (const v of x) walk(v);
      return;
    }
    if (!isRecord(x)) return;
    if (typeof x.name === "string") {
      take(x.name);
    }
    const h = x.hierarchy;
    if (isRecord(h)) {
      const ch = h.children;
      if (Array.isArray(ch)) {
        for (const c of ch) walk(c);
      }
      return;
    }
    for (const [k, v] of Object.entries(x)) {
      if (k === "hierarchy" || k === "name" || k === "handler" || k === "config" || k === "mode") {
        if (k !== "name") walk(v);
        continue;
      }
      if (known.has(k.trim())) {
        take(k);
        walk(v);
      } else {
        walk(v);
      }
    }
  };

  walk(pipeline);
  return out;
}

export type AssociationPair = { source_view_index: number; extraction_rule_name: string };

/** Best-effort al transform head name for an extraction row using ``scope_filters`` and source view entity types. */
export function inferAliasingHeadNameFromConfig(
  extractionRow: Record<string, unknown>,
  knownAlNames: Set<string>,
  assocPairs: AssociationPair[],
  sourceViews: unknown,
  alRows: unknown[]
): string | null {
  const entities = new Set<string>();
  const sff = extractionRow.scope_filters;
  if (isRecord(sff)) {
    const et = sff.entity_type;
    if (Array.isArray(et)) {
      for (const e of et) {
        if (e != null && String(e).trim()) {
          entities.add(String(e).trim());
        }
      }
    }
  }
  const ruleName = String(extractionRow.name ?? "").trim();
  for (const p of assocPairs) {
    if (p.extraction_rule_name !== ruleName) {
      continue;
    }
    const sv = Array.isArray(sourceViews) ? sourceViews[p.source_view_index] : undefined;
    if (isRecord(sv) && sv.entity_type != null) {
      entities.add(String(sv.entity_type).trim());
    }
  }

  const sortRows = (rows: Record<string, unknown>[]): void => {
    rows.sort((a, b) => {
      const pa = typeof a.priority === "number" && Number.isFinite(a.priority) ? a.priority : 100;
      const pb = typeof b.priority === "number" && Number.isFinite(b.priority) ? b.priority : 100;
      if (pa !== pb) {
        return pa - pb;
      }
      return String(a.name ?? "").localeCompare(String(b.name ?? ""));
    });
  };

  const pool: Record<string, unknown>[] = [];
  for (const r of alRows) {
    if (isRecord(r) && String(r.name ?? "").trim() && knownAlNames.has(String(r.name).trim())) {
      pool.push(r);
    }
  }
  if (pool.length === 0) {
    return null;
  }

  if (entities.size === 0) {
    const wild: Record<string, unknown>[] = [];
    for (const r of pool) {
      const sf2 = r.scope_filters;
      if (!isRecord(sf2)) {
        wild.push(r);
        continue;
      }
      const et2 = sf2.entity_type;
      if (!Array.isArray(et2) || et2.length === 0) {
        wild.push(r);
      }
    }
    const use = wild.length > 0 ? wild : pool;
    sortRows(use);
    const n = String(use[0]!.name ?? "").trim();
    return n || null;
  }

  const matched: Record<string, unknown>[] = [];
  for (const r of pool) {
    const sf2 = r.scope_filters;
    if (!isRecord(sf2)) {
      matched.push(r);
      continue;
    }
    const etl = sf2.entity_type;
    if (!Array.isArray(etl) || etl.length === 0) {
      matched.push(r);
      continue;
    }
    const ets = new Set(etl.map((e) => String(e)));
    let hit = false;
    for (const e of entities) {
      if (ets.has(e)) {
        hit = true;
        break;
      }
    }
    if (hit) {
      matched.push(r);
    }
  }
  if (matched.length === 0) {
    return null;
  }
  sortRows(matched);
  return String(matched[0]!.name ?? "").trim() || null;
}

/** Rule id from a ``validation_rules`` step object (full rule definitions use ``name``). */
function validationRuleStepDisplayName(s: unknown): string | null {
  if (typeof s === "string" && s.trim()) {
    return s.trim();
  }
  if (!isRecord(s)) {
    return null;
  }
  const n = s.name;
  if (typeof n === "string" && n.trim()) {
    return n.trim();
  }
  const id = s.id;
  if (typeof id === "string" && id.trim()) {
    return id.trim();
  }
  const vrn = s.validation_rule_name;
  if (typeof vrn === "string" && vrn.trim()) {
    return vrn.trim();
  }
  return null;
}

/**
 * Unwrap ``linearChainToShorthand`` object form from sync
 * (e.g. ``{ a: [ { b: [c] } ] }``) into ``[a, b, c]``).
 */
function linearChainShorthandToNames(x: unknown): string[] {
  if (x === null || x === undefined) {
    return [];
  }
  if (typeof x === "string" && x.trim()) {
    return [x.trim()];
  }
  if (Array.isArray(x)) {
    return x.flatMap((el) => linearChainShorthandToNames(el));
  }
  if (isRecord(x) && isRecord((x as Record<string, unknown>).hierarchy)) {
    const h = (x as Record<string, unknown>).hierarchy as Record<string, unknown>;
    if (Array.isArray(h.children)) {
      return validationRulesStepsToLinearNamesInner(h.children);
    }
    return [];
  }
  if (isRecord(x)) {
    const keys = Object.keys(x);
    if (keys.length === 1) {
      const k = keys[0]!.trim();
      const v = (x as Record<string, unknown>)[keys[0]!];
      if (!k) {
        return linearChainShorthandToNames(v);
      }
      const sub = Array.isArray(v) ? v.flatMap((el) => linearChainShorthandToNames(el)) : linearChainShorthandToNames(v);
      return [k, ...sub];
    }
  }
  return [];
}

/** Collapse consecutive duplicate step names (linearization or YAML can repeat the same id). */
export function dedupeConsecutiveValidationStepNames(names: string[]): string[] {
  const out: string[] = [];
  for (const n of names) {
    if (out.length > 0 && out[out.length - 1] === n) {
      continue;
    }
    out.push(n);
  }
  return out;
}

/**
 * When the full list is ``k`` repeats of the same prefix (e.g. identical concurrent branches flattened),
 * keep one period. Does not remove intentional single repeats; smallest repeating unit must appear at
 * least twice and tile the whole array.
 */
export function collapseRepeatingValidationNameSequence(names: string[]): string[] {
  const n = names.length;
  if (n < 4) return names;
  for (let p = 2; p <= Math.floor(n / 2); p++) {
    if (n % p !== 0) continue;
    const reps = n / p;
    if (reps < 2) continue;
    let ok = true;
    for (let i = 0; i < n; i++) {
      if (names[i] !== names[i % p]) {
        ok = false;
        break;
      }
    }
    if (ok) return names.slice(0, p);
  }
  return names;
}

function dedupeLinearRuleNames(names: string[]): string[] {
  return dedupeConsecutiveValidationStepNames(names);
}

/**
 * Linearize ``validation_rules`` for **match-validation seeding** only. Applies
 * ``collapseRepeatingValidationNameSequence`` to undo accidental k× repetition from bad sync YAML.
 * Do **not** use for aliasing-pipeline shorthand (``planAliasingPipelineComposition`` / ``emitPart``):
 * pipelines may legitimately alternate rule names (e.g. ``[a,b,a,b]``), which must not be collapsed.
 */
export function validationRulesLinearNamesForSeed(steps: unknown): string[] {
  return collapseRepeatingValidationNameSequence(
    dedupeConsecutiveValidationStepNames(validationRulesStepsToLinearNamesInner(steps))
  );
}

export function validationStepChainsEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) {
    return false;
  }
  return a.every((v, i) => v === b[i]);
}

/** Raw linearization (no trailing collapse / consecutive dedupe). */
function validationRulesStepsToLinearNamesInner(steps: unknown): string[] {
  if (steps === null || steps === undefined) {
    return [];
  }
  if (typeof steps === "string" && steps.trim()) {
    return [steps.trim()];
  }
  if (Array.isArray(steps)) {
    const out: string[] = [];
    for (const s of steps) {
      if (typeof s === "string" && s.trim()) {
        out.push(s.trim());
        continue;
      }
      if (isRecord(s) && isRecord(s.hierarchy)) {
        out.push(...validationRulesStepsToLinearNamesInner(s));
        continue;
      }
      if (isRecord(s) && !isRecord(s.hierarchy) && Object.keys(s).length === 1) {
        out.push(...linearChainShorthandToNames(s));
        continue;
      }
      const stepName = validationRuleStepDisplayName(s);
      if (stepName) {
        out.push(stepName);
        continue;
      }
      out.push(...validationRulesStepsToLinearNamesInner(s));
    }
    return out;
  }
  if (isRecord(steps)) {
    const h = steps.hierarchy;
    if (isRecord(h) && Array.isArray(h.children)) {
      const children = h.children as unknown[];
      const modeRaw = h.mode;
      const mode =
        modeRaw === "concurrent" || String(modeRaw).toLowerCase() === "concurrent"
          ? "concurrent"
          : "ordered";
      if (mode === "concurrent" && children.length > 1) {
        const perChild = children.map((c) => validationRulesStepsToLinearNamesInner(c));
        const f = perChild[0] ?? [];
        if (f.length > 0 && perChild.every((arr) => validationStepChainsEqual(arr, f))) {
          return [...f];
        }
      }
      return validationRulesStepsToLinearNamesInner(children);
    }
    const singleName = validationRuleStepDisplayName(steps);
    if (singleName && !isRecord(steps.hierarchy)) {
      return [singleName];
    }
    if (Object.keys(steps).length === 1) {
      return linearChainShorthandToNames(steps);
    }
  }
  return [];
}

/**
 * Turn shorthand / hierarchy / objects into a linear list of string ids (validation **or** aliasing
 * pipeline segments). Only consecutive duplicate names are merged — intentional alternation is kept.
 */
export function validationRulesStepsToLinearNames(steps: unknown): string[] {
  return dedupeLinearRuleNames(validationRulesStepsToLinearNamesInner(steps));
}

const SEED_SEQ: Pick<WorkflowCanvasEdge, "source_handle" | "target_handle" | "kind"> = {
  kind: "sequence",
  source_handle: "out",
  target_handle: "in",
};
type MatchBuildOpts = {
  y: number;
  xStart: number;
  xStep: number;
  idPrefix: string;
};

/**
 * Create ``match_validation_extraction`` chain from a flat name list. Returns head id for a **data** edge
 * from the source extraction, and all nodes/sequence edges to merge into the document.
 */
export function buildExtractionMatchValidationSubgraph(
  stepNames: string[],
  perRuleExtractionName: string | null,
  global: boolean,
  o: MatchBuildOpts
): { nodes: WorkflowCanvasNode[]; edges: WorkflowCanvasEdge[]; headId: string | null; tailId: string | null } {
  const names = dedupeConsecutiveValidationStepNames(stepNames);
  if (names.length === 0) {
    return { nodes: [], edges: [], headId: null, tailId: null };
  }
  const nodes: WorkflowCanvasNode[] = [];
  const edges: WorkflowCanvasEdge[] = [];
  let prev: string | null = null;
  let firstId: string | null = null;
  let lastId: string | null = null;
  for (let i = 0; i < names.length; i++) {
    const nm = names[i]!;
    const id = `mve_${o.idPrefix}_${canvasIdSlug(nm)}_${i}`;
    if (i === 0) {
      firstId = id;
    }
    lastId = id;
    const ref: Record<string, unknown> = global
      ? { extraction_global_validation: true }
      : { extraction_rule_name: perRuleExtractionName };
    nodes.push({
      id,
      kind: "match_validation_extraction",
      position: { x: o.xStart + i * o.xStep, y: o.y },
      data: {
        label: nm,
        validation_rule_context: "extraction",
        validation_rule_name: nm,
        ref,
      },
    });
    if (prev) {
      edges.push({
        id: `e_seq_${prev}_${id}`,
        source: prev,
        target: id,
        ...SEED_SEQ,
      });
    }
    prev = id;
  }
  return {
    nodes,
    edges,
    headId: firstId,
    tailId: lastId,
  };
}

export function buildAliasingMatchValidationSubgraph(
  stepNames: string[],
  perRuleAliasingName: string | null,
  global: boolean,
  o: MatchBuildOpts
): { nodes: WorkflowCanvasNode[]; edges: WorkflowCanvasEdge[]; headId: string | null; tailId: string | null } {
  const names = dedupeConsecutiveValidationStepNames(stepNames);
  if (names.length === 0) {
    return { nodes: [], edges: [], headId: null, tailId: null };
  }
  const nodes: WorkflowCanvasNode[] = [];
  const edges: WorkflowCanvasEdge[] = [];
  let prev: string | null = null;
  let firstId: string | null = null;
  let lastId: string | null = null;
  for (let i = 0; i < names.length; i++) {
    const nm = names[i]!;
    const id = `mva_${o.idPrefix}_${canvasIdSlug(nm)}_${i}`;
    if (i === 0) {
      firstId = id;
    }
    lastId = id;
    const ref: Record<string, unknown> = global
      ? { aliasing_global_validation: true }
      : { aliasing_rule_name: perRuleAliasingName };
    nodes.push({
      id,
      kind: "match_validation_aliasing",
      position: { x: o.xStart + i * o.xStep, y: o.y },
      data: {
        label: nm,
        validation_rule_context: "aliasing",
        validation_rule_name: nm,
        ref,
      },
    });
    if (prev) {
      edges.push({
        id: `e_sqa_${prev}_${id}`,
        source: prev,
        target: id,
        ...SEED_SEQ,
      });
    }
    prev = id;
  }
  return {
    nodes,
    edges,
    headId: firstId,
    tailId: lastId,
  };
}

function canvasIdSlug(s: string): string {
  const t = s.trim().toLowerCase();
  if (!t) {
    return "x";
  }
  const out = t.replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "");
  return out || "x";
}

/** Data from extraction to first match (validation handles; normalized by ``normalizeWorkflowCanvasDocumentEdgeHandles``). */
export function edgeExtractionToMatchFirst(extractId: string, matchHead: string, edgeIdx: number): WorkflowCanvasEdge {
  return {
    id: `e_ext_mve_${extractId}_${matchHead}_${edgeIdx}`,
    source: extractId,
    target: matchHead,
    kind: "data",
    source_handle: "out",
    target_handle: "in",
  };
}

export function edgeAliasingToMatchFirst(aliasingId: string, matchHead: string, edgeIdx: number): WorkflowCanvasEdge {
  return {
    id: `e_al_mva_${aliasingId}_${matchHead}_${edgeIdx}`,
    source: aliasingId,
    target: matchHead,
    kind: "data",
    source_handle: "out",
    target_handle: "in",
  };
}
