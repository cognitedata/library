import {
  emptyWorkflowCanvasDocument,
  normalizeWorkflowCanvasDocumentEdgeHandles,
  type WorkflowCanvasDocument,
  type WorkflowCanvasEdge,
  type WorkflowCanvasNode,
} from "../../types/workflowCanvas";

/** Default primary data path (matches ``Kea*`` node ``out`` / ``in`` handle ids). */
const SEED_DATA_OUT_IN = { source_handle: "out", target_handle: "in" } as const;
/** Chained aliasing sequence steps between ``kind: "aliasing"`` nodes. */
const SEED_SEQ_OUT_IN = { source_handle: "out", target_handle: "in" } as const;

import { getAliasingTransformRuleRows } from "./aliasingScopeData";
import { parseSourceViewToExtractionPairs } from "./workflowScopeAssociations";

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
};

type AliasingRuleSeed = {
  name: string;
  handler: string;
  enabled: boolean;
  priority: number;
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
    out.push({ name, handler, enabled, priority });
  }
  out.sort((a, b) => {
    if (a.priority !== b.priority) return a.priority - b.priority;
    return a.name.localeCompare(b.name);
  });
  return out;
}

function parseAliasingRules(scopeDoc: Record<string, unknown>): AliasingRuleSeed[] {
  const al = scopeDoc.aliasing as Record<string, unknown> | undefined;
  const config = al?.config as Record<string, unknown> | undefined;
  const data = config?.data as Record<string, unknown> | undefined;
  const rules = data ? getAliasingTransformRuleRows(data) : [];
  if (!Array.isArray(rules) || rules.length === 0) return [];
  const out: AliasingRuleSeed[] = [];
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    const name = row.name != null ? String(row.name).trim() : "";
    if (!name) continue;
    const enabled = row.enabled !== false;
    const handler = row.handler != null ? String(row.handler) : "character_substitution";
    const priority = typeof row.priority === "number" && Number.isFinite(row.priority) ? row.priority : 100;
    out.push({ name, handler, enabled, priority });
  }
  out.sort((a, b) => {
    if (a.priority !== b.priority) return a.priority - b.priority;
    return a.name.localeCompare(b.name);
  });
  return out;
}

/**
 * Build a minimal layout graph when the canvas file is empty: Start → source views → extraction
 * (from ``associations``) → aliasing (structural order from ``aliasing_rules[]`` sort) → End.
 *
 * Confidence match-validation subgraphs and per-rule ``aliasing_pipeline`` are **not** inferred
 * from scope YAML; draw them on the canvas and run scope sync so ``validation`` / ``aliasing_pipeline``
 * are written from nodes and edges.
 */
export function seedCanvasFromScope(scopeDoc: Record<string, unknown>): WorkflowCanvasDocument {
  const base = emptyWorkflowCanvasDocument();
  const nodes: WorkflowCanvasNode[] = [];
  const edges: WorkflowCanvasEdge[] = [];

  const COL_START = 0;
  const COL_SV = 200;
  const COL_EXT = 480;
  const COL_AL = 720;
  const COL_END = 960;
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

  const extractionRules = parseExtractionRules(scopeDoc).filter((r) => r.enabled);
  const aliasingRules = parseAliasingRules(scopeDoc).filter((r) => r.enabled);

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
  extractionRules.forEach((rule, j) => {
    const id = `ext_${canvasIdSlug(rule.name)}`;
    extIds.push(id);
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

  const assocPairs = parseSourceViewToExtractionPairs(scopeDoc);
  for (const r of svRows) {
    for (const rule of extractionRules) {
      const bound = assocPairs.some(
        (p) => p.source_view_index === r.index && p.extraction_rule_name === rule.name
      );
      if (!bound) continue;
      const extId = `ext_${canvasIdSlug(rule.name)}`;
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
  aliasingRules.forEach((rule, k) => {
    const id = `al_${canvasIdSlug(rule.name)}`;
    alIds.push(id);
    nodes.push({
      id,
      kind: "aliasing",
      position: { x: COL_AL, y: 48 + k * GAP_Y },
      data: {
        label: rule.name,
        handler_id: rule.handler,
        handler_family: "aliasing",
        ref: { aliasing_rule_name: rule.name },
      },
    });
  });

  const seededEdgeSig = new Set<string>();
  const addSeedEdge = (source: string, target: string, kind: WorkflowCanvasEdge["kind"]): void => {
    const sig = `${source}\0${target}\0${kind}`;
    if (seededEdgeSig.has(sig)) return;
    seededEdgeSig.add(sig);
    const handleProps = kind === "sequence" ? SEED_SEQ_OUT_IN : SEED_DATA_OUT_IN;
    edges.push({
      id: `e_${source}_${target}_${edges.length}`,
      source,
      target,
      kind,
      ...handleProps,
    });
  };

  if (alIds.length > 0) {
    const firstAl = alIds[0]!;
    for (const er of extractionRules) {
      addSeedEdge(`ext_${canvasIdSlug(er.name)}`, firstAl, "data");
    }
    for (let i = 0; i < alIds.length - 1; i++) {
      addSeedEdge(alIds[i]!, alIds[i + 1]!, "sequence");
    }
    addSeedEdge(alIds[alIds.length - 1]!, ID_END, "data");
  } else if (extIds.length > 0) {
    for (const extId of extIds) {
      edges.push({
        id: `e_${extId}_${ID_END}`,
        source: extId,
        target: ID_END,
        kind: "data",
        ...SEED_DATA_OUT_IN,
      });
    }
  }

  if (svRows.length === 0 && extractionRules.length > 0) {
    edges.push({
      id: `e_${ID_START}_${extIds[0]}`,
      source: ID_START,
      target: extIds[0],
      kind: "data",
      ...SEED_DATA_OUT_IN,
    });
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
