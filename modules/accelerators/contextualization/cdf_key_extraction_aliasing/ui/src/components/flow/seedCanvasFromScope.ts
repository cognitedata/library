import {
  emptyWorkflowCanvasDocument,
  type WorkflowCanvasDocument,
  type WorkflowCanvasEdge,
  type WorkflowCanvasNode,
} from "../../types/workflowCanvas";
import {
  confidenceMatchRulesStructureKey,
  resolveConfidenceMatchRuleNames,
} from "../../utils/confidenceMatchRuleNames";
import { resolveSourceViewEntityTypeKey } from "./workflowScopePatch";

/** Slug for stable node ids (letters, digits, underscore). */
function canvasIdSlug(s: string): string {
  const t = s.trim().toLowerCase();
  if (!t) return "x";
  const out = t.replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "");
  return out || "x";
}

/** Short hash segment for canvas node ids (same list → same chain id). */
function shortHashKeyForList(names: string[]): string {
  const s = names.join("\u0001");
  let h = 5381;
  for (let i = 0; i < s.length; i++) {
    h = (h * 33) ^ s.charCodeAt(i);
  }
  return `h${(h >>> 0).toString(16).slice(0, 12)}`;
}

function parseExtractionRuleValidationGroups(
  scopeDoc: Record<string, unknown>
): { ruleName: string; confidenceNames: string[]; validation: unknown }[] {
  const ke = scopeDoc.key_extraction as Record<string, unknown> | undefined;
  const config = ke?.config as Record<string, unknown> | undefined;
  const data = config?.data as Record<string, unknown> | undefined;
  const rules = data?.extraction_rules;
  if (!Array.isArray(rules)) return [];
  const out: { ruleName: string; confidenceNames: string[]; validation: unknown }[] = [];
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    if (row.enabled === false) continue;
    const name = row.name != null ? String(row.name).trim() : "";
    if (!name) continue;
    const names = resolveConfidenceMatchRuleNames(row.validation, scopeDoc);
    if (names.length === 0) continue;
    out.push({ ruleName: name, confidenceNames: names, validation: row.validation });
  }
  return out;
}

function parseAliasingRuleValidationGroups(
  scopeDoc: Record<string, unknown>
): { ruleName: string; confidenceNames: string[]; validation: unknown }[] {
  const al = scopeDoc.aliasing as Record<string, unknown> | undefined;
  const config = al?.config as Record<string, unknown> | undefined;
  const data = config?.data as Record<string, unknown> | undefined;
  const rules = data?.aliasing_rules;
  if (!Array.isArray(rules)) return [];
  const out: { ruleName: string; confidenceNames: string[]; validation: unknown }[] = [];
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    if (row.enabled === false) continue;
    const name = row.name != null ? String(row.name).trim() : "";
    if (!name) continue;
    const names = resolveConfidenceMatchRuleNames(row.validation, scopeDoc);
    if (names.length === 0) continue;
    out.push({ ruleName: name, confidenceNames: names, validation: row.validation });
  }
  return out;
}

/** `key_extraction.config.data.validation` — applies to merged extraction output. */
function globalKeyExtractionValidationNames(scopeDoc: Record<string, unknown>): string[] {
  const ke = scopeDoc.key_extraction as Record<string, unknown> | undefined;
  const config = ke?.config as Record<string, unknown> | undefined;
  const data = config?.data as Record<string, unknown> | undefined;
  return resolveConfidenceMatchRuleNames(data?.validation, scopeDoc);
}

/** `aliasing.config.data.validation` — applies after aliasing rules. */
function globalAliasingValidationNames(scopeDoc: Record<string, unknown>): string[] {
  const al = scopeDoc.aliasing as Record<string, unknown> | undefined;
  const config = al?.config as Record<string, unknown> | undefined;
  const data = config?.data as Record<string, unknown> | undefined;
  return resolveConfidenceMatchRuleNames(data?.validation, scopeDoc);
}

type ExtractionRuleSeed = {
  name: string;
  handler: string;
  entityTypes: string[];
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
    let entityTypes: string[] = [];
    const sf = row.scope_filters;
    if (sf && typeof sf === "object" && !Array.isArray(sf)) {
      const et = (sf as Record<string, unknown>).entity_type;
      if (Array.isArray(et)) {
        entityTypes = et.map((x) => String(x).trim().toLowerCase()).filter(Boolean);
      } else if (typeof et === "string" && et.trim()) {
        entityTypes = [et.trim().toLowerCase()];
      }
    }
    out.push({ name, handler, entityTypes, enabled, priority });
  }
  out.sort((a, b) => {
    if (a.priority !== b.priority) return a.priority - b.priority;
    return a.name.localeCompare(b.name);
  });
  return out;
}

/** Raw `key_extraction.config.data.extraction_rules[]` row for pipeline seeding. */
function getExtractionRuleRow(
  scopeDoc: Record<string, unknown>,
  ruleName: string
): Record<string, unknown> | null {
  const ke = scopeDoc.key_extraction as Record<string, unknown> | undefined;
  const config = ke?.config as Record<string, unknown> | undefined;
  const data = config?.data as Record<string, unknown> | undefined;
  const rules = data?.extraction_rules;
  if (!Array.isArray(rules)) return null;
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    if (String(row.name ?? "").trim() === ruleName) return row;
  }
  return null;
}

/**
 * Linear order of aliasing rule ids for canvas wiring (DFS; enough for typical list / hierarchy seeds).
 */
function linearAliasingRuleNamesFromPipeline(pipeline: unknown): string[] {
  if (!pipeline) return [];
  if (!Array.isArray(pipeline)) return [];
  const out: string[] = [];
  for (const item of pipeline) {
    walkAliasingPipelineItem(item, out);
  }
  return out;
}

function walkAliasingPipelineItem(item: unknown, out: string[]): void {
  if (item === null || item === undefined) return;
  if (typeof item === "string") {
    const s = item.trim();
    if (s) out.push(s);
    return;
  }
  if (Array.isArray(item)) {
    for (const x of item) walkAliasingPipelineItem(x, out);
    return;
  }
  if (typeof item !== "object") return;
  const o = item as Record<string, unknown>;
  if (typeof o.ref === "string" && o.ref.trim()) {
    out.push(o.ref.trim());
    return;
  }
  const seq = o.sequence;
  if (typeof seq === "string" && seq.trim()) {
    out.push(seq.trim());
    return;
  }
  const h = o.hierarchy;
  if (h && typeof h === "object" && !Array.isArray(h)) {
    const children = (h as Record<string, unknown>).children;
    if (Array.isArray(children)) {
      for (const c of children) walkAliasingPipelineItem(c, out);
    }
    return;
  }
  for (const k of Object.keys(o)) {
    if (k === "ref" || k === "hierarchy" || k === "sequence") continue;
    const v = o[k];
    if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") continue;
    out.push(k.trim());
    if (Array.isArray(v)) {
      for (const x of v) walkAliasingPipelineItem(x, out);
    } else if (v && typeof v === "object" && !Array.isArray(v)) {
      walkAliasingPipelineItem(v, out);
    }
  }
}

function parseAliasingRules(scopeDoc: Record<string, unknown>): AliasingRuleSeed[] {
  const al = scopeDoc.aliasing as Record<string, unknown> | undefined;
  const config = al?.config as Record<string, unknown> | undefined;
  const data = config?.data as Record<string, unknown> | undefined;
  const rules = data?.aliasing_rules;
  if (!Array.isArray(rules)) return [];
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

/** Whether an extraction rule applies to a source entity-type bucket (scope_filters.entity_type). */
function extractionRuleMatchesEntityType(
  rule: ExtractionRuleSeed,
  rawEntityKey: string
): boolean {
  if (!rule.enabled) return false;
  if (rule.entityTypes.length === 0) return true;
  const low = rawEntityKey.toLowerCase();
  return rule.entityTypes.includes(low);
}

/**
 * Build a layout graph: Start → source views → extraction → aliasing → End.
 * Edges: Start → each source view; source view → extraction when scope matches;
 * extraction → first aliasing (data), then aliasing sequence; terminal nodes → End.
 */
export function seedCanvasFromScope(scopeDoc: Record<string, unknown>): WorkflowCanvasDocument {
  const base = emptyWorkflowCanvasDocument();
  const nodes: WorkflowCanvasNode[] = [];
  const edges: WorkflowCanvasEdge[] = [];

  const COL_START = 0;
  const COL_SV = 200;
  const COL_VR_SV = 330;
  const COL_EXT = 480;
  const COL_VR_EXT = 560;
  const COL_AL = 760;
  const COL_VR_AL = 840;
  const COL_END = 1040;
  const GAP_Y = 76;
  const VR_STACK = 34;

  const ID_START = "flow_start";
  const ID_END = "flow_end";

  type SvRow = { index: number; entityKey: string; viewLabel: string };
  const svRows: SvRow[] = [];
  const svs = scopeDoc.source_views;
  if (Array.isArray(svs)) {
    for (let i = 0; i < svs.length; i++) {
      const v = svs[i];
      if (!v || typeof v !== "object" || Array.isArray(v)) continue;
      const row = v as Record<string, unknown>;
      const entityKey = resolveSourceViewEntityTypeKey(row, i);
      const ext = row.view_external_id != null ? String(row.view_external_id) : `view_${i}`;
      svRows.push({ index: i, entityKey, viewLabel: ext });
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

  // —— Source views ——
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
          entity_type: r.entityKey,
        },
      },
    });
  });

  // —— Extraction nodes ——
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
    });
  }

  // Source view → confidence validation rules (source_views[].validation); chain = evaluation order.
  // Identical validation lists → one shared chain with data edges from each participating source view.
  type SvVal = { index: number; cmNames: string[]; validation: unknown };
  const svValRows: SvVal[] = [];
  for (const r of svRows) {
    const svsRow = Array.isArray(svs) ? svs[r.index] : undefined;
    const v =
      svsRow && typeof svsRow === "object" && !Array.isArray(svsRow)
        ? (svsRow as Record<string, unknown>).validation
        : undefined;
    const cmNames = resolveConfidenceMatchRuleNames(v, scopeDoc);
    if (cmNames.length === 0) continue;
    svValRows.push({ index: r.index, cmNames, validation: v });
  }
  const svClusters = new Map<string, SvVal[]>();
  for (const row of svValRows) {
    const k = confidenceMatchRulesStructureKey(row.validation, scopeDoc);
    const arr = svClusters.get(k) ?? [];
    arr.push(row);
    svClusters.set(k, arr);
  }
  for (const [, cluster] of svClusters) {
    cluster.sort((a, b) => a.index - b.index);
    const cmNames = cluster[0].cmNames;
    const chainKey = shortHashKeyForList(cmNames);
    const indices = cluster.map((c) => c.index);
    const vrIds = cmNames.map((ruleName) => `vrule_sv_${chainKey}_${canvasIdSlug(ruleName)}`);
    const shared = cluster.length > 1;
    cmNames.forEach((ruleName, rIdx) => {
      const id = vrIds[rIdx];
      const minRow = Math.min(...cluster.map((c) => c.index));
      nodes.push({
        id,
        kind: "match_validation_source_view",
        position: { x: COL_VR_SV, y: 48 + minRow * GAP_Y + rIdx * VR_STACK },
        data: {
          label: ruleName,
          validation_rule_context: "source_view",
          confidence_match_rule_name: ruleName,
          ref: shared
            ? {
                shared_source_view_validation_chain: true,
                source_view_indices: [...indices],
                validation_list_key: chainKey,
                source_view_index: indices[0],
              }
            : { source_view_index: indices[0] },
        },
      });
    });
    if (vrIds.length > 0) {
      for (const c of cluster) {
        const svNodeId = `sv_${c.index}`;
        edges.push({
          id: `e_${svNodeId}_${vrIds[0]}`,
          source: svNodeId,
          target: vrIds[0],
          kind: "data",
        });
      }
      for (let i = 0; i < vrIds.length - 1; i++) {
        edges.push({
          id: `e_${vrIds[i]}_${vrIds[i + 1]}`,
          source: vrIds[i],
          target: vrIds[i + 1],
          kind: "sequence",
        });
      }
    }
  }

  // Source view → extraction (same matching as former entity-type hop)
  for (const r of svRows) {
    for (const rule of extractionRules) {
      if (!extractionRuleMatchesEntityType(rule, r.entityKey)) continue;
      const extId = `ext_${canvasIdSlug(rule.name)}`;
      edges.push({
        id: `e_sv_${r.index}_${extId}`,
        source: `sv_${r.index}`,
        target: extId,
        kind: "data",
      });
    }
  }

  // —— Aliasing nodes (top-to-bottom = ascending priority, then name) ——
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

  // Extraction rule → confidence_match_rules under key_extraction extraction_rules[].validation.
  // Identical validation lists → one shared chain; multiple extraction nodes fan in with data edges.
  const extValGroups = parseExtractionRuleValidationGroups(scopeDoc);
  const extClusters = new Map<string, { ruleName: string; confidenceNames: string[] }[]>();
  for (const g of extValGroups) {
    const k = confidenceMatchRulesStructureKey(g.validation, scopeDoc);
    const arr = extClusters.get(k) ?? [];
    arr.push(g);
    extClusters.set(k, arr);
  }
  for (const [, cluster] of extClusters) {
    cluster.sort((a, b) => a.ruleName.localeCompare(b.ruleName));
    const confidenceNames = cluster[0].confidenceNames;
    const chainKey = shortHashKeyForList(confidenceNames);
    const ruleNames = cluster.map((c) => c.ruleName);
    const shared = cluster.length > 1;
    const vrIds = confidenceNames.map((cmName) => `vrule_ke_${chainKey}_${canvasIdSlug(cmName)}`);
    const js = ruleNames
      .map((rn) => extractionRules.findIndex((er) => er.name === rn))
      .filter((j) => j >= 0);
    const baseY = js.length > 0 ? 48 + Math.min(...js) * GAP_Y : 48;
    confidenceNames.forEach((cmName, rIdx) => {
      const id = vrIds[rIdx];
      nodes.push({
        id,
        kind: "match_validation_extraction",
        position: { x: COL_VR_EXT, y: baseY + rIdx * VR_STACK },
        data: {
          label: cmName,
          validation_rule_context: "extraction",
          confidence_match_rule_name: cmName,
          ref: shared
            ? {
                shared_extraction_validation_chain: true,
                extraction_rule_names: [...ruleNames],
                validation_list_key: chainKey,
              }
            : { extraction_rule_name: ruleNames[0] },
        },
      });
    });
    if (vrIds.length > 0) {
      for (const { ruleName } of cluster) {
        const extId = `ext_${canvasIdSlug(ruleName)}`;
        if (!extIds.includes(extId)) continue;
        edges.push({
          id: `e_${extId}_${vrIds[0]}`,
          source: extId,
          target: vrIds[0],
          kind: "data",
        });
      }
      for (let i = 0; i < vrIds.length - 1; i++) {
        edges.push({
          id: `e_${vrIds[i]}_${vrIds[i + 1]}`,
          source: vrIds[i],
          target: vrIds[i + 1],
          kind: "sequence",
        });
      }
    }
  }

  // key_extraction.config.data.validation — global extraction-stage match rules (chained in list order)
  const globalKeNames = globalKeyExtractionValidationNames(scopeDoc);
  const globalKeIds = globalKeNames.map((cmName) => `vrule_ke_data_${canvasIdSlug(cmName)}`);
  globalKeNames.forEach((cmName, rIdx) => {
    const id = globalKeIds[rIdx];
    nodes.push({
      id,
      kind: "match_validation_extraction",
      position: {
        x: COL_VR_EXT,
        y: 48 + extractionRules.length * GAP_Y + 24 + rIdx * VR_STACK,
      },
      data: {
        label: cmName,
        notes: "key_extraction.config.data.validation",
        validation_rule_context: "extraction",
        confidence_match_rule_name: cmName,
        ref: { extraction_global_validation: true },
      },
    });
  });
  if (globalKeIds.length > 0) {
    for (const extId of extIds) {
      edges.push({
        id: `e_${extId}_${globalKeIds[0]}`,
        source: extId,
        target: globalKeIds[0],
        kind: "data",
      });
    }
    for (let i = 0; i < globalKeIds.length - 1; i++) {
      edges.push({
        id: `e_${globalKeIds[i]}_${globalKeIds[i + 1]}`,
        source: globalKeIds[i],
        target: globalKeIds[i + 1],
        kind: "sequence",
      });
    }
  }

  // Aliasing rule → confidence_match_rules under aliasing_rules[].validation (shared chains like extraction).
  const alValGroups = parseAliasingRuleValidationGroups(scopeDoc);
  const alClusters = new Map<string, { ruleName: string; confidenceNames: string[] }[]>();
  for (const g of alValGroups) {
    const k = confidenceMatchRulesStructureKey(g.validation, scopeDoc);
    const arr = alClusters.get(k) ?? [];
    arr.push(g);
    alClusters.set(k, arr);
  }
  for (const [, cluster] of alClusters) {
    cluster.sort((a, b) => a.ruleName.localeCompare(b.ruleName));
    const confidenceNames = cluster[0].confidenceNames;
    const chainKey = shortHashKeyForList(confidenceNames);
    const ruleNames = cluster.map((c) => c.ruleName);
    const shared = cluster.length > 1;
    const vrIds = confidenceNames.map((cmName) => `vrule_al_${chainKey}_${canvasIdSlug(cmName)}`);
    const ks = ruleNames
      .map((rn) => aliasingRules.findIndex((ar) => ar.name === rn))
      .filter((j) => j >= 0);
    const baseY = ks.length > 0 ? 48 + Math.min(...ks) * GAP_Y : 48;
    confidenceNames.forEach((cmName, rIdx) => {
      const id = vrIds[rIdx];
      nodes.push({
        id,
        kind: "match_validation_aliasing",
        position: { x: COL_VR_AL, y: baseY + rIdx * VR_STACK },
        data: {
          label: cmName,
          validation_rule_context: "aliasing",
          confidence_match_rule_name: cmName,
          ref: shared
            ? {
                shared_aliasing_validation_chain: true,
                aliasing_rule_names: [...ruleNames],
                validation_list_key: chainKey,
              }
            : { aliasing_rule_name: ruleNames[0] },
        },
      });
    });
    if (vrIds.length > 0) {
      for (const { ruleName } of cluster) {
        const alId = `al_${canvasIdSlug(ruleName)}`;
        if (!alIds.includes(alId)) continue;
        edges.push({
          id: `e_${alId}_${vrIds[0]}`,
          source: alId,
          target: vrIds[0],
          kind: "data",
        });
      }
      for (let i = 0; i < vrIds.length - 1; i++) {
        edges.push({
          id: `e_${vrIds[i]}_${vrIds[i + 1]}`,
          source: vrIds[i],
          target: vrIds[i + 1],
          kind: "sequence",
        });
      }
    }
  }

  // aliasing.config.data.validation — global aliasing-stage match rules (chained in list order)
  const globalAlNames = globalAliasingValidationNames(scopeDoc);
  const globalAlIds = globalAlNames.map((cmName) => `vrule_al_data_${canvasIdSlug(cmName)}`);
  globalAlNames.forEach((cmName, rIdx) => {
    const id = globalAlIds[rIdx];
    nodes.push({
      id,
      kind: "match_validation_aliasing",
      position: {
        x: COL_VR_AL,
        y: 48 + aliasingRules.length * GAP_Y + 24 + rIdx * VR_STACK,
      },
      data: {
        label: cmName,
        notes: "aliasing.config.data.validation",
        validation_rule_context: "aliasing",
        confidence_match_rule_name: cmName,
        ref: { aliasing_global_validation: true },
      },
    });
  });
  if (globalAlIds.length > 0) {
    for (const alId of alIds) {
      edges.push({
        id: `e_${alId}_${globalAlIds[0]}`,
        source: alId,
        target: globalAlIds[0],
        kind: "data",
      });
    }
    for (let i = 0; i < globalAlIds.length - 1; i++) {
      edges.push({
        id: `e_${globalAlIds[i]}_${globalAlIds[i + 1]}`,
        source: globalAlIds[i],
        target: globalAlIds[i + 1],
        kind: "sequence",
      });
    }
  }

  // Extraction → aliasing: use each extraction rule's `aliasing_pipeline` for data + sequence edges
  // (not a single global chain — tag-aliasing routes via extraction_rules[].aliasing_pipeline).
  const seededEdgeSig = new Set<string>();
  const addSeedEdge = (
    source: string,
    target: string,
    kind: WorkflowCanvasEdge["kind"]
  ): void => {
    const sig = `${source}\0${target}\0${kind}`;
    if (seededEdgeSig.has(sig)) return;
    seededEdgeSig.add(sig);
    edges.push({
      id: `e_${source}_${target}_${edges.length}`,
      source,
      target,
      kind,
    });
  };

  if (alIds.length > 0) {
    const nameToAlId = new Map<string, string>();
    for (let i = 0; i < aliasingRules.length; i++) {
      nameToAlId.set(aliasingRules[i].name, alIds[i]!);
    }

    for (const er of extractionRules) {
      const extId = `ext_${canvasIdSlug(er.name)}`;
      const row = getExtractionRuleRow(scopeDoc, er.name);
      const pipeline = row?.aliasing_pipeline;
      const orderedNames = linearAliasingRuleNamesFromPipeline(pipeline ?? []);
      const fallbackFirst = alIds[0]!;
      let firstAlId = fallbackFirst;
      if (orderedNames.length > 0) {
        firstAlId = nameToAlId.get(orderedNames[0]!) ?? fallbackFirst;
      }
      addSeedEdge(extId, firstAlId, "data");

      for (let i = 0; i < orderedNames.length - 1; i++) {
        const fromN = orderedNames[i]!;
        const toN = orderedNames[i + 1]!;
        const a = nameToAlId.get(fromN);
        const b = nameToAlId.get(toN);
        if (a && b) addSeedEdge(a, b, "sequence");
      }
    }

    addSeedEdge(alIds[alIds.length - 1]!, ID_END, "data");
  } else if (extIds.length > 0) {
    for (const extId of extIds) {
      edges.push({
        id: `e_${extId}_${ID_END}`,
        source: extId,
        target: ID_END,
        kind: "data",
      });
    }
  }

  if (svRows.length === 0 && extractionRules.length > 0) {
    edges.push({
      id: `e_${ID_START}_${extIds[0]}`,
      source: ID_START,
      target: extIds[0],
      kind: "data",
    });
  }

  return { ...base, nodes, edges };
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
