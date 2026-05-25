/**
 * Graph-aware field resolution for transform canvas nodes (field_map editor).
 */
import type { Edge, Node } from "@xyflow/react";
import { rfTypeToKind, type TransformCanvasNodeKind } from "../types/transformCanvas";
import { readMergeFieldPolicies } from "./mergeNodeConfigModel";
import { materializeTransformSteps } from "./etlTransformNodeConfigModel";
import { readFilters } from "./filtersConfigModel";
import type { JsonObject } from "../types/jsonConfig";

export type FieldMappingRow = {
  input_field: string;
  output_field: string;
};

export type CanvasFieldGraphContext = {
  nodes: readonly Node[];
  edges: readonly Edge[];
  nodeId: string;
};

const PASSTHROUGH_KINDS = new Set<TransformCanvasNodeKind>(["filter", "score", "join"]);

const COHORT_IDENTITY_FIELDS = [
  "externalId",
  "external_id",
  "space",
  "instance_space",
  "instance_id",
  "node",
];

function edgeKind(edge: Edge): string {
  const data = edge.data as { kind?: string } | undefined;
  return data?.kind ?? "data";
}

function isDataEdge(edge: Edge): boolean {
  return edgeKind(edge) === "data";
}

export function nodeKind(node: Node): TransformCanvasNodeKind {
  const data = (node.data ?? {}) as Record<string, unknown>;
  if (typeof data.kind === "string" && data.kind.trim()) {
    return data.kind.trim() as TransformCanvasNodeKind;
  }
  return rfTypeToKind(node.type);
}

export function nodeConfig(node: Node): Record<string, unknown> {
  const data = (node.data ?? {}) as Record<string, unknown>;
  const cfg = data.config;
  if (cfg && typeof cfg === "object" && !Array.isArray(cfg)) {
    return cfg as Record<string, unknown>;
  }
  return {};
}

function nodeById(nodes: readonly Node[]): Map<string, Node> {
  return new Map(nodes.map((n) => [n.id, n]));
}

export function collectPredecessorIds(
  nodes: readonly Node[],
  edges: readonly Edge[],
  nodeId: string
): string[] {
  const ids = new Set<string>();
  for (const e of edges) {
    if (!isDataEdge(e)) continue;
    if (e.target === nodeId && e.source) ids.add(e.source);
  }
  return [...ids];
}

export function collectSuccessorIds(
  nodes: readonly Node[],
  edges: readonly Edge[],
  nodeId: string
): string[] {
  const ids = new Set<string>();
  for (const e of edges) {
    if (!isDataEdge(e)) continue;
    if (e.source === nodeId && e.target) ids.add(e.target);
  }
  return [...ids];
}

function dedupeSorted(fields: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const f of fields) {
    const s = f.trim();
    if (!s || seen.has(s)) continue;
    seen.add(s);
    out.push(s);
  }
  return out.sort((a, b) => a.localeCompare(b));
}

function fieldNamesFromTransformFields(raw: unknown): string[] {
  if (!Array.isArray(raw)) return [];
  const out: string[] = [];
  for (const item of raw) {
    if (!item || typeof item !== "object" || Array.isArray(item)) continue;
    const name = String((item as JsonObject).field_name ?? "").trim();
    if (name) out.push(name);
  }
  return out;
}

function fieldNamesRequiredByTransformConfig(cfg: Record<string, unknown>): string[] {
  const steps = materializeTransformSteps(cfg as JsonObject);
  if (steps.length > 0) {
    const out: string[] = [];
    for (const step of steps) {
      out.push(...fieldNamesFromTransformFields(step.fields));
    }
    return out;
  }
  return fieldNamesFromTransformFields(cfg.fields);
}

function fieldNamesEmittedByTransformConfig(cfg: Record<string, unknown>): string[] {
  const steps = materializeTransformSteps(cfg as JsonObject);
  const out: string[] = [];
  if (steps.length > 0) {
    for (const step of steps) {
      out.push(...fieldNamesFromTransformFields(step.fields));
      const output = String(step.output_field ?? "").trim();
      if (output) out.push(output);
    }
    return out;
  }
  out.push(...fieldNamesFromTransformFields(cfg.fields));
  const legacyOutput = String(cfg.output_field ?? "").trim();
  if (legacyOutput) out.push(legacyOutput);
  return out;
}

function fieldNamesFromFilters(filters: JsonObject[]): string[] {
  const out: string[] = [];
  for (const f of filters) {
    const prop = String(f.target_property ?? "").trim();
    if (prop) out.push(prop);
    const nested = f.filters ?? f.conditions;
    if (Array.isArray(nested)) {
      out.push(...fieldNamesFromFilters(nested.filter((x) => x && typeof x === "object") as JsonObject[]));
    }
  }
  return out;
}

/** Parse SELECT column list from a simple SQL query (best-effort). */
export function parseSqlSelectColumns(sql: string): string[] {
  const q = sql.trim();
  if (!q) return [];
  const match = q.match(/^\s*SELECT\s+([\s\S]+?)\s+FROM\s/i);
  if (!match) return [];
  const clause = match[1]!.trim();
  if (!clause || clause === "*") return [];

  const parts: string[] = [];
  let depth = 0;
  let cur = "";
  for (const ch of clause) {
    if (ch === "(") depth += 1;
    else if (ch === ")") depth = Math.max(0, depth - 1);
    if (ch === "," && depth === 0) {
      if (cur.trim()) parts.push(cur.trim());
      cur = "";
      continue;
    }
    cur += ch;
  }
  if (cur.trim()) parts.push(cur.trim());

  const names: string[] = [];
  for (const part of parts) {
    const asMatch = part.match(/\s+AS\s+(`?)([a-zA-Z_][\w$]*)\1\s*$/i);
    if (asMatch) {
      names.push(asMatch[2]!);
      continue;
    }
    const trimmed = part.trim();
    const dotParts = trimmed.split(".");
    const last = dotParts[dotParts.length - 1] ?? trimmed;
    const bare = last.replace(/^[`"]|[`"]$/g, "").trim();
    if (bare && !/^\*$/.test(bare)) names.push(bare);
  }
  return names;
}

function inferFieldsEmittedByNodeStatic(kind: TransformCanvasNodeKind, cfg: Record<string, unknown>): string[] {
  switch (kind) {
    case "query_view": {
      const props = cfg.include_properties;
      if (Array.isArray(props) && props.length > 0) {
        return props.map((p) => String(p).trim()).filter(Boolean);
      }
      return [];
    }
    case "query_sql": {
      const sql = String(cfg.sql_query ?? cfg.query ?? "");
      const cols = parseSqlSelectColumns(sql);
      const extCol = String(cfg.external_id_column ?? "").trim();
      if (extCol && !cols.includes(extCol)) cols.push(extCol);
      return cols;
    }
    case "query_raw":
      return ["external_id", "raw_columns"];
    case "query_classic":
      return ["externalId", "name", "description"];
    case "transform":
      return fieldNamesEmittedByTransformConfig(cfg);
    case "field_map": {
      const mappings = readFieldMappings(cfg);
      return mappings.map((m) => m.output_field).filter(Boolean);
    }
    case "merge": {
      const policies = readMergeFieldPolicies(cfg);
      return policies
        .map((p) => String((p as JsonObject).property ?? "").trim())
        .filter(Boolean);
    }
    case "score":
      return fieldNamesFromFilters(readFilters(cfg));
    default:
      return [];
  }
}

function inferFieldsRequiredByNodeStatic(kind: TransformCanvasNodeKind, cfg: Record<string, unknown>): string[] {
  switch (kind) {
    case "transform":
      return fieldNamesRequiredByTransformConfig(cfg);
    case "merge": {
      const policies = readMergeFieldPolicies(cfg);
      return policies
        .map((p) => String((p as JsonObject).property ?? "").trim())
        .filter(Boolean);
    }
    case "filter":
    case "score":
      return fieldNamesFromFilters(readFilters(cfg));
    case "save_view": {
      const props = cfg.include_properties ?? cfg.write_properties;
      if (Array.isArray(props) && props.length > 0) {
        return props.map((p) => String(p).trim()).filter(Boolean);
      }
      return [];
    }
    default:
      return [];
  }
}

export function readFieldMappings(cfg: Record<string, unknown>): FieldMappingRow[] {
  const raw = cfg.mappings;
  if (!Array.isArray(raw)) return [];
  const out: FieldMappingRow[] = [];
  for (const item of raw) {
    if (!item || typeof item !== "object" || Array.isArray(item)) continue;
    const input_field = String((item as JsonObject).input_field ?? "").trim();
    const output_field = String((item as JsonObject).output_field ?? "").trim();
    if (!input_field && !output_field) continue;
    out.push({ input_field, output_field });
  }
  return out;
}

export function inferFieldsEmittedByNode(node: Node): string[] {
  const kind = nodeKind(node);
  if (PASSTHROUGH_KINDS.has(kind)) return [];
  return inferFieldsEmittedByNodeStatic(kind, nodeConfig(node));
}

export function inferFieldsRequiredByNode(node: Node): string[] {
  const kind = nodeKind(node);
  return inferFieldsRequiredByNodeStatic(kind, nodeConfig(node));
}

export function resolveAvailableInputFields(ctx: CanvasFieldGraphContext): string[] {
  const byId = nodeById(ctx.nodes);
  const visited = new Set<string>();
  const fields: string[] = [];

  function walk(nodeId: string) {
    if (visited.has(nodeId)) return;
    visited.add(nodeId);
    for (const predId of collectPredecessorIds(ctx.nodes, ctx.edges, nodeId)) {
      const pred = byId.get(predId);
      if (!pred) continue;
      const kind = nodeKind(pred);
      const emitted = inferFieldsEmittedByNode(pred);
      if (emitted.length > 0) {
        fields.push(...emitted);
      } else if (PASSTHROUGH_KINDS.has(kind)) {
        walk(predId);
      } else if (kind.startsWith("query_")) {
        fields.push(...inferFieldsEmittedByNodeStatic(kind, nodeConfig(pred)));
      }
    }
  }

  for (const predId of collectPredecessorIds(ctx.nodes, ctx.edges, ctx.nodeId)) {
    const pred = byId.get(predId);
    if (!pred) continue;
    const kind = nodeKind(pred);
    const emitted = inferFieldsEmittedByNode(pred);
    if (emitted.length > 0) {
      fields.push(...emitted);
    } else if (PASSTHROUGH_KINDS.has(kind)) {
      walk(predId);
    } else {
      fields.push(...inferFieldsEmittedByNodeStatic(kind, nodeConfig(pred)));
    }
  }

  return dedupeSorted([...fields, ...COHORT_IDENTITY_FIELDS]);
}

export function resolveSuggestedOutputFields(ctx: CanvasFieldGraphContext): string[] {
  const byId = nodeById(ctx.nodes);
  const fields: string[] = [];
  for (const succId of collectSuccessorIds(ctx.nodes, ctx.edges, ctx.nodeId)) {
    const succ = byId.get(succId);
    if (!succ) continue;
    fields.push(...inferFieldsRequiredByNode(succ));
  }
  return dedupeSorted(fields);
}

export function countDataPredecessors(ctx: CanvasFieldGraphContext): number {
  return collectPredecessorIds(ctx.nodes, ctx.edges, ctx.nodeId).length;
}

/** Nearest upstream query node SQL / table hint for conceptual SELECT preview. */
export function inferSourceSqlHint(ctx: CanvasFieldGraphContext): string {
  const byId = nodeById(ctx.nodes);
  const visited = new Set<string>();

  function walkFrom(nodeId: string): string | null {
    if (visited.has(nodeId)) return null;
    visited.add(nodeId);
    for (const predId of collectPredecessorIds(ctx.nodes, ctx.edges, nodeId)) {
      const pred = byId.get(predId);
      if (!pred) continue;
      const kind = nodeKind(pred);
      const cfg = nodeConfig(pred);
      if (kind === "query_sql") {
        const sql = String(cfg.sql_query ?? cfg.query ?? "").trim();
        if (sql) return sql;
      }
      if (kind === "query_view") {
        const space = String(cfg.view_space ?? "").trim();
        const ext = String(cfg.view_external_id ?? "").trim();
        const ver = String(cfg.view_version ?? "v1").trim();
        if (space && ext) {
          return `cdf_nodes('${space.replace(/'/g, "''")}', '${ext.replace(/'/g, "''")}', '${ver.replace(/'/g, "''")}')`;
        }
      }
      const nested = walkFrom(predId);
      if (nested) return nested;
    }
    return null;
  }

  for (const predId of collectPredecessorIds(ctx.nodes, ctx.edges, ctx.nodeId)) {
    const hint = walkFrom(predId);
    if (hint) return hint;
  }
  return "_source";
}

export function normalizeFieldKey(name: string): string {
  return name.trim().toLowerCase().replace(/[^a-z0-9]+/g, "");
}

/** Pair emitted fields with required fields by exact then normalized name match. */
export function seedMappingsBetweenNodes(source: Node, target: Node): FieldMappingRow[] {
  const emitted = dedupeSorted(inferFieldsEmittedByNode(source));
  const required = dedupeSorted(inferFieldsRequiredByNode(target));
  if (required.length === 0 && emitted.length === 0) return [];

  const emittedByNorm = new Map<string, string>();
  for (const f of emitted) {
    const n = normalizeFieldKey(f);
    if (!emittedByNorm.has(n)) emittedByNorm.set(n, f);
  }

  const usedInputs = new Set<string>();
  const rows: FieldMappingRow[] = [];

  for (const out of required) {
    let input = emitted.find((e) => e === out) ?? "";
    if (!input) {
      const norm = normalizeFieldKey(out);
      input = emittedByNorm.get(norm) ?? "";
    }
    if (input) usedInputs.add(input);
    rows.push({ input_field: input, output_field: out });
  }

  for (const inp of emitted) {
    if (usedInputs.has(inp)) continue;
  }

  return rows;
}
