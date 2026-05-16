import type { WorkflowCanvasDocument, WorkflowCanvasEdge } from "../../types/workflowCanvas";

export const WORKFLOW_ASSOCIATION_KIND_SOURCE_VIEW_TO_EXTRACTION = "source_view_to_extraction" as const;

export type WorkflowAssociationSourceViewToExtraction = {
  kind: typeof WORKFLOW_ASSOCIATION_KIND_SOURCE_VIEW_TO_EXTRACTION;
  source_view_index: number;
  extraction_rule_name: string;
};

export type WorkflowAssociation = WorkflowAssociationSourceViewToExtraction;

export type SourceViewToExtractionPair = {
  source_view_index: number;
  extraction_rule_name: string;
};

function isRecord(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

function refNum(ref: unknown, key: string): number | undefined {
  if (!isRecord(ref)) return undefined;
  const v = ref[key];
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v.trim() && !Number.isNaN(Number(v))) return Number(v);
  return undefined;
}

function refStr(ref: unknown, key: string): string | undefined {
  if (!isRecord(ref)) return undefined;
  const v = ref[key];
  return v != null && String(v).trim() ? String(v).trim() : undefined;
}

function isSourceViewToExtractionRow(v: unknown): v is WorkflowAssociationSourceViewToExtraction {
  if (!isRecord(v)) return false;
  if (String(v.kind) !== WORKFLOW_ASSOCIATION_KIND_SOURCE_VIEW_TO_EXTRACTION) return false;
  const idx = v.source_view_index;
  const name = v.extraction_rule_name;
  if (typeof idx !== "number" || !Number.isFinite(idx)) return false;
  if (typeof name !== "string" || !name.trim()) return false;
  return true;
}

/** True when the scope document uses the top-level ``associations`` list (including ``[]``). */
export function hasWorkflowAssociationsKey(doc: Record<string, unknown>): boolean {
  return Object.prototype.hasOwnProperty.call(doc, "associations");
}

export function parseSourceViewToExtractionPairs(doc: Record<string, unknown>): SourceViewToExtractionPair[] {
  const raw = doc.associations;
  if (!Array.isArray(raw)) return [];
  const out: SourceViewToExtractionPair[] = [];
  const seen = new Set<string>();
  for (const row of raw) {
    if (!isSourceViewToExtractionRow(row)) continue;
    const source_view_index = Math.floor(row.source_view_index);
    const extraction_rule_name = String(row.extraction_rule_name).trim();
    const k = `${source_view_index}\0${extraction_rule_name}`;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push({ source_view_index, extraction_rule_name });
  }
  out.sort((a, b) =>
    a.source_view_index !== b.source_view_index
      ? a.source_view_index - b.source_view_index
      : a.extraction_rule_name.localeCompare(b.extraction_rule_name)
  );
  return out;
}

function isDataEdge(e: WorkflowCanvasEdge): boolean {
  return e.kind !== "sequence" && e.kind !== "parallel_group";
}

/**
 * Data edges from ``kind: "source_view"`` to ``kind: "extraction"`` (expanded canvas for subgraph sync).
 */
export function collectSourceViewToExtractionPairsFromCanvas(
  canvas: WorkflowCanvasDocument
): SourceViewToExtractionPair[] {
  const byId = new Map(canvas.nodes.map((n) => [n.id, n]));
  const out: SourceViewToExtractionPair[] = [];
  const seen = new Set<string>();
  for (const e of canvas.edges) {
    if (!isDataEdge(e)) continue;
    const src = byId.get(e.source);
    const tgt = byId.get(e.target);
    if (!src || src.kind !== "source_view") continue;
    if (!tgt || tgt.kind !== "extraction") continue;
    const svIdx = refNum(src.data?.ref, "source_view_index");
    if (svIdx === undefined || !Number.isFinite(svIdx) || svIdx < 0) continue;
    const ruleName = refStr(tgt.data?.ref, "extraction_rule_name");
    if (!ruleName) continue;
    const source_view_index = Math.floor(svIdx);
    const extraction_rule_name = ruleName;
    const k = `${source_view_index}\0${extraction_rule_name}`;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push({ source_view_index, extraction_rule_name });
  }
  out.sort((a, b) =>
    a.source_view_index !== b.source_view_index
      ? a.source_view_index - b.source_view_index
      : a.extraction_rule_name.localeCompare(b.extraction_rule_name)
  );
  return out;
}

function otherAssociationRows(doc: Record<string, unknown>): unknown[] {
  const raw = doc.associations;
  if (!Array.isArray(raw)) return [];
  return raw.filter((x) => !isSourceViewToExtractionRow(x));
}

/**
 * Replace ``source_view_to_extraction`` rows; keep other ``associations`` entries.
 * When *pairs* is empty and the document never had ``associations``, returns *doc* unchanged.
 */
/**
 * Add one ``source_view_to_extraction`` row (deduped) while preserving other association rows.
 * Use when a single canvas edge is created without re-running full canvas sync.
 */
export function appendSourceViewToExtractionAssociation(
  doc: Record<string, unknown>,
  pair: SourceViewToExtractionPair
): Record<string, unknown> {
  const existing = parseSourceViewToExtractionPairs(doc);
  const k = `${pair.source_view_index}\0${pair.extraction_rule_name.trim()}`;
  const has = existing.some(
    (p) => `${p.source_view_index}\0${p.extraction_rule_name}` === k
  );
  const next = has ? existing : [...existing, pair];
  return mergeSourceViewToExtractionAssociationsIntoDoc(doc, next);
}

export function mergeSourceViewToExtractionAssociationsIntoDoc(
  doc: Record<string, unknown>,
  pairs: SourceViewToExtractionPair[]
): Record<string, unknown> {
  if (pairs.length === 0 && !hasWorkflowAssociationsKey(doc)) {
    return doc;
  }
  const rows: WorkflowAssociationSourceViewToExtraction[] = pairs.map((p) => ({
    kind: WORKFLOW_ASSOCIATION_KIND_SOURCE_VIEW_TO_EXTRACTION,
    source_view_index: p.source_view_index,
    extraction_rule_name: p.extraction_rule_name,
  }));
  return {
    ...doc,
    associations: [...otherAssociationRows(doc), ...rows],
  };
}

export function applySourceViewExtractionAssociationsFromCanvas(
  canvas: WorkflowCanvasDocument,
  scopeDoc: Record<string, unknown>
): Record<string, unknown> {
  const pairs = collectSourceViewToExtractionPairsFromCanvas(canvas);
  return mergeSourceViewToExtractionAssociationsIntoDoc(scopeDoc, pairs);
}
