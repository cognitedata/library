import type { Edge, Node } from "@xyflow/react";
import { keaFlowEdgeVisualDefaults, type FlowEdgeData } from "./flowDocumentBridge";
import { parseSourceViewToExtractionPairs } from "./workflowScopeAssociations";

function isCanvasDataEdge(e: Edge): boolean {
  const k = (e.data as FlowEdgeData | undefined)?.kind;
  return k !== "sequence" && k !== "parallel_group";
}

function extractionRuleEnabled(doc: Record<string, unknown>, ruleName: string): boolean {
  const ke = doc.key_extraction as Record<string, unknown> | undefined;
  const config = ke?.config as Record<string, unknown> | undefined;
  const data = config?.data as Record<string, unknown> | undefined;
  const rules = data?.extraction_rules;
  if (!Array.isArray(rules)) return false;
  for (const r of rules) {
    if (!r || typeof r !== "object" || Array.isArray(r)) continue;
    const row = r as Record<string, unknown>;
    if (String(row.name ?? "").trim() !== ruleName) continue;
    return row.enabled !== false;
  }
  return false;
}

export type AutoWireSourceViewDropResult = {
  edges: Edge[];
  /** Extraction rule names to append ``source_view_to_extraction`` associations for after drop. */
  mergeExtractionRules: string[];
};

/**
 * Data edges to add after dropping a new source view: Start → SV, SV → extraction nodes
 * whose ``(source_view_index, extraction_rule_name)`` pair exists in scope ``associations``.
 * Aliasing is downstream of extraction; this does not add invalid SV→aliasing links.
 */
export function buildAutoWiredEdgesForDroppedSourceView(opts: {
  nodes: Node[];
  edges: Edge[];
  newSvNode: Node;
  svIndex: number;
  scopeDoc: Record<string, unknown>;
}): AutoWireSourceViewDropResult {
  const { nodes, edges, newSvNode, scopeDoc } = opts;
  const { svIndex } = opts;
  const newId = newSvNode.id;
  const out: Edge[] = [];
  const mergeExtractionRules: string[] = [];
  let edgeSeq = 0;
  const nextEdgeId = () => `e_autosv_${Date.now()}_${edgeSeq++}`;

  const start = nodes.find((n) => n.type === "keaStart");
  if (
    start &&
    !edges.some((e) => e.source === start.id && e.target === newId && isCanvasDataEdge(e)) &&
    !out.some((e) => e.source === start.id && e.target === newId)
  ) {
    out.push({
      ...keaFlowEdgeVisualDefaults,
      id: nextEdgeId(),
      source: start.id,
      target: newId,
      data: { kind: "data" } satisfies FlowEdgeData,
    });
  }

  const assocPairs = parseSourceViewToExtractionPairs(scopeDoc);
  const boundPairs = new Set(
    assocPairs.map((p) => `${p.source_view_index}\0${p.extraction_rule_name.trim()}`)
  );

  for (const en of nodes) {
    if (en.type !== "keaExtraction") continue;
    const d = (en.data ?? {}) as Record<string, unknown>;
    const ref = d.ref as Record<string, unknown> | undefined;
    const rn = ref?.extraction_rule_name != null ? String(ref.extraction_rule_name).trim() : "";
    if (!rn) continue;
    if (!boundPairs.has(`${svIndex}\0${rn}`)) continue;
    if (!extractionRuleEnabled(scopeDoc, rn)) continue;
    if (edges.some((e) => e.source === newId && e.target === en.id && isCanvasDataEdge(e))) continue;
    if (out.some((e) => e.source === newId && e.target === en.id)) continue;
    out.push({
      ...keaFlowEdgeVisualDefaults,
      id: nextEdgeId(),
      source: newId,
      target: en.id,
      data: { kind: "data" } satisfies FlowEdgeData,
    });
    if (!mergeExtractionRules.includes(rn)) mergeExtractionRules.push(rn);
  }

  return { edges: out, mergeExtractionRules };
}
